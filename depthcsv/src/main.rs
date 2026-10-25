//! depthcsv — пишет публичные потоки Binance USDⓈ-M Futures без потерь: diff depth 100 мс, aggTrade и REST-снимки стакана.
//! Строка файла = `<recv_ts_ns> <JSON как пришёл>`; разрывы помечаются строками `{"gap":{...}}`.
//! Файлы `<OUT_DIR>/<SYM>/<SYM>_<depth|trade|snap>_<YYYYMMDD_HHMMSS>.jsonl.gz`, после закрытия рядом маркер `.done`.
//! `depthcsv verify <OUT_DIR> [SYMBOL...]` — проверка записанного (см. verify.rs).
mod verify;

use flate2::{write::GzEncoder, Compression};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::Message;

const QUEUE: usize = 20_000; // строк в очереди записи (~40 МБ в худшем случае)
const KINDS: [&str; 3] = ["depth", "trade", "snap"];
const DEPTH: usize = 0;
const TRADE: usize = 1;
const SNAP: usize = 2;
const NO_DATA: Duration = Duration::from_secs(30);

static ROWS: AtomicU64 = AtomicU64::new(0);
static GAPS: AtomicU64 = AtomicU64::new(0);
static RECONNECTS: AtomicU64 = AtomicU64::new(0);
static PARSE_ERR: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static WRITE_ERR: AtomicU64 = AtomicU64::new(0);
static QUEUED: AtomicU64 = AtomicU64::new(0);

macro_rules! log {
    ($($a:tt)*) => { println!("{} {}", fmt_ts(now_ns() / 1_000_000), format_args!($($a)*)) };
}

/// Строка для потока записи; `file` = символ * 3 + вид (DEPTH/TRADE/SNAP).
struct Line {
    file: usize,
    ns: u64,
    text: String,
}

#[derive(Deserialize)]
struct Wrap<T> {
    data: T,
}

#[derive(Deserialize)]
struct DepthHead<'a> {
    s: &'a str,
    #[serde(rename = "U")]
    first: u64,
    u: u64,
    pu: u64,
}

#[derive(Deserialize)]
struct TradeHead<'a> {
    s: &'a str,
    a: u64,
}

#[derive(Deserialize)]
struct SnapHead {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
}

/// Детектор разрывов цепочки id: depth — `pu` должен быть равен `u` прошлого события, aggTrade — id `a` подряд.
#[derive(Default, Clone)]
struct Chain {
    last: Option<u64>,
}

#[derive(Debug, PartialEq)]
enum Step {
    First,
    Ok,
    Dup,
    Gap(u64), // ожидался prev == этому значению
}

impl Chain {
    fn step(&mut self, prev: u64, cur: u64) -> Step {
        let s = match self.last {
            None => Step::First,
            Some(l) if cur <= l => return Step::Dup,
            Some(l) if prev != l => Step::Gap(l),
            Some(_) => Step::Ok,
        };
        self.last = Some(cur);
        s
    }
}

/// Отправка строк в поток записи. Молча ничего не теряется: при переполнении очереди потери считаются по файлу,
/// и как только место появится, перед следующей строкой пишется GAP `queue_overflow` с их числом.
#[derive(Clone)]
struct Out {
    tx: SyncSender<Line>,
    syms: Arc<Vec<String>>,
    lost: Arc<Vec<AtomicU64>>,
    snap: UnboundedSender<usize>,
}

impl Out {
    fn put(&self, file: usize, ns: u64, text: String) -> bool {
        match self.tx.try_send(Line { file, ns, text }) {
            Ok(()) => {
                QUEUED.fetch_add(1, Relaxed);
                true
            }
            Err(TrySendError::Full(_)) => false,
            Err(TrySendError::Disconnected(_)) => true, // остановка
        }
    }

    fn send(&self, file: usize, ns: u64, text: String) {
        let lost = self.lost[file].load(Relaxed);
        if lost > 0 {
            if !self.put(file, ns, self.gap_text(file, "queue_overflow", &format!(r#","lost":{lost}"#))) {
                return self.lose(file);
            }
            self.lost[file].fetch_sub(lost, Relaxed);
            GAPS.fetch_add(1, Relaxed);
            log!("WARN {} {}: очередь записи переполнилась, потеряно строк: {lost} — записан GAP", self.syms[file / 3], KINDS[file % 3]);
            if file % 3 == DEPTH {
                let _ = self.snap.send(file / 3);
            }
        }
        if !self.put(file, ns, text) {
            self.lose(file)
        }
    }

    fn lose(&self, file: usize) {
        self.lost[file].fetch_add(1, Relaxed);
        DROPPED.fetch_add(1, Relaxed);
    }

    fn gap(&self, file: usize, ns: u64, reason: &str, extra: &str) {
        GAPS.fetch_add(1, Relaxed);
        self.send(file, ns, self.gap_text(file, reason, extra));
    }

    fn gap_text(&self, file: usize, reason: &str, extra: &str) -> String {
        format!(r#"{{"gap":{{"s":"{}","stream":"{}","reason":"{reason}"{extra}}}}}"#, self.syms[file / 3], KINDS[file % 3])
    }
}

struct Cfg {
    syms: Vec<String>,
    out: PathBuf,
    rotate_ns: u64,
    depth_url: String,
    trade_url: String,
    rest: String,
    snap_every: Duration,
    watchdog: usize,
    watchdog_secs: u64,
}

fn cfg() -> Cfg {
    let var = |k: &str, d: &str| env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| d.into());
    let num = |k: &str, d: &str| -> u64 {
        var(k, d).parse().unwrap_or_else(|_| {
            eprintln!("{k}: нужно целое число");
            std::process::exit(1)
        })
    };
    let syms: Vec<String> = var("SYMBOLS", "BTCUSDT,ETHUSDT").split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect();
    let streams = |suffix: &str| syms.iter().map(|s| format!("{}@{suffix}", s.to_lowercase())).collect::<Vec<_>>().join("/");
    let wd = var("WATCHDOG_SYMBOL", "BTCUSDT").to_uppercase();
    Cfg {
        depth_url: format!("{}?streams={}", var("WS_DEPTH_BASE", "wss://fstream.binance.com/public/stream"), streams("depth@100ms")),
        trade_url: format!("{}?streams={}", var("WS_TRADE_BASE", "wss://fstream.binance.com/market/stream"), streams("aggTrade")),
        rest: var("REST_BASE", "https://fapi.binance.com"),
        out: var("OUT_DIR", "./data").into(),
        rotate_ns: num("ROTATE_MIN", "60").max(1) * 60_000_000_000,
        snap_every: Duration::from_secs(num("SNAPSHOT_SEC", "600").max(10)),
        watchdog: syms.iter().position(|s| *s == wd).unwrap_or(0),
        watchdog_secs: num("TRADE_WATCHDOG_SEC", "30"),
        syms,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if env::args().nth(1).as_deref() == Some("verify") {
        std::process::exit(verify::main(env::args().skip(2).collect()));
    }
    let c = cfg();
    fs::create_dir_all(&c.out).expect("не удалось создать OUT_DIR");
    log!("start: symbols={:?} out={} rotate={}мин snapshot={}с", c.syms, c.out.display(), c.rotate_ns / 60_000_000_000, c.snap_every.as_secs());
    log!("ws depth: {}", c.depth_url);
    log!("ws trade: {}", c.trade_url);
    check_time(&c.rest).await;

    let syms = Arc::new(c.syms);
    let (tx, rx) = mpsc::sync_channel::<Line>(QUEUE);
    let (snap_tx, snap_rx) = unbounded_channel();
    let out = Out { tx, syms: syms.clone(), lost: Arc::new((0..syms.len() * 3).map(|_| AtomicU64::new(0)).collect()), snap: snap_tx };
    let first_pu: Arc<Vec<AtomicU64>> = Arc::new((0..syms.len()).map(|_| AtomicU64::new(0)).collect());
    let writer = {
        let (dir, s, r) = (c.out.clone(), syms.clone(), c.rotate_ns);
        std::thread::spawn(move || writer(dir, s, r, rx))
    };
    let wd = (c.watchdog_secs > 0).then(|| (c.watchdog, Duration::from_secs(c.watchdog_secs)));
    let work = {
        let (o, fp) = (out.clone(), first_pu.clone());
        let (du, tu, rest, every) = (c.depth_url, c.trade_url, c.rest, c.snap_every);
        async move {
            tokio::join!(
                ws_loop(DEPTH, du, o.clone(), fp.clone(), None),
                ws_loop(TRADE, tu, o.clone(), fp.clone(), wd),
                snapshots(rest, every, o, snap_rx, fp),
                stats(),
            )
        }
    };
    tokio::select! {
        _ = work => {}
        _ = shutdown() => log!("shutdown: закрываю файлы"),
    }
    let t = now_ns();
    for f in (0..syms.len() * 3).filter(|f| f % 3 != SNAP) {
        out.gap(f, t, "shutdown", "");
    }
    drop(out); // последний отправитель → writer закрывает файлы
    let _ = writer.join();
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! { _ = term.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// Предупреждает в лог, если часы не синхронизированы (NTP) или расходятся с сервером Binance больше чем на 500 мс.
async fn check_time(rest: &str) {
    match std::process::Command::new("timedatectl").args(["show", "-p", "NTPSynchronized", "--value"]).output() {
        Ok(o) if o.stdout.starts_with(b"yes") => log!("время: NTP синхронизировано"),
        Ok(o) => log!("WARN время: NTP не синхронизировано (NTPSynchronized={})", String::from_utf8_lossy(&o.stdout).trim()),
        Err(e) => log!("WARN время: timedatectl недоступен ({e}), синхронизация не проверена"),
    }
    #[derive(Deserialize)]
    struct T {
        #[serde(rename = "serverTime")]
        server_time: u64,
    }
    let c = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("http client");
    let t0 = now_ns() / 1_000_000;
    let r = async { c.get(format!("{rest}/fapi/v1/time")).send().await?.text().await }.await;
    let t1 = now_ns() / 1_000_000;
    match r.ok().and_then(|b| serde_json::from_str::<T>(&b).ok()) {
        Some(t) => {
            let off = t.server_time as i64 - ((t0 + t1) / 2) as i64;
            if off.abs() > 500 {
                log!("WARN время: расхождение с Binance {off} мс (rtt {} мс)", t1 - t0)
            } else {
                log!("время: расхождение с Binance {off} мс (rtt {} мс)", t1 - t0)
            }
        }
        None => log!("WARN время: не получил /fapi/v1/time, расхождение не проверено"),
    }
}

/// Одно combined-соединение (depth или aggTrade) на все символы. Переподключение с паузой 1→30 с; на каждое отключение
/// (в т.ч. суточное от Binance) — GAP `disconnect`. Для aggTrade — сторожевой таймер по одному символу.
async fn ws_loop(kind: usize, url: String, out: Out, first_pu: Arc<Vec<AtomicU64>>, watchdog: Option<(usize, Duration)>) {
    let (n, name) = (out.syms.len(), KINDS[kind]);
    let t = now_ns();
    for i in 0..n {
        out.gap(i * 3 + kind, t, "start", "");
    }
    let mut backoff = 1u64;
    loop {
        match tokio::time::timeout(Duration::from_secs(15), tokio_tungstenite::connect_async(url.as_str())).await {
            Err(_) => log!("ws {name}: таймаут подключения"),
            Ok(Err(e)) => log!("ws {name}: ошибка подключения: {e}"),
            Ok(Ok((mut ws, _))) => {
                log!("ws {name}: подключено");
                RECONNECTS.fetch_add(1, Relaxed);
                backoff = 1;
                let mut chains = vec![Chain::default(); n];
                let (mut last, mut last_wd) = (Instant::now(), Instant::now());
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                let reason = loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            if last.elapsed() > NO_DATA { break "нет данных 30 с".to_string() }
                            if let Some((w, lim)) = watchdog {
                                if last_wd.elapsed() > lim {
                                    out.gap(w * 3 + kind, now_ns(), "trade_watchdog", "");
                                    log!("WARN ws {name}: нет aggTrade по {} {} с — GAP и переподключение", out.syms[w], lim.as_secs());
                                    break "сторожевой таймер aggTrade".to_string()
                                }
                            }
                        }
                        m = ws.next() => match m {
                            None => break "поток закрыт".to_string(),
                            Some(Err(e)) => break format!("ошибка: {e}"),
                            Some(Ok(Message::Text(t))) => {
                                let ns = now_ns();
                                last = Instant::now();
                                match on_text(kind, t.as_str(), ns, &out, &mut chains, &first_pu) {
                                    Some(i) if watchdog.is_some_and(|(w, _)| w == i) => last_wd = last,
                                    Some(_) => {}
                                    None => {
                                        if PARSE_ERR.fetch_add(1, Relaxed) < 5 {
                                            log!("WARN ws {name}: не разобрано: {}", t.as_str().chars().take(200).collect::<String>());
                                        }
                                        out.send(kind, ns, one_line(t.as_str())); // сохраняем как есть в файл первого символа
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(p))) => { last = Instant::now(); let _ = ws.send(Message::Pong(p)).await; }
                            Some(Ok(Message::Close(f))) => break format!("close: {f:?}"),
                            Some(Ok(_)) => {}
                        }
                    }
                };
                log!("ws {name}: отключено: {reason}");
                let (t, detail) = (now_ns(), format!(r#","detail":{}"#, serde_json::to_string(&reason).unwrap()));
                for i in 0..n {
                    out.gap(i * 3 + kind, t, "disconnect", &detail);
                    if kind == DEPTH {
                        first_pu[i].store(0, Relaxed);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

/// Разбирает только заголовок (символ и id) для проверки цепочки; в файл уходит исходный текст целиком.
fn on_text(kind: usize, text: &str, ns: u64, out: &Out, chains: &mut [Chain], first_pu: &[AtomicU64]) -> Option<usize> {
    let idx = |s: &str| out.syms.iter().position(|x| x == s);
    let i = if kind == DEPTH {
        let h = serde_json::from_str::<Wrap<DepthHead>>(text).ok()?.data;
        let i = idx(h.s)?;
        match chains[i].step(h.pu, h.u) {
            // поток идёт и буферизуется в файл — теперь снимок (процедура Binance)
            Step::First => {
                first_pu[i].store(h.pu.max(1), Relaxed);
                let _ = out.snap.send(i);
            }
            Step::Gap(prev) => {
                out.gap(i * 3 + DEPTH, ns, "pu_mismatch", &format!(r#","prev_u":{prev},"pu":{},"U":{},"u":{}"#, h.pu, h.first, h.u));
                log!("WARN {}: разрыв цепочки depth pu={} ≠ u={prev}", h.s, h.pu);
                let _ = out.snap.send(i);
            }
            Step::Ok | Step::Dup => {}
        }
        i
    } else {
        let h = serde_json::from_str::<Wrap<TradeHead>>(text).ok()?.data;
        let i = idx(h.s)?;
        if let Step::Gap(prev) = chains[i].step(h.a.saturating_sub(1), h.a) {
            out.gap(i * 3 + TRADE, ns, "trade_id_jump", &format!(r#","prev_a":{prev},"a":{}"#, h.a));
        }
        i
    };
    out.send(i * 3 + kind, ns, one_line(text));
    Some(i)
}

/// Перевод строки внутри JSON возможен только как пробельный символ — замена на пробел смысла не меняет.
fn one_line(s: &str) -> String {
    if s.contains('\n') { s.replace('\n', " ") } else { s.to_string() }
}

/// REST-снимки по очереди: после первого события каждого соединения, после GAP и раз в SNAPSHOT_SEC.
/// Вес depth?limit=1000 — 20 при лимите 2400/мин на IP: не чаще раза в 5 с на символ, 250 мс между запросами,
/// при X-MBX-USED-WEIGHT-1M > 1800 ждём новую минуту, на 429/418 — Retry-After.
async fn snapshots(rest: String, every: Duration, out: Out, mut rx: UnboundedReceiver<usize>, first_pu: Arc<Vec<AtomicU64>>) {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("http client");
    let n = out.syms.len();
    let (mut pending, mut last) = (vec![false; n], vec![None::<Instant>; n]);
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // при старте снимки запрашивает ws-задача
    loop {
        if !pending.contains(&true) {
            tokio::select! {
                _ = tick.tick() => pending.fill(true),
                Some(i) = rx.recv() => pending[i] = true,
            }
        }
        while let Ok(i) = rx.try_recv() {
            pending[i] = true;
        }
        let Some(i) = pending.iter().position(|&p| p) else { continue };
        pending[i] = false;
        if first_pu[i].load(Relaxed) == 0 {
            continue; // depth-поток не идёт — снимок запросится после подключения
        }
        if let Some(t) = last[i] {
            tokio::time::sleep(Duration::from_secs(5).saturating_sub(t.elapsed())).await;
        }
        last[i] = Some(Instant::now());
        let sym = &out.syms[i];
        match fetch(&client, &format!("{rest}/fapi/v1/depth?symbol={sym}&limit=1000")).await {
            Ok((ns, body, weight)) => {
                let lid = serde_json::from_str::<SnapHead>(&body).map(|s| s.last_update_id);
                out.send(i * 3 + SNAP, ns, one_line(&body));
                match lid {
                    Ok(l) if l < first_pu[i].load(Relaxed) => {
                        log!("WARN снимок {sym}: lastUpdateId={l} раньше начала потока — повторю");
                        pending[i] = true;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        log!("WARN снимок {sym}: не разобран ({e}) — повторю");
                        pending[i] = true;
                    }
                }
                if weight > 1800 {
                    let w = 61 - now_ns() / 1_000_000_000 % 60;
                    log!("WARN вес запросов {weight}/2400 за минуту — пауза {w} с");
                    tokio::time::sleep(Duration::from_secs(w)).await;
                }
            }
            Err((e, wait)) => {
                log!("WARN снимок {sym}: {e}; повтор через {wait} с");
                pending[i] = true;
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// GET снимка: (recv_ts_ns после получения тела, тело, использованный вес) или (ошибка, пауза в секундах).
async fn fetch(c: &reqwest::Client, url: &str) -> Result<(u64, String, u64), (String, u64)> {
    let r = c.get(url).send().await.map_err(|e| (e.to_string(), 2))?;
    let (st, h) = (r.status(), r.headers().clone());
    let hdr = |k: &str| h.get(k).and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    if st.as_u16() == 429 || st.as_u16() == 418 {
        return Err((format!("HTTP {st} (лимит запросов)"), hdr("retry-after").unwrap_or(60)));
    }
    let body = r.text().await.map_err(|e| (e.to_string(), 2))?;
    let ns = now_ns();
    if !st.is_success() {
        return Err((format!("HTTP {st}: {}", body.chars().take(200).collect::<String>()), 5));
    }
    Ok((ns, body, hdr("x-mbx-used-weight-1m").unwrap_or(0)))
}

async fn stats() {
    let mut t = tokio::time::interval(Duration::from_secs(60));
    t.tick().await;
    loop {
        t.tick().await;
        log!(
            "stats/мин: rows={} gaps={} reconnects={} parse_err={} dropped={} write_err={} queue_len={}",
            ROWS.swap(0, Relaxed),
            GAPS.swap(0, Relaxed),
            RECONNECTS.swap(0, Relaxed),
            PARSE_ERR.swap(0, Relaxed),
            DROPPED.swap(0, Relaxed),
            WRITE_ERR.swap(0, Relaxed),
            QUEUED.load(Relaxed)
        );
    }
}

struct Open {
    period: u64,
    enc: GzEncoder<BufWriter<fs::File>>,
    part: PathBuf,
}

/// Поток записи: gzip в `<file>.part`, сброс на диск раз в 5 с (при падении теряется ≤5 с). Закрытие по границе
/// периода (UTC) и при остановке, затем переименование и пустой маркер `<file>.done` — сигнал для выгрузки.
fn writer(out: PathBuf, syms: Arc<Vec<String>>, rotate_ns: u64, rx: mpsc::Receiver<Line>) {
    for s in syms.iter() {
        let dir = out.join(s);
        if let Err(e) = fs::create_dir_all(&dir) {
            log!("WARN создание {}: {e}", dir.display());
        }
        for p in list(&dir, ".part") {
            log!("WARN хвост после падения: {} (читается до последнего сброса)", p.display());
            finish(&p);
        }
    }
    let mut files: Vec<Option<Open>> = (0..syms.len() * 3).map(|_| None).collect();
    let mut tick = Instant::now();
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(l) => {
                QUEUED.fetch_sub(1, Relaxed);
                let (p, sym) = (l.ns / rotate_ns, &syms[l.file / 3]);
                let slot = &mut files[l.file];
                if slot.as_ref().is_some_and(|o| o.period < p) {
                    close(slot.take().unwrap()); // назад по часам не откатываемся: строка уйдёт в текущий файл
                }
                if slot.is_none() {
                    *slot = open(&out.join(sym), &format!("{sym}_{}", KINDS[l.file % 3]), p, l.ns);
                }
                match slot.as_mut().map(|o| writeln!(o.enc, "{} {}", l.ns, l.text)) {
                    Some(Ok(())) => {
                        ROWS.fetch_add(1, Relaxed);
                    }
                    Some(Err(e)) => {
                        if WRITE_ERR.fetch_add(1, Relaxed) == 0 {
                            log!("WARN запись {sym}: {e}");
                        }
                    }
                    None => {
                        WRITE_ERR.fetch_add(1, Relaxed);
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if tick.elapsed() >= Duration::from_secs(5) {
            tick = Instant::now();
            let cur = now_ns().saturating_sub(10_000_000_000) / rotate_ns; // 10 с запаса на строки из очереди
            for slot in files.iter_mut() {
                if slot.as_ref().is_some_and(|o| o.period < cur) {
                    close(slot.take().unwrap());
                } else if let Some(o) = slot {
                    let _ = o.enc.flush();
                }
            }
        }
    }
    files.into_iter().flatten().for_each(close);
    log!("запись: все файлы закрыты");
}

fn open(dir: &Path, base: &str, period: u64, ns: u64) -> Option<Open> {
    let stem = format!("{base}_{}", fmt_name(ns / 1_000_000));
    let mut part = dir.join(format!("{stem}.jsonl.gz.part"));
    for k in 1.. {
        if !part.exists() && !part.with_extension("").exists() {
            break;
        }
        part = dir.join(format!("{stem}_{k}.jsonl.gz.part"));
    }
    match fs::File::create(&part) {
        Ok(f) => Some(Open { period, enc: GzEncoder::new(BufWriter::with_capacity(1 << 16, f), Compression::new(6)), part }),
        Err(e) => {
            log!("WARN создание {}: {e}", part.display());
            None
        }
    }
}

fn close(o: Open) {
    let res = o.enc.finish().and_then(|w| w.into_inner().map_err(|e| e.into_error())).and_then(|f| f.sync_all());
    match res {
        Ok(()) => finish(&o.part),
        Err(e) => log!("WARN закрытие {}: {e} (останется .part, доделается при старте)", o.part.display()),
    }
}

/// `x.jsonl.gz.part` → `x.jsonl.gz` + пустой `x.jsonl.gz.done`.
fn finish(part: &Path) {
    let done = part.with_extension("");
    let mut mark = done.clone().into_os_string();
    mark.push(".done");
    match fs::rename(part, &done).and_then(|_| fs::File::create(&mark).map(|_| ())) {
        Ok(()) => log!("закрыт {} ({:.1} МБ)", done.display(), fs::metadata(&done).map(|m| m.len()).unwrap_or(0) as f64 / 1048576.0),
        Err(e) => log!("WARN закрытие {}: {e}", part.display()),
    }
}

fn list(dir: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .map(|rd| rd.flatten().map(|e| e.path()).filter(|p| p.to_string_lossy().ends_with(suffix)).collect())
        .unwrap_or_default();
    v.sort();
    v
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// (год, месяц, день) по дням от 1970-01-01 (алгоритм Хиннанта).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

fn split(ms: u64) -> (i64, u32, u32, i64, i64, i64) {
    let secs = (ms / 1000) as i64;
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    (y, m, d, s / 3600, s % 3600 / 60, s % 60)
}

fn fmt_ts(ms: u64) -> String {
    let (y, m, d, h, mi, s) = split(ms);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{:03}Z", ms % 1000)
}

fn fmt_name(ms: u64) -> String {
    let (y, m, d, h, mi, s) = split(ms);
    format!("{y:04}{m:02}{d:02}_{h:02}{mi:02}{s:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heads_and_time() {
        let d = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000123,"T":1700000000120,"s":"BTCUSDT","U":10,"u":12,"pu":9,"b":[["37000.10","1.500"]],"a":[["37000.20","0.00000000"]]}}"#;
        let h = serde_json::from_str::<Wrap<DepthHead>>(d).unwrap().data;
        assert_eq!((h.s, h.first, h.u, h.pu), ("BTCUSDT", 10, 12, 9));
        let t = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":77,"p":"1.0","q":"2","f":1,"l":2,"T":1,"m":true}}"#;
        assert_eq!(serde_json::from_str::<Wrap<TradeHead>>(t).unwrap().data.a, 77);
        assert_eq!(fmt_ts(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
        assert_eq!(fmt_name(1_700_000_000_123), "20231114_221320");
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
    }

    #[test]
    fn chain_detector() {
        let mut c = Chain::default();
        assert_eq!(c.step(4, 8), Step::First);
        assert_eq!(c.step(8, 12), Step::Ok);
        assert_eq!(c.step(8, 12), Step::Dup); // дубль
        assert_eq!(c.step(15, 18), Step::Gap(12)); // пропущено событие pu=12..u=15
        assert_eq!(c.step(18, 20), Step::Ok); // цепочка продолжается от нового события
        let mut c = Chain::default(); // реконнект = новая цепочка
        assert_eq!(c.step(100, 105), Step::First);
        let mut t = Chain::default(); // aggTrade: prev = a - 1
        assert_eq!(t.step(76, 77), Step::First);
        assert_eq!(t.step(77, 78), Step::Ok);
        assert_eq!(t.step(80, 81), Step::Gap(78));
    }

    #[test]
    fn queue_overflow_is_marked() {
        let (tx, rx) = mpsc::sync_channel(2);
        let (stx, mut srx) = unbounded_channel();
        let out = Out { tx, syms: Arc::new(vec!["BTCUSDT".into()]), lost: Arc::new((0..3).map(|_| AtomicU64::new(0)).collect()), snap: stx };
        out.send(DEPTH, 1, "a".into());
        out.send(DEPTH, 2, "b".into());
        out.send(DEPTH, 3, "x".into()); // очередь полна — потеряна
        assert_eq!(rx.recv().unwrap().text, "a");
        assert_eq!(rx.recv().unwrap().text, "b");
        out.send(DEPTH, 4, "c".into());
        let m = rx.recv().unwrap();
        assert_eq!(m.text, r#"{"gap":{"s":"BTCUSDT","stream":"depth","reason":"queue_overflow","lost":1}}"#);
        assert_eq!(m.ns, 4);
        assert_eq!(rx.recv().unwrap().text, "c");
        assert_eq!(srx.try_recv().unwrap(), 0); // и запрошен снимок
        assert_eq!(out.lost[DEPTH].load(Relaxed), 0);
    }
}
