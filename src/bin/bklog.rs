//! bklog — логгер публичных потоков Binance USDⓈ-M Futures (diff depth + aggTrade + REST-снимки)
//! в компактный бинарный формат (varint + дельты + zstd), часовые файлы, контроль свободного места.
//! API-ключи не нужны: используются только публичные рыночные данные.

use anyhow::{Context, Result};
use bklog::codec::*;
use bklog::timeutil::{hour_of, hour_parts, now_us};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Parser, Clone)]
#[command(version, about = "Binance Futures order book logger")]
struct Args {
    /// Символы через запятую
    #[arg(long, default_value = "btcusdt,ethusdt", value_delimiter = ',')]
    symbols: Vec<String>,
    #[arg(long, default_value = "./data")]
    out: PathBuf,
    #[arg(long, default_value = "wss://fstream.binance.com/public/stream")]
    public_ws: String,
    #[arg(long, default_value = "wss://fstream.binance.com/market/stream")]
    market_ws: String,
    #[arg(long, default_value = "https://fapi.binance.com")]
    rest: String,
    /// Период регулярных REST-снимков стакана, сек
    #[arg(long, default_value_t = 600)]
    snapshot_secs: u64,
    /// Остановиться, если свободного места меньше (ГБ)
    #[arg(long, default_value_t = 5.0)]
    min_free_gb: f64,
    #[arg(long, default_value_t = 10)]
    zstd_level: i32,
    /// Как часто сбрасывать буфер на диск (макс. потеря при краше), сек
    #[arg(long, default_value_t = 5)]
    flush_secs: u64,
    /// Интервал diff depth: 100 или 500 (мс)
    #[arg(long, default_value_t = 100)]
    depth_ms: u32,
}

// ---------------- shared stats ----------------
#[derive(Default)]
struct Stats {
    depth_msgs: AtomicU64,
    trade_msgs: AtomicU64,
    parse_err: AtomicU64,
    dropped: AtomicU64,
    gaps: AtomicU64,
    reconnects: AtomicU64,
    lat_n: AtomicU64,
    lat_sum_ms: AtomicI64,
    lat_min_ms: AtomicI64,
    lat_max_ms: AtomicI64,
}

enum Msg {
    Rec(usize, Rec),
    Shutdown,
}

// ---------------- writer thread ----------------
struct CountW {
    inner: BufWriter<File>,
    n: Arc<AtomicU64>,
}
impl Write for CountW {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let k = self.inner.write(b)?;
        self.n.fetch_add(k as u64, Relaxed);
        Ok(k)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct SymWriter {
    sym: String,
    pd: u8,
    qd: u8,
    dir: PathBuf,
    level: i32,
    hour: i64,
    enc: Option<zstd::Encoder<'static, CountW>>,
    st: Enc,
    part: PathBuf,
    fin: PathBuf,
    bytes: Arc<AtomicU64>,
    scratch: Vec<u8>,
}

impl SymWriter {
    fn open(&mut self, hour: i64, start_us: u64) -> Result<()> {
        let (y, m, d, h) = hour_parts(hour);
        let day = self.dir.join(&self.sym).join(format!("{y:04}-{m:02}-{d:02}"));
        fs::create_dir_all(&day)?;
        let name = format!("{}_{y:04}-{m:02}-{d:02}T{h:02}.bklog.zst", self.sym);
        self.fin = day.join(&name);
        self.part = day.join(format!("{name}.part"));
        let f = File::create(&self.part).with_context(|| format!("create {}", self.part.display()))?;
        let cw = CountW { inner: BufWriter::with_capacity(1 << 16, f), n: self.bytes.clone() };
        let mut enc = zstd::Encoder::new(cw, self.level)?;
        write_header(&mut enc, &Header { symbol: self.sym.clone(), price_dec: self.pd, qty_dec: self.qd, start_us })?;
        self.enc = Some(enc);
        self.st = Enc::new();
        self.hour = hour;
        info!("{}: opened {}", self.sym, self.fin.display());
        Ok(())
    }

    fn close(&mut self) {
        if let Some(enc) = self.enc.take() {
            match enc.finish().and_then(|mut cw| cw.flush().map(|_| cw)) {
                Ok(_) => {
                    if let Err(e) = fs::rename(&self.part, &self.fin) {
                        error!("{}: rename failed: {e}", self.sym);
                    }
                }
                Err(e) => error!("{}: finish failed: {e}", self.sym),
            }
        }
    }

    fn write(&mut self, rec: &Rec) -> Result<()> {
        let hr = hour_of(rec.recv_us());
        if self.enc.is_none() || hr != self.hour {
            // не откатываемся назад при скачке часов
            if self.enc.is_none() || hr > self.hour {
                self.close();
                self.open(hr, rec.recv_us())?;
            }
        }
        self.scratch.clear();
        self.st.encode(rec, &mut self.scratch);
        self.enc.as_mut().unwrap().write_all(&self.scratch)?;
        Ok(())
    }

    fn flush(&mut self) {
        if let Some(e) = self.enc.as_mut() {
            if let Err(err) = e.flush() {
                error!("{}: flush error: {err}", self.sym);
            }
        }
    }
}

fn free_gb(path: &std::path::Path) -> f64 {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = CString::new(path.as_os_str().as_bytes()) else { return f64::MAX };
    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut s) != 0 {
            return f64::MAX;
        }
        (s.f_bavail as f64) * (s.f_frsize as f64) / 1e9
    }
}

fn writer_thread(
    rx: Receiver<Msg>,
    mut ws: Vec<SymWriter>,
    out: PathBuf,
    flush_every: Duration,
    min_free: f64,
    fatal: Arc<AtomicU64>,
) {
    let mut last_flush = std::time::Instant::now();
    let mut last_disk = std::time::Instant::now() - Duration::from_secs(3600);
    loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Msg::Rec(i, rec)) => {
                if let Err(e) = ws[i].write(&rec) {
                    error!("{}: write error: {e:#}", ws[i].sym);
                    fatal.store(1, Relaxed);
                }
            }
            Ok(Msg::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {}
        }
        if last_flush.elapsed() >= flush_every {
            ws.iter_mut().for_each(|w| w.flush());
            last_flush = std::time::Instant::now();
        }
        if last_disk.elapsed() >= Duration::from_secs(30) {
            last_disk = std::time::Instant::now();
            let g = free_gb(&out);
            if g < min_free {
                error!("свободно {g:.2} ГБ < {min_free} ГБ — останавливаюсь, файлы закрываются корректно");
                fatal.store(2, Relaxed);
            }
        }
        if fatal.load(Relaxed) != 0 {
            break;
        }
    }
    ws.iter_mut().for_each(|w| w.close());
    info!("writer: все файлы закрыты");
}

// ---------------- REST ----------------
#[derive(Deserialize)]
struct ExInfo {
    symbols: Vec<ExSym>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExSym {
    symbol: String,
    price_precision: u8,
    quantity_precision: u8,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapJson<'a> {
    last_update_id: u64,
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

fn send(tx: &SyncSender<Msg>, st: &Stats, m: Msg) {
    match tx.try_send(m) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            st.dropped.fetch_add(1, Relaxed);
        }
        Err(TrySendError::Disconnected(_)) => {}
    }
}

fn parse_levels(v: &[[&str; 2]], pd: u8, qd: u8) -> Option<Levels> {
    let mut o = Vec::with_capacity(v.len());
    for [p, q] in v {
        o.push((parse_scaled(p, pd)?, parse_scaled(q, qd)? as u64));
    }
    Some(o)
}

async fn snapshot_task(
    args: Args,
    syms: Vec<String>,
    dec: Arc<Vec<(u8, u8)>>,
    tx: SyncSender<Msg>,
    st: Arc<Stats>,
    mut req: mpsc::UnboundedReceiver<usize>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap();
    let mut tick = tokio::time::interval(Duration::from_secs(args.snapshot_secs));
    let mut last: Vec<std::time::Instant> = vec![std::time::Instant::now() - Duration::from_secs(3600); syms.len()];
    tick.tick().await; // первый тик сразу — пропускаем: при старте снимки запросит ws-задача
    loop {
        let mut todo: Vec<usize> = vec![];
        tokio::select! {
            _ = tick.tick() => todo.extend(0..syms.len()),
            Some(i) = req.recv() => { todo.push(i); }
            _ = shutdown.changed() => return,
        }
        todo.sort();
        todo.dedup();
        for i in todo {
            if last[i].elapsed() < Duration::from_secs(5) {
                continue;
            }
            last[i] = std::time::Instant::now();
            let url = format!("{}/fapi/v1/depth?symbol={}&limit=1000", args.rest, syms[i].to_uppercase());
            let r = async { client.get(&url).send().await?.error_for_status()?.text().await }.await;
            let recv_us = now_us();
            let ok = match r {
                Ok(body) => match serde_json::from_str::<SnapJson>(&body) {
                    Ok(s) => {
                        let (pd, qd) = dec[i];
                        match (parse_levels(&s.bids, pd, qd), parse_levels(&s.asks, pd, qd)) {
                            (Some(bids), Some(asks)) => {
                                send(&tx, &st, Msg::Rec(i, Rec::Snapshot { recv_us, last_update_id: s.last_update_id, bids, asks }));
                                true
                            }
                            _ => false,
                        }
                    }
                    Err(e) => {
                        warn!("snapshot parse {}: {e}", syms[i]);
                        false
                    }
                },
                Err(e) => {
                    warn!("snapshot {}: {e}", syms[i]);
                    false
                }
            };
            if !ok {
                send(&tx, &st, Msg::Rec(i, Rec::Marker { recv_us, kind: M_SNAPSHOT_FAIL, arg: 0 }));
            }
        }
    }
}

// ---------------- WS ----------------
#[derive(Deserialize)]
struct Wrap<'a> {
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}
#[derive(Deserialize)]
struct Head<'a> {
    e: &'a str,
    s: &'a str,
}
#[derive(Deserialize)]
struct DepthJ<'a> {
    #[serde(rename = "E")]
    ev: i64,
    #[serde(rename = "T", default)]
    tx: i64,
    #[serde(rename = "U")]
    uu: u64,
    u: u64,
    #[serde(default)]
    pu: u64,
    #[serde(borrow)]
    b: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    a: Vec<[&'a str; 2]>,
}
#[derive(Deserialize)]
struct TradeJ<'a> {
    a: u64,
    #[serde(borrow)]
    p: &'a str,
    #[serde(borrow)]
    q: &'a str,
    #[serde(rename = "T")]
    t: i64,
    m: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Public,
    Market,
}

struct WsCtx {
    kind: Kind,
    url: String,
    idx: Arc<HashMap<String, usize>>, // SYMBOL -> index
    dec: Arc<Vec<(u8, u8)>>,
    tx: SyncSender<Msg>,
    st: Arc<Stats>,
    snap: mpsc::UnboundedSender<usize>,
}

fn handle(ctx: &WsCtx, text: &str, recv_us: u64, last_u: &mut [u64]) -> Option<()> {
    let w: Wrap = serde_json::from_str(text).ok()?;
    let raw = w.data.get();
    let h: Head = serde_json::from_str(raw).ok()?;
    let &i = ctx.idx.get(h.s)?;
    let (pd, qd) = ctx.dec[i];
    match h.e {
        "depthUpdate" if ctx.kind == Kind::Public => {
            let d: DepthJ = serde_json::from_str(raw).ok()?;
            let bids = parse_levels(&d.b, pd, qd)?;
            let asks = parse_levels(&d.a, pd, qd)?;
            ctx.st.depth_msgs.fetch_add(1, Relaxed);
            let lat = (recv_us / 1000) as i64 - d.ev;
            ctx.st.lat_n.fetch_add(1, Relaxed);
            ctx.st.lat_sum_ms.fetch_add(lat, Relaxed);
            ctx.st.lat_min_ms.fetch_min(lat, Relaxed);
            ctx.st.lat_max_ms.fetch_max(lat, Relaxed);
            if last_u[i] != 0 {
                if d.u <= last_u[i] {
                    return Some(()); // дубликат
                }
                if d.pu != last_u[i] {
                    ctx.st.gaps.fetch_add(1, Relaxed);
                    send(&ctx.tx, &ctx.st, Msg::Rec(i, Rec::Marker { recv_us, kind: M_GAP, arg: last_u[i] }));
                    let _ = ctx.snap.send(i);
                }
            }
            last_u[i] = d.u;
            send(&ctx.tx, &ctx.st, Msg::Rec(i, Rec::Depth { recv_us, event_ms: d.ev, tx_ms: d.tx, first_u: d.uu, last_u: d.u, prev_u: d.pu, bids, asks }));
        }
        "aggTrade" if ctx.kind == Kind::Market => {
            let t: TradeJ = serde_json::from_str(raw).ok()?;
            ctx.st.trade_msgs.fetch_add(1, Relaxed);
            let rec = Rec::Trade { recv_us, trade_ms: t.t, agg_id: t.a, price: parse_scaled(t.p, pd)?, qty: parse_scaled(t.q, qd)? as u64, buyer_maker: t.m };
            send(&ctx.tx, &ctx.st, Msg::Rec(i, rec));
        }
        _ => {}
    }
    Some(())
}

async fn ws_task(ctx: WsCtx, mut shutdown: watch::Receiver<bool>) {
    let mut backoff = 1u64;
    let kid = if ctx.kind == Kind::Public { 0 } else { 1 };
    let n = ctx.dec.len();
    loop {
        if *shutdown.borrow() {
            return;
        }
        info!("ws[{kid}] connecting {}", ctx.url);
        let conn = tokio::time::timeout(Duration::from_secs(15), tokio_tungstenite::connect_async(ctx.url.as_str())).await;
        let mut ws = match conn {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => {
                warn!("ws[{kid}] connect error: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(30);
                continue;
            }
            Err(_) => {
                warn!("ws[{kid}] connect timeout");
                continue;
            }
        };
        backoff = 1;
        ctx.st.reconnects.fetch_add(1, Relaxed);
        let t0 = now_us();
        let mut last_u = vec![0u64; n];
        for i in 0..n {
            send(&ctx.tx, &ctx.st, Msg::Rec(i, Rec::Marker { recv_us: t0, kind: M_CONNECT, arg: kid }));
            if ctx.kind == Kind::Public {
                let _ = ctx.snap.send(i);
            }
        }
        let reason = loop {
            tokio::select! {
                _ = shutdown.changed() => break "shutdown".to_string(),
                m = tokio::time::timeout(Duration::from_secs(30), ws.next()) => match m {
                    Err(_) => break "no data for 30s".to_string(),
                    Ok(None) => break "stream ended".to_string(),
                    Ok(Some(Err(e))) => break format!("error: {e}"),
                    Ok(Some(Ok(Message::Text(t)))) => {
                        let recv_us = now_us();
                        if handle(&ctx, t.as_str(), recv_us, &mut last_u).is_none() {
                            ctx.st.parse_err.fetch_add(1, Relaxed);
                        }
                    }
                    Ok(Some(Ok(Message::Ping(p)))) => { let _ = ws.send(Message::Pong(p)).await; }
                    Ok(Some(Ok(Message::Close(_)))) => break "close frame".to_string(),
                    Ok(Some(Ok(_))) => {}
                }
            }
        };
        warn!("ws[{kid}] disconnected: {reason}");
        let t1 = now_us();
        for i in 0..n {
            send(&ctx.tx, &ctx.st, Msg::Rec(i, Rec::Marker { recv_us: t1, kind: M_DISCONNECT, arg: kid }));
        }
        if reason == "shutdown" {
            let _ = ws.close(None).await;
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn fetch_decimals(args: &Args, syms: &[String]) -> Result<Vec<(u8, u8)>> {
    let url = format!("{}/fapi/v1/exchangeInfo", args.rest);
    let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let body = client.get(&url).send().await?.error_for_status()?.text().await?;
    let info: ExInfo = serde_json::from_str(&body)?;
    let mut out = vec![];
    for s in syms {
        let e = info.symbols.iter().find(|x| x.symbol.eq_ignore_ascii_case(s)).with_context(|| format!("символ {s} не найден в exchangeInfo"))?;
        out.push((e.price_precision, e.quantity_precision));
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
    let args = Args::parse();
    let syms: Vec<String> = args.symbols.iter().map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty()).collect();
    fs::create_dir_all(&args.out)?;
    info!("symbols={syms:?} out={} depth={}ms", args.out.display(), args.depth_ms);

    let dec = loop {
        match fetch_decimals(&args, &syms).await {
            Ok(d) => break Arc::new(d),
            Err(e) => {
                warn!("exchangeInfo: {e:#}; повтор через 5 с");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    info!("precision (price,qty): {:?}", dec);

    let st = Arc::new(Stats::default());
    st.lat_min_ms.store(i64::MAX, Relaxed);
    st.lat_max_ms.store(i64::MIN, Relaxed);
    let (tx, rx) = sync_channel::<Msg>(500_000);
    let fatal = Arc::new(AtomicU64::new(0));
    let bytes: Vec<Arc<AtomicU64>> = syms.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();
    let writers: Vec<SymWriter> = syms
        .iter()
        .enumerate()
        .map(|(i, s)| SymWriter {
            sym: s.to_uppercase(),
            pd: dec[i].0,
            qd: dec[i].1,
            dir: args.out.clone(),
            level: args.zstd_level,
            hour: 0,
            enc: None,
            st: Enc::new(),
            part: PathBuf::new(),
            fin: PathBuf::new(),
            bytes: bytes[i].clone(),
            scratch: Vec::with_capacity(8192),
        })
        .collect();
    let wh = {
        let (out, fl, mf, fa) = (args.out.clone(), Duration::from_secs(args.flush_secs), args.min_free_gb, fatal.clone());
        std::thread::Builder::new().name("writer".into()).spawn(move || writer_thread(rx, writers, out, fl, mf, fa))?
    };

    let (sd_tx, sd_rx) = watch::channel(false);
    let idx: Arc<HashMap<String, usize>> = Arc::new(syms.iter().enumerate().map(|(i, s)| (s.to_uppercase(), i)).collect());
    let (snap_tx, snap_rx) = mpsc::unbounded_channel();

    let depth_streams = syms.iter().map(|s| format!("{s}@depth@{}ms", args.depth_ms)).collect::<Vec<_>>().join("/");
    let trade_streams = syms.iter().map(|s| format!("{s}@aggTrade")).collect::<Vec<_>>().join("/");
    let mk = |kind, url: String| WsCtx { kind, url, idx: idx.clone(), dec: dec.clone(), tx: tx.clone(), st: st.clone(), snap: snap_tx.clone() };
    let mut tasks = vec![];
    tasks.push(tokio::spawn(ws_task(mk(Kind::Public, format!("{}?streams={}", args.public_ws, depth_streams)), sd_rx.clone())));
    tasks.push(tokio::spawn(ws_task(mk(Kind::Market, format!("{}?streams={}", args.market_ws, trade_streams)), sd_rx.clone())));
    tasks.push(tokio::spawn(snapshot_task(args.clone(), syms.clone(), dec.clone(), tx.clone(), st.clone(), snap_rx, sd_rx.clone())));

    // периодическая статистика
    {
        let (st, bytes, out, fatal, mut sd) = (st.clone(), bytes.clone(), args.out.clone(), fatal.clone(), sd_rx.clone());
        let syms = syms.clone();
        let tx2 = tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            let mut prev_dropped = 0;
            let mut prev_perr = 0;
            tick.tick().await;
            loop {
                tokio::select! { _ = tick.tick() => {}, _ = sd.changed() => return }
                let n = st.lat_n.swap(0, Relaxed);
                let sum = st.lat_sum_ms.swap(0, Relaxed);
                let (mn, mx) = (st.lat_min_ms.swap(i64::MAX, Relaxed), st.lat_max_ms.swap(i64::MIN, Relaxed));
                let b: Vec<String> = syms.iter().zip(&bytes).map(|(s, b)| format!("{}={:.1}MB", s, b.load(Relaxed) as f64 / 1e6)).collect();
                info!(
                    "depth={} trades={} gaps={} reconnects={} dropped={} parse_err={} | lat(recv-E) ms avg={:.0} min={} max={} | written(compressed) {} | free={:.1}GB",
                    st.depth_msgs.load(Relaxed), st.trade_msgs.load(Relaxed), st.gaps.load(Relaxed), st.reconnects.load(Relaxed),
                    st.dropped.load(Relaxed), st.parse_err.load(Relaxed),
                    if n > 0 { sum as f64 / n as f64 } else { 0.0 }, if n > 0 { mn } else { 0 }, if n > 0 { mx } else { 0 },
                    b.join(" "), free_gb(&out)
                );
                let (d, p) = (st.dropped.load(Relaxed), st.parse_err.load(Relaxed));
                if d > prev_dropped {
                    let _ = tx2.try_send(Msg::Rec(0, Rec::Marker { recv_us: now_us(), kind: M_DROPPED, arg: d - prev_dropped }));
                    prev_dropped = d;
                }
                if p > prev_perr {
                    let _ = tx2.try_send(Msg::Rec(0, Rec::Marker { recv_us: now_us(), kind: M_PARSE_ERR, arg: p - prev_perr }));
                    prev_perr = p;
                }
                if fatal.load(Relaxed) != 0 {
                    return;
                }
            }
        }));
    }

    // ожидание сигнала или аварии писателя
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { info!("SIGINT"); break; }
            _ = term.recv() => { info!("SIGTERM"); break; }
            _ = tokio::time::sleep(Duration::from_secs(1)) => { if fatal.load(Relaxed) != 0 { break; } }
        }
    }
    let t = now_us();
    for i in 0..syms.len() {
        let _ = tx.try_send(Msg::Rec(i, Rec::Marker { recv_us: t, kind: M_SHUTDOWN, arg: 0 }));
    }
    let _ = sd_tx.send(true);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
    }
    let _ = tx.send(Msg::Shutdown);
    let _ = wh.join();
    let code = fatal.load(Relaxed);
    if code != 0 {
        std::process::exit(code as i32);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bklog::reader::RecReader;

    #[test]
    fn rotation_and_readback() {
        let dir = std::env::temp_dir().join(format!("bklog_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut w = SymWriter {
            sym: "BTCUSDT".into(), pd: 2, qd: 3, dir: dir.clone(), level: 3, hour: 0, enc: None, st: Enc::new(),
            part: PathBuf::new(), fin: PathBuf::new(), bytes: Arc::new(AtomicU64::new(0)), scratch: vec![],
        };
        let base: u64 = 1_700_000_000_000_000; // 2023-11-14T22:13:20Z
        let mut expected = vec![];
        for k in 0..3000u64 {
            let rec = Rec::Depth { recv_us: base + k * 100_000_000, event_ms: ((base + k * 100_000_000) / 1000) as i64 - 5, tx_ms: ((base + k * 100_000_000) / 1000) as i64 - 6,
                first_u: 10 + k * 3, last_u: 12 + k * 3, prev_u: 9 + k * 3, bids: vec![(10_000_000 + k as i64, k), (9_999_990, 0)], asks: vec![(10_000_010, 5)] };
            w.write(&rec).unwrap();
            expected.push(rec);
        }
        w.close();
        let mut files: Vec<_> = walk(&dir);
        files.sort();
        assert!(files.len() >= 80, "ожидались часовые файлы, получено {}", files.len()); // 3000*100s = 83 часа
        assert!(files.iter().all(|f| f.extension().unwrap() == "zst"), "остались .part");
        let mut got = vec![];
        for f in &files {
            let mut r = RecReader::open(f).unwrap();
            got.extend(&mut r);
            assert!(r.truncated.is_none());
        }
        assert_eq!(got, expected);
        let _ = fs::remove_dir_all(&dir);
    }

    fn walk(p: &std::path::Path) -> Vec<PathBuf> {
        let mut out = vec![];
        for e in fs::read_dir(p).unwrap() {
            let e = e.unwrap().path();
            if e.is_dir() { out.extend(walk(&e)) } else { out.push(e) }
        }
        out
    }
}
