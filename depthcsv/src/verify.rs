//! `depthcsv verify <OUT_DIR> [SYMBOL...]` — собирает стакан из REST-снимков и диффов по процедуре Binance и сверяет
//! верхние 50 уровней на каждом следующем снимке. Выводит несовпадения, GAP с длительностью, покрытие, сводку по дням.
//!
//! RPI-ордера (Retail Price Improvement) по документации Binance исключены и из `@depth`, и из `/fapi/v1/depth`, так что
//! источники сравнимы; снимки `/fapi/v1/rpiDepth` и поток `@rpiDepth` (с RPI) сюда смешивать нельзя.
use crate::{civil_from_days, fmt_ts, list, Chain, Step, TradeHead, Wrap};
use flate2::read::MultiGzDecoder;
use serde::Deserialize;
use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

const TOP: usize = 50;
const DAY_NS: u64 = 86_400_000_000_000;

type Lv = Vec<(u128, u128)>;

/// Десятичная строка биржи → целое в единицах 1e-18 (точное сравнение без float).
fn num(s: &str) -> Option<u128> {
    let (i, f) = s.split_once('.').unwrap_or((s, ""));
    if f.len() > 18 || (i.is_empty() && f.is_empty()) {
        return None;
    }
    let int: u128 = if i.is_empty() { 0 } else { i.parse().ok()? };
    let frac: u128 = if f.is_empty() { 0 } else { f.parse::<u128>().ok()? * 10u128.pow(18 - f.len() as u32) };
    int.checked_mul(10u128.pow(18))?.checked_add(frac)
}

fn dec(x: u128) -> String {
    let s = format!("{}.{:018}", x / 10u128.pow(18), x % 10u128.pow(18));
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn lv(v: &[(&str, &str)]) -> Option<Lv> {
    v.iter().map(|(p, q)| Some((num(p)?, num(q)?))).collect()
}

#[derive(Default, Clone, PartialEq, Debug)]
struct Book {
    bids: BTreeMap<u128, u128>,
    asks: BTreeMap<u128, u128>,
}

impl Book {
    fn apply(&mut self, bids: &Lv, asks: &Lv) {
        for (side, lv) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for &(p, q) in lv {
                if q == 0 {
                    side.remove(&p);
                } else {
                    side.insert(p, q);
                }
            }
        }
    }

    fn top(&self) -> (Lv, Lv) {
        (self.bids.iter().rev().take(TOP).map(|(p, q)| (*p, *q)).collect(), self.asks.iter().take(TOP).map(|(p, q)| (*p, *q)).collect())
    }
}

struct Ev {
    ns: u64,
    u: u64,
    pu: u64,
    bids: Lv,
    asks: Lv,
}

struct Snap {
    ns: u64,
    lid: u64,
    bids: Lv,
    asks: Lv,
}

enum Item {
    Ev(Ev),
    Gap(u64, String),
}

#[derive(Default)]
struct Day {
    events: u64,
    checks: u64,
    mismatches: u64,
    gaps: u64,
    synced_ns: u64,
    first: u64,
    last: u64,
}

struct Gap {
    start: u64,
    end: Option<u64>,
    reason: String,
}

#[derive(Default)]
struct Report {
    events: u64,
    dups: u64,
    checks: u64,
    mismatches: u64,
    skipped_snaps: u64,
    crossed: u64,
    unexplained: u64,
    parse_err: u64,
    trades: u64,
    trade_dups: u64,
    trade_gaps: u64,
    trade_unexplained: u64,
    synced_ns: u64,
    first_ns: u64,
    last_ns: u64,
    gaps: Vec<Gap>,
    examples: Vec<String>,
    bad_files: Vec<String>,
    days: BTreeMap<u64, Day>,
}

impl Report {
    fn day(&mut self, ns: u64) -> &mut Day {
        self.first_ns = self.first_ns.min(ns);
        self.last_ns = self.last_ns.max(ns);
        let d = self.days.entry(ns / DAY_NS).or_insert(Day { first: u64::MAX, ..Default::default() });
        d.first = d.first.min(ns);
        d.last = d.last.max(ns);
        d
    }
}

/// Состояние сборки стакана одного символа.
#[derive(Default)]
struct V {
    book: Book,
    synced: bool,
    last_u: Option<u64>,
    expect: bool, // был GAP-маркер — следующий разрыв цепочки объяснён
    prev_ns: u64,
    trades: Chain,
    texpect: bool,
    r: Report,
}

impl V {
    fn new() -> Self {
        let mut v = V::default();
        v.r.first_ns = u64::MAX;
        v
    }

    fn open_gap(&mut self, ns: u64, reason: &str) {
        if let Some(g) = self.r.gaps.last_mut().filter(|g| g.end.is_none()) {
            if !g.reason.split('+').any(|r| r == reason) {
                g.reason = format!("{}+{reason}", g.reason);
            }
            return;
        }
        self.r.gaps.push(Gap { start: ns, end: None, reason: reason.into() });
        self.r.day(ns).gaps += 1;
    }

    fn close_gap(&mut self, ns: u64) {
        if let Some(g) = self.r.gaps.last_mut().filter(|g| g.end.is_none()) {
            g.end = Some(ns);
        }
    }

    fn marker(&mut self, ns: u64, reason: &str) {
        self.r.day(ns);
        self.open_gap(ns, reason);
        self.expect = true;
    }

    /// Одно depth-событие. `snaps` — снимки с lastUpdateId ≤ u этого события, по возрастанию.
    /// Синхронизация: события с u < L отбрасываются, первое применяемое должно накрывать L (U ≤ L ≤ u; используем
    /// pu ≤ L ≤ u — то же условие, плюс случай L между событиями), дальше pu == u предыдущего.
    fn event(&mut self, e: &Ev, snaps: &mut VecDeque<Snap>) {
        self.r.events += 1;
        self.r.day(e.ns).events += 1;
        if let Some(l) = self.last_u {
            if e.u <= l {
                self.r.dups += 1;
                return;
            }
            if e.pu != l {
                if !self.expect {
                    self.r.unexplained += 1;
                    self.open_gap(e.ns, &format!("НЕОБЪЯСНЁН: pu={} ≠ u={l}", e.pu));
                }
                self.synced = false;
            } else if self.synced && self.expect {
                self.close_gap(e.ns); // маркер был, но цепочка не прервалась — потерь нет
            }
        } else if !self.synced && !self.expect {
            self.open_gap(e.ns, "начало данных");
        }
        let dt = if self.prev_ns > 0 { e.ns.saturating_sub(self.prev_ns) } else { 0 };
        self.prev_ns = e.ns;
        if self.synced {
            self.r.synced_ns += dt;
            self.r.day(e.ns).synced_ns += dt;
        }
        self.expect = false;
        self.last_u = Some(e.u);

        while snaps.front().is_some_and(|s| s.lid < e.pu) {
            snaps.pop_front(); // L попал в разрыв или раньше начала потока
            self.r.skipped_snaps += 1;
        }
        if self.synced {
            self.book.apply(&e.bids, &e.asks);
        }
        if let Some(s) = snaps.pop_front() {
            // lid ≤ u гарантирует вызывающий, pu ≤ lid — цикл выше: снимок + это событие = стакан на момент u
            let mut want = Book::default();
            want.apply(&s.bids, &s.asks);
            want.apply(&e.bids, &e.asks);
            if self.synced {
                self.check(e.ns, &want);
            } else {
                self.synced = true;
                self.close_gap(e.ns);
            }
            self.book = want;
        }
    }

    fn check(&mut self, ns: u64, want: &Book) {
        self.r.checks += 1;
        self.r.day(ns).checks += 1;
        let (ours, theirs) = (self.book.top(), want.top());
        if ours.0.first().zip(ours.1.first()).is_some_and(|(b, a)| b.0 >= a.0) {
            self.r.crossed += 1;
        }
        if ours == theirs {
            return;
        }
        self.r.mismatches += 1;
        self.r.day(ns).mismatches += 1;
        if self.r.examples.len() < 10 {
            let (a, b, side) = if ours.0 != theirs.0 { (&ours.0, &theirs.0, "bid") } else { (&ours.1, &theirs.1, "ask") };
            let k = a.iter().zip(b).position(|(x, y)| x != y).unwrap_or(a.len().min(b.len()));
            let f = |l: Option<&(u128, u128)>| l.map_or("-".into(), |(p, q)| format!("{}×{}", dec(*p), dec(*q)));
            self.r.examples.push(format!("{} {side} уровень {k}: у нас {}, в снимке {}", fmt_ts(ns / 1_000_000), f(a.get(k)), f(b.get(k))));
        }
    }

    fn trade(&mut self, ns: u64, a: u64) {
        self.r.trades += 1;
        self.r.day(ns);
        match self.trades.step(a.saturating_sub(1), a) {
            Step::Dup => self.r.trade_dups += 1,
            Step::Gap(_) if !self.texpect => self.r.trade_unexplained += 1,
            _ => {}
        }
        self.texpect = false;
    }

    fn trade_marker(&mut self, ns: u64) {
        self.r.day(ns);
        self.r.trade_gaps += 1;
        self.texpect = true;
    }
}

fn run_depth(v: &mut V, items: impl Iterator<Item = Option<Item>>, snaps: impl Iterator<Item = Snap>) {
    let mut snaps = snaps.peekable();
    let mut q = VecDeque::new();
    for it in items {
        match it {
            Some(Item::Gap(ns, r)) => v.marker(ns, &r),
            Some(Item::Ev(e)) => {
                while let Some(s) = snaps.next_if(|s| s.lid <= e.u) {
                    v.r.day(s.ns);
                    q.push_back(s);
                }
                v.event(&e, &mut q);
            }
            None => v.r.parse_err += 1,
        }
    }
}

#[derive(Deserialize)]
struct GapL {
    gap: GapJ,
}
#[derive(Deserialize)]
struct GapJ {
    reason: String,
}

#[derive(Deserialize)]
struct DepthJ<'a> {
    u: u64,
    pu: u64,
    #[serde(borrow)]
    b: Vec<(&'a str, &'a str)>,
    #[serde(borrow)]
    a: Vec<(&'a str, &'a str)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapJ<'a> {
    last_update_id: u64,
    #[serde(borrow)]
    bids: Vec<(&'a str, &'a str)>,
    #[serde(borrow)]
    asks: Vec<(&'a str, &'a str)>,
}

fn is_gap(j: &str) -> bool {
    j.starts_with(r#"{"gap""#)
}

fn parse_depth(ns: u64, j: &str) -> Option<Item> {
    if is_gap(j) {
        return Some(Item::Gap(ns, serde_json::from_str::<GapL>(j).ok()?.gap.reason));
    }
    let d = serde_json::from_str::<Wrap<DepthJ>>(j).ok()?.data;
    Some(Item::Ev(Ev { ns, u: d.u, pu: d.pu, bids: lv(&d.b)?, asks: lv(&d.a)? }))
}

/// Ok(None) — служебная строка (GAP в файле снимков), Err — не разобрано.
fn parse_snap(ns: u64, j: &str) -> Result<Option<Snap>, ()> {
    if is_gap(j) {
        return Ok(None);
    }
    let s = serde_json::from_str::<SnapJ>(j).map_err(|_| ())?;
    Ok(Some(Snap { ns, lid: s.last_update_id, bids: lv(&s.bids).ok_or(())?, asks: lv(&s.asks).ok_or(())? }))
}

/// Строки `<ns> <json>` из цепочки gzip-файлов. Оборванный хвост (`.part` после падения или ещё пишущийся файл)
/// читается до последнего целого сброса и попадает в `bad`.
struct Lines {
    files: VecDeque<PathBuf>,
    cur: Option<(PathBuf, Box<dyn BufRead>)>,
    buf: String,
    bad: Vec<String>,
    junk: u64,
}

fn lines(files: Vec<PathBuf>) -> Lines {
    Lines { files: files.into(), cur: None, buf: String::new(), bad: vec![], junk: 0 }
}

impl Iterator for Lines {
    type Item = (u64, String);
    fn next(&mut self) -> Option<(u64, String)> {
        loop {
            if self.cur.is_none() {
                let p = self.files.pop_front()?;
                match fs::File::open(&p) {
                    Ok(f) => self.cur = Some((p, Box::new(BufReader::with_capacity(1 << 16, MultiGzDecoder::new(f))) as Box<dyn BufRead>)),
                    Err(e) => self.bad.push(format!("{}: {e}", p.display())),
                }
                continue;
            }
            let (p, rd) = self.cur.as_mut().unwrap();
            self.buf.clear();
            match rd.read_line(&mut self.buf) {
                Ok(0) => self.cur = None,
                Err(e) => {
                    self.bad.push(format!("{}: оборван ({e})", p.display()));
                    self.cur = None;
                }
                Ok(_) if !self.buf.ends_with('\n') => {
                    self.bad.push(format!("{}: оборванная последняя строка", p.display()));
                    self.cur = None;
                }
                Ok(_) => match self.buf.trim_end().split_once(' ').and_then(|(ns, j)| Some((ns.parse().ok()?, j))) {
                    Some((ns, j)) => return Some((ns, j.to_string())),
                    None => self.junk += 1,
                },
            }
        }
    }
}

fn verify_sym(dir: &Path, sym: &str) -> Report {
    let files = |kind: &str| -> Vec<PathBuf> {
        let pre = format!("{sym}_{kind}_");
        list(&dir.join(sym), "")
            .into_iter()
            .filter(|p| p.file_name().map(|n| n.to_string_lossy()).is_some_and(|n| n.starts_with(&pre) && (n.ends_with(".jsonl.gz") || n.ends_with(".jsonl.gz.part"))))
            .collect()
    };
    let mut v = V::new();
    let (mut dl, mut sl, mut tl) = (lines(files("depth")), lines(files("snap")), lines(files("trade")));
    let snap_err = Cell::new(0u64);
    let snaps = sl.by_ref().filter_map(|(ns, j)| parse_snap(ns, &j).unwrap_or_else(|_| {
        snap_err.set(snap_err.get() + 1);
        None
    }));
    run_depth(&mut v, dl.by_ref().map(|(ns, j)| parse_depth(ns, &j)), snaps);
    for (ns, j) in tl.by_ref() {
        if is_gap(&j) {
            v.trade_marker(ns);
        } else if let Ok(w) = serde_json::from_str::<Wrap<TradeHead>>(&j) {
            v.trade(ns, w.data.a);
        } else {
            v.r.parse_err += 1;
        }
    }
    v.r.parse_err += snap_err.get() + dl.junk + sl.junk + tl.junk;
    v.r.bad_files = [dl.bad, sl.bad, tl.bad].concat();
    v.r
}

fn pct(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { 100.0 * a as f64 / b as f64 }
}

fn ts(ns: u64) -> String {
    fmt_ts(ns / 1_000_000)
}

fn print(sym: &str, r: &Report) {
    if r.first_ns == u64::MAX {
        println!("== {sym}: нет данных");
        return;
    }
    let span = r.last_ns - r.first_ns;
    println!("== {sym}: {} .. {} ({:.0} с)", ts(r.first_ns), ts(r.last_ns), span as f64 / 1e9);
    println!("   depth: событий {}, дублей {}, ошибок разбора {}", r.events, r.dups, r.parse_err);
    println!(
        "   контрольных снимков {}, несовпадений top-{TOP}: {} ({:.3}%); снимков вне синхронизации {}",
        r.checks, r.mismatches, pct(r.mismatches, r.checks), r.skipped_snaps
    );
    println!("   покрытие {:.2}% (стакан синхронизирован {:.0} с из {:.0} с)", pct(r.synced_ns, span), r.synced_ns as f64 / 1e9, span as f64 / 1e9);
    println!("   aggTrade: сделок {}, дублей {}, GAP-маркеров {}, необъяснённых скачков id {}", r.trades, r.trade_dups, r.trade_gaps, r.trade_unexplained);
    if r.crossed > 0 {
        println!("   ⚠ перекрещенный стакан (bid ≥ ask) на {} проверках", r.crossed);
    }
    for e in &r.examples {
        println!("   ✗ {e}");
    }
    println!("   разрывы depth: {} (необъяснённых {})", r.gaps.len(), r.unexplained);
    for g in &r.gaps {
        let d = g.end.map_or("не закрыт".to_string(), |e| format!("{:.1} с", e.saturating_sub(g.start) as f64 / 1e9));
        println!("     {}  {d:>10}  {}", ts(g.start), g.reason);
    }
    for b in &r.bad_files {
        println!("   ⚠ {b}");
    }
    println!("   по дням (UTC):   события  проверки  несовп    доля   gaps  покрытие");
    for (d, x) in &r.days {
        let (y, m, dd) = civil_from_days(*d as i64);
        println!(
            "     {y:04}-{m:02}-{dd:02}  {:>9}  {:>8}  {:>6}  {:>6.3}%  {:>5}  {:>7.2}%",
            x.events, x.checks, x.mismatches, pct(x.mismatches, x.checks), x.gaps, pct(x.synced_ns, x.last - x.first)
        );
    }
}

/// Код возврата: 0 — всё сошлось, 1 — несовпадения/необъяснённые разрывы/нет ни одной проверки, 2 — ошибка аргументов.
pub fn main(args: Vec<String>) -> i32 {
    let Some(dir) = args.first().map(PathBuf::from) else {
        eprintln!("использование: depthcsv verify <OUT_DIR> [SYMBOL...]");
        return 2;
    };
    let mut syms: Vec<String> = args[1..].iter().map(|s| s.to_uppercase()).collect();
    if syms.is_empty() {
        syms = list(&dir, "").into_iter().filter(|p| p.is_dir()).filter_map(|p| Some(p.file_name()?.to_string_lossy().into_owned())).collect();
    }
    let mut bad = syms.is_empty();
    for s in &syms {
        let r = verify_sym(&dir, s);
        print(s, &r);
        bad |= r.checks == 0 || r.mismatches > 0 || r.unexplained > 0 || r.trade_unexplained > 0;
    }
    println!("{}", if bad { "ИТОГ: есть проблемы (см. выше)" } else { "ИТОГ: OK" });
    bad as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    type L<'a> = &'a [(&'a str, &'a str)];

    fn ev(ns: u64, u: u64, pu: u64, b: L<'_>, a: L<'_>) -> Option<Item> {
        Some(Item::Ev(Ev { ns, u, pu, bids: lv(b).unwrap(), asks: lv(a).unwrap() }))
    }
    fn gap(ns: u64, r: &str) -> Option<Item> {
        Some(Item::Gap(ns, r.into()))
    }
    fn snap(ns: u64, lid: u64, b: L<'_>, a: L<'_>) -> Snap {
        Snap { ns, lid, bids: lv(b).unwrap(), asks: lv(a).unwrap() }
    }
    fn run(items: Vec<Option<Item>>, snaps: Vec<Snap>) -> V {
        let mut v = V::new();
        run_depth(&mut v, items.into_iter(), snaps.into_iter());
        v
    }

    const S1B: L<'static> = &[("100.0", "1"), ("99.9", "2")];
    const S1A: L<'static> = &[("100.1", "1")];
    // стакан после событий до u=15: bid 99.9×2, 99.8×5; ask 100.1×1, 100.2×3
    const S2B: L<'static> = &[("99.9", "2"), ("99.8", "5")];
    const S2A: L<'static> = &[("100.1", "1.000"), ("100.2", "3")];

    fn base() -> Vec<Option<Item>> {
        vec![
            gap(1, "start"),
            ev(2, 8, 4, &[("99.0", "1")], &[]),                     // до снимка (u < L) — отбрасывается
            ev(3, 12, 8, &[("100.0", "0")], &[("100.2", "3")]),     // накрывает L=10: синхронизация
            ev(4, 15, 12, &[("99.8", "5")], &[]),
        ]
    }

    #[test]
    fn sync_and_check() {
        let v = run(base(), vec![snap(3, 10, S1B, S1A), snap(5, 15, S2B, S2A)]);
        assert!(v.synced);
        assert_eq!((v.r.checks, v.r.mismatches, v.r.unexplained), (1, 0, 0));
        assert_eq!(v.r.gaps.len(), 1);
        assert_eq!((v.r.gaps[0].start, v.r.gaps[0].end, v.r.gaps[0].reason.as_str()), (1, Some(3), "start"));
        assert_eq!(v.book.bids.len(), 2); // 100.0 удалён объёмом 0
    }

    #[test]
    fn missing_message_is_unexplained() {
        let mut it = base();
        it.pop(); // потеряно событие u=15
        it.push(ev(5, 18, 15, &[], &[]));
        let v = run(it, vec![snap(3, 10, S1B, S1A), snap(6, 17, S2B, S2A)]);
        assert_eq!(v.r.unexplained, 1);
        assert_eq!(v.r.checks, 0); // второй снимок пошёл на ресинхронизацию, не на проверку
        assert!(v.synced);
        assert_eq!(v.r.gaps.len(), 2);
        assert!(v.r.gaps[1].reason.starts_with("НЕОБЪЯСНЁН") && v.r.gaps[1].end == Some(5));
    }

    #[test]
    fn marker_explains_gap() {
        let mut it = base();
        it.pop();
        it.push(gap(5, "pu_mismatch"));
        it.push(ev(5, 18, 15, &[], &[]));
        it.push(ev(7, 20, 18, &[], &[]));
        let v = run(it, vec![snap(3, 10, S1B, S1A), snap(6, 19, S2B, S2A)]);
        assert_eq!(v.r.unexplained, 0);
        assert_eq!((v.r.gaps[1].reason.as_str(), v.r.gaps[1].start, v.r.gaps[1].end), ("pu_mismatch", 5, Some(7)));
        assert!(v.synced);
    }

    #[test]
    fn duplicate_is_ignored() {
        let mut it = base();
        it.push(ev(5, 15, 12, &[("99.8", "5")], &[]));
        let v = run(it, vec![snap(3, 10, S1B, S1A), snap(6, 15, S2B, S2A)]);
        assert_eq!((v.r.dups, v.r.unexplained, v.r.checks, v.r.mismatches), (1, 0, 1, 0));
    }

    #[test]
    fn reconnect_resyncs() {
        let mut it = base();
        it.push(gap(5, "disconnect"));
        it.push(gap(6, "start")); // рестарт процесса
        it.push(ev(7, 105, 99, &[("99.7", "1")], &[]));
        it.push(ev(8, 110, 105, &[], &[]));
        let after = &[("99.9", "2"), ("99.8", "5"), ("99.7", "1")];
        let v = run(it, vec![snap(3, 10, S1B, S1A), snap(8, 107, after, S2A), snap(9, 110, after, S2A)]);
        assert_eq!(v.r.unexplained, 0);
        let g = &v.r.gaps[1];
        assert_eq!((g.reason.as_str(), g.start, g.end), ("disconnect+start", 5, Some(8)));
        assert_eq!((v.r.checks, v.r.mismatches), (0, 0)); // снимок 110 ждёт следующего события
        assert!(v.synced);
    }

    #[test]
    fn mismatch_is_reported() {
        let v = run(base(), vec![snap(3, 10, S1B, S1A), snap(5, 15, &[("99.9", "3"), ("99.8", "5")], S2A)]);
        assert_eq!((v.r.checks, v.r.mismatches), (1, 1));
        assert!(v.r.examples[0].contains("bid уровень 0"), "{}", v.r.examples[0]);
    }

    #[test]
    fn marker_without_break_closes_gap() {
        let mut it = base();
        it.push(gap(5, "disconnect"));
        it.push(ev(6, 18, 15, &[], &[]));
        let v = run(it, vec![snap(3, 10, S1B, S1A)]);
        assert!(v.synced);
        assert_eq!(v.r.gaps[1].end, Some(6));
    }

    #[test]
    fn numbers() {
        assert_eq!(num("0.00000000"), Some(0));
        assert_eq!(num("1.500"), num("1.5"));
        assert_eq!(dec(num("37000.10").unwrap()), "37000.1");
        assert!(num("1.2.3").is_none() && num("-1").is_none() && num("").is_none());
    }
}
