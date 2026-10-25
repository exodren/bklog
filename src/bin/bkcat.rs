//! bkcat — чтение и проверка логов bklog.
//!   bkcat stats  <файлы...>             сводка: записи, разрывы, задержка, размер
//!   bkcat dump   <файл> [-n 20]         первые n записей в читаемом виде
//!   bkcat book   <файл...> [--every-ms 1000]  CSV: восстановленный top-of-book (проверка пригодности для бэктеста)

use anyhow::Result;
use bklog::book::Book;
use bklog::codec::*;
use bklog::reader::RecReader;
use bklog::timeutil::fmt_ts;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}
#[derive(Subcommand)]
enum Cmd {
    Stats { files: Vec<PathBuf> },
    Dump {
        file: PathBuf,
        #[arg(short, default_value_t = 20)]
        n: usize,
    },
    Book {
        files: Vec<PathBuf>,
        #[arg(long, default_value_t = 1000)]
        every_ms: u64,
    },
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Stats { files } => stats(files),
        Cmd::Dump { file, n } => dump(file, n),
        Cmd::Book { files, every_ms } => book(files, every_ms),
    }
}

fn fmt_px(p: i64, d: u8) -> String {
    let s = 10i64.pow(d as u32);
    format!("{}.{:0w$}", p / s, (p % s).abs(), w = d as usize)
}

fn dump(file: PathBuf, n: usize) -> Result<()> {
    let mut rd = RecReader::open(&file)?;
    let h = rd.header.clone();
    println!("{h:?}");
    for rec in (&mut rd).take(n) {
        match rec {
            Rec::Depth { recv_us, event_ms, first_u, last_u, prev_u, bids, asks, .. } => println!(
                "{} DEPTH lat={}ms U={first_u} u={last_u} pu={prev_u} bids={} asks={} best_bid_chg={:?}",
                fmt_ts(recv_us), recv_us as i64 / 1000 - event_ms, bids.len(), asks.len(), bids.first().map(|l| (fmt_px(l.0, h.price_dec), l.1))
            ),
            Rec::Trade { recv_us, agg_id, price, qty, buyer_maker, .. } => println!(
                "{} TRADE id={agg_id} px={} qty={qty} {}", fmt_ts(recv_us), fmt_px(price, h.price_dec), if buyer_maker { "SELL(aggr)" } else { "BUY(aggr)" }
            ),
            Rec::Snapshot { recv_us, last_update_id, bids, asks } => println!("{} SNAPSHOT lastUpdateId={last_update_id} bids={} asks={}", fmt_ts(recv_us), bids.len(), asks.len()),
            Rec::Marker { recv_us, kind, arg } => println!("{} MARKER kind={kind} arg={arg}", fmt_ts(recv_us)),
        }
    }
    Ok(())
}

fn stats(files: Vec<PathBuf>) -> Result<()> {
    let (mut tot_bytes, mut tot_depth, mut tot_trades) = (0u64, 0u64, 0u64);
    for f in &files {
        let size = std::fs::metadata(f)?.len();
        tot_bytes += size;
        let mut rd = match RecReader::open(f) {
            Ok(r) => r,
            Err(e) => {
                println!("{}: ОШИБКА {e:#}", f.display());
                continue;
            }
        };
        let (mut nd, mut nt, mut ns, mut nm) = (0u64, 0u64, 0u64, 0u64);
        let (mut first, mut last) = (u64::MAX, 0u64);
        let (mut lat_sum, mut lat_min, mut lat_max) = (0i64, i64::MAX, i64::MIN);
        let mut chain_breaks = 0u64;
        let mut prev_u = 0u64;
        let mut markers = [0u64; 8];
        let mut levels = 0u64;
        let mut book = Book::new();
        let mut synced_rec = 0u64;
        for rec in &mut rd {
            first = first.min(rec.recv_us());
            last = last.max(rec.recv_us());
            if book.feed(&rec) {
                synced_rec += 1;
            }
            match &rec {
                Rec::Depth { recv_us, event_ms, last_u, prev_u: pu, bids, asks, .. } => {
                    nd += 1;
                    levels += (bids.len() + asks.len()) as u64;
                    let lat = *recv_us as i64 / 1000 - event_ms;
                    lat_sum += lat;
                    lat_min = lat_min.min(lat);
                    lat_max = lat_max.max(lat);
                    if prev_u != 0 && *pu != prev_u {
                        chain_breaks += 1;
                    }
                    prev_u = *last_u;
                }
                Rec::Trade { .. } => nt += 1,
                Rec::Snapshot { .. } => ns += 1,
                Rec::Marker { kind, .. } => {
                    nm += 1;
                    if (*kind as usize) < markers.len() {
                        markers[*kind as usize] += 1;
                    }
                }
            }
        }
        tot_depth += nd;
        tot_trades += nt;
        let span = if last > first { (last - first) as f64 / 1e6 } else { 0.0 };
        println!("== {} [{}] {} .. {} ({:.0}s)", f.display(), rd.header.symbol, if first == u64::MAX { "-".into() } else { fmt_ts(first) }, fmt_ts(last), span);
        println!(
            "   размер {:.2} МБ | depth={nd} trades={nt} snapshots={ns} markers={nm} | уровней/depth-сообщение={:.1}",
            size as f64 / 1e6, if nd > 0 { levels as f64 / nd as f64 } else { 0.0 }
        );
        if nd > 0 {
            println!("   латентность recv-E: avg={:.0} min={lat_min} max={lat_max} мс | разрывов цепочки pu: {chain_breaks} | стакан синхронизирован на {:.1}% depth/trade-записей", lat_sum as f64 / nd as f64, 100.0 * synced_rec as f64 / (nd + nt + ns).max(1) as f64);
        }
        println!(
            "   маркеры: connect={} disconnect={} gap={} dropped={} snap_fail={} shutdown={} parse_err={}",
            markers[1], markers[2], markers[3], markers[4], markers[5], markers[6], markers[7]
        );
        if let Some(t) = &rd.truncated {
            println!("   ⚠ файл оборван/нечитаем с некоторого места: {t}");
        }
        if span > 0.0 {
            println!("   ≈ {:.1} МБ/сутки для этого потока", size as f64 / span * 86400.0 / 1e6);
        }
    }
    println!("ИТОГО: файлов {} | {:.2} МБ | depth={tot_depth} trades={tot_trades}", files.len(), tot_bytes as f64 / 1e6);
    Ok(())
}

fn book(files: Vec<PathBuf>, every_ms: u64) -> Result<()> {
    println!("ts,symbol,best_bid,best_ask,mid,spread_bps,obi5,last_u");
    let mut next_ms = 0u64;
    let mut book = Book::new();
    for f in files {
        let mut rd = RecReader::open(&f)?;
        let h = rd.header.clone();
        let sc = 10f64.powi(h.price_dec as i32);
        book = Book::new();
        for rec in &mut rd {
            let ms = rec.recv_us() / 1000;
            let ok = book.feed(&rec);
            if ok && ms >= next_ms {
                if let (Some((b, _)), Some((a, _))) = (book.best_bid(), book.best_ask()) {
                    let mid = (a + b) as f64 / 2.0 / sc;
                    println!("{},{},{},{},{:.4},{:.3},{:.4},{}", fmt_ts(ms * 1000), h.symbol, b as f64 / sc, a as f64 / sc, mid, (a - b) as f64 / sc / mid * 1e4, book.obi(5), book.last_u);
                    next_ms = ms - ms % every_ms + every_ms;
                }
            }
        }
        if let Some(t) = &rd.truncated {
            eprintln!("{}: файл оборван: {t}", f.display());
        }
    }
    eprintln!("resyncs={} chain_breaks={}", book.resyncs, book.chain_breaks);
    Ok(())
}
