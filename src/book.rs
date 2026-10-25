//! Восстановление стакана из лога по процедуре Binance (U <= lastUpdateId <= u, затем цепочка pu == u).
use crate::codec::{Levels, Rec, M_CONNECT, M_DISCONNECT, M_GAP};
use std::collections::{BTreeMap, VecDeque};

#[derive(Default)]
pub struct Book {
    pub bids: BTreeMap<i64, u64>,
    pub asks: BTreeMap<i64, u64>,
    pub last_u: u64,
    pub synced: bool,
    buf: VecDeque<Rec>,
    pub resyncs: u64,
    pub chain_breaks: u64,
}

fn apply(side: &mut BTreeMap<i64, u64>, lv: &Levels) {
    for &(p, q) in lv {
        if q == 0 {
            side.remove(&p);
        } else {
            side.insert(p, q);
        }
    }
}

impl Book {
    pub fn new() -> Self {
        Self::default()
    }

    fn desync(&mut self) {
        if self.synced {
            self.synced = false;
        }
        self.buf.clear();
    }

    /// Подает запись. Возвращает true, если стакан синхронизирован и изменился/актуален после этой записи.
    pub fn feed(&mut self, rec: &Rec) -> bool {
        match rec {
            Rec::Marker { kind, .. } if matches!(*kind, M_CONNECT | M_DISCONNECT | M_GAP) => {
                if self.synced && *kind == M_GAP {
                    self.chain_breaks += 1;
                }
                self.desync();
                false
            }
            Rec::Snapshot { last_update_id, bids, asks, .. } => {
                self.bids.clear();
                self.asks.clear();
                apply(&mut self.bids, bids);
                apply(&mut self.asks, asks);
                self.synced = false;
                // ищем в буфере первое событие, покрывающее lastUpdateId
                let lid = *last_update_id;
                let pending: Vec<Rec> = self.buf.drain(..).collect();
                let mut started = false;
                for ev in pending {
                    if let Rec::Depth { first_u, last_u, prev_u, bids, asks, .. } = &ev {
                        if !started {
                            if *last_u <= lid {
                                continue;
                            }
                            if *first_u <= lid + 1 {
                                started = true;
                            } else {
                                return false; // между снимком и буфером дыра — ждем следующий снимок
                            }
                        } else if *prev_u != self.last_u {
                            self.chain_breaks += 1;
                            return false;
                        }
                        apply(&mut self.bids, bids);
                        apply(&mut self.asks, asks);
                        self.last_u = *last_u;
                    }
                }
                if started {
                    self.synced = true;
                    self.resyncs += 1;
                } else {
                    // событий ещё нет: примем следующее с U <= lid+1 <= u как стартовое
                    self.last_u = lid;
                    self.synced = true;
                    self.resyncs += 1;
                }
                self.synced
            }
            Rec::Depth { first_u, last_u, prev_u, bids, asks, .. } => {
                if self.synced {
                    if *last_u <= self.last_u {
                        return true; // дубликат
                    }
                    if *prev_u != self.last_u && !(*first_u <= self.last_u + 1 && *last_u >= self.last_u + 1) {
                        self.chain_breaks += 1;
                        self.synced = false;
                        self.buf.clear();
                        self.buf.push_back(rec.clone());
                        return false;
                    }
                    apply(&mut self.bids, bids);
                    apply(&mut self.asks, asks);
                    self.last_u = *last_u;
                    true
                } else {
                    self.buf.push_back(rec.clone());
                    if self.buf.len() > 4000 {
                        self.buf.pop_front();
                    }
                    false
                }
            }
            _ => false,
        }
    }

    pub fn best_bid(&self) -> Option<(i64, u64)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }
    pub fn best_ask(&self) -> Option<(i64, u64)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }
    /// Дисбаланс по top-n уровням: (sum_bid - sum_ask) / (sum_bid + sum_ask)
    pub fn obi(&self, n: usize) -> f64 {
        let b: f64 = self.bids.iter().rev().take(n).map(|(_, q)| *q as f64).sum();
        let a: f64 = self.asks.iter().take(n).map(|(_, q)| *q as f64).sum();
        if a + b == 0.0 { 0.0 } else { (b - a) / (b + a) }
    }
}
