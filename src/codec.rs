//! Компактный бинарный формат BKLOG v1.
//!
//! Файл = zstd-поток: [Header][Record]*.
//! Record = [type:u8][len:uvarint][payload].
//! Все числа в payload — varint (LEB128), знаковые — zigzag, времена и id — дельты.
//! Состояние кодера/декодера (последняя цена и т.д.) сбрасывается в начале каждого
//! файла, поэтому каждый часовой файл читается независимо.

use anyhow::{bail, Result};
use std::io::{Read, Write};

pub const MAGIC: &[u8; 4] = b"BKLG";
pub const VERSION: u8 = 1;

pub const T_DEPTH: u8 = 1;
pub const T_TRADE: u8 = 2;
pub const T_SNAPSHOT: u8 = 3;
pub const T_MARKER: u8 = 4;

// Типы маркеров
pub const M_CONNECT: u8 = 1; // arg: 0 = public, 1 = market
pub const M_DISCONNECT: u8 = 2; // arg: 0 = public, 1 = market
pub const M_GAP: u8 = 3; // разрыв цепочки pu != prev u; arg = prev u
pub const M_DROPPED: u8 = 4; // переполнение очереди записи; arg = число потерянных сообщений
pub const M_SNAPSHOT_FAIL: u8 = 5;
pub const M_SHUTDOWN: u8 = 6;
pub const M_PARSE_ERR: u8 = 7; // arg = число ошибок разбора с прошлого маркера

pub type Levels = Vec<(i64, u64)>; // (цена в тиках 10^-price_dec, объем в 10^-qty_dec)

#[derive(Debug, Clone, PartialEq)]
pub enum Rec {
    Depth {
        recv_us: u64,
        event_ms: i64,
        tx_ms: i64,
        first_u: u64,
        last_u: u64,
        prev_u: u64,
        bids: Levels,
        asks: Levels,
    },
    Trade {
        recv_us: u64,
        trade_ms: i64,
        agg_id: u64,
        price: i64,
        qty: u64,
        buyer_maker: bool,
    },
    Snapshot {
        recv_us: u64,
        last_update_id: u64,
        bids: Levels,
        asks: Levels,
    },
    Marker {
        recv_us: u64,
        kind: u8,
        arg: u64,
    },
}

impl Rec {
    pub fn recv_us(&self) -> u64 {
        match self {
            Rec::Depth { recv_us, .. }
            | Rec::Trade { recv_us, .. }
            | Rec::Snapshot { recv_us, .. }
            | Rec::Marker { recv_us, .. } => *recv_us,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub symbol: String,
    pub price_dec: u8,
    pub qty_dec: u8,
    pub start_us: u64,
}

// ---------- varint ----------
pub fn put_uvar(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}
pub fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
pub fn unzz(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}
pub fn put_ivar(buf: &mut Vec<u8>, v: i64) {
    put_uvar(buf, zz(v))
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn uvar(&mut self) -> Result<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let Some(&x) = self.b.get(self.p) else { bail!("truncated varint") };
            self.p += 1;
            v |= ((x & 0x7f) as u64) << shift;
            if x & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift > 63 {
                bail!("varint overflow");
            }
        }
    }
    fn ivar(&mut self) -> Result<i64> {
        Ok(unzz(self.uvar()?))
    }
    fn u8(&mut self) -> Result<u8> {
        let Some(&x) = self.b.get(self.p) else { bail!("truncated") };
        self.p += 1;
        Ok(x)
    }
}

// ---------- header ----------
pub fn write_header<W: Write>(w: &mut W, h: &Header) -> std::io::Result<()> {
    w.write_all(MAGIC)?;
    w.write_all(&[VERSION, h.symbol.len() as u8])?;
    w.write_all(h.symbol.as_bytes())?;
    w.write_all(&[h.price_dec, h.qty_dec])?;
    w.write_all(&h.start_us.to_le_bytes())
}

pub fn read_header<R: Read>(r: &mut R) -> Result<Header> {
    let mut m = [0u8; 4];
    r.read_exact(&mut m)?;
    if &m != MAGIC {
        bail!("bad magic");
    }
    let mut v = [0u8; 2];
    r.read_exact(&mut v)?;
    if v[0] != VERSION {
        bail!("unsupported version {}", v[0]);
    }
    let mut s = vec![0u8; v[1] as usize];
    r.read_exact(&mut s)?;
    let mut d = [0u8; 2];
    r.read_exact(&mut d)?;
    let mut t = [0u8; 8];
    r.read_exact(&mut t)?;
    Ok(Header {
        symbol: String::from_utf8(s)?,
        price_dec: d[0],
        qty_dec: d[1],
        start_us: u64::from_le_bytes(t),
    })
}

// ---------- state ----------
#[derive(Default, Clone)]
struct State {
    prev_recv: u64,
    prev_u: u64,
    last_px: i64,
    last_trade_px: i64,
    prev_agg: u64,
}

#[derive(Default)]
pub struct Enc {
    st: State,
    tmp: Vec<u8>,
}

fn put_levels(st: &mut State, buf: &mut Vec<u8>, bids: &Levels, asks: &Levels) {
    put_uvar(buf, bids.len() as u64);
    put_uvar(buf, asks.len() as u64);
    for &(p, q) in bids.iter().chain(asks.iter()) {
        put_ivar(buf, p - st.last_px);
        put_uvar(buf, q);
        st.last_px = p;
    }
}
fn get_levels(st: &mut State, c: &mut Cur) -> Result<(Levels, Levels)> {
    let nb = c.uvar()? as usize;
    let na = c.uvar()? as usize;
    if nb > 1_000_000 || na > 1_000_000 {
        bail!("absurd level count");
    }
    let mut rd = |n: usize, st: &mut State| -> Result<Levels> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let p = st.last_px + c.ivar()?;
            let q = c.uvar()?;
            st.last_px = p;
            v.push((p, q));
        }
        Ok(v)
    };
    let b = rd(nb, st)?;
    let a = rd(na, st)?;
    Ok((b, a))
}

impl Enc {
    pub fn new() -> Self {
        Self::default()
    }

    /// Добавляет запись в `out` (тип, длина, payload).
    pub fn encode(&mut self, rec: &Rec, out: &mut Vec<u8>) {
        let mut b = std::mem::take(&mut self.tmp);
        b.clear();
        let st = &mut self.st;
        let ty;
        match rec {
            Rec::Depth { recv_us, event_ms, tx_ms, first_u, last_u, prev_u, bids, asks } => {
                ty = T_DEPTH;
                put_ivar(&mut b, *recv_us as i64 - st.prev_recv as i64);
                st.prev_recv = *recv_us;
                put_ivar(&mut b, *recv_us as i64 - *event_ms * 1000);
                put_ivar(&mut b, *event_ms - *tx_ms);
                put_ivar(&mut b, *first_u as i64 - (st.prev_u as i64 + 1));
                put_uvar(&mut b, last_u - first_u);
                put_ivar(&mut b, *prev_u as i64 - st.prev_u as i64);
                st.prev_u = *last_u;
                put_levels(st, &mut b, bids, asks);
            }
            Rec::Trade { recv_us, trade_ms, agg_id, price, qty, buyer_maker } => {
                ty = T_TRADE;
                put_ivar(&mut b, *recv_us as i64 - st.prev_recv as i64);
                st.prev_recv = *recv_us;
                put_ivar(&mut b, *recv_us as i64 - *trade_ms * 1000);
                put_ivar(&mut b, *agg_id as i64 - (st.prev_agg as i64 + 1));
                st.prev_agg = *agg_id;
                put_ivar(&mut b, *price - st.last_trade_px);
                st.last_trade_px = *price;
                put_uvar(&mut b, *qty);
                b.push(*buyer_maker as u8);
            }
            Rec::Snapshot { recv_us, last_update_id, bids, asks } => {
                ty = T_SNAPSHOT;
                put_ivar(&mut b, *recv_us as i64 - st.prev_recv as i64);
                st.prev_recv = *recv_us;
                put_uvar(&mut b, *last_update_id);
                put_levels(st, &mut b, bids, asks);
            }
            Rec::Marker { recv_us, kind, arg } => {
                ty = T_MARKER;
                put_ivar(&mut b, *recv_us as i64 - st.prev_recv as i64);
                st.prev_recv = *recv_us;
                b.push(*kind);
                put_uvar(&mut b, *arg);
            }
        }
        out.push(ty);
        put_uvar(out, b.len() as u64);
        out.extend_from_slice(&b);
        self.tmp = b;
    }
}

#[derive(Default)]
pub struct Dec {
    st: State,
}

impl Dec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn decode(&mut self, ty: u8, payload: &[u8]) -> Result<Option<Rec>> {
        let mut c = Cur { b: payload, p: 0 };
        let st = &mut self.st;
        let rec = match ty {
            T_DEPTH => {
                let recv_us = (st.prev_recv as i64 + c.ivar()?) as u64;
                st.prev_recv = recv_us;
                let event_ms = (recv_us as i64 - c.ivar()?) / 1000;
                // lat_us хранится точно как recv_us - event_ms*1000, поэтому деление точное
                let tx_ms = event_ms - c.ivar()?;
                let first_u = (st.prev_u as i64 + 1 + c.ivar()?) as u64;
                let last_u = first_u + c.uvar()?;
                let prev_u = (st.prev_u as i64 + c.ivar()?) as u64;
                st.prev_u = last_u;
                let (bids, asks) = get_levels(st, &mut c)?;
                Rec::Depth { recv_us, event_ms, tx_ms, first_u, last_u, prev_u, bids, asks }
            }
            T_TRADE => {
                let recv_us = (st.prev_recv as i64 + c.ivar()?) as u64;
                st.prev_recv = recv_us;
                let trade_ms = (recv_us as i64 - c.ivar()?) / 1000;
                let agg_id = (st.prev_agg as i64 + 1 + c.ivar()?) as u64;
                st.prev_agg = agg_id;
                let price = st.last_trade_px + c.ivar()?;
                st.last_trade_px = price;
                let qty = c.uvar()?;
                let buyer_maker = c.u8()? != 0;
                Rec::Trade { recv_us, trade_ms, agg_id, price, qty, buyer_maker }
            }
            T_SNAPSHOT => {
                let recv_us = (st.prev_recv as i64 + c.ivar()?) as u64;
                st.prev_recv = recv_us;
                let last_update_id = c.uvar()?;
                let (bids, asks) = get_levels(st, &mut c)?;
                Rec::Snapshot { recv_us, last_update_id, bids, asks }
            }
            T_MARKER => {
                let recv_us = (st.prev_recv as i64 + c.ivar()?) as u64;
                st.prev_recv = recv_us;
                let kind = c.u8()?;
                let arg = c.uvar()?;
                Rec::Marker { recv_us, kind, arg }
            }
            _ => return Ok(None), // неизвестный тип — пропускаем (длина известна)
        };
        Ok(Some(rec))
    }
}

/// Читает одну "сырую" запись (тип + payload). None = чистый конец потока.
pub fn read_raw<R: Read>(r: &mut R) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut t = [0u8; 1];
    if r.read(&mut t)? == 0 {
        return Ok(None);
    }
    let mut len = 0u64;
    let mut shift = 0;
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        len |= ((b[0] & 0x7f) as u64) << shift;
        if b[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 35 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad length"));
        }
    }
    if len > 64 << 20 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "record too large"));
    }
    let mut p = vec![0u8; len as usize];
    r.read_exact(&mut p)?;
    Ok(Some((t[0], p)))
}

/// Разбор десятичной строки без float: "43210.50" при dec=2 -> 4321050.
/// None, если в строке больше значащих знаков после точки, чем dec (несовпадение точности).
pub fn parse_scaled(s: &str, dec: u8) -> Option<i64> {
    let b = s.as_bytes();
    let (neg, b) = match b.first() {
        Some(b'-') => (true, &b[1..]),
        _ => (false, b),
    };
    if b.is_empty() {
        return None;
    }
    let mut i = 0;
    let mut int: i64 = 0;
    while i < b.len() && b[i] != b'.' {
        let d = b[i].checked_sub(b'0').filter(|d| *d < 10)? as i64;
        int = int.checked_mul(10)?.checked_add(d)?;
        i += 1;
    }
    let mut frac: i64 = 0;
    let mut nd = 0u8;
    if i < b.len() {
        i += 1;
        while i < b.len() {
            let d = b[i].checked_sub(b'0').filter(|d| *d < 10)? as i64;
            if nd < dec {
                frac = frac * 10 + d;
                nd += 1;
            } else if d != 0 {
                return None;
            }
            i += 1;
        }
    }
    while nd < dec {
        frac *= 10;
        nd += 1;
    }
    let v = int.checked_mul(10i64.checked_pow(dec as u32)?)?.checked_add(frac)?;
    Some(if neg { -v } else { v })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled() {
        assert_eq!(parse_scaled("43210.50", 2), Some(4321050));
        assert_eq!(parse_scaled("43210.5", 2), Some(4321050));
        assert_eq!(parse_scaled("43210", 2), Some(4321000));
        assert_eq!(parse_scaled("0.001", 3), Some(1));
        assert_eq!(parse_scaled("0.00100000", 3), Some(1));
        assert_eq!(parse_scaled("0.0015", 3), None);
        assert_eq!(parse_scaled("", 3), None);
        assert_eq!(parse_scaled("x1", 3), None);
        assert_eq!(parse_scaled("-1.5", 1), Some(-15));
    }

    #[test]
    fn zigzag() {
        for v in [0i64, 1, -1, 63, -64, 1 << 40, -(1 << 40), i64::MAX / 2, i64::MIN / 2] {
            assert_eq!(unzz(zz(v)), v);
        }
    }

    #[test]
    fn roundtrip() {
        let recs = vec![
            Rec::Marker { recv_us: 1_700_000_000_000_000, kind: M_CONNECT, arg: 0 },
            Rec::Snapshot { recv_us: 1_700_000_000_100_000, last_update_id: 999, bids: vec![(4321000, 5), (4320990, 7)], asks: vec![(4321010, 3)] },
            Rec::Depth { recv_us: 1_700_000_000_230_500, event_ms: 1_700_000_000_000, tx_ms: 1_699_999_999_998, first_u: 1000, last_u: 1004, prev_u: 999, bids: vec![(4321000, 0), (4320980, 11)], asks: vec![(4321010, 4)] },
            Rec::Depth { recv_us: 1_700_000_000_330_500, event_ms: 1_700_000_000_100, tx_ms: 1_700_000_000_099, first_u: 1005, last_u: 1005, prev_u: 1004, bids: vec![], asks: vec![(4321020, 1)] },
            Rec::Trade { recv_us: 1_700_000_000_331_000, trade_ms: 1_700_000_000_120, agg_id: 77, price: 4321010, qty: 2, buyer_maker: true },
            Rec::Trade { recv_us: 1_700_000_000_331_900, trade_ms: 1_700_000_000_121, agg_id: 78, price: 4321000, qty: 9, buyer_maker: false },
            Rec::Marker { recv_us: 1_700_000_001_000_000, kind: M_GAP, arg: 1005 },
            // скачок вперед по update id (после разрыва)
            Rec::Depth { recv_us: 1_700_000_001_100_000, event_ms: 1_700_000_000_900, tx_ms: 1_700_000_000_900, first_u: 1500, last_u: 1502, prev_u: 1499, bids: vec![(4321500, 1)], asks: vec![] },
        ];
        let mut enc = Enc::new();
        let mut buf = Vec::new();
        for r in &recs {
            enc.encode(r, &mut buf);
        }
        let mut dec = Dec::new();
        let mut rd = &buf[..];
        let mut out = vec![];
        while let Some((t, p)) = read_raw(&mut rd).unwrap() {
            out.push(dec.decode(t, &p).unwrap().unwrap());
        }
        assert_eq!(out, recs);
    }
}
