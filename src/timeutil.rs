use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
}

/// (год, месяц, день) по числу дней от 1970-01-01 (алгоритм Хиннанта).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// UTC-час как целое (часов от эпохи) и его календарные части.
pub fn hour_of(us: u64) -> i64 {
    (us / 3_600_000_000) as i64
}
pub fn hour_parts(hour: i64) -> (i64, u32, u32, u32) {
    let (y, m, d) = civil_from_days(hour.div_euclid(24));
    (y, m, d, hour.rem_euclid(24) as u32)
}
pub fn fmt_ts(us: u64) -> String {
    let secs = (us / 1_000_000) as i64;
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z", s / 3600, s % 3600 / 60, s % 60, us / 1000 % 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
        assert_eq!(hour_parts(hour_of(1_700_000_000_000_000)), (2023, 11, 14, 22));
        assert_eq!(fmt_ts(1_700_000_000_123_000), "2023-11-14T22:13:20.123Z");
    }
}
