use crate::codec::{read_header, read_raw, Dec, Header, Rec};
use anyhow::{Context, Result};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub struct RecReader {
    pub header: Header,
    rd: BufReader<zstd::Decoder<'static, BufReader<File>>>,
    dec: Dec,
    /// Если файл оборван (краш/.part), сюда пишется причина; чтение при этом заканчивается без ошибки.
    pub truncated: Option<String>,
}

impl RecReader {
    pub fn open(path: &Path) -> Result<Self> {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let z = zstd::Decoder::new(f)?;
        let mut rd = BufReader::with_capacity(1 << 16, z);
        let header = read_header(&mut rd).context("header")?;
        Ok(Self { header, rd, dec: Dec::new(), truncated: None })
    }
}

impl Iterator for RecReader {
    type Item = Rec;
    fn next(&mut self) -> Option<Rec> {
        loop {
            match read_raw(&mut self.rd) {
                Ok(None) => return None,
                Ok(Some((t, p))) => match self.dec.decode(t, &p) {
                    Ok(Some(r)) => return Some(r),
                    Ok(None) => continue,
                    Err(e) => {
                        self.truncated = Some(format!("decode error: {e}"));
                        return None;
                    }
                },
                Err(e) => {
                    self.truncated = Some(format!("{e}"));
                    return None;
                }
            }
        }
    }
}
