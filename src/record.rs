use std::{path::Path, time::Instant};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    time::{Duration, interval},
};

use crate::book::Touch;

#[derive(Clone)]
pub struct Clock {
    started: Instant,
    run_id: u128,
}

#[derive(Debug, Serialize)]
pub struct Record {
    pub run_id: u128,
    pub source: &'static str,
    pub kind: String,
    pub recv_unix_ns: u128,
    pub recv_mono_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ts_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sid: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub book: Option<Touch>,
    pub payload: Value,
}

impl Clock {
    pub fn new() -> Result<Self> {
        Ok(Self {
            started: Instant::now(),
            run_id: unix_ns()?,
        })
    }

    pub fn stamp(&self) -> Result<(u128, u64)> {
        let monotonic = self
            .started
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        Ok((unix_ns()?, monotonic))
    }

    pub const fn run_id(&self) -> u128 {
        self.run_id
    }
}

pub async fn run_writer(path: &Path, mut records: mpsc::Receiver<Record>) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options
        .open(path)
        .await
        .with_context(|| format!("failed to open output file {}", path.display()))?;
    #[cfg(unix)]
    fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .await
        .with_context(|| format!("failed to secure output file {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);
    let mut flush = interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            record = records.recv() => match record {
                Some(record) => {
                    let mut line = serde_json::to_vec(&record).context("failed to encode record")?;
                    line.push(b'\n');
                    writer.write_all(&line).await.context("failed to write record")?;
                }
                None => break,
            },
            _ = flush.tick() => writer.flush().await.context("failed to flush records")?,
        }
    }
    writer
        .flush()
        .await
        .context("failed to flush final records")
}

fn unix_ns() -> Result<u128> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos())
}
