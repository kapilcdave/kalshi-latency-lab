use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::{
    fs::File,
    io::{self, AsyncBufRead, AsyncBufReadExt, BufReader},
    sync::{mpsc, watch},
    time::{Duration, sleep},
};

use crate::record::{Clock, Record};

pub async fn run(
    source: &str,
    replay_delay_ms: u64,
    clock: Clock,
    records: mpsc::Sender<Record>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if source == "-" {
        read_lines(
            BufReader::new(io::stdin()),
            0,
            true,
            clock,
            records,
            shutdown,
        )
        .await
    } else {
        let file = File::open(Path::new(source))
            .await
            .with_context(|| format!("failed to open tennis event source {source}"))?;
        read_lines(
            BufReader::new(file),
            replay_delay_ms,
            false,
            clock,
            records,
            shutdown,
        )
        .await
    }
}

async fn read_lines<R: AsyncBufRead + Unpin>(
    mut reader: R,
    replay_delay_ms: u64,
    eof_is_error: bool,
    clock: Clock,
    records: mpsc::Sender<Record>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let read = tokio::select! {
            read = reader.read_line(&mut line) => read.context("failed to read tennis event")?,
            _ = shutdown.changed() => return Ok(()),
        };
        if read == 0 {
            if eof_is_error {
                bail!("tennis event source ended");
            }
            return Ok(());
        }
        if replay_delay_ms > 0 {
            tokio::select! {
                _ = sleep(Duration::from_millis(replay_delay_ms)) => {},
                _ = shutdown.changed() => return Ok(()),
            }
        }
        let (recv_unix_ns, recv_mono_ns) = clock.stamp()?;
        let payload: Value =
            serde_json::from_str(line.trim()).context("invalid tennis event JSON")?;
        let kind = payload
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or("tennis_event")
            .to_owned();
        let source_ts_ns = payload.get("source_ts_ns").and_then(Value::as_u64);
        records
            .try_send(Record {
                run_id: clock.run_id(),
                source: "tennis",
                kind,
                recv_unix_ns,
                recv_mono_ns,
                source_ts_ns,
                venue_ts_ms: None,
                ticker: None,
                sid: None,
                seq: payload.get("seq").and_then(Value::as_u64),
                book: None,
                payload,
            })
            .context("record queue is full or the writer stopped")?;
    }
}
