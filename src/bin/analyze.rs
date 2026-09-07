use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    input: String,
    output: String,
    ticker: String,
    match_id: String,
    #[serde(default = "default_horizon_ms")]
    horizon_ms: u64,
    #[serde(default = "default_min_touch_move_fp")]
    min_touch_move_fp: i64,
    #[serde(default = "default_max_book_age_ms")]
    max_book_age_ms: u64,
    #[serde(default = "default_event_kinds")]
    event_kinds: Vec<String>,
    #[serde(default)]
    cost_buffer_fp: i64,
    #[serde(default)]
    min_edge_fp: i64,
}

#[derive(Debug, Deserialize)]
struct WireRecord {
    run_id: u128,
    source: String,
    kind: String,
    recv_mono_ns: u64,
    ticker: Option<String>,
    book: Option<Touch>,
    payload: Value,
    seq: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct Touch {
    yes_bid_fp: Option<i64>,
    yes_bid_size_fp: Option<i64>,
    yes_ask_fp: Option<i64>,
    yes_ask_size_fp: Option<i64>,
}

#[derive(Debug)]
struct Pending {
    run_id: u128,
    event_seq: Option<u64>,
    event_ns: u64,
    deadline_ns: u64,
    baseline: Touch,
    fair_yes_fp: Option<i64>,
    signal: Option<&'static str>,
    edge_fp: Option<i64>,
    book_move_ns: Option<u64>,
    trade_ns: Option<u64>,
    after: Option<Touch>,
}

#[derive(Clone, Debug)]
struct TimedTouch {
    touch: Touch,
    recv_mono_ns: u64,
}

#[derive(Default, Serialize)]
struct Summary {
    events: u64,
    with_baseline: u64,
    valid_windows: u64,
    invalid_windows: u64,
    censored_windows: u64,
    opportunities: u64,
    censored_opportunities: u64,
    observed_book_moves: u64,
    observed_trades: u64,
    complete_book_responses: u64,
    complete_trade_responses: u64,
    book_response_rate: Option<f64>,
    trade_response_rate: Option<f64>,
    observed_book_move_latency_ms: Option<LatencyStats>,
    observed_trade_latency_ms: Option<LatencyStats>,
    #[serde(skip)]
    book_samples_ns: Vec<u64>,
    #[serde(skip)]
    trade_samples_ns: Vec<u64>,
}

#[derive(Serialize)]
struct LatencyStats {
    count: usize,
    min: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
}

struct Analyzer<W> {
    config: Config,
    output: W,
    current_run: Option<u128>,
    current_touch: Option<TimedTouch>,
    last_kalshi_ns: Option<u64>,
    last_tennis_seq: Option<u64>,
    market_open: Option<bool>,
    pending: Option<Pending>,
    summary: Summary,
}

impl Config {
    fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read analysis config at {}", path.display()))?;
        let config: Self =
            serde_json::from_slice(&bytes).context("invalid analysis configuration JSON")?;
        if config.ticker.is_empty() || config.match_id.is_empty() {
            bail!("ticker and match_id must not be empty");
        }
        if config.horizon_ms == 0
            || config.min_touch_move_fp <= 0
            || config.max_book_age_ms == 0
            || config.event_kinds.is_empty()
        {
            bail!("horizon_ms, min_touch_move_fp, max_book_age_ms, and event_kinds must be set");
        }
        if config.cost_buffer_fp < 0 || config.min_edge_fp < 0 {
            bail!("cost_buffer_fp and min_edge_fp must not be negative");
        }
        Ok(config)
    }
}

impl<W: Write> Analyzer<W> {
    fn new(config: Config, mut output: W) -> Result<Self> {
        writeln!(
            output,
            "run_id,event_seq,event_mono_ns,status,baseline_bid_fp,baseline_ask_fp,fair_yes_fp,signal,edge_fp,book_move_ns,trade_ns,after_bid_fp,after_ask_fp"
        )?;
        Ok(Self {
            config,
            output,
            current_run: None,
            current_touch: None,
            last_kalshi_ns: None,
            last_tennis_seq: None,
            market_open: None,
            pending: None,
            summary: Summary::default(),
        })
    }

    fn process(&mut self, record: WireRecord) -> Result<()> {
        if self.current_run != Some(record.run_id) {
            self.finish_pending("run_ended", false)?;
            self.current_run = Some(record.run_id);
            self.current_touch = None;
            self.last_kalshi_ns = None;
            self.last_tennis_seq = None;
            self.market_open = None;
        }

        if record.source == "kalshi"
            && matches!(
                record.kind.as_str(),
                "connection_open" | "connection_error" | "sequence_gap"
            )
        {
            self.finish_pending("feed_reset", false)?;
            self.current_touch = None;
            self.last_kalshi_ns = Some(record.recv_mono_ns);
            self.market_open = None;
            return Ok(());
        }

        if record.source == "kalshi" {
            if record.kind == "market_status_snapshot"
                && record.ticker.as_deref() == Some(self.config.ticker.as_str())
            {
                let status = record
                    .payload
                    .get("market")
                    .and_then(|market| market.get("status"))
                    .and_then(Value::as_str)
                    .context("market status snapshot is missing market.status")?;
                // REST does not expose the separate per-market pause flag.
                // An active status must be confirmed by a lifecycle event.
                self.market_open = if status == "active" {
                    None
                } else {
                    Some(false)
                };
                if status != "active" {
                    self.finish_pending("market_closed", false)?;
                    self.current_touch = None;
                }
            } else if record.kind == "market_lifecycle_v2"
                && record.ticker.as_deref() == Some(self.config.ticker.as_str())
            {
                let message = record.payload.get("msg").unwrap_or(&record.payload);
                let event_type = message.get("event_type").and_then(Value::as_str);
                let next_open = match event_type {
                    Some("activated") => Some(true),
                    Some("deactivated") => Some(
                        !message
                            .get("is_deactivated")
                            .and_then(Value::as_bool)
                            .unwrap_or(true),
                    ),
                    Some("determined" | "settled") => Some(false),
                    _ => self.market_open,
                };
                if self.market_open != next_open {
                    self.current_touch = None;
                }
                if self.market_open == Some(true) && next_open == Some(false) {
                    self.finish_pending("market_closed", false)?;
                }
                self.market_open = next_open;
            }
            self.expire(record.recv_mono_ns)?;
            self.last_kalshi_ns = Some(record.recv_mono_ns);
        }

        if record.source == "tennis"
            && self
                .config
                .event_kinds
                .iter()
                .any(|kind| kind == &record.kind)
            && record.payload.get("match_id").and_then(Value::as_str)
                == Some(self.config.match_id.as_str())
        {
            let seq = record.seq.context("matching tennis event is missing seq")?;
            if let Some(previous) = self.last_tennis_seq
                && seq != previous + 1
            {
                self.finish_pending("tennis_gap", false)?;
                bail!(
                    "tennis sequence gap: expected {}, received {seq}",
                    previous + 1
                );
            }
            self.last_tennis_seq = Some(seq);
            self.start_event(&record)?;
            return Ok(());
        }

        if record.source != "kalshi"
            || record.ticker.as_deref() != Some(self.config.ticker.as_str())
        {
            return Ok(());
        }

        if record.kind == "trade" {
            if let Some(pending) = &mut self.pending
                && pending.trade_ns.is_none()
                && trade_matches_signal(&record.payload, pending.signal)
            {
                pending.trade_ns = Some(record.recv_mono_ns.saturating_sub(pending.event_ns));
            }
            return Ok(());
        }

        if let Some(touch) = record.book {
            if self.market_open != Some(true) {
                return Ok(());
            }
            if let Some(pending) = &mut self.pending
                && pending.book_move_ns.is_none()
                && directional_touch_changed(
                    pending.signal,
                    &pending.baseline,
                    &touch,
                    self.config.min_touch_move_fp,
                )
            {
                pending.book_move_ns = Some(record.recv_mono_ns.saturating_sub(pending.event_ns));
                pending.after = Some(touch.clone());
            }
            self.current_touch = Some(TimedTouch {
                touch,
                recv_mono_ns: record.recv_mono_ns,
            });
        }
        Ok(())
    }

    fn start_event(&mut self, record: &WireRecord) -> Result<()> {
        let feed_fresh = self.feed_fresh(record.recv_mono_ns);
        self.finish_pending(
            if feed_fresh {
                "next_event_censored"
            } else {
                "stale_feed"
            },
            false,
        )?;
        self.summary.events += 1;
        let Some(baseline) = self.current_touch.clone() else {
            return self.write_row(
                record.run_id,
                record.seq,
                record.recv_mono_ns,
                "no_baseline",
                &Touch::default(),
                None,
                None,
                None,
                None,
                None,
                None,
            );
        };
        let max_age_ns = self.config.max_book_age_ms.saturating_mul(1_000_000);
        if self.market_open != Some(true)
            || record.recv_mono_ns.saturating_sub(baseline.recv_mono_ns) > max_age_ns
            || !tradable(&baseline.touch)
        {
            return self.write_row(
                record.run_id,
                record.seq,
                record.recv_mono_ns,
                "closed_stale_or_untradable_book",
                &baseline.touch,
                None,
                None,
                None,
                None,
                None,
                None,
            );
        }
        self.summary.with_baseline += 1;
        let fair_yes_fp = record.payload.get("fair_yes_fp").and_then(Value::as_i64);
        if let Some(fair) = fair_yes_fp
            && !(0..=10_000).contains(&fair)
        {
            bail!("fair_yes_fp must be between 0 and 10000");
        }
        let (signal, edge_fp) = signal(
            fair_yes_fp,
            &baseline.touch,
            self.config.cost_buffer_fp,
            self.config.min_edge_fp,
        );
        self.pending = Some(Pending {
            run_id: record.run_id,
            event_seq: record.seq,
            event_ns: record.recv_mono_ns,
            deadline_ns: record
                .recv_mono_ns
                .saturating_add(self.config.horizon_ms.saturating_mul(1_000_000)),
            baseline: baseline.touch,
            fair_yes_fp,
            signal,
            edge_fp,
            book_move_ns: None,
            trade_ns: None,
            after: None,
        });
        Ok(())
    }

    fn expire(&mut self, now_ns: u64) -> Result<()> {
        if let Some(pending) = self.pending.as_ref()
            && now_ns >= pending.deadline_ns
        {
            let timely = now_ns - pending.deadline_ns
                <= self.config.max_book_age_ms.saturating_mul(1_000_000);
            self.finish_pending(
                if timely {
                    "horizon"
                } else {
                    "stale_confirmation"
                },
                timely,
            )?;
        }
        Ok(())
    }

    fn finish_pending(&mut self, status: &str, valid: bool) -> Result<()> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        if valid {
            self.summary.valid_windows += 1;
            if pending.signal.is_some() {
                self.summary.opportunities += 1;
            }
            if let Some(latency) = pending.book_move_ns {
                self.summary.observed_book_moves += 1;
                self.summary.complete_book_responses += 1;
                self.summary.book_samples_ns.push(latency);
            }
            if let Some(latency) = pending.trade_ns {
                self.summary.observed_trades += 1;
                self.summary.complete_trade_responses += 1;
                self.summary.trade_samples_ns.push(latency);
            }
        } else if status.ends_with("_censored") {
            self.summary.censored_windows += 1;
            if pending.signal.is_some() {
                self.summary.censored_opportunities += 1;
                if let Some(latency) = pending.book_move_ns {
                    self.summary.observed_book_moves += 1;
                    self.summary.book_samples_ns.push(latency);
                }
                if let Some(latency) = pending.trade_ns {
                    self.summary.observed_trades += 1;
                    self.summary.trade_samples_ns.push(latency);
                }
            }
        } else {
            self.summary.invalid_windows += 1;
        }
        let after = pending.after.unwrap_or_default();
        self.write_row(
            pending.run_id,
            pending.event_seq,
            pending.event_ns,
            status,
            &pending.baseline,
            pending.fair_yes_fp,
            pending.signal,
            pending.edge_fp,
            pending.book_move_ns,
            pending.trade_ns,
            Some(&after),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_row(
        &mut self,
        run_id: u128,
        event_seq: Option<u64>,
        event_ns: u64,
        status: &str,
        baseline: &Touch,
        fair_yes_fp: Option<i64>,
        signal: Option<&str>,
        edge_fp: Option<i64>,
        book_move_ns: Option<u64>,
        trade_ns: Option<u64>,
        after: Option<&Touch>,
    ) -> Result<()> {
        let after = after.cloned().unwrap_or_default();
        writeln!(
            self.output,
            "{run_id},{},{event_ns},{status},{},{},{},{},{},{},{},{},{}",
            cell(event_seq),
            cell(baseline.yes_bid_fp),
            cell(baseline.yes_ask_fp),
            cell(fair_yes_fp),
            signal.unwrap_or(""),
            cell(edge_fp),
            cell(book_move_ns),
            cell(trade_ns),
            cell(after.yes_bid_fp),
            cell(after.yes_ask_fp),
        )
        .context("failed to write analysis row")?;
        if matches!(status, "no_baseline" | "closed_stale_or_untradable_book") {
            self.summary.invalid_windows += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Summary> {
        self.finish_pending("truncated", false)?;
        self.output
            .flush()
            .context("failed to flush analysis CSV")?;
        self.summary.observed_book_move_latency_ms = latency_stats(&self.summary.book_samples_ns);
        self.summary.observed_trade_latency_ms = latency_stats(&self.summary.trade_samples_ns);
        self.summary.book_response_rate = response_rate(
            self.summary.complete_book_responses,
            self.summary.opportunities,
        );
        self.summary.trade_response_rate = response_rate(
            self.summary.complete_trade_responses,
            self.summary.opportunities,
        );
        Ok(self.summary)
    }

    fn feed_fresh(&self, now_ns: u64) -> bool {
        self.last_kalshi_ns.is_some_and(|last| {
            now_ns >= last && now_ns - last <= self.config.max_book_age_ms.saturating_mul(1_000_000)
        })
    }
}

fn signal(
    fair_yes_fp: Option<i64>,
    touch: &Touch,
    cost_buffer_fp: i64,
    min_edge_fp: i64,
) -> (Option<&'static str>, Option<i64>) {
    let Some(fair) = fair_yes_fp else {
        return (None, None);
    };
    let yes_edge = touch.yes_ask_fp.map(|ask| fair - ask - cost_buffer_fp);
    let no_edge = touch.yes_bid_fp.map(|bid| bid - fair - cost_buffer_fp);
    let best = match (yes_edge, no_edge) {
        (Some(yes), Some(no)) if yes >= no => Some(("buy_yes", yes)),
        (Some(_), Some(no)) => Some(("buy_no", no)),
        (Some(yes), None) => Some(("buy_yes", yes)),
        (None, Some(no)) => Some(("buy_no", no)),
        (None, None) => None,
    };
    match best {
        Some((side, edge)) if edge >= min_edge_fp => (Some(side), Some(edge)),
        Some((_, edge)) => (None, Some(edge)),
        None => (None, None),
    }
}

fn directional_touch_changed(
    signal: Option<&str>,
    before: &Touch,
    after: &Touch,
    minimum: i64,
) -> bool {
    match signal {
        Some("buy_yes") => moved_up(before.yes_ask_fp, after.yes_ask_fp, minimum),
        Some("buy_no") => moved_down(before.yes_bid_fp, after.yes_bid_fp, minimum),
        _ => false,
    }
}

fn moved_up(before: Option<i64>, after: Option<i64>, minimum: i64) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => after - before >= minimum,
        (Some(_), None) => true,
        _ => false,
    }
}

fn moved_down(before: Option<i64>, after: Option<i64>, minimum: i64) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => before - after >= minimum,
        (Some(_), None) => true,
        _ => false,
    }
}

fn trade_matches_signal(payload: &Value, signal: Option<&str>) -> bool {
    let message = payload.get("msg").unwrap_or(payload);
    if message
        .get("is_block_trade")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return false;
    }
    let side = message
        .get("taker_outcome_side")
        .or_else(|| message.get("taker_side"))
        .and_then(Value::as_str);
    matches!(
        (signal, side),
        (Some("buy_yes"), Some("yes")) | (Some("buy_no"), Some("no"))
    )
}

fn tradable(touch: &Touch) -> bool {
    matches!(
        (
            touch.yes_bid_fp,
            touch.yes_ask_fp,
            touch.yes_bid_size_fp,
            touch.yes_ask_size_fp,
        ),
        (Some(bid), Some(ask), Some(bid_size), Some(ask_size))
            if bid > 0 && ask < 10_000 && bid < ask && bid_size > 0 && ask_size > 0
    )
}

fn response_rate(responses: u64, opportunities: u64) -> Option<f64> {
    if opportunities == 0 {
        None
    } else {
        Some(responses as f64 / opportunities as f64)
    }
}

fn latency_stats(samples: &[u64]) -> Option<LatencyStats> {
    if samples.is_empty() {
        return None;
    }
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    Some(LatencyStats {
        count: samples.len(),
        min: ns_to_ms(samples[0]),
        p50: ns_to_ms(percentile(&samples, 50)),
        p90: ns_to_ms(percentile(&samples, 90)),
        p99: ns_to_ms(percentile(&samples, 99)),
        max: ns_to_ms(*samples.last().unwrap_or(&samples[0])),
    })
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let rank = (samples.len() * percentile).div_ceil(100);
    let index = rank.saturating_sub(1).min(samples.len() - 1);
    samples[index]
}

fn ns_to_ms(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

fn cell<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn secure_output(path: &Path) -> Result<BufWriter<File>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(BufWriter::new(file))
}

fn run(config: Config) -> Result<Summary> {
    let input = File::open(&config.input)
        .with_context(|| format!("failed to open capture {}", config.input))?;
    let output = secure_output(Path::new(&config.output))?;
    let mut analyzer = Analyzer::new(config, output)?;
    for (index, line) in BufReader::new(input).lines().enumerate() {
        let line = line.with_context(|| format!("failed to read capture line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let record: WireRecord = serde_json::from_str(&line)
            .with_context(|| format!("invalid capture JSON on line {}", index + 1))?;
        analyzer.process(record)?;
    }
    analyzer.finish()
}

fn main() -> Result<()> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("analysis.json"));
    if env::args_os().nth(2).is_some() {
        bail!("usage: analyze [analysis.json]");
    }
    let summary = run(Config::load(&path)?)?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &summary)?;
    println!();
    Ok(())
}

const fn default_horizon_ms() -> u64 {
    5_000
}

const fn default_min_touch_move_fp() -> i64 {
    1
}

const fn default_max_book_age_ms() -> u64 {
    2_000
}

fn default_event_kinds() -> Vec<String> {
    vec!["point".to_owned()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> Config {
        Config {
            input: String::new(),
            output: String::new(),
            ticker: "TENNIS".into(),
            match_id: "match".into(),
            horizon_ms: 1_000,
            min_touch_move_fp: 1,
            max_book_age_ms: 100,
            event_kinds: vec!["point".into()],
            cost_buffer_fp: 100,
            min_edge_fp: 100,
        }
    }

    fn record(
        source: &str,
        kind: &str,
        ns: u64,
        ticker: Option<&str>,
        book: Option<Touch>,
        payload: Value,
    ) -> WireRecord {
        WireRecord {
            run_id: 1,
            source: source.into(),
            kind: kind.into(),
            recv_mono_ns: ns,
            ticker: ticker.map(str::to_owned),
            book,
            payload,
            seq: Some(1),
        }
    }

    fn touch(bid: i64, ask: i64) -> Touch {
        Touch {
            yes_bid_fp: Some(bid),
            yes_bid_size_fp: Some(100),
            yes_ask_fp: Some(ask),
            yes_ask_size_fp: Some(100),
        }
    }

    fn activate<W: Write>(analyzer: &mut Analyzer<W>) {
        analyzer
            .process(record(
                "kalshi",
                "market_lifecycle_v2",
                500_000,
                Some("TENNIS"),
                None,
                json!({"msg": {
                    "market_ticker": "TENNIS",
                    "event_type": "activated"
                }}),
            ))
            .unwrap();
    }

    #[test]
    fn joins_point_to_first_book_move_and_trade() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1_000_000,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    2_000_000,
                    None,
                    None,
                    json!({"match_id": "match", "fair_yes_fp": 5600}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_delta",
                    4_000_000,
                    Some("TENNIS"),
                    Some(touch(5300, 5500)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "trade",
                    5_000_000,
                    Some("TENNIS"),
                    None,
                    json!({"msg": {
                        "taker_outcome_side": "yes",
                        "is_block_trade": false
                    }}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "ticker",
                    1_002_000_000,
                    Some("TENNIS"),
                    None,
                    json!({}),
                ))
                .unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.valid_windows, 1);
        assert_eq!(summary.observed_book_moves, 1);
        assert_eq!(summary.observed_trades, 1);
        assert_eq!(summary.opportunities, 1);
        let csv = String::from_utf8(output).unwrap();
        assert!(csv.contains("horizon,5000,5200,5600,buy_yes,300,2000000,3000000"));
    }

    #[test]
    fn feed_gap_invalidates_window() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    2,
                    None,
                    None,
                    json!({"match_id": "match"}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "sequence_gap",
                    3,
                    Some("TENNIS"),
                    None,
                    json!({}),
                ))
                .unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.invalid_windows, 1);
        assert_eq!(summary.valid_windows, 0);
        assert!(String::from_utf8(output).unwrap().contains("feed_reset"));
    }

    #[test]
    fn no_side_passes_cost_and_edge_threshold() {
        let (side, edge) = signal(Some(5300), &touch(5000, 5200), 100, 200);
        assert_eq!(side, None);
        assert_eq!(edge, Some(0));
    }

    #[test]
    fn percentile_uses_observed_sample() {
        let stats = latency_stats(&[1_000_000, 2_000_000, 9_000_000]).unwrap();
        assert_eq!(stats.p50, 2.0);
        assert_eq!(stats.p90, 9.0);
        assert_eq!(stats.p99, 9.0);
        assert_eq!(stats.max, 9.0);
    }

    #[test]
    fn opposite_move_and_block_trade_do_not_count() {
        assert!(!directional_touch_changed(
            Some("buy_yes"),
            &touch(5000, 5200),
            &touch(4900, 5100),
            1,
        ));
        assert!(!trade_matches_signal(
            &json!({"msg": {
                "taker_outcome_side": "yes",
                "is_block_trade": true
            }}),
            Some("buy_yes")
        ));
        assert!(!trade_matches_signal(
            &json!({"msg": {
                "taker_outcome_side": "no",
                "is_block_trade": false
            }}),
            Some("buy_yes")
        ));
    }

    #[test]
    fn stale_book_is_not_a_baseline() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    200_000_002,
                    None,
                    None,
                    json!({"match_id": "match", "fair_yes_fp": 5600}),
                ))
                .unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.with_baseline, 0);
        assert_eq!(summary.invalid_windows, 1);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("stale_or_untradable_book")
        );
    }

    #[test]
    fn tennis_sequence_gap_fails_analysis() {
        let mut output = Vec::new();
        let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
        analyzer
            .process(record(
                "tennis",
                "point",
                1,
                None,
                None,
                json!({"match_id": "match"}),
            ))
            .unwrap();
        let mut skipped = record(
            "tennis",
            "point",
            2,
            None,
            None,
            json!({"match_id": "match"}),
        );
        skipped.seq = Some(3);
        assert!(analyzer.process(skipped).is_err());
    }

    #[test]
    fn late_horizon_confirmation_invalidates_window() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1_000_000,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    2_000_000,
                    None,
                    None,
                    json!({"match_id": "match", "fair_yes_fp": 5600}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "ticker",
                    1_202_000_001,
                    Some("TENNIS"),
                    None,
                    json!({}),
                ))
                .unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.valid_windows, 0);
        assert_eq!(summary.invalid_windows, 1);
        assert_eq!(summary.opportunities, 0);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("stale_confirmation")
        );
    }

    #[test]
    fn overlapping_point_is_censored_not_a_failure() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1_000_000,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    2_000_000,
                    None,
                    None,
                    json!({"match_id": "match", "fair_yes_fp": 5600}),
                ))
                .unwrap();
            let mut second = record(
                "tennis",
                "point",
                3_000_000,
                None,
                None,
                json!({"match_id": "match", "fair_yes_fp": 5600}),
            );
            second.seq = Some(2);
            analyzer.process(second).unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.censored_windows, 1);
        assert_eq!(summary.opportunities, 0);
        assert_eq!(summary.book_response_rate, None);
    }

    #[test]
    fn response_before_overlap_is_retained() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1_000_000,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    2_000_000,
                    None,
                    None,
                    json!({"match_id": "match", "fair_yes_fp": 5600}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_delta",
                    3_000_000,
                    Some("TENNIS"),
                    Some(touch(5300, 5500)),
                    json!({}),
                ))
                .unwrap();
            let mut second = record(
                "tennis",
                "point",
                4_000_000,
                None,
                None,
                json!({"match_id": "match", "fair_yes_fp": 5600}),
            );
            second.seq = Some(2);
            analyzer.process(second).unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.censored_opportunities, 1);
        assert_eq!(summary.observed_book_moves, 1);
        assert_eq!(summary.opportunities, 0);
        assert_eq!(summary.observed_book_move_latency_ms.unwrap().p50, 1.0);
    }

    #[test]
    fn paused_book_cannot_be_reused_after_unpause() {
        let mut output = Vec::new();
        let summary = {
            let mut analyzer = Analyzer::new(config(), &mut output).unwrap();
            activate(&mut analyzer);
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_snapshot",
                    1_000_000,
                    Some("TENNIS"),
                    Some(touch(5000, 5200)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "market_lifecycle_v2",
                    2_000_000,
                    Some("TENNIS"),
                    None,
                    json!({"msg": {
                        "event_type": "deactivated",
                        "is_deactivated": true
                    }}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "orderbook_delta",
                    3_000_000,
                    Some("TENNIS"),
                    Some(touch(5100, 5300)),
                    json!({}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "kalshi",
                    "market_lifecycle_v2",
                    4_000_000,
                    Some("TENNIS"),
                    None,
                    json!({"msg": {
                        "event_type": "deactivated",
                        "is_deactivated": false
                    }}),
                ))
                .unwrap();
            analyzer
                .process(record(
                    "tennis",
                    "point",
                    5_000_000,
                    None,
                    None,
                    json!({"match_id": "match", "fair_yes_fp": 5600}),
                ))
                .unwrap();
            analyzer.finish().unwrap()
        };
        assert_eq!(summary.with_baseline, 0);
        assert_eq!(summary.invalid_windows, 1);
    }
}
