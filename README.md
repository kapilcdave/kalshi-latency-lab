# kalshi-latency-lab

A small, read-only Rust recorder for measuring whether an external live-sports
signal reaches you before Kalshi's order book reprices. It records authenticated
Kalshi WebSocket data and newline-delimited tennis events into one timestamped
JSONL stream. Every row includes a run ID so appending multiple sessions remains
unambiguous.

This repository does **not** place orders and does not claim that latency creates
a profitable strategy. Measure the complete signal-to-book distribution before
building execution logic.

## What It Records

- Kalshi order-book snapshots and deltas
- Kalshi public trades
- Initial REST market status plus pause, activation, and settlement updates
- Exact fixed-point best bid, ask, and displayed sizes
- Source, venue, Unix, and process-monotonic timestamps
- Sequence gaps, followed by a clean reconnect and snapshot
- Arbitrary tennis events supplied as NDJSON

The recorder uses a single-thread Tokio runtime, a bounded channel, one buffered
writer, and fixed-point integers. It is intended to fit comfortably on a 1 GB
EC2 instance.

## Run

Install Rust, copy the example configuration, and provide Kalshi API credentials:

```bash
cp config.example.json config.json
export KALSHI_API_KEY_ID='your-key-id'
export KALSHI_PRIVATE_KEY_PATH="$HOME/.config/kalshi/private-key.pem"
cargo run --release -- config.json
```

Set the key file to owner-only access:

```bash
chmod 600 "$KALSHI_PRIVATE_KEY_PATH"
```

Use production only after replacing the sample ticker:

```json
{
  "environment": "production",
  "market_tickers": ["YOUR-TENNIS-MARKET-TICKER"],
  "output": "data/events.jsonl",
  "tennis_events": "-",
  "channel_capacity": 8192
}
```

`tennis_events: "-"` reads one JSON object per line from standard input. A file
path replays a captured feed; `replay_delay_ms` optionally spaces replayed lines.
The adapter deliberately accepts arbitrary JSON so a vendor-specific decoder can
be added without changing the recorder format.

## Analyze Reactions

The `analyze` binary streams a capture without loading it into memory. It joins
each configured tennis match event to the first Kalshi touch move and trade,
then writes event-level CSV and prints aggregate latency quantiles:

```bash
cargo run --release --bin analyze -- analysis.example.json
```

The included synthetic capture produces `data/reactions.csv`. For a real feed,
add `fair_yes_fp` to an event only when it comes from a frozen, independently
validated model. It uses four-decimal probability units: `5600` means `0.5600`.

`cost_buffer_fp` is a user-supplied all-in allowance for fees, slippage, and
model uncertainty. The analyzer reports a candidate only when the best
marketable YES or NO edge, after that buffer, is at least `min_edge_fp`. It does
not hardcode Kalshi's fee formula.

`event_kinds` defaults to `point`, and `max_book_age_ms` bounds baseline
staleness. The input adapter must emit a contiguous sequence over the normalized
point events selected by `event_kinds`; vendor-wide sequence numbers should be
translated before recording.

For pause safety, REST `active` status alone is not considered tradable because
it lacks Kalshi's separate pause flag. Start the recorder before the market's
activation; an activation or unpause lifecycle event is required, and only a
fresh post-activation book can become a baseline.

Windows require an `active` market, fresh two-sided displayed liquidity, and a
timely Kalshi message at the end of the full horizon. They are invalidated by
missing snapshots, sequence gaps, reconnects, pauses, tennis sequence gaps, run
boundaries, late horizon confirmation, and truncated captures. Only quote moves
and non-block trades in the modeled signal direction count as responses. A new
point censors the previous window, excluding it from response statistics, so
point cadence cannot turn short observation windows into apparent misses.

Latency percentiles are explicitly conditional on an observed response. Always
read them with `book_response_rate` and `trade_response_rate`; a fast conditional
median with a low response rate is not a usable speed edge.

## Record Format

Each output line contains receive timestamps and the original payload. Prices use
four-decimal fixed point (`0.6150` is `6150`) and sizes use two-decimal fixed
point (`25.00` is `2500`). Example:

```json
{"run_id":1788541200000000000,"source":"kalshi","kind":"orderbook_delta","recv_unix_ns":1788541202123456789,"recv_mono_ns":5123456789,"venue_ts_ms":1788541202121,"ticker":"...","sid":2,"seq":44,"book":{"yes_bid_fp":6150,"yes_bid_size_fp":2500,"yes_ask_fp":6200,"yes_ask_size_fp":8000},"payload":{"type":"orderbook_delta","sid":2,"seq":44,"msg":{"market_ticker":"..."}}}
```

For credible measurements:

1. Run `chrony` and alert on clock offset.
2. Pin the process to a quiet CPU if the instance has more than one vCPU.
3. Record the sports feed's source timestamp, not only its local arrival time.
4. Keep the raw stream and analyze it offline.
5. Reject intervals containing reconnects, gaps, market pauses, or stale inputs.

## Safety

- No private keys, `.env` files, PEM files, or captured data are tracked.
- Authentication is regenerated for every WebSocket reconnect.
- Missing snapshots and sequence gaps invalidate the book rather than being
  guessed through.
- A full writer queue terminates the recorder instead of delaying receive
  timestamps or silently dropping evidence.
- Output files are forced to owner-only `0600` permissions on Unix.

## Roadmap

The next stage is a vendor-specific tennis decoder and a frozen point-by-point
win-probability model. Execution should only be considered if the resulting
out-of-sample report shows a durable margin after realistic costs and misses.

## Disclaimer

Trading involves substantial risk of loss. Follow Kalshi's rules, use properly
licensed data, and do not use deceptive orders, message flooding, or other market
manipulation.
