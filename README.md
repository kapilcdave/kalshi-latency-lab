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

## Record Format

Each output line contains receive timestamps and the original payload. Prices use
four-decimal fixed point (`0.6150` is `6150`) and sizes use two-decimal fixed
point (`25.00` is `2500`). Example:

```json
{"source":"kalshi","kind":"orderbook_delta","recv_unix_ns":1788541202123456789,"recv_mono_ns":5123456789,"venue_ts_ms":1788541202121,"ticker":"...","sid":2,"seq":44,"book":{"yes_bid_fp":6150,"yes_bid_size_fp":2500,"yes_ask_fp":6200,"yes_ask_size_fp":8000},"payload":{"market_ticker":"..."}}
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

The next stage is an offline report that joins each tennis state transition to
the first Kalshi book move and trade, then reports reaction-time quantiles and
fee-adjusted hypothetical fills. Execution should only be considered if that
out-of-sample report shows a durable margin.

## Disclaimer

Trading involves substantial risk of loss. Follow Kalshi's rules, use properly
licensed data, and do not use deceptive orders, message flooding, or other market
manipulation.
