use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, watch},
    time::{Duration, sleep},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};

use crate::{
    auth::Auth,
    book::Book,
    record::{Clock, Record},
};

pub async fn run(
    websocket_url: &'static str,
    rest_url: &'static str,
    tickers: Vec<String>,
    auth: Auth,
    clock: Clock,
    records: mpsc::Sender<Record>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build Kalshi HTTP client")?;
    let mut backoff = Duration::from_millis(250);
    while !*shutdown.borrow() {
        match connect_once(
            websocket_url,
            rest_url,
            &tickers,
            &auth,
            &http,
            &clock,
            &records,
            shutdown.clone(),
        )
        .await
        {
            Ok(()) if *shutdown.borrow() => return Ok(()),
            Ok(()) => eprintln!("Kalshi connection ended; reconnecting"),
            Err(error) => {
                eprintln!("Kalshi connection error: {error:#}");
                write_control(
                    &records,
                    &clock,
                    "connection_error",
                    json!({"error": format!("{error:#}")}),
                )?;
            }
        }
        tokio::select! {
            _ = sleep(backoff) => {},
            _ = shutdown.changed() => return Ok(()),
        }
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn connect_once(
    websocket_url: &str,
    rest_url: &str,
    tickers: &[String],
    auth: &Auth,
    http: &reqwest::Client,
    clock: &Clock,
    records: &mpsc::Sender<Record>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let headers = auth.websocket_headers()?;
    let mut request = websocket_url
        .into_client_request()
        .context("invalid Kalshi WebSocket URL")?;
    for (name, value) in [
        ("KALSHI-ACCESS-KEY", headers.key_id),
        ("KALSHI-ACCESS-TIMESTAMP", headers.timestamp),
        ("KALSHI-ACCESS-SIGNATURE", headers.signature),
    ] {
        request.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(&value)?,
        );
    }
    let (mut socket, _) = tokio::select! {
        result = connect_async(request) => result.context("Kalshi WebSocket handshake failed")?,
        _ = shutdown.changed() => return Ok(()),
    };
    write_control(
        records,
        clock,
        "connection_open",
        json!({"url": websocket_url}),
    )?;
    for ticker in tickers {
        let response = tokio::select! {
            result = http.get(format!("{rest_url}/markets/{ticker}")).send() => {
                result.with_context(|| format!("failed to fetch initial status for {ticker}"))?
            },
            _ = shutdown.changed() => return Ok(()),
        }
        .error_for_status()
        .with_context(|| format!("initial market status failed for {ticker}"))?;
        let payload: Value = tokio::select! {
            result = response.json() => {
                result.with_context(|| format!("invalid initial market status for {ticker}"))?
            },
            _ = shutdown.changed() => return Ok(()),
        };
        let (recv_unix_ns, recv_mono_ns) = clock.stamp()?;
        write_record(
            records,
            Record {
                run_id: clock.run_id(),
                source: "kalshi",
                kind: "market_status_snapshot".into(),
                recv_unix_ns,
                recv_mono_ns,
                source_ts_ns: None,
                venue_ts_ms: None,
                ticker: Some(ticker.clone()),
                sid: None,
                seq: None,
                book: None,
                payload,
            },
        )?;
    }
    tokio::select! {
        result = socket.send(Message::Text(
            market_subscription(tickers).into(),
        )) => result.context("failed to send Kalshi subscription")?,
        _ = shutdown.changed() => return Ok(()),
    }
    tokio::select! {
        result = socket.send(Message::Text(
            lifecycle_subscription().into(),
        )) => result.context("failed to send Kalshi lifecycle subscription")?,
        _ = shutdown.changed() => return Ok(()),
    }

    let mut sequences = HashMap::<u64, u64>::new();
    let mut books = HashMap::<String, Book>::new();
    loop {
        let message = tokio::select! {
            message = socket.next() => message.context("Kalshi WebSocket closed")??,
            _ = shutdown.changed() => return Ok(()),
        };
        let Message::Text(text) = message else {
            continue;
        };
        let (recv_unix_ns, recv_mono_ns) = clock.stamp()?;
        let payload: Value = serde_json::from_str(&text).context("invalid Kalshi JSON")?;
        let kind = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let message = payload.get("msg").unwrap_or(&Value::Null);
        let ticker = message
            .get("market_ticker")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let sid = payload.get("sid").and_then(Value::as_u64);
        let seq = payload.get("seq").and_then(Value::as_u64);

        if matches!(
            kind.as_str(),
            "orderbook_snapshot"
                | "orderbook_delta"
                | "trade"
                | "market_lifecycle_v2"
                | "event_lifecycle"
                | "event_fee_update"
        ) {
            let sid = sid.with_context(|| format!("{kind} is missing sid"))?;
            let seq = seq.with_context(|| format!("{kind} is missing seq"))?;
            if let Err((expected, received)) = observe_sequence(&mut sequences, sid, seq) {
                write_record(
                    records,
                    Record {
                        run_id: clock.run_id(),
                        source: "kalshi",
                        kind: "sequence_gap".into(),
                        recv_unix_ns,
                        recv_mono_ns,
                        source_ts_ns: None,
                        venue_ts_ms: message.get("ts_ms").and_then(Value::as_u64),
                        ticker: ticker.clone(),
                        sid: Some(sid),
                        seq: Some(seq),
                        book: None,
                        payload,
                    },
                )?;
                bail!("sequence gap on sid {sid}: expected {expected}, received {received}");
            }
        }

        if matches!(
            kind.as_str(),
            "orderbook_snapshot" | "orderbook_delta" | "trade"
        ) && ticker.is_none()
        {
            bail!("{kind} is missing market_ticker");
        }
        if matches!(kind.as_str(), "event_lifecycle" | "event_fee_update")
            || (kind == "market_lifecycle_v2"
                && !ticker
                    .as_ref()
                    .is_some_and(|ticker| tickers.contains(ticker)))
        {
            continue;
        }

        let touch = match (kind.as_str(), ticker.as_deref()) {
            ("orderbook_snapshot", Some(ticker)) => {
                let book = Book::from_snapshot(message)
                    .with_context(|| format!("invalid snapshot for {ticker}"))?;
                let touch = book.touch();
                books.insert(ticker.to_owned(), book);
                Some(touch)
            }
            ("orderbook_delta", Some(ticker)) => {
                let book = books
                    .get_mut(ticker)
                    .with_context(|| format!("delta arrived before snapshot for {ticker}"))?;
                book.apply_delta(message)
                    .with_context(|| format!("invalid delta for {ticker}"))?;
                Some(book.touch())
            }
            _ => None,
        };
        let subscription_error = (kind == "error").then(|| message.to_string());
        write_record(
            records,
            Record {
                run_id: clock.run_id(),
                source: "kalshi",
                kind,
                recv_unix_ns,
                recv_mono_ns,
                source_ts_ns: None,
                venue_ts_ms: message.get("ts_ms").and_then(Value::as_u64),
                ticker,
                sid,
                seq,
                book: touch,
                payload,
            },
        )?;
        if let Some(error) = subscription_error {
            bail!("Kalshi subscription error: {error}");
        }
    }
}

fn observe_sequence(
    sequences: &mut HashMap<u64, u64>,
    sid: u64,
    seq: u64,
) -> std::result::Result<(), (u64, u64)> {
    let previous = sequences.insert(sid, seq);
    match previous {
        Some(previous) if seq != previous + 1 => Err((previous + 1, seq)),
        _ => Ok(()),
    }
}

fn market_subscription(tickers: &[String]) -> String {
    json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta", "trade"],
            "market_tickers": tickers,
            "use_yes_price": true
        }
    })
    .to_string()
}

fn lifecycle_subscription() -> String {
    json!({
        "id": 2,
        "cmd": "subscribe",
        "params": {
            "channels": ["market_lifecycle_v2"]
        }
    })
    .to_string()
}

fn write_control(
    records: &mpsc::Sender<Record>,
    clock: &Clock,
    kind: &str,
    payload: Value,
) -> Result<()> {
    let (recv_unix_ns, recv_mono_ns) = clock.stamp()?;
    write_record(
        records,
        Record {
            run_id: clock.run_id(),
            source: "kalshi",
            kind: kind.to_owned(),
            recv_unix_ns,
            recv_mono_ns,
            source_ts_ns: None,
            venue_ts_ms: None,
            ticker: None,
            sid: None,
            seq: None,
            book: None,
            payload,
        },
    )
}

fn write_record(records: &mpsc::Sender<Record>, record: Record) -> Result<()> {
    records
        .try_send(record)
        .context("record queue is full or the writer stopped")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{lifecycle_subscription, market_subscription, observe_sequence};
    use serde_json::json;

    #[test]
    fn subscription_uses_explicit_yes_axis() {
        let tickers = ["TEST-TICKER".to_owned()];
        let message: serde_json::Value =
            serde_json::from_str(&market_subscription(&tickers)).unwrap();
        assert_eq!(message["params"]["use_yes_price"], true);
    }

    #[test]
    fn lifecycle_subscription_has_no_unsupported_market_filter() {
        let message: serde_json::Value = serde_json::from_str(&lifecycle_subscription()).unwrap();
        assert_eq!(
            message["params"]["channels"],
            json!(["market_lifecycle_v2"])
        );
        assert!(message["params"].get("market_tickers").is_none());
    }

    #[test]
    fn sequence_is_shared_by_markets_on_each_subscription() {
        let mut sequences = HashMap::new();
        assert_eq!(observe_sequence(&mut sequences, 7, 10), Ok(()));
        assert_eq!(observe_sequence(&mut sequences, 7, 11), Ok(()));
        assert_eq!(observe_sequence(&mut sequences, 8, 40), Ok(()));
        assert_eq!(observe_sequence(&mut sequences, 7, 13), Err((12, 13)));
    }
}
