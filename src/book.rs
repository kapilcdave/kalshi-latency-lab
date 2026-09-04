use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;

const PRICE_SCALE: i64 = 10_000;
const SIZE_SCALE: i64 = 100;

#[derive(Debug, Default)]
pub struct Book {
    yes: BTreeMap<i64, i64>,
    no: BTreeMap<i64, i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Touch {
    pub yes_bid_fp: Option<i64>,
    pub yes_bid_size_fp: Option<i64>,
    pub yes_ask_fp: Option<i64>,
    pub yes_ask_size_fp: Option<i64>,
}

impl Book {
    pub fn from_snapshot(message: &Value) -> Result<Self> {
        let mut book = Self::default();
        apply_levels(&mut book.yes, message.get("yes_dollars_fp"))?;
        apply_levels(&mut book.no, message.get("no_dollars_fp"))?;
        Ok(book)
    }

    pub fn apply_delta(&mut self, message: &Value) -> Result<()> {
        let side = message
            .get("side")
            .and_then(Value::as_str)
            .context("delta is missing side")?;
        let price = parse_value(message.get("price_dollars"), PRICE_SCALE)
            .context("invalid delta price_dollars")?;
        let delta = parse_value(message.get("delta_fp"), SIZE_SCALE).context("invalid delta_fp")?;
        let levels = match side {
            "yes" => &mut self.yes,
            "no" => &mut self.no,
            _ => bail!("unknown book side {side}"),
        };
        let current = levels.get(&price).copied().unwrap_or_default();
        let next = current.checked_add(delta).context("book size overflow")?;
        if next < 0 {
            bail!("delta removes more size than exists at the price level");
        }
        if next == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, next);
        }
        Ok(())
    }

    pub fn touch(&self) -> Touch {
        let bid = self.yes.last_key_value();
        let ask = self.no.first_key_value();
        Touch {
            yes_bid_fp: bid.map(|(price, _)| *price),
            yes_bid_size_fp: bid.map(|(_, size)| *size),
            yes_ask_fp: ask.map(|(price, _)| *price),
            yes_ask_size_fp: ask.map(|(_, size)| *size),
        }
    }
}

fn apply_levels(target: &mut BTreeMap<i64, i64>, levels: Option<&Value>) -> Result<()> {
    let Some(levels) = levels else {
        return Ok(());
    };
    for row in levels.as_array().context("book levels are not an array")? {
        let row = row.as_array().context("book level is not an array")?;
        if row.len() != 2 {
            bail!("book level must contain price and size");
        }
        let price = parse_value(row.first(), PRICE_SCALE).context("invalid book price")?;
        let size = parse_value(row.get(1), SIZE_SCALE).context("invalid book size")?;
        if size > 0 {
            target.insert(price, size);
        }
    }
    Ok(())
}

fn parse_value(value: Option<&Value>, scale: i64) -> Result<i64> {
    let value = value.context("missing fixed-point value")?;
    match value {
        Value::String(value) => parse_fixed(value, scale),
        Value::Number(value) => parse_fixed(&value.to_string(), scale),
        _ => bail!("fixed-point value is not a string or number"),
    }
}

fn parse_fixed(value: &str, scale: i64) -> Result<i64> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let decimals = scale.ilog10() as usize;
    if fraction.len() > decimals {
        bail!("{value} has more than {decimals} decimal places");
    }
    let whole: i64 = whole.parse().context("invalid whole number")?;
    let fraction: i64 = if fraction.is_empty() {
        0
    } else {
        let padded = format!("{fraction:0<decimals$}");
        padded.parse().context("invalid fractional number")?
    };
    let result = whole
        .checked_mul(scale)
        .and_then(|whole| whole.checked_add(fraction))
        .context("fixed-point value overflow")?;
    Ok(if negative { -result } else { result })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snapshot_and_delta_preserve_yes_axis() {
        let mut book = Book::from_snapshot(&json!({
            "yes_dollars_fp": [["0.6000", "100.00"], ["0.5900", "50.00"]],
            "no_dollars_fp": [["0.6200", "80.00"], ["0.6300", "40.00"]]
        }))
        .unwrap();
        assert_eq!(
            book.touch(),
            Touch {
                yes_bid_fp: Some(6000),
                yes_bid_size_fp: Some(10_000),
                yes_ask_fp: Some(6200),
                yes_ask_size_fp: Some(8_000),
            }
        );

        book.apply_delta(&json!({
            "side": "yes", "price_dollars": "0.6100", "delta_fp": "25.00"
        }))
        .unwrap();
        assert_eq!(book.touch().yes_bid_fp, Some(6100));
    }

    #[test]
    fn removing_level_exposes_next_price() {
        let mut book = Book::from_snapshot(&json!({
            "yes_dollars_fp": [["0.6000", "100.00"], ["0.5900", "50.00"]]
        }))
        .unwrap();
        book.apply_delta(&json!({
            "side": "yes", "price_dollars": "0.6000", "delta_fp": "-100.00"
        }))
        .unwrap();
        assert_eq!(book.touch().yes_bid_fp, Some(5900));
    }

    #[test]
    fn fixed_point_is_exact() {
        assert_eq!(parse_fixed("0.615", PRICE_SCALE).unwrap(), 6150);
        assert_eq!(parse_fixed("-54.00", SIZE_SCALE).unwrap(), -5400);
        assert!(parse_fixed("0.12345", PRICE_SCALE).is_err());
    }

    #[test]
    fn over_removal_invalidates_book() {
        let mut book = Book::from_snapshot(&json!({
            "yes_dollars_fp": [["0.6000", "10.00"]]
        }))
        .unwrap();
        assert!(
            book.apply_delta(&json!({
                "side": "yes", "price_dollars": "0.6000", "delta_fp": "-10.01"
            }))
            .is_err()
        );
    }
}
