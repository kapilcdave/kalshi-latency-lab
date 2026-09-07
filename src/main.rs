mod auth;
mod book;
mod config;
mod kalshi;
mod record;
mod tennis;

use std::{env, future::pending, path::PathBuf};

use anyhow::{Context, Result, bail};
use tokio::{
    sync::{mpsc, watch},
    task::JoinError,
};

use crate::{auth::Auth, config::Config, record::Clock};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.json"));
    if env::args_os().nth(2).is_some() {
        bail!("usage: kalshi-latency-lab [config.json]");
    }
    let config = Config::load(&path)?;
    let auth = Auth::from_env()?;
    let clock = Clock::new()?;
    let (records_tx, records_rx) = mpsc::channel(config.channel_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let writer_path = PathBuf::from(&config.output);
    let mut writer =
        tokio::spawn(async move { record::run_writer(&writer_path, records_rx).await });
    let mut kalshi = tokio::spawn(kalshi::run(
        config.environment.websocket_url(),
        config.environment.rest_url(),
        config.market_tickers,
        auth,
        clock.clone(),
        records_tx.clone(),
        shutdown_rx.clone(),
    ));
    let mut tennis = config.tennis_events.map(|source| {
        let tennis_records = records_tx.clone();
        let mut tennis_shutdown = shutdown_rx;
        tokio::spawn(async move {
            tennis::run(
                &source,
                config.replay_delay_ms,
                clock,
                tennis_records,
                tennis_shutdown.clone(),
            )
            .await?;
            if source != "-" {
                let _ = tennis_shutdown.changed().await;
            }
            Ok(())
        })
    });

    enum Stopped {
        Signal(std::io::Result<()>),
        Kalshi(std::result::Result<Result<()>, JoinError>),
        Tennis(std::result::Result<Result<()>, JoinError>),
        Writer(std::result::Result<Result<()>, JoinError>),
    }

    let stopped = tokio::select! {
        result = tokio::signal::ctrl_c() => Stopped::Signal(result),
        result = &mut kalshi => Stopped::Kalshi(result),
        result = async {
            match tennis.as_mut() {
                Some(task) => task.await,
                None => pending().await,
            }
        } => Stopped::Tennis(result),
        result = &mut writer => Stopped::Writer(result),
    };
    let _ = shutdown_tx.send(true);

    let kalshi_stopped = matches!(stopped, Stopped::Kalshi(_));
    let tennis_stopped = matches!(stopped, Stopped::Tennis(_));
    let writer_stopped = matches!(stopped, Stopped::Writer(_));
    let mut failure = match stopped {
        Stopped::Signal(result) => result.context("failed to install Ctrl-C handler").err(),
        Stopped::Kalshi(result) => Some(stopped_task_error("Kalshi", result)),
        Stopped::Tennis(result) => Some(stopped_task_error("tennis", result)),
        Stopped::Writer(result) => Some(stopped_task_error("writer", result)),
    };

    if !kalshi_stopped {
        keep_first_error(&mut failure, task_result("Kalshi", kalshi.await));
    }
    if !tennis_stopped && let Some(tennis) = tennis {
        keep_first_error(&mut failure, task_result("tennis", tennis.await));
    }
    drop(records_tx);
    if !writer_stopped {
        keep_first_error(&mut failure, task_result("writer", writer.await));
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn task_result(name: &str, result: std::result::Result<Result<()>, JoinError>) -> Result<()> {
    result.with_context(|| format!("{name} task panicked"))?
}

fn stopped_task_error(
    name: &str,
    result: std::result::Result<Result<()>, JoinError>,
) -> anyhow::Error {
    match task_result(name, result) {
        Ok(()) => anyhow::anyhow!("{name} task stopped unexpectedly"),
        Err(error) => error,
    }
}

fn keep_first_error(failure: &mut Option<anyhow::Error>, result: Result<()>) {
    if failure.is_none() {
        *failure = result.err();
    }
}
