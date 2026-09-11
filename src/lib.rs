pub mod book;
pub mod calc;
pub mod config;
pub mod discovery;
pub mod domain;
pub mod error;
pub mod exec;
pub mod hedge;
pub mod notify;
pub mod platforms;
pub mod reconcile;
pub mod settlement;
pub mod settlement_fees;
pub mod signing;
pub mod stats;
pub mod store;
pub mod take_profit;

use crate::book::{BookStore, DirtyCoalescer};
use crate::config::{Config, OUTCOME, POLYMARKET};
use crate::domain::TopicKey;
use crate::exec::Engine;
use crate::platforms::outcome::{self, OutcomeVenue};
use crate::platforms::polymarket::{self, PolymarketVenue};
use crate::stats::MinuteStats;
use crate::store::{connect_common, Store};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::time::MissedTickBehavior;

pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cfg = Config::from_env()?;
    let store = Store::connect(&cfg.app_postgres_uri).await?;
    store.migrate().await?;
    let common = connect_common(&cfg.common_postgres_uri).await?;
    let stats = Arc::new(MinuteStats::new());
    let pm = PolymarketVenue::connect(&cfg, stats.clone()).await?;
    let outcome = OutcomeVenue::connect(&cfg)?;
    let books = Arc::new(Mutex::new(BookStore::new(cfg.book_stale)));
    let dirty = Arc::new(Mutex::new(DirtyCoalescer::default()));
    let topics = Arc::new(RwLock::new(HashMap::new()));
    let (calc_tx, mut calc_rx) = mpsc::channel::<TopicKey>(256);
    let calc_tx_resync = cfg.platform_enabled(POLYMARKET).then(|| calc_tx.clone());
    let (pm_sub_tx, pm_sub_rx) = mpsc::channel::<Vec<String>>(16);
    let (out_sub_tx, out_sub_rx) = mpsc::channel::<Vec<String>>(16);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let notify = crate::notify::connect(&cfg).await;
    let engine = Arc::new(Engine {
        cfg: cfg.clone(),
        store: store.clone(),
        common,
        books: books.clone(),
        dirty: dirty.clone(),
        topics: topics.clone(),
        pm,
        outcome,
        pm_sub_tx,
        out_sub_tx,
        notify,
        stats,
        position_scan_cursor: Mutex::new(0),
        actuals_scan_cursor: Mutex::new(0),
        settlement_scan_cursor: Mutex::new(0),
        last_settlement_sweep: Mutex::new(None),
        reported_stale_unknown: Mutex::new(Default::default()),
    });
    engine.refresh_discovery().await?;

    // 已有订单对账先启动，费用 API 失败/超时只影响后续新增交易评估。
    let engine_rec = engine.clone();
    let rec_interval = cfg.reconcile_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(rec_interval);
        loop {
            tick.tick().await;
            if let Err(err) = engine_rec.reconcile().await {
                tracing::error!(error = %err, "reconcile failed");
            }
        }
    });

    if cfg.platform_enabled(OUTCOME) {
        match engine.outcome.refresh_fees().await {
            Ok(()) => engine.stats.outcome_fee_refresh_ok(),
            Err(_) => {
                engine.stats.outcome_fee_refresh_failed();
                tracing::info!(
                    service = "outcome",
                    api = "fee_snapshot",
                    available = false,
                    "outcome fees unavailable at startup; reconciliation remains enabled"
                );
            }
        }
        let engine_fees = engine.clone();
        let mut fee_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(outcome::fees::FEE_REFRESH_INTERVAL);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick.tick().await; // 首次刷新已完成，不在启动时重复请求。
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        tokio::select! {
                            result = engine_fees.outcome.refresh_fees() => match result {
                                Ok(()) => engine_fees.stats.outcome_fee_refresh_ok(),
                                Err(_) => engine_fees.stats.outcome_fee_refresh_failed(),
                            },
                            _ = fee_shutdown.changed() => break,
                        }
                    }
                    _ = fee_shutdown.changed() => break,
                }
            }
        });
    }

    if cfg.platform_enabled(POLYMARKET) {
        tokio::spawn(polymarket::run_market_ws(
            cfg.polymarket_ws_url.clone(),
            books.clone(),
            calc_tx.clone(),
            pm_sub_rx,
            shutdown_rx.clone(),
        ));
        let engine_auth = engine.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(err) = engine_auth.pm.refresh_oldest_expiring_auth().await {
                    tracing::error!(error = %err, "polymarket auth refresh failed");
                }
            }
        });
    }
    if cfg.platform_enabled(OUTCOME) {
        tokio::spawn(outcome::run_l2_ws(
            cfg.hyperliquid_ws_url.clone(),
            books.clone(),
            calc_tx,
            out_sub_rx,
            shutdown_rx.clone(),
        ));
    }

    let engine_calc = engine.clone();
    tokio::spawn(async move {
        while let Some(topic) = calc_rx.recv().await {
            let engine_topic = engine_calc.clone();
            tokio::spawn(async move {
                if let Err(err) = engine_topic.handle_topic(topic).await {
                    engine_topic.stats.exec_err();
                    tracing::error!(
                        topic = %topic.as_str(),
                        error = %err,
                        "calc/exec failed"
                    );
                }
            });
        }
    });

    let engine_stats = engine.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            engine_stats.stats.log_and_reset();
        }
    });

    let engine_disc = engine.clone();
    let disc_interval = cfg.discovery_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(disc_interval);
        loop {
            tick.tick().await;
            if let Err(err) = engine_disc.refresh_discovery().await {
                tracing::error!(error = %err, "discovery failed");
            }
        }
    });

    let engine_hedge = engine.clone();
    let hedge_interval = cfg.hedge_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(hedge_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(err) = engine_hedge.hedge_once().await {
                tracing::error!(error = %err, "hedge failed");
            }
        }
    });

    if let Some(calc_tx_resync) = calc_tx_resync {
        let engine_resync = engine.clone();
        let resync_interval = cfg.book_resync;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(resync_interval);
            loop {
                tick.tick().await;
                match engine_resync.resync_stale_pm_books().await {
                    Ok(topics) => {
                        for topic in topics {
                            let _ = calc_tx_resync.send(topic).await;
                        }
                    }
                    Err(err) => tracing::error!(error = %err, "polymarket book resync failed"),
                }
            }
        });
    }

    shutdown_signal().await;
    let _ = shutdown_tx.send(true);
    tracing::info!("shutdown");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install sigterm");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
