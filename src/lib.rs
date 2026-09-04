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
pub mod signing;
pub mod store;

use crate::book::{BookStore, DirtyCoalescer};
use crate::config::{Config, OUTCOME, POLYMARKET};
use crate::domain::TopicKey;
use crate::exec::Engine;
use crate::platforms::outcome::{self, OutcomeVenue};
use crate::platforms::polymarket::{self, PolymarketVenue};
use crate::store::{connect_common, Store};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex, RwLock};

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
    let pm = PolymarketVenue::connect(&cfg).await?;
    let outcome = OutcomeVenue::connect(&cfg)?;
    let books = Arc::new(Mutex::new(BookStore::default()));
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
    });
    engine.refresh_discovery().await?;

    if cfg.platform_enabled(POLYMARKET) {
        tokio::spawn(polymarket::run_market_ws(
            cfg.polymarket_ws_url.clone(),
            books.clone(),
            calc_tx.clone(),
            pm_sub_rx,
            shutdown_rx.clone(),
        ));
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
            if let Err(err) = engine_calc.handle_topic(topic).await {
                tracing::error!(error = %err, "calc/exec failed");
            }
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

    let engine_hedge = engine.clone();
    let hedge_interval = cfg.hedge_interval;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(hedge_interval);
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
