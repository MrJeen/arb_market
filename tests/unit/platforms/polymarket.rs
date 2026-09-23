use super::*;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
struct WsLogEvent {
    level: tracing::Level,
    fields: HashMap<String, String>,
}

impl tracing::field::Visit for WsLogEvent {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
}

struct WsLogCapture(mpsc::UnboundedSender<WsLogEvent>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WsLogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut captured = WsLogEvent {
            level: *event.metadata().level(),
            fields: HashMap::new(),
        };
        event.record(&mut captured);
        let _ = self.0.send(captured);
    }
}

#[test]
fn timestamp_conflict_diagnostics_preserve_pm_behavior() {
    use crate::book::{BookReject, BookUpdate};
    use tracing_subscriber::prelude::*;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let subscriber = tracing_subscriber::registry().with(WsLogCapture(tx));
    tracing::subscriber::with_default(subscriber, || {
        let now = Instant::now();
        let mut books = BookStore::default();
        let snapshot = json!({"event_type":"book", "asset_id":"t", "timestamp":"100",
                "bids":[], "asks":[{"price":"0.5", "size":"3"}]});
        assert_eq!(apply_ws_message(&mut books, &snapshot, now).len(), 1);
        let before = books.begin_rest(POLYMARKET, "t");
        let mut conflicting = snapshot.clone();
        conflicting["asks"][0]["size"] = json!("4");
        assert_eq!(
            apply_ws_message(&mut books, &conflicting, now),
            vec![("t".into(), true)]
        );
        assert!(!books.get(POLYMARKET, "t").unwrap().stale);
        assert_eq!(
            books.get(POLYMARKET, "t").unwrap().asks[0].size,
            Decimal::from(4)
        );
        assert_eq!(
            books.begin_rest(POLYMARKET, "t").revision,
            before.revision + 1
        );
        // 同 epoch stale 可由同毫秒全量恢复，并通知计算链。
        books.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
        assert_eq!(
            apply_ws_message(&mut books, &snapshot, now),
            vec![("t".into(), true)]
        );
        assert!(!books.get(POLYMARKET, "t").unwrap().stale);
        let ticket = books.begin_rest(POLYMARKET, "t");
        let asks = books.get(POLYMARKET, "t").unwrap().asks.clone();
        books
            .accept_rest(&ticket, vec![], asks.clone(), 101, now, None)
            .unwrap();
        let delta = json!({"event_type":"price_change", "timestamp":"101",
                "price_changes":[{"asset_id":"t", "side":"SELL", "price":"0.5", "size":"4"}]});
        assert!(apply_ws_message(&mut books, &delta, now).is_empty());
        let ticket = books.begin_rest(POLYMARKET, "t");
        let mut changed_asks = asks.clone();
        changed_asks[0].size = Decimal::from(4);
        assert_eq!(
            books
                .accept_rest(&ticket, vec![], changed_asks, 101, now, None)
                .unwrap_err(),
            BookReject::TimestampConflict
        );
        assert_eq!(books.begin_rest(POLYMARKET, "t").revision, ticket.revision);
        let tick = json!({"event_type":"tick_size_change", "asset_id":"t", "timestamp":"200", "new_tick_size":"0.01"});
        assert_eq!(apply_ws_message(&mut books, &tick, now).len(), 1);
        let mut conflicting_tick = tick.clone();
        conflicting_tick["new_tick_size"] = json!("0.001");
        assert!(apply_ws_message(&mut books, &conflicting_tick, now).is_empty());
        assert_eq!(books.tick_size(POLYMARKET, "t"), None);
        assert_eq!(books.get(POLYMARKET, "t").unwrap().received_at, now);
        assert_eq!(
            books.replace_snapshot(POLYMARKET, "t", vec![], asks.clone(), 150, now),
            BookUpdate::Rejected(BookReject::OlderTimestamp)
        );
        let ticket = books.begin_rest(POLYMARKET, "t");
        assert_eq!(
            books
                .accept_rest(&ticket, vec![], asks.clone(), 150, now, None)
                .unwrap_err(),
            BookReject::OlderTimestamp
        );
        assert_eq!(
            books.set_tick_size_at(POLYMARKET, "t", Decimal::new(1, 2), 150),
            BookUpdate::Rejected(BookReject::OlderTimestamp)
        );
        // Outcome 的 REST candidate 仍独立比较，不能误标为当前 WS book。
        let ticket = books.begin_rest(crate::config::OUTCOME, "candidate");
        books
            .accept_rest(&ticket, vec![], asks.clone(), 300, now, None)
            .unwrap();
        let mut changed_asks = asks.clone();
        changed_asks[0].size = Decimal::from(5);
        assert_eq!(
            books.replace_snapshot(
                crate::config::OUTCOME,
                "candidate",
                vec![],
                changed_asks,
                300,
                now
            ),
            BookUpdate::Rejected(BookReject::TimestampConflict)
        );
        // 快照携带的 tick 也必须保留 WS / REST 事件来源。
        for rest in [false, true] {
            let mut store = BookStore::default();
            store.set_tick_size_at(POLYMARKET, "embedded", Decimal::new(1, 2), 200);
            if rest {
                let ticket = store.begin_rest(POLYMARKET, "embedded");
                assert_eq!(
                    store
                        .accept_rest(
                            &ticket,
                            vec![],
                            asks.clone(),
                            200,
                            now,
                            Some(Decimal::new(1, 3))
                        )
                        .unwrap_err(),
                    BookReject::TimestampConflict
                );
            } else {
                assert_eq!(
                    store.replace_snapshot_with_tick(
                        POLYMARKET,
                        "embedded",
                        vec![],
                        asks.clone(),
                        200,
                        now,
                        Some(Decimal::new(1, 3))
                    ),
                    BookUpdate::Rejected(BookReject::TimestampConflict)
                );
            }
        }
    });
    let mut logs = Vec::new();
    while let Ok(log) = rx.try_recv() {
        logs.push(log);
    }
    assert!(!logs.iter().any(
        |log| log.fields.get("platform").is_some_and(|v| v == POLYMARKET)
            && log.fields.get("event").is_some_and(|v| v == "ws_snapshot")
            && log
                .fields
                .get("conflict")
                .is_some_and(|v| v == "snapshot_depth" || v == "stale_same_epoch")
    ));
    for (event, conflict) in [
        ("ws_snapshot", "snapshot_depth"),
        ("ws_delta", "rest_boundary"),
        ("rest_snapshot", "snapshot_depth"),
        ("ws_tick", "tick_observation"),
        ("ws_snapshot", "tick_observation"),
        ("rest_snapshot", "tick_observation"),
        ("ws_snapshot", "tick_high_water"),
        ("rest_snapshot", "tick_high_water"),
    ] {
        let log = logs
            .iter()
            .find(|log| {
                log.fields.get("event").is_some_and(|v| v == event)
                    && log.fields.get("conflict").is_some_and(|v| v == conflict)
            })
            .unwrap();
        assert_eq!(log.level, tracing::Level::DEBUG);
        for field in [
            "platform",
            "token",
            "source",
            "reason",
            "current_exchange_ts_ms",
            "incoming_exchange_ts_ms",
            "rest_boundary",
            "epoch",
            "revision",
        ] {
            assert!(
                log.fields.contains_key(field),
                "{event}/{conflict} missing {field}"
            );
        }
    }
    let delta = logs
        .iter()
        .find(|log| {
            log.fields
                .get("conflict")
                .is_some_and(|v| v == "rest_boundary")
        })
        .unwrap();
    assert_eq!(delta.fields["side"], "ask");
    assert_eq!(delta.fields["current_size"], "3");
    assert_eq!(delta.fields["incoming_size"], "4");
    let candidate = logs
        .iter()
        .find(|log| {
            log.fields
                .get("compared_book")
                .is_some_and(|v| v == "rest_candidate")
        })
        .unwrap();
    assert_eq!(candidate.fields["current_exchange_ts_ms"], "300");
    assert!(candidate.fields["first_diff"].contains("size: 5"));
    for log in &logs {
        assert!(!log.fields.contains_key("bids") && !log.fields.contains_key("asks"));
        if log
            .fields
            .get("message")
            .is_some_and(|v| v == "book WS completeness lost")
        {
            assert!(log.fields.contains_key("event"));
            assert!(log.fields.contains_key("incoming_exchange_ts_ms"));
        }
    }
    assert!(!logs.iter().any(|log| log.level == tracing::Level::WARN
        && log
            .fields
            .get("event")
            .is_some_and(|v| v == "rest_snapshot")
        && log
            .fields
            .get("message")
            .is_some_and(|v| v != "tick trust lost")));
}

struct MarketWsFixture {
    listener: tokio::net::TcpListener,
    server: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    books: Arc<Mutex<BookStore>>,
    subscriptions: mpsc::Sender<Vec<String>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    logs: mpsc::UnboundedReceiver<WsLogEvent>,
    client: tokio::task::JoinHandle<()>,
}

impl MarketWsFixture {
    async fn new() -> Self {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/private-url-marker", listener.local_addr().unwrap());
        let books = Arc::new(Mutex::new(BookStore::default()));
        let (calc_tx, _calc_rx) = mpsc::channel(10);
        let (subscriptions, sub_rx) = mpsc::channel(10);
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (log_tx, logs) = mpsc::unbounded_channel();
        let subscriber = tracing_subscriber::registry().with(WsLogCapture(log_tx));
        let client = tokio::spawn(
            run_market_ws(url, books.clone(), calc_tx, sub_rx, shutdown_rx)
                .with_subscriber(subscriber),
        );
        let (socket, _) = listener.accept().await.unwrap();
        let server = tokio_tungstenite::accept_async(socket).await.unwrap();
        let mut fixture = Self {
            listener,
            server,
            books,
            subscriptions,
            shutdown,
            logs,
            client,
        };
        fixture.log("ws_connected").await;
        assert_eq!(
            fixture.server.next().await.unwrap().unwrap(),
            Message::Text("PING".into())
        );
        fixture
    }

    async fn log(&mut self, event: &str) -> WsLogEvent {
        loop {
            let entry = self.logs.recv().await.unwrap();
            if entry.fields.get("event").map(String::as_str) == Some(event) {
                assert_eq!(entry.fields["service"], "polymarket");
                assert!(entry.fields.contains_key("elapsed_ms"));
                assert!(entry.fields.contains_key("subscription_count"));
                let rendered = format!("{:?}", entry.fields);
                for secret in [
                    "private-url-marker",
                    "private-frame-marker",
                    "private-close-marker",
                ] {
                    assert!(!rendered.contains(secret));
                }
                return entry;
            }
        }
    }

    async fn seed_book(&mut self) {
        self.server
            .send(Message::Binary(
                json!({"event_type":"book","asset_id":"t",
                "timestamp":"100","bids":[],"asks":[]})
                .to_string()
                .into_bytes()
                .into(),
            ))
            .await
            .unwrap();
        // Pong 是已处理前一帧的屏障，不依赖 sleep 或调度次数。
        self.server
            .send(Message::Ping(
                "private-frame-marker".as_bytes().to_vec().into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            self.server.next().await.unwrap().unwrap(),
            Message::Pong("private-frame-marker".as_bytes().to_vec().into())
        );
        assert!(!self.books.lock().await.get(POLYMARKET, "t").unwrap().stale);
    }

    async fn subscribe(&mut self, tokens: &[&str]) {
        self.subscriptions
            .send(tokens.iter().map(|token| (*token).into()).collect())
            .await
            .unwrap();
        let message = self
            .server
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&message).unwrap(),
            json!({"operation":"subscribe","assets_ids":tokens})
        );
    }

    async fn close_frame(&mut self) {
        use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
        self.server
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "private-close-marker".into(),
            })))
            .await
            .unwrap();
        let entry = self.log("ws_close_received").await;
        assert_eq!(entry.level, tracing::Level::INFO);
        assert_eq!(entry.fields["close_code"], "Some(1000)");
        assert!(!self.client.is_finished());
        assert!(!self.books.lock().await.get(POLYMARKET, "t").unwrap().stale);
    }
}

#[tokio::test]
async fn market_ws_normal_stops_and_close_keep_books_current() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for stop in [
            "shutdown",
            "shutdown_channel_closed",
            "subscription_channel_closed",
        ] {
            let mut fixture = MarketWsFixture::new().await;
            fixture.seed_book().await;
            fixture.close_frame().await;
            // 服务端保持 TCP 打开，Close 本身不会触发 stale 或重连。
            let replacement_sub = mpsc::channel(1).0;
            let replacement_shutdown = tokio::sync::watch::channel(false).0;
            match stop {
                "shutdown" => fixture.shutdown.send(true).unwrap(),
                "shutdown_channel_closed" => drop(std::mem::replace(
                    &mut fixture.shutdown,
                    replacement_shutdown,
                )),
                "subscription_channel_closed" => drop(std::mem::replace(
                    &mut fixture.subscriptions,
                    replacement_sub,
                )),
                _ => unreachable!(),
            }
            let entry = fixture.log("ws_stopped").await;
            assert_eq!(entry.level, tracing::Level::INFO);
            assert_eq!(entry.fields["reason"], stop);
            fixture.client.await.unwrap();
            assert!(
                !fixture
                    .books
                    .lock()
                    .await
                    .get(POLYMARKET, "t")
                    .unwrap()
                    .stale
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn market_ws_close_then_subscription_failure_marks_stale() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for (initial, next, reason) in [
            (vec![], vec!["t"], "subscribe_failed"),
            (vec!["t"], vec![], "unsubscribe_failed"),
        ] {
            let mut fixture = MarketWsFixture::new().await;
            fixture.seed_book().await;
            if !initial.is_empty() {
                fixture.subscribe(&initial).await;
            }
            fixture.close_frame().await;
            fixture
                .subscriptions
                .send(next.iter().map(|t| (*t).into()).collect())
                .await
                .unwrap();
            let entry = fixture.log("ws_disconnected").await;
            assert_eq!(entry.level, tracing::Level::WARN);
            assert_eq!(entry.fields["reason"], reason);
            assert_eq!(entry.fields["error_kind"], "protocol");
            fixture.log("ws_reconnect_wait").await;
            assert!(
                fixture
                    .books
                    .lock()
                    .await
                    .get(POLYMARKET, "t")
                    .unwrap()
                    .stale
            );
            fixture.client.abort();
            assert!(fixture.client.await.unwrap_err().is_cancelled());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn market_ws_read_error_reconnects_after_two_seconds_and_resubscribes() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut fixture = MarketWsFixture::new().await;
        fixture.seed_book().await;
        fixture.subscribe(&["t"]).await;
        let before = fixture.books.lock().await.begin_rest(POLYMARKET, "t");
        // 不发送 Close 的 TCP EOF 在 tungstenite 中是 read_error。
        fixture.server.get_mut().shutdown().await.unwrap();
        let entry = fixture.log("ws_disconnected").await;
        assert_eq!(entry.fields["reason"], "read_error");
        fixture.log("ws_reconnect_wait").await;
        assert!(
            fixture
                .books
                .lock()
                .await
                .get(POLYMARKET, "t")
                .unwrap()
                .stale
        );
        // 网络事件完成后再冻结时间，避免 I/O 等待触发虚拟时钟自动推进。
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(1999)).await;
        assert!(fixture.logs.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::resume();
        let (socket, _) = fixture.listener.accept().await.unwrap();
        let mut reconnected = tokio_tungstenite::accept_async(socket).await.unwrap();
        let entry = fixture.log("ws_connected").await;
        assert_eq!(entry.fields["subscription_count"], "1");
        let message = reconnected
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&message).unwrap(),
            json!({"operation":"subscribe","assets_ids":["t"]})
        );
        assert!(fixture.books.lock().await.begin_rest(POLYMARKET, "t").epoch > before.epoch);
        fixture.shutdown.send(true).unwrap();
        fixture.client.await.unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn market_ws_failure_logs_only_safe_error_categories() {
    use tokio_tungstenite::tungstenite::Error as WsError;
    use tracing_subscriber::prelude::*;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let subscriber = tracing_subscriber::registry().with(WsLogCapture(tx));
    tracing::subscriber::with_default(subscriber, || {
        for (error, expected) in [
            (
                WsError::Io(std::io::Error::other("private-url-marker")),
                "io",
            ),
            (
                WsError::WriteBufferFull(Message::Text("private-frame-marker".into())),
                "write_buffer_full",
            ),
            (
                WsError::Http(tokio_tungstenite::tungstenite::http::Response::new(Some(
                    b"private-frame-marker".to_vec(),
                ))),
                "http",
            ),
        ] {
            for reason in [
                "initial_subscribe_failed",
                "pong_send_failed",
                "ping_send_failed",
                "connect_failed",
            ] {
                log_market_ws_failure("ws_send_failed", reason, Instant::now(), 3, &error);
                let entry = rx.try_recv().unwrap();
                assert_eq!(entry.level, tracing::Level::WARN);
                assert_eq!(entry.fields["error_kind"], expected);
                assert_eq!(
                    entry.fields["io_kind"],
                    if expected == "io" {
                        "Some(Other)"
                    } else {
                        "None"
                    }
                );
                assert_eq!(entry.fields["reason"], reason);
                assert_eq!(entry.fields["subscription_count"], "3");
                assert!(!format!("{:?}", entry.fields).contains("private-"));
            }
        }
    });
}

fn cache_test_venue() -> PolymarketVenue {
    PolymarketVenue {
        http: reqwest::Client::new(),
        base: "http://127.0.0.1".into(),
        funders: Vec::new(),
        authed: Arc::new(Mutex::new(HashMap::new())),
        init_lock: Arc::new(Mutex::new(())),
        cursor_path: PathBuf::new(),
        creds_path: PathBuf::new(),
        auth_ttl: Duration::from_secs(1),
        neg_risk_cache: Arc::new(Mutex::new(HashMap::new())),
        rr: Arc::new(Mutex::new(0)),
        usdc_balance_cache: Arc::new(Mutex::new(HashMap::new())),
        usdc_balance_refreshes: Arc::new(Mutex::new(HashMap::new())),
        stats: Arc::new(MinuteStats::new()),
    }
}

#[test]
fn funder_balance_cache_expires_after_ten_seconds() {
    let now = Instant::now();
    let fresh = FunderBalanceEntry {
        value: Some((Decimal::ONE, now - Duration::from_secs(9))),
        generation: 0,
    };
    let expired = FunderBalanceEntry {
        value: Some((Decimal::ONE, now - Duration::from_secs(10))),
        generation: 0,
    };
    assert_eq!(fresh.get_fresh(now), Some(Decimal::ONE));
    assert_eq!(expired.get_fresh(now), None);
}

#[tokio::test]
async fn funder_balance_cache_reuses_and_normalizes_address() {
    let venue = cache_test_venue();
    let calls = Arc::new(AtomicUsize::new(0));
    let fetch = || {
        let calls = calls.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Decimal::from(11))
        }
    };
    assert_eq!(
        venue.cached_usdc_balance("0xAbC", fetch).await.unwrap(),
        Decimal::from(11)
    );
    assert_eq!(
        venue.cached_usdc_balance("0xaBc", fetch).await.unwrap(),
        Decimal::from(11)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    venue.invalidate_usdc_balance("0xABC").await;
    assert_eq!(
        venue.cached_usdc_balance("0xabc", fetch).await.unwrap(),
        Decimal::from(11)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn concurrent_same_funder_uses_single_refresh() {
    let venue = cache_test_venue();
    let calls = Arc::new(AtomicUsize::new(0));
    let futures = (0..8).map(|_| {
        let venue = venue.clone();
        let calls = calls.clone();
        async move {
            venue
                .cached_usdc_balance("0xabc", || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(Decimal::from(13))
                })
                .await
        }
    });
    let results = futures_util::future::join_all(futures).await;
    assert!(results
        .into_iter()
        .all(|value| value.unwrap() == Decimal::from(13)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn different_funders_refresh_independently() {
    let venue = cache_test_venue();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let fetch = |value| {
        let active = active.clone();
        let peak = peak.clone();
        async move {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(Decimal::from(value))
        }
    };
    let (a, b) = tokio::join!(
        venue.cached_usdc_balance("0xa", || fetch(1)),
        venue.cached_usdc_balance("0xb", || fetch(2))
    );
    assert_eq!(a.unwrap(), Decimal::ONE);
    assert_eq!(b.unwrap(), Decimal::from(2));
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn funder_balance_failures_are_not_cached_and_invalidation_wins() {
    let venue = cache_test_venue();
    let failed_calls = AtomicUsize::new(0);
    let error = venue
        .cached_usdc_balance("0xa", || async {
            failed_calls.fetch_add(1, Ordering::SeqCst);
            Err(Error::msg("fail"))
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Msg(message) if message == "fail"));
    assert_eq!(failed_calls.load(Ordering::SeqCst), 1);
    let failed = venue.stats.snapshot_and_reset();
    assert_eq!(failed.pm_balance_refresh, 1);
    assert_eq!(failed.pm_balance_refresh_fail, 1);
    assert_eq!(failed.pm_balance_cache_hit, 0);
    assert!(!venue.usdc_balance_cache.lock().await.contains_key("0xa"));
    let calls = Arc::new(AtomicUsize::new(0));
    let value = venue
        .cached_usdc_balance("0xa", || {
            let venue = venue.clone();
            let calls = calls.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    venue.invalidate_usdc_balance("0xa").await;
                    Ok(Decimal::from(20))
                } else {
                    Ok(Decimal::from(15))
                }
            }
        })
        .await
        .unwrap();
    assert_eq!(value, Decimal::from(15));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let recovered = venue.stats.snapshot_and_reset();
    assert_eq!(recovered.pm_balance_refresh, 2);
    assert_eq!(recovered.pm_balance_refresh_fail, 0);
}

#[tokio::test]
async fn funder_balance_generation_conflicts_exhaust_after_three_fetches() {
    let venue = cache_test_venue();
    let calls = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        venue.cached_usdc_balance("0xa", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            venue.invalidate_usdc_balance("0xa").await;
            tokio::task::yield_now().await;
            Ok(Decimal::from(20))
        }),
    )
    .await
    .expect("generation conflicts must terminate without an unbounded retry");
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        Error::Msg(message)
            if message == "polymarket usdc balance invalidated during all 3 refresh attempts"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let stats = venue.stats.snapshot_and_reset();
    assert_eq!(stats.pm_balance_refresh, 3);
    assert_eq!(stats.pm_balance_refresh_fail, 1);
    assert_eq!(stats.pm_balance_cache_hit, 0);
    assert_eq!(stats.pm_balance_call, 0);
    assert!(venue.usdc_balance_cache.lock().await["0xa"].value.is_none());
    let refresh = venue.usdc_balance_refreshes.lock().await["0xa"].clone();
    assert!(refresh.try_lock().is_ok());
    let recovered = tokio::time::timeout(
        Duration::from_secs(1),
        venue.cached_usdc_balance("0xa", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Decimal::from(15))
        }),
    )
    .await
    .expect("refresh lock must be released after exhausted attempts")
    .unwrap();
    assert_eq!(recovered, Decimal::from(15));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn funder_balance_generation_conflicts_can_succeed_on_third_fetch() {
    let venue = cache_test_venue();
    let calls = AtomicUsize::new(0);
    let value = tokio::time::timeout(
        Duration::from_secs(1),
        venue.cached_usdc_balance("0xa", || async {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                venue.invalidate_usdc_balance("0xa").await;
                tokio::task::yield_now().await;
                Ok(Decimal::from(20))
            } else {
                Ok(Decimal::from(15))
            }
        }),
    )
    .await
    .expect("third fetch must complete")
    .unwrap();
    assert_eq!(value, Decimal::from(15));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let stats = venue.stats.snapshot_and_reset();
    assert_eq!(stats.pm_balance_refresh, 3);
    assert_eq!(stats.pm_balance_refresh_fail, 0);
    assert_eq!(stats.pm_balance_cache_hit, 0);
    assert_eq!(stats.pm_balance_call, 0);
    let cached = venue
        .cached_usdc_balance("0xa", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Decimal::from(99))
        })
        .await
        .unwrap();
    assert_eq!(cached, Decimal::from(15));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let cached_stats = venue.stats.snapshot_and_reset();
    assert_eq!(cached_stats.pm_balance_cache_hit, 1);
    assert_eq!(cached_stats.pm_balance_refresh, 0);
    assert_eq!(cached_stats.pm_balance_refresh_fail, 0);
}

#[test]
fn order_submit_payload_matches_clob_v2_wire_types() {
    let signed = SignedOrder {
        builder: B256::ZERO,
        expiration: 0,
        maker: Address::ZERO,
        maker_amount: 132_0000,
        metadata: B256::ZERO,
        order_type: "FAK".into(),
        salt: 479_249_096_354,
        side: "BUY".into(),
        signature: "0xabc".into(),
        signature_type: 2,
        signer: Address::ZERO,
        taker_amount: 3_000_000,
        timestamp: 1_780_000_000_000,
        token_id: "1".into(),
        post_only: false,
    };
    let payload = order_submit_payload(&signed, "api-key");
    let order = payload.get("order").expect("order");
    assert!(order.get("salt").unwrap().is_number());
    assert_eq!(order.get("salt").unwrap().as_u64(), Some(479_249_096_354));
    assert!(order.get("expiration").unwrap().is_string());
    assert!(order.get("makerAmount").unwrap().is_string());
    assert!(order.get("takerAmount").unwrap().is_string());
    assert!(order.get("timestamp").unwrap().is_string());
    assert!(order.get("signatureType").unwrap().is_number());
    assert_eq!(order.get("side").and_then(|v| v.as_str()), Some("BUY"));
    assert_eq!(order.get("signatureType").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(payload["orderType"], "FAK");
    let mut gtc = signed.clone();
    gtc.order_type = "GTC".into();
    assert_eq!(order_submit_payload(&gtc, "api-key")["orderType"], "GTC");
    let envelope = signed_envelope(
        &gtc,
        "1",
        OrderSide::Buy,
        Decimal::from(3),
        Decimal::new(44, 2),
        Decimal::new(1, 2),
        false,
        "hash",
    );
    assert_eq!(envelope["order_type"], "GTC");
    assert_eq!(envelope["signed_order"]["order_type"], "GTC");
}

#[test]
fn parses_fak_unfilled() {
    let body =
        json!({"success": false, "status": "unmatched", "orderID": "", "errorMsg": FAK_UNFILLED});
    match parse_submit(&body, "0x1".into(), json!({})) {
        SubmitResult::NoMatch { .. } => {}
        other => panic!("expected no match, got {other:?}"),
    }
}

#[test]
fn explicit_reject_is_failed_but_ambiguous_http_statuses_stay_unknown() {
    let fak = Error::Http {
        status: 400,
        message: FAK_UNFILLED.into(),
    };
    match classify_submit_error(&fak, "0x1".into(), json!({})) {
        SubmitResult::Failed { status, .. } => assert_eq!(status, 400),
        other => panic!("expected Failed, got {other:?}"),
    }
    // 5xx / 408 / 425 / 429 未证明订单没被撮合，记零成交会漏记真实持仓。
    for status in [408u16, 425, 429, 500, 502, 503, 504] {
        let err = Error::Http {
            status,
            message: "bad gateway".into(),
        };
        match classify_submit_error(&err, "0x1".into(), json!({})) {
            SubmitResult::Unknown { message, .. } => {
                assert!(
                    message.contains("bad gateway"),
                    "status {status}: {message}"
                )
            }
            other => panic!("expected Unknown for {status}, got {other:?}"),
        }
    }
}

#[test]
fn transport_error_is_unknown() {
    let err = Error::msg("connection reset");
    match classify_submit_error(&err, "0x1".into(), json!({})) {
        SubmitResult::Unknown { message, .. } => assert!(message.contains("connection reset")),
        other => panic!("expected Unknown, got {other:?}"),
    }
}

#[test]
fn price_change_applies_whole_token_batch_once() {
    let mut books = BookStore::default();
    let now = Instant::now();
    for token in ["a", "b"] {
        apply_ws_message(
            &mut books,
            &json!({
                "event_type":"book", "asset_id":token, "timestamp":"100",
                "tick_size":"0.01",
                "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.50","size":"10"}]
            }),
            now,
        );
    }
    let payload = json!({
        "event_type":"price_change", "timestamp":"101", "price_changes":[
            {"asset_id":"a","side":"BUY","price":"0.40","size":"0"},
            {"asset_id":"b","side":"SELL","price":"0.50","size":"3"},
            {"asset_id":"a","side":"SELL","price":"0.50","size":"0"},
            {"asset_id":"a","side":"BUY","price":"0.41","size":"1"},
            {"asset_id":"a","side":"BUY","price":"0.42","size":"2"},
            {"asset_id":"a","side":"BUY","price":"0.41","size":"4"},
            {"asset_id":"a","side":"SELL","price":"0.53","size":"6"},
            {"asset_id":"a","side":"SELL","price":"0.52","size":"5"}
        ]
    });
    let mut changed = apply_ws_message(&mut books, &payload, now);
    changed.sort();
    assert_eq!(changed, vec![("a".into(), true), ("b".into(), true)]);
    let book = books.get(POLYMARKET, "a").unwrap();
    assert_eq!(
        book.bids
            .iter()
            .map(|l| (l.price, l.size))
            .collect::<Vec<_>>(),
        vec![
            ("0.42".parse().unwrap(), Decimal::from(2)),
            ("0.41".parse().unwrap(), Decimal::from(4))
        ]
    );
    assert_eq!(
        book.asks
            .iter()
            .map(|l| (l.price, l.size))
            .collect::<Vec<_>>(),
        vec![
            ("0.52".parse().unwrap(), Decimal::from(5)),
            ("0.53".parse().unwrap(), Decimal::from(6))
        ]
    );
    assert_eq!(book.tick_size, Some("0.01".parse().unwrap()));
    assert_eq!(
        books.get(POLYMARKET, "b").unwrap().asks[0].size,
        Decimal::from(3)
    );
    let later = now + Duration::from_secs(1);
    for ts in ["99"] {
        let mut replay = payload.clone();
        replay["timestamp"] = json!(ts);
        assert!(apply_ws_message(&mut books, &replay, later).is_empty());
        assert_eq!(books.get(POLYMARKET, "a").unwrap().received_at, now);
    }
    // 一个 token 的无变化批次不能阻止同消息中另一个 token 的有效更新。
    books.replace_snapshot(POLYMARKET, "b", vec![], vec![], 102, later);
    let mut mixed = payload;
    mixed["timestamp"] = json!("102");
    assert_eq!(
        apply_ws_message(&mut books, &mixed, later),
        vec![("b".into(), true)]
    );
}

#[test]
fn disconnected_book_waits_for_full_snapshot_through_ws_and_rest() {
    for use_rest in [false, true] {
        let now = Instant::now();
        let mut books = BookStore::default();
        let mut snapshot = json!({
            "event_type":"book", "asset_id":"t", "timestamp":"100", "tick_size":"0.01",
            "bids":[{"price":"0.40","size":"10"}],
            "asks":[{"price":"0.50","size":"10"}]
        });
        apply_ws_message(&mut books, &snapshot, now);
        books.mark_platform_stale(POLYMARKET);
        let later = now + Duration::from_secs(1);
        apply_ws_message(
            &mut books,
            &json!({
                "event_type":"price_change", "timestamp":"102", "price_changes":[
                    {"asset_id":"t","side":"BUY","price":"0.40","size":"8"}
                ]
            }),
            later,
        );
        assert!(!books
            .get(POLYMARKET, "t")
            .unwrap()
            .is_fresh(Duration::from_secs(5), later));
        snapshot["timestamp"] = json!("101");
        assert!(apply_ws_message(&mut books, &snapshot, later).is_empty());
        assert!(books.get(POLYMARKET, "t").unwrap().stale);
        snapshot["timestamp"] = json!("103");
        snapshot["asks"] = json!([]);
        snapshot["bids"][0]["size"] = json!("8");
        snapshot.as_object_mut().unwrap().remove("tick_size");
        if use_rest {
            let tickets = vec![books.begin_rest(POLYMARKET, "t")];
            assert_eq!(
                apply_rest_books(&mut books, &[snapshot.clone()], &tickets, later),
                (vec!["t".into()], RestBookSkipCounts::default())
            );
        } else {
            assert_eq!(
                apply_ws_message(&mut books, &snapshot, later),
                vec![("t".into(), true)]
            );
        }
        let book = books.get(POLYMARKET, "t").unwrap();
        assert!(book.is_fresh(Duration::from_secs(5), later));
        assert!(book.asks.is_empty());
        assert_eq!(book.bids[0].size, Decimal::from(8));
        assert_eq!(book.tick_size, Some("0.01".parse().unwrap()));
        let initialized = apply_ws_message(&mut books, &snapshot, later);
        assert!(initialized.is_empty());
        // REST 已恢复同一个完整底本，重复 WS 全量不重算；后续明确增量保留空卖侧。
        for ts in [104, 105] {
            assert_eq!(
                apply_ws_message(
                    &mut books,
                    &json!({
                        "event_type":"price_change", "timestamp":ts.to_string(), "price_changes":[
                            {"asset_id":"t","side":"BUY","price":"0.40","size":ts.to_string()}
                        ]
                    }),
                    later
                ),
                vec![("t".into(), true)]
            );
            let book = books.get(POLYMARKET, "t").unwrap();
            assert!(!book.stale);
            assert!(book.asks.is_empty());
            assert_eq!(book.bids[0].size, Decimal::from(ts));
        }
    }
}

#[test]
fn malformed_ws_payload_invalidates_identified_token_atomically() {
    let now = Instant::now();
    let valid = json!({"event_type":"book", "asset_id":"t", "timestamp":"100",
            "bids":[], "asks":[{"price":"0.5","size":"3"}]});
    for bad in [
        json!({"event_type":"book", "asset_id":"t", "timestamp":"bad", "bids":[], "asks":[]}),
        json!({"event_type":"book", "asset_id":"t", "timestamp":"101", "bids":[]}),
        json!({"event_type":"price_change", "timestamp":"bad", "price_changes":[{"asset_id":"t","side":"BUY","price":"0.4","size":"1"}]}),
        json!({"event_type":"price_change", "timestamp":"101", "price_changes":[
                {"asset_id":"t","side":"SELL","price":"0.5","size":"0"},
                {"asset_id":"t","side":"unexpected","price":"0.4","size":"1"}]}),
        json!({"event_type":"price_change", "timestamp":"101", "price_changes":[{"asset_id":"t","side":"BUY","price":"0.4","size":"-1"}]}),
    ] {
        let mut books = BookStore::default();
        apply_ws_message(&mut books, &valid, now);
        assert!(apply_ws_message(&mut books, &bad, now).is_empty());
        let book = books.get_at(POLYMARKET, "t", now).unwrap();
        assert!(book.stale);
        assert_eq!(book.exchange_ts_ms, 100);
        assert_eq!(book.asks[0].size, Decimal::from(3));
    }
}

#[test]
fn strict_book_parser_rejects_incomplete_identity_timestamp_and_levels() {
    let valid =
        json!({"asset_id":"t", "timestamp":"100", "bids":[], "asks":[{"price":"0.5","size":"3"}]});
    for (field, bad) in [
        ("timestamp", json!(null)),
        ("timestamp", json!("invalid")),
        ("timestamp", json!(0)),
        ("timestamp", json!(-1)),
        ("timestamp", json!(1.5)),
        ("asset_id", json!("")),
        ("asks", json!(null)),
        ("asks", json!([{"price":"0.5"}])),
        ("asks", json!([{"price":"0.5","size":"-1"}])),
        ("asks", json!([{"price":"1.1","size":"1"}])),
        (
            "asks",
            json!([{"price":"0.5","size":"1"},{"price":"0.5","size":"2"}]),
        ),
        ("tick_size", json!("oops")),
    ] {
        let mut payload = valid.clone();
        payload[field] = bad;
        assert!(parse_book_json(&payload).is_err(), "field={field}");
    }
    assert!(validate_book_identity(&valid, "other").is_err());
    assert!(parse_book_json(&valid).is_ok());
}

#[test]
fn batch_rest_checks_requested_identity_and_each_ticket_independently() {
    let now = Instant::now();
    let mut books = BookStore::default();
    let tickets = ["a", "b", "missing"].map(|t| books.begin_rest(POLYMARKET, t));
    books.set_tick_size(POLYMARKET, "a", Decimal::new(1, 2));
    let payloads: Vec<_> = ["a", "b", "unsolicited"]
        .map(|t| json!({"asset_id":t,"timestamp":"101","bids":[],"asks":[]}))
        .into();
    let (accepted, rejected) = apply_rest_books(&mut books, &payloads, &tickets, now);
    assert_eq!(accepted, vec!["b"]);
    assert_eq!(
        rejected,
        RestBookSkipCounts {
            no_ticket: 1,
            revision_changed: 1,
            ..Default::default()
        }
    );
    assert!(books.get(POLYMARKET, "unsolicited").is_none());
    assert!(books.get(POLYMARKET, "missing").is_none());
    assert_eq!(
        apply_rest_books(&mut books, &payloads, &tickets, now).1,
        RestBookSkipCounts {
            no_ticket: 1,
            revision_changed: 2,
            ..Default::default()
        }
    );
}

#[test]
fn rest_skip_counts_map_every_book_rejection_and_total() {
    use crate::book::BookReject;
    for (reason, expected) in [
        (
            BookReject::InvalidPayload,
            RestBookSkipCounts {
                invalid_payload: 1,
                ..Default::default()
            },
        ),
        (
            BookReject::OlderTimestamp,
            RestBookSkipCounts {
                older_timestamp: 1,
                ..Default::default()
            },
        ),
        (
            BookReject::TimestampConflict,
            RestBookSkipCounts {
                timestamp_conflict: 1,
                ..Default::default()
            },
        ),
        (
            BookReject::EpochChanged,
            RestBookSkipCounts {
                epoch_changed: 1,
                ..Default::default()
            },
        ),
        (
            BookReject::RevisionChanged,
            RestBookSkipCounts {
                revision_changed: 1,
                ..Default::default()
            },
        ),
    ] {
        let mut counts = RestBookSkipCounts::default();
        counts.record_rejection(reason);
        assert_eq!(counts, expected);
        assert_eq!(counts.total(), 1);
    }
    assert_eq!(RestBookSkipCounts::default().total(), 0);
    assert_eq!(
        RestBookSkipCounts {
            missing_token: 1,
            no_ticket: 2,
            parse_error: 3,
            invalid_payload: 4,
            older_timestamp: 5,
            timestamp_conflict: 6,
            epoch_changed: 7,
            revision_changed: 8,
        }
        .total(),
        36
    );
}

#[test]
fn rest_skip_counts_preserve_reachable_branch_precedence() {
    let now = Instant::now();
    let mut books = BookStore::default();
    for token in ["old", "conflict"] {
        books.replace_snapshot(POLYMARKET, token, vec![], vec![], 200, now);
    }
    let tickets = ["bad", "old", "conflict", "epoch", "revision", "ok"]
        .map(|token| books.begin_rest(POLYMARKET, token));
    let mut tickets = tickets.to_vec();
    tickets
        .iter_mut()
        .find(|t| t.key.token_id == "epoch")
        .unwrap()
        .epoch += 1;
    books.set_tick_size(POLYMARKET, "revision", Decimal::new(1, 2));
    let mut foreign = books.begin_rest("other_platform", "foreign");
    foreign.revision += 1;
    tickets.push(foreign);
    // 身份、票据、解析先于 accept_rest；无效载荷已由严格解析器拦截。
    let payloads = vec![
        json!({"timestamp":"bad"}),
        json!({"asset_id":"unsolicited","timestamp":"bad"}),
        json!({"asset_id":"foreign","timestamp":"bad"}),
        json!({"asset_id":"bad","timestamp":"bad"}),
        json!({"asset_id":"epoch","timestamp":"bad"}),
        json!({"asset_id":"old","timestamp":"100","bids":[],"asks":[]}),
        json!({"asset_id":"conflict","timestamp":"200","bids":[],"asks":[{"price":"0.5","size":"1"}]}),
        json!({"asset_id":"epoch","timestamp":"100","bids":[],"asks":[]}),
        json!({"asset_id":"revision","timestamp":"100","bids":[],"asks":[]}),
        json!({"asset_id":"ok","timestamp":"100","bids":[],"asks":[]}),
    ];
    let (applied, skipped) = apply_rest_books(&mut books, &payloads, &tickets, now);
    assert_eq!(applied, vec!["ok"]);
    assert_eq!(
        skipped,
        RestBookSkipCounts {
            missing_token: 1,
            no_ticket: 2,
            parse_error: 2,
            older_timestamp: 1,
            timestamp_conflict: 1,
            epoch_changed: 1,
            revision_changed: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        applied.len() as u64 + skipped.total(),
        payloads.len() as u64
    );
    assert!(books.get(POLYMARKET, "conflict").unwrap().asks.is_empty());
    assert!(books.get(POLYMARKET, "epoch").is_none());
}

#[test]
fn rest_skip_counts_count_returned_payloads_not_missing_responses() {
    let now = Instant::now();
    let mut books = BookStore::default();
    let tickets = ["a", "not_returned"].map(|token| books.begin_rest(POLYMARKET, token));
    assert_eq!(
        apply_rest_books(&mut books, &[], &tickets, now),
        (vec![], RestBookSkipCounts::default())
    );
    assert_eq!(
        apply_rest_books(&mut books, &[], &[], now),
        (vec![], RestBookSkipCounts::default())
    );
    let payload = json!({"asset_id":"a","timestamp":"100","bids":[],"asks":[]});
    assert_eq!(
        apply_rest_books(&mut books, &[payload.clone()], &[], now),
        (
            vec![],
            RestBookSkipCounts {
                no_ticket: 1,
                ..Default::default()
            }
        )
    );
    assert_eq!(
        apply_rest_books(&mut books, &[payload.clone(), payload], &tickets, now),
        (
            vec!["a".into()],
            RestBookSkipCounts {
                revision_changed: 1,
                ..Default::default()
            }
        )
    );
    assert!(books.get(POLYMARKET, "not_returned").is_none());
}

#[tokio::test]
async fn frame_array_preserves_same_millisecond_set_delete_order() {
    let books = Arc::new(Mutex::new(BookStore::default()));
    let (tx, _rx) = mpsc::channel(10);
    let frame = json!([
        {"event_type":"book","asset_id":"t","timestamp":"100","bids":[],"asks":[{"price":"0.5","size":"3"}]},
        {"event_type":"price_change","timestamp":"100","price_changes":[{"asset_id":"t","side":"SELL","price":"0.5","size":"2"}]},
        {"event_type":"price_change","timestamp":"100","price_changes":[{"asset_id":"t","side":"SELL","price":"0.5","size":"0"}]},
        {"event_type":"book","asset_id":"t","timestamp":"100","bids":[],"asks":[{"price":"0.5","size":"3"}]}
    ]);
    // 先验证增量删除顺序，再验证同帧尾部全量按接收顺序接管。
    let prefix = Value::Array(frame.as_array().unwrap()[..3].to_vec());
    handle_ws_text(&prefix.to_string(), &books, &tx).await;
    assert!(books
        .lock()
        .await
        .get(POLYMARKET, "t")
        .unwrap()
        .asks
        .is_empty());
    handle_ws_text(&frame.to_string(), &books, &tx).await;
    let books = books.lock().await;
    let book = books.get(POLYMARKET, "t").unwrap();
    assert_eq!(book.asks.len(), 1);
    assert_eq!(book.asks[0].size, Decimal::from(3));
    assert!(!book.stale);
}

#[test]
fn tick_200_blocks_new_ticket_rest_150_with_or_without_tick() {
    for tick in [None, Some("0.01")] {
        let now = Instant::now();
        let mut books = BookStore::default();
        apply_ws_message(
            &mut books,
            &json!({
                "event_type":"book", "asset_id":"t", "timestamp":"100",
                "bids":[], "asks":[], "tick_size":"0.01"
            }),
            now,
        );
        apply_ws_message(
            &mut books,
            &json!({
                "event_type":"tick_size_change", "asset_id":"t", "timestamp":"200",
                "new_tick_size":"0.001"
            }),
            now,
        );
        let ticket = books.begin_rest(POLYMARKET, "t");
        assert_eq!(
            books
                .accept_rest(
                    &ticket,
                    vec![],
                    vec![],
                    150,
                    now,
                    tick.map(|v| v.parse().unwrap())
                )
                .unwrap_err(),
            crate::book::BookReject::OlderTimestamp
        );
        assert_eq!(
            books.get(POLYMARKET, "t").unwrap().tick_size,
            Some("0.001".parse().unwrap())
        );
    }
}

#[test]
fn ws_and_batch_rest_share_tick_ordering_and_atomic_conflict_rules() {
    let now = Instant::now();
    let mut books = BookStore::default();
    apply_ws_message(
        &mut books,
        &json!({"event_type":"book","asset_id":"t","timestamp":"100","bids":[],"asks":[],"tick_size":"0.01"}),
        now,
    );
    apply_ws_message(
        &mut books,
        &json!({"event_type":"tick_size_change","asset_id":"t","timestamp":"200","new_tick_size":"0.001"}),
        now,
    );
    for tick in [None, Some("0.01")] {
        let mut old =
            json!({"event_type":"book","asset_id":"t","timestamp":"150","bids":[],"asks":[]});
        if let Some(tick) = tick {
            old["tick_size"] = json!(tick);
        }
        assert!(apply_ws_message(&mut books, &old, now).is_empty());
        let ticket = books.begin_rest(POLYMARKET, "t");
        assert_eq!(
            apply_rest_books(&mut books, &[old], &[ticket], now),
            (
                vec![],
                RestBookSkipCounts {
                    older_timestamp: 1,
                    ..Default::default()
                }
            )
        );
        assert_eq!(books.tick_size(POLYMARKET, "t"), Some(Decimal::new(1, 3)));
        assert_eq!(books.get(POLYMARKET, "t").unwrap().exchange_ts_ms, 100);
    }
    apply_ws_message(
        &mut books,
        &json!({"event_type":"tick_size_change","asset_id":"t","timestamp":"150","new_tick_size":"0.01"}),
        now,
    );
    assert!(!books.get(POLYMARKET, "t").unwrap().stale);
    let conflict = json!({"event_type":"book","asset_id":"t","timestamp":"200","bids":[],"asks":[{"price":"0.5","size":"3"}],"tick_size":"0.01"});
    assert!(apply_ws_message(&mut books, &conflict, now).is_empty());
    let book = books.get(POLYMARKET, "t").unwrap();
    assert!(book.asks.is_empty());
    assert!(!book.stale);
    assert_eq!(book.tick_size, None);
    let ticket = books.begin_rest(POLYMARKET, "t");
    let restored =
        json!({"asset_id":"t","timestamp":"201","bids":[],"asks":[],"tick_size":"0.001"});
    assert_eq!(
        apply_rest_books(&mut books, &[restored], &[ticket], now),
        (vec!["t".into()], RestBookSkipCounts::default())
    );
    assert_eq!(books.tick_size(POLYMARKET, "t"), Some(Decimal::new(1, 3)));
}

#[test]
fn stores_tick_size_from_book_payload() {
    let mut books = BookStore::default();
    let now = Instant::now();
    apply_ws_message(
        &mut books,
        &json!({
            "event_type": "book",
            "asset_id": "t1",
            "timestamp": "100",
            "tick_size": "0.001",
            "bids": [{"price": "0.45", "size": "10"}],
            "asks": [{"price": "0.46", "size": "8"}]
        }),
        now,
    );
    assert_eq!(
        books.get(POLYMARKET, "t1").unwrap().tick_size,
        Some(Decimal::from_str("0.001").unwrap())
    );
    apply_ws_message(
        &mut books,
        &json!({
            "event_type": "tick_size_change",
            "asset_id": "t1",
            "old_tick_size": "0.001",
            "new_tick_size": "0.01",
            "timestamp": "101"
        }),
        now,
    );
    assert_eq!(
        books.get(POLYMARKET, "t1").unwrap().tick_size,
        Some(Decimal::from_str("0.01").unwrap())
    );
}

#[test]
fn leaves_tick_size_empty_when_book_omits_field() {
    let mut books = BookStore::default();
    apply_ws_message(
        &mut books,
        &json!({
            "event_type": "book",
            "asset_id": "t1",
            "timestamp": "100",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.41", "size": "8"}]
        }),
        Instant::now(),
    );
    assert_eq!(books.get(POLYMARKET, "t1").unwrap().tick_size, None);
}

#[test]
fn applies_rest_books_and_skips_older_snapshot() {
    let mut books = BookStore::default();
    let now = Instant::now();
    books.replace_snapshot(
        POLYMARKET,
        "t1",
        vec![],
        vec![Level {
            price: Decimal::from_str("0.40").unwrap(),
            size: Decimal::from_str("10").unwrap(),
        }],
        200,
        now,
    );
    let tickets = vec![
        books.begin_rest(POLYMARKET, "t1"),
        books.begin_rest(POLYMARKET, "t2"),
    ];
    let (applied, skipped_old) = apply_rest_books(
        &mut books,
        &[
            json!({
                "asset_id": "t1",
                "timestamp": "100",
                "tick_size": "0.001",
                "bids": [],
                "asks": [{"price": "0.99", "size": "1"}]
            }),
            json!({
                "asset_id": "t2",
                "timestamp": "150",
                "tick_size": "0.01",
                "bids": [{"price": "0.45", "size": "10"}],
                "asks": [{"price": "0.46", "size": "8"}]
            }),
        ],
        &tickets,
        now,
    );
    assert_eq!(applied, vec!["t2".to_string()]);
    assert_eq!(
        skipped_old,
        RestBookSkipCounts {
            older_timestamp: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        books.get(POLYMARKET, "t1").unwrap().asks[0].price,
        Decimal::from_str("0.40").unwrap()
    );
    assert_eq!(
        books.get(POLYMARKET, "t2").unwrap().tick_size,
        Some(Decimal::from_str("0.01").unwrap())
    );
    assert_eq!(
        books.get(POLYMARKET, "t2").unwrap().asks[0].price,
        Decimal::from_str("0.46").unwrap()
    );
}

#[test]
fn status_warnings_are_rate_limited_per_operation() {
    let order = AtomicU64::new(0);
    let trade = AtomicU64::new(0);
    assert!(status_warning_due(&order, 100));
    assert!(status_warning_due(&trade, 100));
    for now in [100, 102, 130, 159, 99] {
        assert!(!status_warning_due(&order, now), "now={now}");
        assert!(!status_warning_due(&trade, now), "now={now}");
    }
    assert!(status_warning_due(&order, 160));
    assert!(!status_warning_due(&order, 160));
    assert!(status_warning_due(&trade, 160));
}

#[test]
fn concurrent_status_warnings_have_one_winner() {
    let last_warn_secs = AtomicU64::new(0);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    status_warning_due(&last_warn_secs, 100)
                })
            })
            .collect();
        let winners = threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();
        assert_eq!(winners, 1);
    });
}

#[test]
fn status_log_value_is_bounded_and_never_serializes_a_body() {
    let long_status = "状".repeat(100);
    assert_eq!(status_log_value(Some(&json!(long_status))), "状".repeat(80));
    assert_eq!(status_log_value(None), "<missing>");
    for invalid in [
        Value::Null,
        json!(true),
        json!({"status": "body"}),
        json!(["body"]),
    ] {
        assert_eq!(status_log_value(Some(&invalid)), "<invalid>");
    }
}

#[test]
fn order_status_matrix_normalizes_only_known_full_values_and_preserves_raw() {
    for (bare, prefixed, expected) in [
        ("MATCHED", "ORDER_STATUS_MATCHED", "matched"),
        ("LIVE", "ORDER_STATUS_LIVE", "live"),
        ("INVALID", "ORDER_STATUS_INVALID", "invalid"),
        ("CANCELED", "ORDER_STATUS_CANCELED", "cancelled"),
        (
            "CANCELED_MARKET_RESOLVED",
            "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
            "cancelled",
        ),
    ] {
        for status in [bare, prefixed] {
            for status in [status.to_string(), status.to_ascii_lowercase()] {
                let raw = json!({"id": "remote-oid", "status": status});
                let poll = parse_order_poll(raw.clone(), "requested-oid");
                assert!(poll.found);
                assert_eq!(poll.status, expected, "status={status}");
                assert_eq!(normalized_order_status(&status), Some(expected));
                assert_eq!(poll.raw, raw);
            }
        }
    }
    for status in ["cancelled", "canceled", "expired", "unmatched", "rejected"] {
        for status in [status.to_string(), status.to_ascii_uppercase()] {
            let raw = json!({"status": status});
            let poll = parse_order_poll(raw.clone(), "oid-1");
            assert_eq!(poll.status, "cancelled", "status={status}");
            assert_eq!(normalized_order_status(&status), Some("cancelled"));
            assert_eq!(poll.raw, raw);
        }
    }
}

#[test]
fn order_status_matrix_preserves_unknown_and_invalid_values() {
    for status in [
        json!("UNKNOWN"),
        json!("ORDER_STATUS_UNKNOWN"),
        json!("FUTURE_CANCELED"),
        json!("NOT_CANCELLED"),
        json!("ORDER_STATUS_REJECTED_FUTURE"),
        json!("ORDER_STATUS_REJECTED"),
        json!("ORDER_STATUS_EXPIRED"),
        json!("ORDER_STATUS_UNMATCHED"),
        json!("ORDER_STATUS_CANCELLED"),
        json!("ORDER_STATUS_ORDER_STATUS_MATCHED"),
        json!("TRADE_STATUS_CONFIRMED"),
        json!(" MATCHED "),
        json!(""),
        Value::Null,
        json!(true),
        json!(17),
        json!({"status": "MATCHED"}),
        json!(["MATCHED"]),
    ] {
        let raw = json!({"status": status});
        let poll = parse_order_poll(raw.clone(), "oid-1");
        assert!(poll.found);
        assert_eq!(poll.status, status.as_str().unwrap_or_default());
        assert_eq!(normalized_order_status(&poll.status), None);
        assert_eq!(poll.raw, raw);
    }
    let raw = json!({"id": "oid-1"});
    let poll = parse_order_poll(raw.clone(), "oid-1");
    assert!(poll.status.is_empty());
    assert_eq!(poll.raw, raw);
}

#[test]
fn parse_order_poll_keeps_identity_and_matched_quantity_separate() {
    let raw = json!({
        "id": "remote-oid", "status": "MATCHED", "asset_id": "yes",
        "original_size": "20", "size_matched": "3", "price": "0.4",
        "associate_trades": ["trade-1", "trade-2"]
    });
    let poll = parse_order_poll(raw.clone(), "requested-oid");
    assert_eq!(poll.order_id.as_deref(), Some("remote-oid"));
    assert_eq!(poll.coin.as_deref(), Some("yes"));
    assert_eq!(poll.shares, Some(d("3")));
    assert_eq!(poll.original_shares, Some(d("20")));
    assert_eq!(poll.remaining_shares, Some(d("17")));
    assert_eq!(poll.associated_trades, ["trade-1", "trade-2"]);
    assert!(poll.client_order_id.is_none());
    assert_eq!(poll.raw, raw);
    let empty = parse_order_poll(
        json!({"status": "MATCHED", "original_size": "20", "price": "0.4"}),
        "oid-1",
    );
    assert_eq!(empty.order_id.as_deref(), Some("oid-1"));
    assert!(empty.shares.is_none());
    assert!(empty.remaining_shares.is_none());
    assert!(empty.associated_trades.is_empty());
    assert!(empty.raw.get("associate_trades").is_none());
    let explicit_empty = parse_order_poll(json!({"associate_trades": []}), "oid-1");
    assert_eq!(explicit_empty.raw["associate_trades"], json!([]));
    let invalid = parse_order_poll(
        json!({
            "original_size": "2", "size_matched": "3", "associate_trades": ["t1", null]
        }),
        "oid-1",
    );
    assert!(invalid.remaining_shares.is_none());
    assert!(invalid.associated_trades.is_empty());
    assert_eq!(invalid.raw["associate_trades"], json!(["t1", null]));
}

fn trade_fixture(id: &str) -> Value {
    json!({
        "id": id, "taker_order_id": "taker-1", "asset_id": "yes",
        "size": "5", "price": "0.4", "side": "BUY", "outcome": "Yes",
        "status": "CONFIRMED", "fee_rate_bps": "700", "maker_orders": []
    })
}

#[test]
fn trade_status_matrix_requires_explicit_confirmation() {
    for (status, expected) in [
        (json!("CONFIRMED"), FillFinality::Confirmed),
        (json!("TRADE_STATUS_CONFIRMED"), FillFinality::Confirmed),
        (json!("FAILED"), FillFinality::Failed),
        (json!("TRADE_STATUS_FAILED"), FillFinality::Failed),
        (json!("MATCHED"), FillFinality::Pending),
        (json!("TRADE_STATUS_MATCHED"), FillFinality::Pending),
        (json!("MATCHED_NOT_BROADCASTED"), FillFinality::Pending),
        (
            json!("TRADE_STATUS_MATCHED_NOT_BROADCASTED"),
            FillFinality::Pending,
        ),
        (json!("MINED"), FillFinality::Pending),
        (json!("TRADE_STATUS_MINED"), FillFinality::Pending),
        (json!("RETRYING"), FillFinality::Pending),
        (json!("TRADE_STATUS_RETRYING"), FillFinality::Pending),
        (json!("UNKNOWN"), FillFinality::Pending),
        (json!("TRADE_STATUS_UNKNOWN"), FillFinality::Pending),
        (json!("FUTURE_CONFIRMED"), FillFinality::Pending),
        (json!("NOT_FAILED"), FillFinality::Pending),
        (
            json!("TRADE_STATUS_TRADE_STATUS_CONFIRMED"),
            FillFinality::Pending,
        ),
        (json!("ORDER_STATUS_CONFIRMED"), FillFinality::Pending),
        (json!("confirmed"), FillFinality::Pending),
        (json!("trade_status_confirmed"), FillFinality::Pending),
        (json!(" CONFIRMED "), FillFinality::Pending),
        (json!(""), FillFinality::Pending),
        (Value::Null, FillFinality::Pending),
        (json!(true), FillFinality::Pending),
        (json!(17), FillFinality::Pending),
        (json!({"status": "CONFIRMED"}), FillFinality::Pending),
        (json!(["CONFIRMED"]), FillFinality::Pending),
    ] {
        let mut trade = trade_fixture("t1");
        trade["status"] = status.clone();
        trade["maker_orders"] = json!([{
            "order_id": "maker-1", "asset_id": "yes",
            "matched_amount": "5", "price": "0.4"
        }]);
        let fills = parse_trades(&json!([trade])).unwrap();
        assert_eq!(fills.len(), 2);
        for fill in fills {
            assert_eq!(fill.finality, expected, "status={status}");
            assert_eq!(fill.raw["status"], status);
            assert_eq!(fill.fee, None);
            assert_eq!(fill.fee_token, None);
        }
    }
    let mut missing = trade_fixture("missing-status");
    missing.as_object_mut().unwrap().remove("status");
    assert_eq!(
        parse_trades(&json!([missing])).unwrap()[0].finality,
        FillFinality::Pending
    );
}

#[test]
fn parse_trades_assigns_each_maker_only_its_own_size_asset_and_fee() {
    let mut trade = trade_fixture("t1");
    trade["fee_amount"] = json!("0.01");
    trade["fee_token"] = json!("pUSD");
    trade["maker_orders"] = json!([
        {"order_id": "maker-1", "owner": "same-user", "matched_amount": "2",
         "price": "0.4", "asset_id": "yes", "outcome": "Yes", "side": "SELL",
         "fee_rate_bps": "0", "fee_amount": "0", "feeToken": "pUSD"},
        {"order_id": "maker-2", "owner": "same-user", "matched_amount": "3",
         "price": "0.6", "asset_id": "no", "outcome": "No", "fee_rate_bps": "700"}
    ]);
    let fills = parse_trades(&json!({"data": [trade.clone()]})).unwrap();
    assert_eq!(fills.len(), 3);
    assert_eq!(fills[0].shares, d("5"));
    assert_eq!(fills[0].raw["role"], "taker");
    assert!(fills[0].matches(Some("taker-1"), None));
    assert!(!fills[0].matches(Some("maker-1"), None));
    assert_eq!(fills[0].fee, Some(d("0.01")));
    assert_eq!(fills[0].fee_token.as_deref(), Some("pUSD"));
    assert_eq!(fills[1].shares, d("2"));
    assert_eq!(fills[1].coin.as_deref(), Some("yes"));
    assert_eq!(fills[1].raw["side"], "SELL");
    assert_eq!(fills[1].raw["role"], "maker");
    assert_eq!(fills[1].fee, Some(Decimal::ZERO));
    assert_eq!(fills[1].fee_token.as_deref(), Some("pUSD"));
    assert!(fills[1].matches(Some("maker-1"), None));
    assert!(!fills[1].matches(Some("maker-2"), None));
    assert!(!fills[1].matches(Some("taker-1"), None));
    assert_eq!(fills[2].shares, d("3"));
    assert_eq!(fills[2].price, d("0.6"));
    assert_eq!(fills[2].coin.as_deref(), Some("no"));
    assert_eq!(fills[2].raw["outcome"], "No");
    assert!(fills[2].raw.get("side").is_none());
    assert_eq!(fills[2].fee, None);
    assert!(fills.iter().all(|fill| fill.fee_rate_bps.is_none()));
    assert_eq!(fills[2].raw["fee_rate_bps"], "700");
    assert_eq!(fills[2].fee_token, None);
    assert_eq!(fills[2].raw["taker_trade"], trade);
    assert_eq!(fills[2].raw["maker_order"], trade["maker_orders"][1]);
    for fill in fills {
        assert_eq!(fill.trade_id, "t1");
        assert_eq!(fill.order_ids.len(), 1);
        assert_eq!(fill.finality, FillFinality::Confirmed);
    }
}

#[test]
fn parse_trades_ignores_untrusted_fee_rates_but_preserves_raw() {
    for value in [
        json!("broken"),
        json!("-1"),
        json!("700"),
        json!(true),
        json!({"rate": 700}),
        json!([700]),
        Value::Null,
    ] {
        let mut trade = trade_fixture("untrusted-rate");
        trade["fee_rate_bps"] = value.clone();
        trade["maker_orders"] = json!([{
            "order_id": "maker-1", "asset_id": "yes",
            "matched_amount": "5", "price": "0.4", "fee_rate_bps": value
        }]);
        let fills = parse_trades(&json!([trade])).unwrap();
        assert_eq!(fills.len(), 2);
        for fill in fills {
            assert_eq!(fill.fee_rate_bps, None);
            assert_eq!(fill.fee, None);
            assert_eq!(fill.raw["fee_rate_bps"], value);
            if fill.raw["role"] == "maker" {
                assert_eq!(fill.raw["maker_order"]["fee_rate_bps"], value);
            }
        }
    }
    for (field, value) in [
        ("order_id", json!("")),
        ("asset_id", json!(null)),
        ("matched_amount", json!("broken")),
        ("price", json!("1.1")),
        ("fee_amount", json!("broken")),
        ("fee", json!("-1")),
        ("fee_token", json!(false)),
    ] {
        let mut trade = trade_fixture("invalid-maker");
        trade["maker_orders"] = json!([{
            "order_id": "maker-1", "asset_id": "yes", "matched_amount": "5",
            "price": "0.4", "fee_rate_bps": "broken"
        }]);
        trade["maker_orders"][0][field] = value;
        assert!(parse_trades(&json!([trade])).is_err(), "{field}");
    }
}

#[test]
fn parse_trades_rejects_malformed_records_instead_of_skipping_them() {
    for field in [
        "id",
        "taker_order_id",
        "asset_id",
        "size",
        "price",
        "maker_orders",
    ] {
        let mut bad = trade_fixture("bad");
        bad.as_object_mut().unwrap().remove(field);
        assert!(
            parse_trades(&json!([trade_fixture("good"), bad])).is_err(),
            "{field}"
        );
    }
    for (field, value) in [
        ("size", json!("invalid")),
        ("size", json!("0")),
        ("price", json!("1.1")),
        ("price", json!("-1")),
        ("fee_amount", json!("broken")),
        ("fee_amount", json!("-1")),
        ("fee", json!("broken")),
        ("fee_token", json!(5)),
        ("maker_orders", Value::Null),
    ] {
        let mut bad = trade_fixture("bad");
        bad[field] = value;
        assert!(parse_trades(&json!([bad])).is_err(), "{field}");
    }
    let mut bad_maker = trade_fixture("maker-error");
    bad_maker["maker_orders"] = json!([{"order_id": "maker-1"}]);
    assert!(parse_trades(&json!([bad_maker])).is_err());
    for bad_page in [Value::Null, json!({}), json!({"data": {}}), json!([null])] {
        assert!(parse_trades(&bad_page).is_err());
    }
    assert!(parse_trades(&json!({"data": []})).unwrap().is_empty());
}

pub(crate) async fn poll_stub(
    responses: Vec<(u16, Value)>,
) -> (PolymarketVenue, tokio::task::JoinHandle<Vec<String>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
                .await
                .expect("local stub request timed out")
                .unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut buffer = [0u8; 1024];
                let count = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
                if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                assert!(bytes.len() < 16_384);
            }
            // 不保留/输出认证头；这里只核对方法、路径和查询参数。
            requests.push(
                String::from_utf8(bytes)
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .to_string(),
            );
            let body = body.to_string();
            let response = format!(
                    "HTTP/1.1 {status} Stub\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    let mut venue = cache_test_venue();
    venue.base = format!("http://{address}");
    venue.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    venue.auth_ttl = Duration::ZERO;
    venue.authed.lock().await.insert(
        "test-funder".into(),
        PolymarketAccount {
            funder: "test-funder".into(),
            service: None,
            signature_type: 2,
            signer: PrivateKeySigner::random(),
            api_key: "stub-key".into(),
            api_secret: "dGVzdA==".into(),
            api_passphrase: "stub-passphrase".into(),
            created_at: unix_secs(),
        },
    );
    (venue, server)
}

#[tokio::test]
async fn books_http_failures_keep_safe_root_causes() {
    use tokio::io::AsyncReadExt;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    for case in [
        "timeout",
        "body_timeout",
        "connect",
        "body",
        "decode",
        "status",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = if case == "connect" {
            drop(listener);
            None
        } else {
            Some(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = [0; 4096];
                socket.read(&mut buffer).await.unwrap();
                match case {
                    "timeout" => tokio::time::sleep(Duration::from_secs(2)).await,
                    "body_timeout" => {
                        socket
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n")
                            .await
                            .unwrap();
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    "body" => socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\nsecret-payload")
                        .await
                        .unwrap(),
                    "decode" | "status" => {
                        let status = if case == "status" { 503 } else { 200 };
                        let body = "secret-payload";
                        socket.write_all(format!("HTTP/1.1 {status} Stub\r\nContent-Length: {}\r\nX-Secret: secret-header\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    }
                    _ => unreachable!(),
                }
            }))
        };
        let mut venue = cache_test_venue();
        venue.base =
            format!("http://secret-user:secret-password@{address}/secret-path?token=secret-query");
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let subscriber = tracing_subscriber::registry()
            .with(WsLogCapture(tx).with_filter(tracing_subscriber::filter::LevelFilter::WARN));
        let error = venue
            .rest_books(&["secret-token".into()])
            .with_subscriber(subscriber)
            .await
            .unwrap_err();
        let message = error.to_string();
        let event = rx.try_recv().unwrap();
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(event.fields["interface"], "/books");
        let phase = match case {
            "timeout" | "connect" => "send",
            "decode" => "decode",
            _ => "read_body",
        };
        assert_eq!(event.fields["phase"], phase);
        match case {
            "timeout" | "body_timeout" => {
                assert!(message.contains("kind=timeout"), "{message}")
            }
            "connect" => {
                assert!(message.contains("kind=connect"), "{message}");
                assert!(message.contains("ConnectionRefused"), "{message}");
            }
            // reqwest 的 text/bytes 路径会将部分读取错误标为 decode，而非 body。
            "body" => assert!(
                message.contains("decode=true") || message.contains("body=true"),
                "{message}"
            ),
            _ => assert!(message.contains(&format!("kind={case}")), "{message}"),
        }
        assert_eq!(event.fields["error"], message);
        let all_output = format!("{error:?} {event:?}");
        assert!(!all_output.contains("secret-"), "{all_output}");
        if let Some(server) = server {
            if case.contains("timeout") {
                server.abort();
                assert!(server.await.unwrap_err().is_cancelled());
            } else {
                server.await.unwrap();
            }
        }
    }
}

// 同时捕获 span，验证 HTTP 事件继承 order_hash 和执行层 leg_id。
struct SubmitLogCapture(mpsc::UnboundedSender<WsLogEvent>);

impl<S> tracing_subscriber::Layer<S> for SubmitLogCapture
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut captured = WsLogEvent {
            level: *attrs.metadata().level(),
            fields: HashMap::new(),
        };
        attrs.record(&mut captured);
        ctx.span(id).unwrap().extensions_mut().insert(captured);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut captured = WsLogEvent {
            level: *event.metadata().level(),
            fields: HashMap::new(),
        };
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(values) = span.extensions().get::<WsLogEvent>() {
                    captured.fields.extend(values.fields.clone());
                }
            }
        }
        event.record(&mut captured);
        let _ = self.0.send(captured);
    }
}

const SUBMIT_LOG_SECRET: &str = "PRIVATE_SUBMIT_SENTINEL";

async fn capture_submit_response(
    status: Option<u16>,
    body: &str,
    declared_length: Option<usize>,
    hold_open: bool,
) -> (
    SubmitResult,
    super::super::SubmissionResponse,
    Vec<WsLogEvent>,
) {
    use tokio::io::AsyncReadExt;
    use tracing::instrument::WithSubscriber;
    use tracing::Instrument;
    use tracing_subscriber::prelude::*;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response = status.map(|status| format!(
            "HTTP/1.1 {status} Stub\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            declared_length.unwrap_or(body.len())
        ));
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut buffer = [0u8; 1024];
            let count = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                assert!(headers.starts_with("POST /order HTTP/1.1"));
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break;
                }
            }
            assert!(bytes.len() < 16_384);
        }
        if let Some(response) = response {
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        if hold_open {
            let _ = release_rx.await;
        }
    });
    let (mut venue, funder) = execution_test_venue(format!("http://{address}")).await;
    venue.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(250))
        .build()
        .unwrap();
    {
        let mut accounts = venue.authed.lock().await;
        let account = accounts.get_mut(&funder).unwrap();
        account.api_key = SUBMIT_LOG_SECRET.into();
        account.api_passphrase = SUBMIT_LOG_SECRET.into();
    }
    let prepared = PreparedOrder {
        order_hash: format!("0x{}", "a".repeat(64)),
        envelope: json!({"signature": SUBMIT_LOG_SECRET}),
        payload: json!({"signature": SUBMIT_LOG_SECRET}),
        funder: Some(funder),
    };
    let (log_tx, mut log_rx) = mpsc::unbounded_channel();
    let subscriber = tracing_subscriber::registry().with(SubmitLogCapture(log_tx));
    let (result, raw) = async {
        venue
            .post_prepared(&prepared)
            .instrument(tracing::info_span!("exec_leg", leg_id = "test-leg"))
            .await
            .unwrap()
    }
    .with_subscriber(subscriber)
    .await;
    let _ = release_tx.send(());
    server.await.unwrap();
    let mut logs = Vec::new();
    while let Ok(entry) = log_rx.try_recv() {
        // 检查所有捕获事件及 span，而不只是新事件的字段。
        let rendered = format!("{:?}", entry.fields);
        assert!(!rendered.contains(SUBMIT_LOG_SECRET));
        assert!(!rendered.contains("dGVzdA=="));
        if matches!(
            entry.fields.get("event").map(String::as_str),
            Some("submit_http" | "submit_classified")
        ) {
            assert_eq!(entry.fields["service"], "polymarket");
            assert_eq!(entry.fields["order_hash"], prepared.order_hash);
            assert_eq!(entry.fields["leg_id"], "test-leg");
            assert!(entry.fields["elapsed_ms"].parse::<u64>().is_ok());
            logs.push(entry);
        }
    }
    assert_eq!(
        logs.len(),
        2,
        "one HTTP event and one classification per submit"
    );
    assert_eq!(logs[0].fields["event"], "submit_http");
    assert_eq!(logs[1].fields["event"], "submit_classified");
    assert_eq!(logs[0].fields["operation"], "POST");
    assert_eq!(logs[0].fields["endpoint"], "/order");
    if let Some(status) = status {
        assert_eq!(logs[0].fields["http_status"], status.to_string());
    } else {
        assert!(!logs[0].fields.contains_key("http_status"));
    }
    (result, raw, logs)
}

#[tokio::test]
async fn submit_logging_classifications_and_safe_fields() {
    let oid = format!("0x{}", "b".repeat(64));
    let cases = [
        (
            json!({"success":true,"status":"live","orderID":oid,"makingAmount":"1.25","takingAmount":"2.5"}),
            "ack",
            "accepted",
        ),
        (
            json!({"success":true,"status":"matched","orderId":oid}),
            "ack",
            "accepted",
        ),
        (
            json!({"success":false,"status":"unmatched","errorMsg":SUBMIT_LOG_SECRET}),
            "no_match",
            "fak_no_match",
        ),
        (
            json!({"errorMsg":format!("FAK {SUBMIT_LOG_SECRET}")}),
            "no_match",
            "fak_no_match",
        ),
        (
            json!({"success":true,"status":"matched","orderID":SUBMIT_LOG_SECRET}),
            "ack",
            "accepted",
        ),
        (
            json!({"success":true,"status":SUBMIT_LOG_SECRET,"orderID":SUBMIT_LOG_SECRET,"errorMsg":SUBMIT_LOG_SECRET}),
            "unknown",
            "unrecognized_response",
        ),
        (
            json!({"success":false,"status":"ORDER_STATUS_MATCHED","orderID":oid}),
            "unknown",
            "unrecognized_response",
        ),
        (
            json!({"success":true,"status":"matched","orderID":oid,"makingAmount":SUBMIT_LOG_SECRET,"takingAmount":{"secret":SUBMIT_LOG_SECRET}}),
            "ack",
            "accepted",
        ),
    ];
    for (body, classification, reason) in cases {
        let (result, raw, logs) =
            capture_submit_response(Some(200), &body.to_string(), None, false).await;
        assert_eq!(*raw, body);
        assert_eq!(logs[1].fields["classification"], classification);
        assert_eq!(logs[1].fields["reason"], reason);
        assert_eq!(
            logs[1].level,
            if classification == "unknown" {
                tracing::Level::WARN
            } else {
                tracing::Level::INFO
            }
        );
        assert_eq!(logs[0].fields["reason"], submit_body_log_reason(&body));
        assert_eq!(
            logs[0].level,
            if submit_body_log_reason(&body) == "received" {
                tracing::Level::INFO
            } else {
                tracing::Level::WARN
            }
        );
        assert_eq!(
            logs[1].fields.get("order_id").map(String::as_str),
            safe_submit_order_id(&body)
        );
        assert_eq!(
            logs[1].fields.get("status").map(String::as_str),
            body.get("status")
                .and_then(Value::as_str)
                .and_then(normalized_order_status)
        );
        assert_eq!(
            logs[1].fields.get("success").cloned(),
            body.get("success")
                .and_then(Value::as_bool)
                .map(|v| v.to_string())
        );
        assert_eq!(
            logs[1].fields["making"],
            format!("{:?}", body.get("makingAmount").and_then(parse_decimal))
        );
        assert_eq!(
            logs[1].fields["taking"],
            format!("{:?}", body.get("takingAmount").and_then(parse_decimal))
        );
        match (classification, result) {
            ("ack", SubmitResult::Ack { .. })
            | ("no_match", SubmitResult::NoMatch { .. })
            | ("unknown", SubmitResult::Unknown { .. }) => {}
            _ => panic!("classification changed"),
        }
    }
}

#[tokio::test]
async fn submit_long_json_preserves_unknown_fields_without_logging_body() {
    let body = json!({"success":true,"orderID":format!("0x{}", "a".repeat(64)),"status":"live", "error":"a real JSON field", "unknown":{"nested":[null,42,{"secret":SUBMIT_LOG_SECRET.repeat(80)}]}});
    for status in [200, 400, 429, 500] {
        let (_, raw, _) =
            capture_submit_response(Some(status), &body.to_string(), None, false).await;
        assert!(matches!(raw, super::super::SubmissionResponse::Http(_)));
        if status == 200 {
            assert_eq!(*raw, body);
        } else {
            assert_eq!(raw["body"], body);
            assert_eq!(raw["http_status"], status);
        }
        assert!(!format!("{raw:?}").contains(SUBMIT_LOG_SECRET));
    }
}

#[tokio::test]
async fn submit_logging_http_errors_preserve_classification() {
    for status in [400, 401, 408, 429, 500, 503] {
        let (result, raw, logs) =
            capture_submit_response(Some(status), SUBMIT_LOG_SECRET, None, false).await;
        assert_eq!(logs[0].level, tracing::Level::WARN);
        assert_eq!(logs[0].fields["reason"], "http_status");
        assert_eq!(logs[1].level, tracing::Level::WARN);
        assert_eq!(raw["http_status"], status);
        assert_eq!(raw["body"], SUBMIT_LOG_SECRET);
        if super::super::http_status_proves_reject(status) {
            assert!(matches!(result, SubmitResult::Failed { .. }));
            assert_eq!(logs[1].fields["classification"], "failed");
            assert_eq!(logs[1].fields["reason"], "http_rejected");
        } else {
            assert!(matches!(result, SubmitResult::Unknown { .. }));
            assert_eq!(logs[1].fields["classification"], "unknown");
            assert_eq!(logs[1].fields["reason"], "http_ambiguous");
        }
    }
}

#[tokio::test]
async fn submit_logging_malformed_responses_preserve_raw() {
    for (body, reason, expected) in [
        (
            "",
            "empty_body",
            json!({"http_status":200,"body":"","body_format":"empty"}),
        ),
        ("null", "null_body", Value::Null),
        (" null ", "null_body", Value::Null),
        (
            SUBMIT_LOG_SECRET,
            "invalid_json",
            json!({"http_status":200,"body":SUBMIT_LOG_SECRET,"body_format":"non_json"}),
        ),
        ("{}", "malformed_body", json!({})),
        ("[]", "malformed_body", json!([])),
        ("42", "malformed_body", json!(42)),
        (
            "{\"success\":\"PRIVATE_SUBMIT_SENTINEL\"}",
            "malformed_body",
            json!({"success":SUBMIT_LOG_SECRET}),
        ),
        (
            "{\"status\":{\"secret\":\"PRIVATE_SUBMIT_SENTINEL\"}}",
            "malformed_body",
            json!({"status":{"secret":SUBMIT_LOG_SECRET}}),
        ),
    ] {
        let (result, raw, logs) = capture_submit_response(Some(200), body, None, false).await;
        assert!(matches!(result, SubmitResult::Unknown { .. }));
        assert_eq!(*raw, expected);
        assert_eq!(logs[0].fields["reason"], reason);
        assert_eq!(logs[0].level, tracing::Level::WARN);
        assert_eq!(logs[1].fields["classification"], "unknown");
    }
}

#[tokio::test]
async fn submit_logging_transport_timeout_and_body_failure() {
    for (status, body, length, hold_open, reason) in [
        (None, "", None, false, "transport"),
        (None, "", None, true, "timeout"),
        (
            Some(200),
            SUBMIT_LOG_SECRET,
            Some(1000),
            false,
            "response_body",
        ),
        (
            Some(200),
            SUBMIT_LOG_SECRET,
            Some(1000),
            true,
            "response_body_timeout",
        ),
        (
            Some(400),
            SUBMIT_LOG_SECRET,
            Some(1000),
            false,
            "response_body",
        ),
    ] {
        let (result, raw, logs) = capture_submit_response(status, body, length, hold_open).await;
        assert_eq!(logs[0].fields["reason"], reason);
        assert_eq!(logs[0].level, tracing::Level::WARN);
        assert_eq!(logs[1].level, tracing::Level::WARN);
        if status == Some(400) {
            assert!(matches!(result, SubmitResult::Failed { status: 400, .. }));
            assert_eq!(*raw, json!({"http_status":400,"body_error":reason}));
        } else {
            assert!(matches!(result, SubmitResult::Unknown { .. }));
            if status.is_some() {
                assert_eq!(*raw, json!({"http_status":200,"body_error":reason}));
            } else {
                assert_eq!(logs[1].fields["reason"], reason);
            }
        }
    }
}

#[test]
fn submit_logging_order_id_format_is_strict() {
    let valid = format!("0x{}", "aB12".repeat(16));
    assert_eq!(
        safe_submit_order_id(&json!({"orderID":valid})),
        Some(valid.as_str())
    );
    for invalid in [
        SUBMIT_LOG_SECRET.into(),
        "".into(),
        format!("0x{}", "a".repeat(63)),
        format!("0x{}", "a".repeat(65)),
        format!("0x{}g", "a".repeat(63)),
        format!("0X{}", "a".repeat(64)),
    ] {
        assert_eq!(safe_submit_order_id(&json!({"orderID":invalid})), None);
    }
    assert_eq!(
        safe_submit_order_id(&json!({"orderID":123,"orderId":valid})),
        None
    );
}

pub(crate) async fn execution_test_venue(base: String) -> (PolymarketVenue, String) {
    let mut venue = cache_test_venue();
    venue.base = base;
    venue.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    venue.auth_ttl = Duration::ZERO;
    let signer = PrivateKeySigner::random();
    let funder = format!("{:#x}", signer.address());
    venue.authed.lock().await.insert(
        funder.clone(),
        PolymarketAccount {
            funder: funder.clone(),
            service: None,
            signature_type: 0,
            signer,
            api_key: "stub-key".into(),
            api_secret: "dGVzdA==".into(),
            api_passphrase: "stub-passphrase".into(),
            created_at: unix_secs(),
        },
    );
    (venue, funder)
}

#[tokio::test]
async fn tick_bootstrap_in_flight_obeys_ws_rest_conflict_and_epoch() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for mutation in ["ws", "rest", "disconnect", "conflict", "tickless"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 2048];
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(std::str::from_utf8(&buffer[..count])
                .unwrap()
                .starts_with("GET /tick-size?token_id=t "));
            arrived_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let body = json!({"minimum_tick_size":"0.1"}).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        });
        let mut venue = cache_test_venue();
        venue.base = format!("http://{address}");
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut books = BookStore::default();
        let ticket = books.begin_rest(POLYMARKET, "t");
        let request = tokio::spawn(async move { venue.fetch_tick_size("t").await });
        tokio::time::timeout(Duration::from_secs(3), arrived_rx)
            .await
            .unwrap()
            .unwrap();
        let expected = match mutation {
            "ws" => {
                apply_ws_message(
                    &mut books,
                    &json!({"event_type":"tick_size_change","asset_id":"t","timestamp":"200","new_tick_size":"0.001"}),
                    Instant::now(),
                );
                Some(Decimal::new(1, 3))
            }
            "rest" => {
                let current = books.begin_rest(POLYMARKET, "t");
                books
                    .accept_rest(
                        &current,
                        vec![],
                        vec![],
                        200,
                        Instant::now(),
                        Some(Decimal::new(1, 2)),
                    )
                    .unwrap();
                Some(Decimal::new(1, 2))
            }
            "conflict" => {
                books.set_tick_size_at(POLYMARKET, "t", Decimal::new(1, 2), 200);
                books.set_tick_size_at(POLYMARKET, "t", Decimal::new(1, 3), 200);
                None
            }
            "tickless" => {
                books.replace_snapshot(POLYMARKET, "t", vec![], vec![], 200, Instant::now());
                None
            }
            _ => {
                books.mark_platform_stale(POLYMARKET);
                None
            }
        };
        release_tx.send(()).unwrap();
        let fetched = request.await.unwrap().unwrap();
        assert_eq!(
            books.seed_tick_size(&ticket, fetched),
            expected,
            "mutation={mutation}"
        );
        assert_eq!(books.tick_size(POLYMARKET, "t"), expected);
        if mutation == "conflict" {
            let current = books.begin_rest(POLYMARKET, "t");
            assert_eq!(books.seed_tick_size(&current, fetched), None);
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn tick_fetch_has_no_permanent_venue_cache_and_rejects_bad_values() {
    let (venue, server) = poll_stub(vec![
        (200, json!({"minimum_tick_size":"0.01"})),
        (200, json!({"minimum_tick_size":"0.001"})),
        (200, json!({"minimum_tick_size":"1.1"})),
        (200, json!({"minimum_tick_size":"0"})),
        (500, json!({})),
    ])
    .await;
    assert_eq!(
        venue.fetch_tick_size("t").await.unwrap(),
        Decimal::new(1, 2)
    );
    assert_eq!(
        venue.fetch_tick_size("t").await.unwrap(),
        Decimal::new(1, 3)
    );
    for _ in 0..3 {
        assert!(venue.fetch_tick_size("t").await.is_err());
    }
    assert_eq!(server.await.unwrap().len(), 5);
}

#[tokio::test]
async fn rest_book_http_rejects_wrong_identity_and_malformed_success() {
    let valid =
        json!({"asset_id":"t", "timestamp":"100", "bids":[], "asks":[], "tick_size":"0.001"});
    let (venue, server) = poll_stub(vec![
        (
            200,
            json!({"asset_id":"other", "timestamp":"100", "bids":[], "asks":[]}),
        ),
        (
            200,
            json!({"asset_id":"t", "timestamp":"invalid", "bids":[], "asks":[]}),
        ),
        (200, valid),
    ])
    .await;
    assert!(venue.rest_book("t").await.is_err());
    assert!(venue.rest_book("t").await.is_err());
    let snapshot = venue.rest_book("t").await.unwrap();
    assert_eq!(snapshot.exchange_ts_ms, 100);
    assert_eq!(snapshot.tick_size, Some(Decimal::new(1, 3)));
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test]
async fn rest_book_http_in_flight_delete_rejects_newer_response() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 2048];
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        arrived_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let body = json!({"asset_id":"t", "timestamp":"200", "bids":[], "asks":[{"price":"0.5","size":"3"}]}).to_string();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
    });
    let mut venue = cache_test_venue();
    venue.base = format!("http://{address}");
    venue.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let mut books = BookStore::default();
    let now = Instant::now();
    apply_ws_message(
        &mut books,
        &json!({"event_type":"book", "asset_id":"t", "timestamp":"100", "bids":[], "asks":[{"price":"0.5","size":"3"}]}),
        now,
    );
    let ticket = books.begin_rest(POLYMARKET, "t");
    let request = tokio::spawn(async move { venue.rest_book("t").await });
    tokio::time::timeout(Duration::from_secs(3), arrived_rx)
        .await
        .unwrap()
        .unwrap();
    apply_ws_message(
        &mut books,
        &json!({"event_type":"price_change", "timestamp":"100", "price_changes":[{"asset_id":"t","side":"SELL","price":"0.5","size":"0"}]}),
        now,
    );
    release_tx.send(()).unwrap();
    let BookSnapshot {
        bids,
        asks,
        exchange_ts_ms: ts,
        ..
    } = request.await.unwrap().unwrap();
    assert_eq!(
        books
            .accept_rest(&ticket, bids, asks, ts, Instant::now(), None)
            .unwrap_err(),
        crate::book::BookReject::RevisionChanged
    );
    assert!(books.get(POLYMARKET, "t").unwrap().asks.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn fee_schedule_fetches_public_market_and_preserves_valuation_policy() {
    let (venue, server) = poll_stub(vec![(200, json!({"c": "0xABC", "fd": {"r": 0.07}}))]).await;
    // 此公开查询不需要账户初始化、凭证文件或 L2 认证。
    venue.authed.lock().await.clear();
    let before = unix_millis();
    let snapshot = venue.fee_schedule("0xabc").await.unwrap();
    let after = unix_millis();
    assert_eq!(snapshot["condition_id"], "0xabc");
    assert_eq!(snapshot["rate"], "0.07");
    assert_eq!(snapshot.as_object().unwrap().len(), 7);
    assert_eq!(snapshot["source"], "clob-markets");
    assert_eq!(snapshot["currency"], "pUSD");
    assert_eq!(snapshot["valuation"], "1 USD");
    assert_eq!(snapshot["rounding"], "midpoint_away_from_zero_5dp");
    let observed_at = snapshot["observed_at_ms"].as_u64().unwrap();
    assert!((before..=after).contains(&observed_at));
    assert_eq!(server.await.unwrap(), ["GET /clob-markets/0xabc HTTP/1.1"]);
}

#[test]
fn fee_schedule_accepts_rate_boundaries_and_ignores_unrelated_fields() {
    for rate in ["0", "1", "0.07"] {
        let snapshot = parse_fee_schedule(
            &json!({
                "condition_id": "condition", "fd": {"r": rate}
            }),
            "condition",
            123,
        )
        .unwrap();
        assert_eq!(snapshot["rate"], rate);
        assert_eq!(snapshot["observed_at_ms"], 123);
        assert_eq!(snapshot.as_object().unwrap().len(), 7);
    }
    // 只消费费率；响应中其他参数缺失或畸形均不能阻挡已验证的 rate。
    let rate_only = parse_fee_schedule(&json!({"fd": {"r": "0.05"}}), "condition", 123).unwrap();
    for unrelated in [Value::Null, json!(-1), json!("invalid"), json!({})] {
        assert_eq!(
            parse_fee_schedule(
                &json!({"fd": {"r": "0.05", "e": unrelated}}),
                "condition",
                123
            )
            .unwrap(),
            rate_only
        );
    }
}

#[tokio::test]
async fn fee_schedule_rejects_invalid_values_identity_and_http_error() {
    let invalid = vec![
        json!({}),
        json!({"fd": {}}),
        json!({"fd": null}),
        json!({"fd": {"r": "-0.01"}}),
        json!({"fd": {"r": "1.01"}}),
        json!({"fd": {"r": "NaN"}}),
        json!({"fd": {"r": true}}),
        json!({"fd": {"r": null}}),
        json!({"c": "different", "fd": {"r": "0.07"}}),
        json!({"c": null, "fd": {"r": "0.07"}}),
        json!({"c": "condition", "condition_id": "different", "fd": {"r": "0.07"}}),
    ];
    let count = invalid.len();
    let mut responses: Vec<_> = invalid.into_iter().map(|raw| (200, raw)).collect();
    responses.push((503, json!({"error": "do not expose payload"})));
    let (venue, server) = poll_stub(responses).await;
    for _ in 0..count {
        assert!(venue.fee_schedule("condition").await.is_err());
    }
    let error = venue.fee_schedule("condition").await.unwrap_err();
    assert!(matches!(error, Error::Http { status: 503, .. }));
    assert!(!error.to_string().contains("do not expose payload"));
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), count + 1);
    assert!(requests
        .iter()
        .all(|request| request == "GET /clob-markets/condition HTTP/1.1"));
}

#[tokio::test]
async fn fee_schedule_rejects_empty_condition_without_network() {
    assert!(cache_test_venue().fee_schedule(" ").await.is_err());
}

const TRADES_TEST_AFTER: i64 = 1_700_000_000;
const TRADES_TEST_BEFORE: i64 = TRADES_TEST_AFTER + 300;

fn assert_trade_request(request: &str, cursor: &str) {
    assert!(request.starts_with("GET /data/trades?"));
    let target = request.split_whitespace().nth(1).unwrap();
    let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
    assert_eq!(url.query_pairs().count(), 4);
    let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(query.get("asset_id").map(String::as_str), Some("yes"));
    assert_eq!(query.get("next_cursor").map(String::as_str), Some(cursor));
    assert_eq!(query["after"], (TRADES_TEST_AFTER - 10).to_string());
    assert_eq!(query["before"], TRADES_TEST_BEFORE.to_string());
}

fn trade_progress_fixture(cursor: &str, seen: &[&str], trade_ids: &[&str]) -> Value {
    json!({
        "version": 2,
        "funder": "test-funder",
        "asset_id": "yes",
        "order_id": "taker-1",
        "after": TRADES_TEST_AFTER,
        "before": TRADES_TEST_BEFORE,
        "next_cursor": cursor,
        "seen_cursors": seen,
        "trade_ids": trade_ids,
    })
}

#[tokio::test]
async fn trade_pagination_resumes_second_page_and_restarts_after_end() {
    let mut unrelated = trade_fixture("unrelated");
    unrelated["taker_order_id"] = json!("other-order");
    let mut case_mismatch = trade_fixture("case-mismatch");
    case_mismatch["taker_order_id"] = json!("TAKER-1");
    let (venue, server) = poll_stub(vec![
            (200, json!({"data": [trade_fixture("first"), unrelated, case_mismatch], "next_cursor": "MQ=="})),
            // fills 不去重，上层按 trade + order ID 持久化；progress 则收集本单去重证据。
            (200, json!({"data": [trade_fixture("first"), trade_fixture("second")], "next_cursor": "LTE="})),
            (200, json!({"data": [trade_fixture("rescan-only")], "next_cursor": "LTE="})),
        ]).await;
    let first = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &Value::Null,
        )
        .await
        .unwrap();
    assert!(!first.complete && !first.history_complete);
    assert_eq!(first.fills.len(), 3);
    assert_eq!(first.fills[0].trade_id, "first");
    assert_eq!(
        first.progress,
        trade_progress_fixture("MQ==", &["MA=="], &["first"])
    );
    let saved = serde_json::to_string(&first.progress).unwrap();
    let restored: Value = serde_json::from_str(&saved).unwrap();
    let second = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &restored,
        )
        .await
        .unwrap();
    assert!(second.complete && second.history_complete);
    assert_eq!(second.fills.len(), 2);
    assert_eq!(second.fills[1].trade_id, "second");
    assert_eq!(
        second.progress,
        trade_progress_fixture("LTE=", &["MA==", "MQ=="], &["first", "second"])
    );
    let next = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &second.progress,
        )
        .await
        .unwrap();
    assert!(next.complete && next.history_complete);
    assert_eq!(
        next.progress,
        trade_progress_fixture("LTE=", &["MA=="], &["rescan-only"])
    );
    let requests = server.await.unwrap();
    assert_trade_request(&requests[0], "MA==");
    assert_trade_request(&requests[1], "MQ==");
    assert_trade_request(&requests[2], "MA==");
}

#[tokio::test]
async fn trade_pagination_collects_only_matching_maker_ids() {
    let mut trade = trade_fixture("maker-trade");
    trade["asset_id"] = json!("no");
    trade["maker_orders"] = json!([
        {"order_id": "maker-1", "matched_amount": "2", "price": "0.4", "asset_id": "yes"},
        {"order_id": "maker-2", "matched_amount": "3", "price": "0.6", "asset_id": "no"}
    ]);
    let (venue, server) = poll_stub(vec![(
        200,
        json!({"data": [trade.clone(), trade, trade_fixture("unrelated")], "next_cursor": "LTE="}),
    )])
    .await;
    let page = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "maker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &Value::Null,
        )
        .await
        .unwrap();
    assert_eq!(page.fills.len(), 7);
    let mut expected = trade_progress_fixture("LTE=", &["MA=="], &["maker-trade"]);
    expected["order_id"] = json!("maker-1");
    assert_eq!(page.progress, expected);
    assert_trade_request(&server.await.unwrap()[0], "MA==");
}

#[tokio::test]
async fn trade_pagination_rejects_matching_order_with_wrong_asset() {
    let mut wrong_taker = trade_fixture("wrong-taker");
    wrong_taker["asset_id"] = json!("no");
    let mut wrong_maker = trade_fixture("wrong-maker");
    wrong_maker["maker_orders"] = json!([
        {"order_id": "maker-1", "matched_amount": "2", "price": "0.4", "asset_id": "no"}
    ]);
    let (venue, server) = poll_stub(vec![
        (
            200,
            json!({"data": [trade_fixture("staged"), wrong_taker], "next_cursor": "LTE="}),
        ),
        (200, json!({"data": [wrong_maker], "next_cursor": "LTE="})),
    ])
    .await;
    for order_id in ["taker-1", "maker-1"] {
        let mut progress = trade_progress_fixture("MQ==", &["MA=="], &["saved"]);
        progress["order_id"] = json!(order_id);
        let saved = progress.clone();
        let err = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                order_id,
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &progress,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("order asset mismatch"));
        assert_eq!(progress, saved);
    }
    for request in server.await.unwrap() {
        assert_trade_request(&request, "MQ==");
    }
}

#[tokio::test]
async fn trade_pagination_rejects_repeated_cursor_and_preserves_saved_progress() {
    let (venue, server) = poll_stub(vec![
        (
            200,
            json!({"data": [trade_fixture("saved")], "next_cursor": "MQ=="}),
        ),
        (200, json!({"data": [], "next_cursor": "MA=="})),
        (200, json!({"data": [], "next_cursor": "MQ=="})),
    ])
    .await;
    let first = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &json!({}),
        )
        .await
        .unwrap();
    let saved = first.progress.clone();
    for _ in 0..2 {
        let err = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &first.progress,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("repeated cursor"));
        assert_eq!(first.progress, saved);
    }
    let requests = server.await.unwrap();
    assert_trade_request(&requests[0], "MA==");
    assert_trade_request(&requests[1], "MQ==");
    assert_trade_request(&requests[2], "MQ==");
}

#[tokio::test]
async fn trade_pagination_rejects_bad_page_and_http_failure_without_advancing() {
    let (venue, server) = poll_stub(vec![
        (
            200,
            json!({"data": [trade_fixture("saved")], "next_cursor": "MQ=="}),
        ),
        (200, json!({"data": {}, "next_cursor": "LTE="})),
        (200, json!({"data": [null], "next_cursor": "LTE="})),
        (200, json!({"data": []})),
        (503, json!({"error": "unavailable"})),
        (
            200,
            json!({"data": [trade_fixture("recovered")], "next_cursor": "LTE="}),
        ),
    ])
    .await;
    let first = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &Value::Null,
        )
        .await
        .unwrap();
    let saved = first.progress.clone();
    for _ in 0..4 {
        assert!(venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &first.progress
            )
            .await
            .is_err());
        assert_eq!(first.progress, saved);
    }
    let recovered = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &first.progress,
        )
        .await
        .unwrap();
    assert!(recovered.complete && recovered.history_complete);
    assert_eq!(recovered.fills[0].trade_id, "recovered");
    assert_eq!(
        recovered.progress,
        trade_progress_fixture("LTE=", &["MA==", "MQ=="], &["saved", "recovered"])
    );
    let requests = server.await.unwrap();
    assert_trade_request(&requests[0], "MA==");
    for request in &requests[1..] {
        assert_trade_request(request, "MQ==");
    }
}

#[tokio::test]
async fn trade_pagination_migrates_legacy_progress_by_restarting_same_window() {
    let (venue, server) = poll_stub(vec![
        (
            200,
            json!({"data": [trade_fixture("fresh")], "next_cursor": "MQ=="}),
        ),
        (
            200,
            json!({"data": [trade_fixture("fresh")], "next_cursor": "MQ=="}),
        ),
    ])
    .await;
    for cursor in ["Mg==", "LTE="] {
        let legacy = json!({
            "funder": "TEST-FUNDER", "asset_id": "yes", "next_cursor": cursor,
            "seen_cursors": ["MA==", "MQ=="]
        });
        let page = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE,
                &legacy,
            )
            .await
            .unwrap();
        assert!(!page.complete && !page.history_complete);
        assert_eq!(
            page.progress,
            trade_progress_fixture("MQ==", &["MA=="], &["fresh"])
        );
    }
    for request in server.await.unwrap() {
        assert_trade_request(&request, "MA==");
    }
}

#[test]
fn trade_pagination_rejects_unknown_history_and_wrong_query_progress() {
    for progress in [
        json!(false),
        json!([]),
        json!({"next_cursor": "MQ=="}),
        json!({"funder": "other", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA=="]}),
        json!({"funder": "test-funder", "asset_id": "no", "next_cursor": "MQ==", "seen_cursors": ["MA=="]}),
        json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "LTE=", "seen_cursors": []}),
        json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA==", "MQ=="]}),
        json!({"funder": "test-funder", "asset_id": "yes", "next_cursor": "MQ==", "seen_cursors": ["MA=="], "after": TRADES_TEST_AFTER}),
    ] {
        assert!(trade_page_cursor(
            &progress,
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE
        )
        .is_err());
    }
    let valid = trade_progress_fixture("MQ==", &["MA=="], &["saved"]);
    for (field, value) in [
        ("version", json!(1)),
        ("version", json!(3)),
        ("version", json!("2")),
        ("version", Value::Null),
        ("funder", json!("other")),
        ("asset_id", json!("no")),
        ("order_id", json!("TAKER-1")),
        ("after", json!(TRADES_TEST_AFTER + 1)),
        ("before", json!(TRADES_TEST_BEFORE + 1)),
        ("after", json!(TRADES_TEST_AFTER.to_string())),
        ("before", Value::Null),
        ("next_cursor", json!("")),
        ("next_cursor", json!("MA==")),
        ("seen_cursors", json!([])),
        ("seen_cursors", json!(["MQ=="])),
        ("seen_cursors", json!(["MA==", "MA=="])),
        ("seen_cursors", json!(["MA==", "LTE="])),
        ("seen_cursors", json!(["MA==", null])),
        ("seen_cursors", json!(["MA==", ""])),
        ("trade_ids", Value::Null),
        ("trade_ids", json!(["saved", "saved"])),
        ("trade_ids", json!([""])),
        ("trade_ids", json!([5])),
    ] {
        let mut progress = valid.clone();
        progress[field] = value;
        assert!(
            trade_page_cursor(
                &progress,
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE
            )
            .is_err(),
            "{field}"
        );
    }
    for field in valid.as_object().unwrap().keys() {
        let mut progress = valid.clone();
        progress.as_object_mut().unwrap().remove(field);
        assert!(
            trade_page_cursor(
                &progress,
                "test-funder",
                "yes",
                "taker-1",
                TRADES_TEST_AFTER,
                TRADES_TEST_BEFORE
            )
            .is_err(),
            "missing {field}"
        );
    }
    // END 不能绕过损坏 progress 的校验。
    let corrupt_end = trade_progress_fixture("LTE=", &["MA=="], &["duplicate", "duplicate"]);
    assert!(trade_page_cursor(
        &corrupt_end,
        "test-funder",
        "yes",
        "taker-1",
        TRADES_TEST_AFTER,
        TRADES_TEST_BEFORE
    )
    .is_err());
}

#[tokio::test]
async fn trade_pagination_rejects_changed_order_or_window_before_request() {
    let (venue, server) = poll_stub(vec![(200, json!({"data": [], "next_cursor": "LTE="}))]).await;
    for cursor in ["MQ==", "LTE="] {
        let progress = trade_progress_fixture(cursor, &["MA=="], &["saved"]);
        for (order_id, after, before) in [
            ("other-order", TRADES_TEST_AFTER, TRADES_TEST_BEFORE),
            ("taker-1", TRADES_TEST_AFTER + 1, TRADES_TEST_BEFORE + 1),
        ] {
            let err = venue
                .poll_trade_page("test-funder", "yes", order_id, after, before, &progress)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("pagination progress"));
        }
    }
    let page = venue
        .poll_trade_page(
            "TEST-FUNDER",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &Value::Null,
        )
        .await
        .unwrap();
    assert_eq!(
        page.progress,
        trade_progress_fixture("LTE=", &["MA=="], &[])
    );
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_trade_request(&requests[0], "MA==");
}

#[tokio::test]
async fn trade_queries_require_order_and_exact_nonnegative_300_second_window() {
    let venue = cache_test_venue();
    for (after, before) in [
        (-1, 299),
        (0, 299),
        (0, 301),
        (300, 0),
        (i64::MAX, i64::MIN),
    ] {
        let err = venue
            .poll_trade_page("test-funder", "yes", "taker-1", after, before, &Value::Null)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("300-second window"));
        let err = venue
            .poll_trades("test-funder", "yes", "taker-1", after, before)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("300-second window"));
    }
    for order_id in ["", " "] {
        let err = venue
            .poll_trade_page("test-funder", "yes", order_id, 0, 300, &Value::Null)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("require an order_id"));
    }
    for (after, before) in [(0, 300), (i64::MAX - 300, i64::MAX)] {
        assert_eq!(
            trade_page_cursor(&Value::Null, "test-funder", "yes", "taker-1", after, before)
                .unwrap(),
            (TRADES_INITIAL_CURSOR.into(), Vec::new(), Vec::new())
        );
    }
}

#[tokio::test]
async fn poll_trades_keeps_bounded_query_and_rejects_incomplete_first_page() {
    let (venue, server) = poll_stub(vec![
        (
            200,
            json!({"data": [trade_fixture("incomplete")], "next_cursor": "MQ=="}),
        ),
        (
            200,
            json!({"data": [trade_fixture("complete")], "next_cursor": "LTE="}),
        ),
    ])
    .await;
    let err = venue
        .poll_trades(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("require resumable poll_trade_page"));
    let fills = venue
        .poll_trades(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
        )
        .await
        .unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].trade_id, "complete");
    for request in server.await.unwrap() {
        assert_trade_request(&request, "MA==");
    }
}

#[tokio::test]
async fn trade_request_lookback_preserves_window_and_clamps_at_epoch() {
    for after in [0, 5, TRADES_TEST_AFTER] {
        let mut trade = trade_fixture("same-second");
        trade["match_time"] = json!(after.to_string());
        let (venue, server) =
            poll_stub(vec![(200, json!({"data": [trade], "next_cursor": "LTE="}))]).await;
        let page = venue
            .poll_trade_page(
                "test-funder",
                "yes",
                "taker-1",
                after,
                after + 300,
                &Value::Null,
            )
            .await
            .unwrap();
        assert_eq!(page.fills[0].trade_id, "same-second");
        assert_eq!(page.progress["after"], json!(after));
        assert_eq!(page.progress["before"], json!(after + 300));
        let requests = server.await.unwrap();
        let target = requests[0].split_whitespace().nth(1).unwrap();
        let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query["after"], (after - 10).max(0).to_string());
        assert_eq!(query["before"], (after + 300).to_string());
    }
}

#[tokio::test]
async fn trade_window_is_sent_to_server_without_local_last_update_filter() {
    let mut old_update = trade_fixture("old-update");
    old_update["match_time"] = json!((TRADES_TEST_AFTER + 1).to_string());
    old_update["last_update"] = json!((TRADES_TEST_AFTER - 1).to_string());
    let mut later_update = trade_fixture("later-update");
    later_update["match_time"] = json!((TRADES_TEST_AFTER + 2).to_string());
    later_update["last_update"] = json!((TRADES_TEST_BEFORE + 600).to_string());
    let (venue, server) = poll_stub(vec![
            (200, json!({"data": [old_update.clone(), later_update.clone(), trade_fixture("no-update")], "next_cursor": "LTE="})),
        ]).await;
    let page = venue
        .poll_trade_page(
            "test-funder",
            "yes",
            "taker-1",
            TRADES_TEST_AFTER,
            TRADES_TEST_BEFORE,
            &Value::Null,
        )
        .await
        .unwrap();
    // 只验证请求结构和本地不丢状态更新；mock 不证明真实服务的时间过滤语义。
    assert_eq!(page.fills.len(), 3);
    assert_eq!(page.fills[0].raw["last_update"], old_update["last_update"]);
    assert_eq!(
        page.fills[1].raw["last_update"],
        later_update["last_update"]
    );
    assert_eq!(
        page.progress,
        trade_progress_fixture(
            "LTE=",
            &["MA=="],
            &["old-update", "later-update", "no-update"]
        )
    );
    assert_trade_request(&server.await.unwrap()[0], "MA==");
}

fn assert_missing_order(order: &OrderPoll, source: &str) {
    assert!(!order.found);
    assert_eq!(order.status, "not_found");
    assert_eq!(order.order_id.as_deref(), Some("order-id"));
    assert!(
        order.shares.is_none()
            && order.original_shares.is_none()
            && order.remaining_shares.is_none()
    );
    assert!(order.price.is_none() && order.fee.is_none() && order.coin.is_none());
    assert!(order.associated_trades.is_empty());
    assert_eq!(order.raw, json!({"lookup_missing": source}));
}

#[test]
fn parse_order_poll_null_is_not_found() {
    assert_missing_order(&parse_order_poll(Value::Null, "order-id"), "null_body");
}

#[tokio::test]
async fn poll_order_404_and_null_are_missing_with_distinct_evidence() {
    let (venue, server) = poll_stub(vec![
        (404, json!({"error": "not found"})),
        (200, Value::Null),
    ])
    .await;
    for source in ["http_404", "null_body"] {
        let order = venue.poll_order("test-funder", "order-id").await.unwrap();
        assert_missing_order(&order, source);
    }
    assert_eq!(
        server.await.unwrap(),
        ["GET /data/order/order-id HTTP/1.1"; 2]
    );
}

#[tokio::test]
async fn poll_order_rejects_malformed_success_and_other_http_errors() {
    let invalid = vec![
        json!([]),
        json!(false),
        json!(17),
        json!("null"),
        json!({}),
        json!({"id": "order-id"}),
        json!({"status": null}),
        json!({"status": true}),
        json!({"status": ""}),
        json!({"status": " "}),
    ];
    let count = invalid.len();
    let mut responses: Vec<_> = invalid.into_iter().map(|raw| (200, raw)).collect();
    responses.push((503, json!({"error": "do not expose payload"})));
    // 合法对象仍沿用原 parser 的可选字段行为，不要求完整订单字段。
    responses.push((200, json!({"status": "MATCHED"})));
    let (venue, server) = poll_stub(responses).await;
    for _ in 0..count {
        let err = venue
            .poll_order("test-funder", "order-id")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing or invalid status"));
    }
    let err = venue
        .poll_order("test-funder", "order-id")
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Http { status: 503, .. }));
    assert!(!err.to_string().contains("do not expose payload"));
    let found = venue.poll_order("test-funder", "order-id").await.unwrap();
    assert!(found.found);
    assert_eq!(found.status, "matched");
    assert_eq!(found.raw["status"], "MATCHED");
    assert_eq!(found.order_id.as_deref(), Some("order-id"));
    assert!(found.shares.is_none() && found.associated_trades.is_empty());
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), count + 2);
    assert!(requests
        .iter()
        .all(|request| request == "GET /data/order/order-id HTTP/1.1"));
}

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

#[test]
fn market_buy_rejects_cent_rounding_above_cap() {
    let err = market_order_base_units(OrderSide::Buy, d("7"), d("0.333")).unwrap_err();
    assert!(err.to_string().contains("exceed cap"));
    assert_eq!(
        market_buy_base_units(d("10"), d("0.333")).unwrap(),
        (3_330_000, 10_000_000)
    );
}

#[test]
fn market_buy_rejects_lossy_shares_and_accepts_trailing_zeros() {
    let err = market_order_base_units(OrderSide::Buy, d("1.234567"), d("0.50")).unwrap_err();
    assert!(err.to_string().contains("shares exceed"));
    assert_eq!(
        market_buy_base_units(d("1.25000"), d("0.4")).unwrap(),
        (500_000, 1_250_000)
    );
}

#[test]
fn market_buy_keeps_cent_usdc_unchanged() {
    let (maker, taker) = market_order_base_units(OrderSide::Buy, d("10"), d("0.45")).unwrap();
    assert_eq!(maker, 4_500_000);
    assert_eq!(taker, 10_000_000);
}

#[test]
fn market_sell_floors_maker_shares_to_2_decimals() {
    let (maker, taker) = market_order_base_units(OrderSide::Sell, d("5.129"), d("0.40")).unwrap();
    assert_eq!(maker, 5_120_000);
    assert_eq!(taker, 2_048_000);
}

#[test]
fn market_buy_rejects_when_shares_trunc_to_zero() {
    let err = market_order_base_units(OrderSide::Buy, d("0.000001"), d("0.50")).unwrap_err();
    assert!(err.to_string().contains("shares exceed"));
}

#[test]
fn market_buy_amounts_preserve_exact_original_constraints() {
    for cap in [
        "0.01",
        "0.333",
        "0.0001",
        "0.9999",
        "0.1234567890123456789012345678",
    ] {
        let cap = d(cap);
        for qty in 1..=200 {
            if let Ok((maker, taker)) = market_buy_base_units(Decimal::from(qty), cap) {
                assert_eq!(taker, qty as u128 * 1_000_000);
                assert_eq!(maker % 10_000, 0);
                assert!(
                    BigInt::from(maker) * BigInt::from(10u8).pow(cap.scale())
                        <= BigInt::from(taker) * BigInt::from(cap.mantissa())
                );
            }
        }
    }
    for (qty, cap) in [
        (Decimal::ZERO, d("0.5")),
        (d("-1"), d("0.5")),
        (d("1"), Decimal::ZERO),
        (d("1"), Decimal::ONE),
        (d("1"), d("-0.1")),
    ] {
        assert!(market_buy_base_units(qty, cap).is_err());
    }
    // 不通过 Decimal 乘法构造金额，最大尾数仍可安全转换为 u128 基础单位。
    assert!(market_buy_base_units(Decimal::MAX, d("0.5")).is_ok());
}

#[test]
fn unsigned_buy_rejects_realignment_and_preserves_payload_amounts() {
    let account = PolymarketAccount {
        funder: "0x0000000000000000000000000000000000000001".into(),
        service: None,
        signature_type: 0,
        signer: PrivateKeySigner::random(),
        api_key: String::new(),
        api_secret: String::new(),
        api_passphrase: String::new(),
        created_at: 0,
    };
    let mut req = MarketOrderRequest {
        token_id: "1".into(),
        shares: d("10"),
        cap_price: d("0.333"),
        side: OrderSide::Buy,
        neg_risk: Some(false),
        tick_size: Some(d("0.001")),
        asset_id: None,
        funder_address: None,
    };
    assert!(build_unsigned_order(&account, &req, d("0.01"), "FAK").is_err());
    let order = build_unsigned_order(&account, &req, d("0.001"), "FAK").unwrap();
    let payload = order_submit_payload(&order, "test-owner");
    assert_eq!(payload["order"]["makerAmount"], "3330000");
    assert_eq!(payload["order"]["takerAmount"], "10000000");
    req.shares = d("7");
    assert!(build_unsigned_order(&account, &req, d("0.001"), "FAK").is_err());

    req.side = OrderSide::Sell;
    req.shares = d("1.23");
    req.cap_price = d("0.3333");
    let order = build_unsigned_order(&account, &req, d("0.0001"), "GTC").unwrap();
    assert_eq!(order.order_type, "GTC");
    assert_eq!(
        (order.maker_amount, order.taker_amount),
        (1_230_000, 409_959)
    );
    // 最小 tick 与最小股数的有效限价单不能被 FAK 五位金额截断误拒绝。
    req.shares = d("0.01");
    req.cap_price = d("0.0001");
    let order = build_unsigned_order(&account, &req, d("0.0001"), "GTC").unwrap();
    assert_eq!((order.maker_amount, order.taker_amount), (10_000, 1));
    req.cap_price = d("0.00001");
    assert!(build_unsigned_order(&account, &req, d("0.00001"), "GTC").is_err());
}

fn dummy_funder(addr: &str) -> PolymarketFunderConfig {
    PolymarketFunderConfig {
        funder_address: addr.into(),
        wallet_private_key: "0x".into(),
        is_wallet_v2: false,
        service: None,
    }
}

#[test]
fn funder_cursor_resumes_saved_address() {
    let path = std::env::temp_dir().join(format!(
        "pm-funder-cursor-{}-{}",
        std::process::id(),
        "resume"
    ));
    let funders = vec![
        dummy_funder("0xaaa"),
        dummy_funder("0xbbb"),
        dummy_funder("0xccc"),
    ];
    assert_eq!(load_funder_rr(&funders, &path), 0);
    save_funder_rr(&path, "0xBBB").unwrap();
    assert_eq!(load_funder_rr(&funders, &path), 1);
    save_funder_rr(&path, "0xmissing").unwrap();
    assert_eq!(load_funder_rr(&funders, &path), 0);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(path.with_extension("cursor.tmp"));
}

#[test]
fn api_creds_roundtrip_and_ttl() {
    let path = std::env::temp_dir().join(format!(
        "pm-api-creds-{}-{}.json",
        std::process::id(),
        "ttl"
    ));
    let _ = fs::remove_file(&path);
    let creds = StoredApiCreds {
        api_key: "k".into(),
        secret: "s".into(),
        passphrase: "p".into(),
        created_at: 1_000,
    };
    save_api_cred(&path, "0xAbC", &creds).unwrap();
    assert!(creds_fresh(1_000, Duration::from_secs(100), 1_050));
    assert!(!creds_fresh(1_000, Duration::from_secs(100), 1_100));
    assert!(creds_fresh(1_000, Duration::ZERO, 9_999));
    let loaded = load_fresh_api_cred(&path, "0xabc", Duration::from_secs(100), 1_050).unwrap();
    assert_eq!(loaded.api_key, "k");
    assert!(load_fresh_api_cred(&path, "0xabc", Duration::from_secs(100), 1_100).is_none());
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(path.with_extension("json.tmp"));
}

#[test]
fn auth_ttl_jitter_is_stable_and_in_range() {
    let a = auth_ttl_jitter_secs("0xAbc");
    let b = auth_ttl_jitter_secs("0xabc");
    assert_eq!(a, b);
    assert_eq!(a % 60, 0);
    assert!((AUTH_TTL_JITTER_MIN_MINS * 60..=AUTH_TTL_JITTER_MAX_MINS * 60).contains(&a));
    assert_eq!(effective_auth_ttl(Duration::ZERO, "0xabc"), Duration::ZERO);
    let ttl = effective_auth_ttl(Duration::from_secs(86_400), "0xabc");
    assert_eq!(ttl, Duration::from_secs(86_400 + a));
    let other = auth_ttl_jitter_secs("0xdef");
    assert_ne!(a, other);
}

fn sample_cred(created_at: u64) -> StoredApiCreds {
    StoredApiCreds {
        api_key: "k".into(),
        secret: "s".into(),
        passphrase: "p".into(),
        created_at,
    }
}

#[test]
fn oldest_stored_cred_picks_earliest_created_at() {
    let mut creds = HashMap::new();
    creds.insert("0xbbb".into(), sample_cred(2_000));
    creds.insert("0xaaa".into(), sample_cred(3_000));
    creds.insert("0xccc".into(), sample_cred(1_000));
    assert_eq!(oldest_stored_cred(&creds), Some(("0xccc".into(), 1_000)));
}

#[test]
fn auth_refresh_due_only_within_ten_minutes() {
    let ttl = Duration::from_secs(86_400);
    assert!(!auth_refresh_due(1_000, ttl, 1_000 + 86_400 - 600));
    assert!(auth_refresh_due(1_000, ttl, 1_000 + 86_400 - 599));
    assert!(auth_refresh_due(1_000, ttl, 1_000 + 86_400));
    assert!(!auth_refresh_due(1_000, Duration::ZERO, 9_999));
}
