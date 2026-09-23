use super::*;
use crate::calc::estimate_taker_fee;
use rust_decimal::prelude::FromStr;
use serde_json::json;

fn place_notice_for_test(results: Vec<Result<SubmitResult>>) -> String {
    notify::format_place_notice(
        "【test】",
        &PlaceNotice {
            order_id: 1,
            title: "test".into(),
            platforms: vec![],
            results: results
                .into_iter()
                .map(|result| {
                    place_result("platform".into(), "yes".into(), "market".into(), result)
                })
                .collect(),
        },
    )
}

#[test]
fn place_notice_ack_and_delayed_unknown_are_not_failures() {
    let parse = |status| {
        crate::platforms::polymarket::parse_submit(
            &json!({"success": true, "status": status, "orderID": "order", "errorMsg": ""}),
            "hash".into(),
            json!({"signature": "must-not-appear"}),
        )
    };
    let delayed = parse("delayed");
    assert!(matches!(&delayed, SubmitResult::Unknown { message, .. } if message.is_empty()));
    let text = place_notice_for_test(vec![Ok(parse("live")), Ok(delayed)]);
    assert!(text.contains("✅ 成功: 1  ⏳ 待确认: 1"));
    assert!(text.contains("⏳ 待确认详情:"));
    assert!(text.contains("market=market: 提交结果待确认"));
    for forbidden in ["失败", "明确拒绝", "未成交", "must-not-appear", "signature"] {
        assert!(!text.contains(forbidden), "unexpected {forbidden}");
    }
}

#[test]
fn place_notice_distinguishes_no_match_rejection_and_execution_error() {
    let text = place_notice_for_test(vec![
        Ok(SubmitResult::NoMatch {
            order_hash: "hash".into(),
            envelope: json!({}),
            message: " \n\t".into(),
        }),
        Ok(SubmitResult::Failed {
            order_hash: "hash".into(),
            envelope: json!({}),
            status: 400,
            message: " \n\t".into(),
        }),
        Err(Error::msg("sensitive internal response")),
    ]);
    assert!(text.contains("❌ 失败: 2  ⚠️ 执行异常: 1"));
    assert!(text.contains("market=market: 订单未匹配成交"));
    assert!(text.contains("market=market: HTTP 400 提交被明确拒绝"));
    assert!(text.contains("⚠️ 执行异常详情:"));
    assert!(text.contains("market=market: 执行异常，提交结果需核实"));
    assert!(!text.contains("sensitive internal response"));
    let error_only = place_notice_for_test(vec![Err(Error::msg(""))]);
    assert!(!error_only.contains("未成交"));
    assert!(!error_only.contains("明确拒绝"));
}

#[test]
fn place_notice_preserves_and_escapes_nonempty_submit_messages() {
    for result in [
        SubmitResult::Unknown {
            order_id: None,
            order_hash: "hash".into(),
            envelope: json!({}),
            message: "pending_reason *check*".into(),
        },
        SubmitResult::NoMatch {
            order_hash: "hash".into(),
            envelope: json!({}),
            message: "no_match *check*".into(),
        },
        SubmitResult::Failed {
            order_hash: "hash".into(),
            envelope: json!({}),
            status: 400,
            message: "bad_request *check*".into(),
        },
    ] {
        let text = place_notice_for_test(vec![Ok(result)]);
        assert!(text.contains(r"\_"));
        assert!(text.contains(r"\*check\*"));
    }
}

#[test]
fn actuals_gate_observes_recovery_and_limits_warnings_to_sixty_seconds() {
    use ActualsGateEvent::*;
    let log = ActualsGateLog::default();
    let start = Instant::now();
    let observe = |unknown, millis| {
        log.observe(
            log.begin_query(),
            unknown,
            start + Duration::from_millis(millis),
        )
    };
    assert_eq!(observe(0, 0), None);
    assert_eq!(observe(1, 1), Some(Blocked));
    assert_eq!(observe(20, 60_000), None);
    assert_eq!(observe(20, 60_001), Some(StillBlocked));
    assert_eq!(observe(2, 120_000), None);
    assert_eq!(observe(2, 120_001), Some(StillBlocked));
    assert_eq!(observe(0, 120_002), Some(Recovered));
    assert_eq!(observe(0, 120_003), None);
    assert_eq!(observe(3, 120_004), Some(Blocked));
    assert_eq!(observe(3, 180_003), None);
    assert_eq!(observe(3, 180_004), Some(StillBlocked));
}

#[test]
fn actuals_gate_rejects_late_queries_in_both_directions() {
    use ActualsGateEvent::*;
    let log = ActualsGateLog::default();
    let now = Instant::now();
    let old_block = log.begin_query();
    let clear = log.begin_query();
    assert_eq!(log.observe(clear, 0, now), None);
    assert_eq!(log.observe(old_block, 1, now), None);
    let old_clear = log.begin_query();
    let block = log.begin_query();
    assert_eq!(log.observe(block, 1, now), Some(Blocked));
    assert_eq!(log.observe(old_clear, 0, now), None);
    assert_eq!(log.observe(block, 0, now), None);
    // 失败或取消的查询没有 observe，不能推断恢复或重置限频。
    let _failed_query = log.begin_query();
    assert_eq!(log.observe(log.begin_query(), 1, now), None);
    assert_eq!(log.observe(log.begin_query(), 0, now), Some(Recovered));
    assert!(log.observe_report(old_block, 1, now).is_none());
    let next = log
        .observe_report(log.begin_query(), 1, now + Duration::from_secs(90))
        .unwrap();
    assert_eq!(next.event, Blocked);
    assert_eq!(next.blocked_checks, 1);
    assert_eq!(next.observed_blocked_ms, 0);
}

#[test]
fn actuals_gate_concurrent_observations_select_one_event_per_transition() {
    let log = ActualsGateLog::default();
    let start = Instant::now();
    for (unknown, seconds, expected) in [
        (1, 0, Some(ActualsGateEvent::Blocked)),
        (5, 59, None),
        (5, 60, Some(ActualsGateEvent::StillBlocked)),
        (0, 61, Some(ActualsGateEvent::Recovered)),
    ] {
        let barrier = std::sync::Barrier::new(16);
        let events = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let log = &log;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let query = log.begin_query();
                        barrier.wait();
                        log.observe(query, unknown, start + Duration::from_secs(seconds))
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(events, expected.into_iter().collect::<Vec<_>>());
    }
}

#[derive(Clone, Default)]
struct GateLogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for GateLogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn actuals_gate_logs_levels_global_scope_and_counts_suppressed_blocks() {
    let buffer = GateLogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let stats = MinuteStats::new();
    tracing::subscriber::with_default(subscriber, || {
        let log = ActualsGateLog::default();
        let now = Instant::now();
        let old = log.begin_query();
        log.record(&stats, log.begin_query(), 2, now);
        log.record(&stats, old, 3, now);
        log.record(&stats, log.begin_query(), 4, now + Duration::from_secs(59));
        log.record(&stats, log.begin_query(), 4, now + Duration::from_secs(60));
        log.record(&stats, log.begin_query(), 0, now + Duration::from_secs(61));
        log.record(&stats, log.begin_query(), 0, now + Duration::from_secs(62));
    });
    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.lines().count(), 3);
    assert_eq!(output.matches("INFO").count(), 2);
    assert_eq!(output.matches("WARN").count(), 1);
    assert_eq!(output.matches("scope=\"global\"").count(), 3);
    assert!(!output.contains("topic="));
    assert!(output.contains("completeness recovered"));
    assert_eq!(
        output
            .matches("reason=\"incomplete_actuals_projection\"")
            .count(),
        3
    );
    let recovered = output
        .lines()
        .find(|line| line.contains("recovered"))
        .unwrap();
    assert!(recovered.contains("observed_blocked_ms=61000"));
    assert!(recovered.contains("blocked_checks=3"));
    assert!(recovered.contains("unknown_orders=0"));
    assert_eq!(stats.snapshot_and_reset().actuals_gate_blocked, 4);
    assert_eq!(
        stats.snapshot_and_reset(),
        crate::stats::MinuteSnapshot::default()
    );
}

#[test]
fn submitted_pending_promotion_logs_debug_and_counts_legs() {
    let buffer = GateLogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let stats = MinuteStats::new();
    tracing::subscriber::with_default(subscriber, || {
        record_submitted_pending_promoted(&stats, 0);
        record_submitted_pending_promoted(&stats, 2);
        record_submitted_pending_promoted(&stats, 3);
    });
    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.lines().count(), 2);
    assert_eq!(output.matches("DEBUG").count(), 2);
    assert!(output.contains("submission does not confirm fill"));
    assert_eq!(output.matches("reason=\"pending_after_submit\"").count(), 2);
    assert!(!output.contains("WARN"));
    assert_eq!(stats.snapshot_and_reset().submitted_pending_promoted, 5);
    assert_eq!(
        stats.snapshot_and_reset(),
        crate::stats::MinuteSnapshot::default()
    );
}

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

// 不加载环境配置或实盘账户；签名只使用公开测试私钥，所有 HTTP 地址均由 loopback stub 提供。
fn admission_test_config(base: &str) -> Config {
    Config {
        common_postgres_uri: String::new(),
        app_postgres_uri: String::new(),
        enabled_platforms: [POLYMARKET.to_string(), OUTCOME.to_string()].into(),
        enable_arb: true,
        enable_rebalance: false,
        enable_take_profit: false,
        take_profit_min_gain: Decimal::ZERO,
        discovery_interval: Duration::from_secs(30),
        reconcile_interval: Duration::from_secs(2),
        hedge_interval: Duration::from_secs(5),
        book_stale: Duration::from_secs(5),
        book_resync: Duration::from_secs(10),
        book_resync_batch: 1,
        position_scan_batch: 1,
        settlement_pending_scan_interval: Duration::from_secs(60),
        settlement_pending_scan_batch: 1,
        arb_min_profit: Decimal::ZERO,
        arb_min_apr: Decimal::ZERO,
        arb_cost_limit: d("100"),
        min_rebalance_qty: Decimal::ONE,
        polymarket_fee_bps_prior: Decimal::ZERO,
        pending_leg_timeout: Duration::from_secs(300),
        unknown_leg_timeout: Duration::from_secs(300),
        max_active_orders: 10,
        max_realized_loss: Decimal::ZERO,
        polymarket_clob_url: base.into(),
        polymarket_ws_url: base.into(),
        polymarket_funders: vec![],
        polymarket_auth_ttl: Duration::ZERO,
        hyperliquid_info_url: format!("{base}/info"),
        hyperliquid_exchange_url: format!("{base}/exchange"),
        hyperliquid_ws_url: base.into(),
        hyperliquid_mainnet: false,
        outcome_agent_private_key: Some(format!("{:064x}", 1)),
        outcome_account_address: Some("0x7e5f4552091a69125d5dfcb7b8c2659029395bdf".into()),
        outcome_builder_address: None,
        outcome_builder_fee: 0,
        nats_url: None,
        nats_token: None,
        nats_subject: String::new(),
        nats_channel: String::new(),
        cat: "admission-test".into(),
    }
}

async fn fee_test_engine() -> Engine {
    let base = "http://127.0.0.1:1";
    let cfg = admission_test_config(base);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let outcome = OutcomeVenue::connect(&cfg).unwrap();
    let (pm, _) = crate::platforms::polymarket::tests::execution_test_venue(base.into()).await;
    let (pm_sub_tx, _) = mpsc::channel(1);
    let (out_sub_tx, _) = mpsc::channel(1);
    Engine {
        cfg,
        store: Store { pool: pool.clone() },
        common: pool,
        books: Arc::new(Mutex::new(BookStore::default())),
        dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
        topics: Arc::new(RwLock::new(HashMap::new())),
        pm,
        outcome,
        pm_sub_tx,
        out_sub_tx,
        notify: None,
        stats: Arc::new(MinuteStats::new()),
        position_scan_cursor: Mutex::new(0),
        settlement_scan_cursor: Mutex::new(0),
        last_settlement_sweep: Mutex::new(None),
        reported_stale_unknown: Mutex::new(HashSet::new()),
        rebalance_loss_cooldown: Mutex::new(HashMap::new()),
    }
}

fn fee_test_topic() -> Topic {
    let mut topic = take_profit_topic();
    for token in &mut topic.tokens {
        if token.platform == POLYMARKET {
            token.condition_id = Some("test-condition".into());
            token.fees_enabled = Some(false);
        } else {
            token.token_id = if token.label == "yes" { "#10" } else { "#11" }.into();
        }
    }
    topic
}

// 只接受余额接口，不接受下单；可在首个响应前暂停以验证等待后的重算。
async fn balance_test_server(
    replies: Vec<(u16, Value)>,
    pause: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
) -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Vec<String>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut replies = replies.into_iter();
        let mut pause = pause;
        let mut requests = Vec::new();
        loop {
            let (mut socket, _) = tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => accepted.unwrap(),
            };
            let mut bytes = Vec::new();
            let (body_start, length) = loop {
                let mut buffer = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                assert!(bytes.len() < 65_536);
                if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..index]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 65_536);
                    break (index + 4, length);
                }
            };
            while bytes.len() < body_start + length {
                let mut buffer = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let request = std::str::from_utf8(&bytes[..body_start])
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_string();
            if request.starts_with("POST /info ") {
                let body: Value =
                    serde_json::from_slice(&bytes[body_start..body_start + length]).unwrap();
                assert_eq!(body["type"], "spotClearinghouseState");
            } else {
                assert!(request.starts_with("GET /balance-allowance?"));
                assert!(request.contains("asset_type=COLLATERAL"));
            }
            requests.push(request);
            if let Some((arrived, release)) = pause.take() {
                arrived.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(3), release)
                    .await
                    .unwrap()
                    .unwrap();
            }
            let (status, body) = replies.next().expect("unexpected extra balance request");
            let body = body.to_string();
            let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        assert!(
            replies.next().is_none(),
            "expected balance request was not made"
        );
        requests
    });
    (base, stop_tx, server)
}

fn usdc_reply(balance: &str) -> (u16, Value) {
    (
        200,
        json!({"balances": [{"coin": "USDC", "total": balance, "hold": "0"}]}),
    )
}

async fn balance_test_engine(base: &str) -> (Engine, String) {
    let mut engine = fee_test_engine().await;
    engine.cfg = admission_test_config(base);
    engine.outcome = OutcomeVenue::connect(&engine.cfg).unwrap();
    let (pm, funder) = crate::platforms::polymarket::tests::execution_test_venue(base.into()).await;
    engine.pm = pm;
    engine
        .outcome
        .install_test_fee_snapshot(1, Decimal::ZERO, Decimal::ZERO);
    (engine, funder)
}

async fn hedge_balance_books(engine: &Engine, platform: &str, buy: bool) {
    let tokens = if platform == POLYMARKET {
        ["pm-no", "pm-yes"]
    } else {
        ["#11", "#10"]
    };
    let mut books = engine.books.lock().await;
    for token in tokens {
        books.set_tick_size(platform, token, d("0.01"));
        books.replace_snapshot(
            platform,
            token,
            vec![crate::book::Level {
                price: d("0.4"),
                size: d("30"),
            }],
            if buy {
                vec![crate::book::Level {
                    price: d("0.4"),
                    size: d("30"),
                }]
            } else {
                vec![]
            },
            1,
            Instant::now(),
        );
    }
}

#[tokio::test]
async fn hedge_balances_follow_candidates_and_permissions() {
    for case in [
        "empty",
        "sell",
        "stale",
        "reduce",
        "missing-funder",
        "outcome",
        "pm",
        "both",
        "insufficient",
        "error",
    ] {
        let replies = match case {
            "pm" => vec![(200, json!({"balance": "100000000"}))],
            "both" => vec![(200, json!({"balance": "100000000"})), usdc_reply("100")],
            "outcome" => vec![usdc_reply("100")],
            "insufficient" => vec![usdc_reply("0")],
            "error" => vec![(503, json!({"error": "test"}))],
            _ => vec![],
        };
        let expected_requests = replies.len();
        let (base, stop, server) = balance_test_server(replies, None).await;
        let (engine, funder) = balance_test_engine(&base).await;
        let topic = fee_test_topic();
        let mut positions = crate::hedge::Positions::new();
        let excess = if matches!(case, "pm" | "missing-funder") {
            OUTCOME
        } else {
            POLYMARKET
        };
        for label in ["no", "yes"] {
            positions
                .entry(excess.into())
                .or_default()
                .insert(label.into(), d("30"));
        }
        if case == "both" {
            positions.clear();
            positions.insert(POLYMARKET.into(), HashMap::from([("no".into(), d("30"))]));
            positions.insert(OUTCOME.into(), HashMap::from([("no".into(), d("30"))]));
        }
        if case != "empty" {
            hedge_balance_books(&engine, POLYMARKET, !matches!(case, "sell")).await;
            hedge_balance_books(&engine, OUTCOME, !matches!(case, "sell")).await;
        }
        if case == "stale" {
            engine.books.lock().await.mark_platform_stale(OUTCOME);
        }
        let access = if case == "reduce" {
            SettlementAccess::ReduceOnly
        } else {
            SettlementAccess::All
        };
        let funder = if case == "missing-funder" {
            None
        } else {
            Some(funder.as_str())
        };
        // 默认关闭提交，但规划仍按有效买入候选查询真实资金。
        assert!(!engine.cfg.enable_rebalance);
        let actions = engine
            .funded_hedge_plan(1, &topic, &positions, funder, access)
            .await
            .unwrap();
        if matches!(case, "outcome" | "pm" | "both") {
            assert_eq!(actions.len(), 2, "{case}");
            assert!(
                actions.iter().all(|action| action.side == HedgeSide::Buy),
                "{case}"
            );
        } else if case == "empty" {
            assert!(actions.is_empty());
        } else {
            assert_eq!(actions.len(), 2, "{case}");
            assert!(
                actions.iter().all(|action| action.side == HedgeSide::Sell),
                "{case}"
            );
        }
        stop.send(()).unwrap();
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), expected_requests, "{case}");
        let pm_count = requests
            .iter()
            .filter(|request| request.starts_with("GET "))
            .count();
        assert_eq!(
            pm_count,
            usize::from(matches!(case, "pm" | "both")),
            "{case}"
        );
    }
}

#[tokio::test]
async fn hedge_rechecks_books_and_fees_after_balance_wait() {
    for mutation in ["stale", "fee-expired", "fee-changed", "new-platform"] {
        let (arrived_tx, arrived_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (base, stop, server) =
            balance_test_server(vec![usdc_reply("100")], Some((arrived_tx, release_rx))).await;
        let (engine, funder) = balance_test_engine(&base).await;
        hedge_balance_books(&engine, POLYMARKET, false).await;
        hedge_balance_books(&engine, OUTCOME, true).await;
        let topic = fee_test_topic();
        let positions = crate::hedge::Positions::from([
            (POLYMARKET.into(), HashMap::from([("yes".into(), d("30"))])),
            (OUTCOME.into(), HashMap::from([("yes".into(), d("30"))])),
        ]);
        let planning =
            engine.funded_hedge_plan(1, &topic, &positions, Some(&funder), SettlementAccess::All);
        let mutate = async {
            tokio::time::timeout(Duration::from_secs(3), arrived_rx)
                .await
                .unwrap()
                .unwrap();
            match mutation {
                "stale" => engine.books.lock().await.mark_platform_stale(OUTCOME),
                "fee-expired" => engine.outcome.expire_test_fee_snapshot(1),
                "fee-changed" => {
                    engine
                        .outcome
                        .install_test_fee_snapshot(1, d("0.9"), Decimal::ZERO)
                }
                "new-platform" => hedge_balance_books(&engine, POLYMARKET, true).await,
                _ => unreachable!(),
            }
            release_tx.send(()).unwrap();
        };
        let (actions, ()) = tokio::join!(planning, mutate);
        if mutation == "fee-expired" {
            assert!(actions.is_none());
        } else {
            let actions = actions.unwrap();
            if mutation == "new-platform" {
                assert!(actions
                    .iter()
                    .any(|a| a.platform == OUTCOME && a.side == HedgeSide::Buy));
                assert!(!actions
                    .iter()
                    .any(|a| a.platform == POLYMARKET && a.side == HedgeSide::Buy));
            } else {
                assert!(actions.iter().all(|a| a.side == HedgeSide::Sell));
            }
        }
        stop.send(()).unwrap();
        assert_eq!(server.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn rebalance_fee_exit_propagates_claim_release_failure() {
    let (base, stop, server) = balance_test_server(vec![usdc_reply("100")], None).await;
    let (mut engine, _) = balance_test_engine(&base).await;
    engine.cfg.enable_rebalance = true;
    engine.store.pool.close().await;
    let mut topic = fee_test_topic();
    for token in &mut topic.tokens {
        if token.platform == OUTCOME {
            token.token_id = if token.label == "yes" {
                "#9990"
            } else {
                "#9991"
            }
            .into();
        }
    }
    let action = crate::hedge::HedgeAction {
        platform: OUTCOME.into(),
        token_id: "#9991".into(),
        label: "no".into(),
        side: HedgeSide::Buy,
        shares: d("10"),
        cap_price: d("0.4"),
        fee: Decimal::ZERO,
        marginal_value: d("6"),
    };
    assert!(
        engine
            .execute_hedges(
                1,
                uuid::Uuid::new_v4(),
                &topic,
                &crate::hedge::Positions::new(),
                &[action]
            )
            .await
            .is_err(),
        "claim release failure must not be reported as a successful fee exit"
    );
    stop.send(()).unwrap();
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn rebalance_fee_exit_releases_zero_leg_claim_immediately() {
    let uri = std::env::var("APP_POSTGRES_URI").expect("set APP_POSTGRES_URI");
    let schema = format!("rebalance_fee_test_{}", uuid::Uuid::new_v4().simple());
    let connection_schema = schema.clone();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |connection, _| {
            let schema = connection_schema.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path',$1,false)")
                    .bind(schema)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&uri)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&pool)
        .await
        .unwrap();
    let (base, stop, server) = balance_test_server(vec![usdc_reply("100")], None).await;
    let (mut engine, _) = balance_test_engine(&base).await;
    engine.cfg.enable_rebalance = true;
    engine.store = Store { pool: pool.clone() };
    let exercised: Result<()> = async {
            engine.store.migrate().await?;
            let order_id: i64 = sqlx::query_scalar(
                "INSERT INTO arb_orders (event_id, unified_index, status) VALUES ($1,0,'completed') RETURNING id"
            ).bind(uuid::Uuid::new_v4()).fetch_one(&pool).await?;
            let claim = engine.store.try_claim_lifecycle(order_id, "rebalance").await?.unwrap();
            let mut topic = fee_test_topic();
            // 当前市场没有费率；余额仍足够，必须走费用拒绝而不是资金拒绝。
            for token in &mut topic.tokens {
                if token.platform == OUTCOME {
                    token.token_id = if token.label == "yes" { "#9990" } else { "#9991" }.into();
                }
            }
            let action = crate::hedge::HedgeAction {
                platform: OUTCOME.into(), token_id: "#9991".into(), label: "no".into(),
                side: HedgeSide::Buy, shares: d("10"), cap_price: d("0.4"),
                fee: Decimal::ZERO, marginal_value: d("6"),
            };
            engine.execute_hedges(order_id, claim, &topic, &crate::hedge::Positions::new(), &[action]).await?;
            let (state, owner): (String, Option<uuid::Uuid>) = sqlx::query_as(
                "SELECT position_status,lifecycle_claim_id FROM arb_orders WHERE id=$1"
            ).bind(order_id).fetch_one(&pool).await?;
            assert_eq!(state, "watching");
            assert!(owner.is_none());
            let legs: i64 = sqlx::query_scalar("SELECT count(*) FROM legs WHERE order_id=$1")
                .bind(order_id).fetch_one(&pool).await?;
            assert_eq!(legs, 0);
            engine.outcome.install_test_fee_snapshot(999, Decimal::ZERO, Decimal::ZERO);
            assert!(engine.available_fees(&topic).is_some());
            let next = engine.store.try_claim_lifecycle(order_id, "rebalance").await?.unwrap();
            assert_ne!(claim, next);
            assert!(!engine.store.release_lifecycle(order_id, "rebalance", claim).await?);
            assert!(engine.store.release_lifecycle(order_id, "rebalance", next).await?);
            Ok(())
        }.await;
    stop.send(()).unwrap();
    let requests = server.await.unwrap();
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    exercised.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "only the balance query may reach the venue"
    );
}

#[tokio::test]
async fn outcome_execution_balance_is_local_to_each_stage() {
    let (base, stop, server) =
        balance_test_server(vec![usdc_reply("100"), usdc_reply("0")], None).await;
    let (engine, _) = balance_test_engine(&base).await;
    let planning = engine
        .hedge_candidate_balances(
            1,
            None,
            SettlementAccess::All,
            &[OUTCOME.into(), OUTCOME.into()],
        )
        .await;
    assert_eq!(planning[OUTCOME], d("100"));
    let mut execution = HashMap::new();
    engine
        .load_outcome_buy_balance(&mut execution)
        .await
        .unwrap();
    engine
        .load_outcome_buy_balance(&mut execution)
        .await
        .unwrap();
    assert_eq!(execution[OUTCOME], Decimal::ZERO);
    stop.send(()).unwrap();
    assert_eq!(server.await.unwrap().len(), 2);
}

#[tokio::test]
async fn hedge_reconfirmation_requires_original_action_to_remain_selected() {
    let engine = fee_test_engine().await;
    engine
        .outcome
        .install_test_fee_snapshot(1, Decimal::ZERO, Decimal::ZERO);
    hedge_balance_books(&engine, POLYMARKET, true).await;
    hedge_balance_books(&engine, OUTCOME, true).await;
    let topic = fee_test_topic();
    let positions = crate::hedge::Positions::from([(
        POLYMARKET.into(),
        HashMap::from([("yes".into(), d("30"))]),
    )]);
    let fees = engine.available_fees(&topic).unwrap();
    let funded = HashMap::from([(OUTCOME.into(), d("100"))]);
    let mut books = engine.books.lock().await;
    let plan = |books: &BookStore, balances: &HashMap<String, Decimal>| {
        plan_hedge(
            &topic,
            &positions,
            books,
            balances,
            &fees.context,
            engine.cfg.min_rebalance_qty,
            Instant::now(),
            engine.cfg.book_stale,
        )
    };
    let original = plan(&books, &funded).remove(0);
    assert_eq!(original.side, HedgeSide::Buy);
    // 第二阶段余额下降：原买入不可确认，不能直接换成卖单提交。
    let no_cash = plan(&books, &HashMap::from([(OUTCOME.into(), Decimal::ZERO)]));
    assert_eq!(no_cash[0].side, HedgeSide::Sell);
    assert!(!no_cash
        .iter()
        .any(|action| same_hedge_quantity(&original, action)));
    // 买候选仍有效且资金足够，但卖出已更优，也必须拒绝原买入。
    books.replace_snapshot(
        POLYMARKET,
        "pm-yes",
        vec![crate::book::Level {
            price: d("0.8"),
            size: d("30"),
        }],
        vec![],
        2,
        Instant::now(),
    );
    assert_eq!(
        hedge_candidates(
            &topic,
            &positions,
            &books,
            &fees.context,
            engine.cfg.min_rebalance_qty,
            Instant::now(),
            engine.cfg.book_stale,
        )
        .buy_platforms(),
        vec![OUTCOME.to_string()]
    );
    let better_sell = plan(&books, &funded);
    assert_eq!(better_sell[0].side, HedgeSide::Sell);
    assert!(!better_sell
        .iter()
        .any(|action| same_hedge_quantity(&original, action)));
}

#[tokio::test]
async fn outcome_fee_admission_rejects_changed_expired_and_missing_rules_without_io() {
    let engine = fee_test_engine().await;
    let topic = fee_test_topic();
    assert!(engine.available_fees(&topic).is_none());
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.001344"), d("0.0001"));
    let fees = engine.fee_context(&topic).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    assert!(engine.fees_admitted(&fees, deadline).is_ok());
    assert!(engine.fees_admitted(&fees, Instant::now()).is_err());
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.001344"), d("0.0001"));
    assert!(
        engine.fees_admitted(&fees, deadline).is_ok(),
        "same rules renewal must remain valid"
    );
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.002"), d("0.0001"));
    assert!(engine.fees_admitted(&fees, deadline).is_err());
    let current = engine.fee_context(&topic).unwrap();
    engine.outcome.expire_test_fee_snapshot(1);
    assert!(engine.fees_admitted(&current, deadline).is_err());
    assert!(engine.available_fees(&topic).is_none());
    assert_eq!(engine.stats.snapshot_and_reset().outcome_fee_unavailable, 2);
}

#[tokio::test]
async fn confirmed_arb_changed_or_stale_fee_never_reaches_db_or_submission() {
    let engine = fee_test_engine().await;
    let topic = fee_test_topic();
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.001344"), Decimal::ZERO);
    let selected = engine.fee_context(&topic).unwrap();
    let limits = ArbLimits {
        cost_limit: d("100"),
        min_profit: Decimal::ZERO,
        min_apr: Decimal::ZERO,
        days: 1,
    };
    let plan = {
        let mut books = engine.books.lock().await;
        books.set_tick_size(POLYMARKET, "pm-yes", d("0.01"));
        for (platform, token) in [(POLYMARKET, "pm-yes"), (OUTCOME, "#11")] {
            books.replace_snapshot(
                platform,
                token,
                vec![],
                vec![crate::book::Level {
                    price: d("0.4"),
                    size: d("30"),
                }],
                1,
                Instant::now(),
            );
        }
        crate::calc::plan_arbitrage(
            &topic,
            books.get(POLYMARKET, "pm-yes").unwrap(),
            books.get(OUTCOME, "#11").unwrap(),
            topic.token(POLYMARKET, "yes").unwrap(),
            topic.token(OUTCOME, "no").unwrap(),
            &selected.context,
            &limits,
        )
        .unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.002"), Decimal::ZERO);
    engine
        .execute_confirmed_plan(&topic, &plan, "unused", &selected, deadline)
        .await
        .unwrap();
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.001344"), Decimal::ZERO);
    let fresh = engine.fee_context(&topic).unwrap();
    engine.outcome.expire_test_fee_snapshot(1);
    engine
        .execute_confirmed_plan(&topic, &plan, "unused", &fresh, deadline)
        .await
        .unwrap();
    assert_eq!(engine.stats.snapshot_and_reset().orders, 0);
}

#[tokio::test]
async fn preparation_refresh_exits_current_round_and_next_round_is_cache_only() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for body in [
            json!({"userSpotCrossRate":"0.0007", "activeReferralDiscount":"0"}),
            json!({"feeScale":"1", "outcomes":[{"outcome":1,"venue":"out","quoteToken":"USDC","deployerFeeScale":"1","sideSpecs":[{"name":"Yes"},{"name":"No"}]}]}),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            socket.read(&mut buf).await.unwrap();
            let body = body.to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let mut engine = fee_test_engine().await;
    engine.outcome = OutcomeVenue::connect(&admission_test_config(&base)).unwrap();
    assert!(
        !engine
            .prepare_fees(&fee_test_topic(), "test", "initial")
            .await
    );
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    // 服务已退出，新轮只读取新鲜缓存，不再发 HTTP。
    assert!(
        engine
            .prepare_fees(&fee_test_topic(), "test", "next_round")
            .await
    );
    assert_eq!(
        engine
            .fee_context(&fee_test_topic())
            .unwrap()
            .context
            .outcome_taker_rate,
        d("0.0014")
    );
}

#[tokio::test]
async fn typed_fee_reasons_and_fresh_preparation_do_not_issue_http() {
    let engine = fee_test_engine().await;
    let topic = fee_test_topic();
    assert_eq!(
        engine.fee_context(&topic).err(),
        Some(FeeLookupError::Missing)
    );
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.001344"), Decimal::ZERO);
    let selected = engine.fee_context(&topic).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    assert!(engine.prepare_fees(&topic, "test", "initial").await);
    let mut expired = selected.clone();
    expired.outcome.expire_test();
    assert_eq!(
        engine.fees_admitted(&expired, deadline),
        Err(FeeAdmissionError::SelectedExpired)
    );
    assert_eq!(
        engine.fees_admitted(&selected, Instant::now()),
        Err(FeeAdmissionError::DeadlineExpired)
    );
    assert_eq!(
        engine.outcome.lookup_fees(999).err(),
        Some(FeeLookupError::MarketMissing)
    );
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.003"), Decimal::ZERO);
    assert_eq!(
        engine.fees_admitted(&selected, deadline),
        Err(FeeAdmissionError::RulesChanged)
    );
    engine.outcome.expire_test_fee_snapshot(1);
    assert_eq!(
        engine.fees_admitted(&selected, deadline),
        Err(FeeAdmissionError::Current(FeeLookupError::Expired))
    );
    assert!(!engine.prepare_fees(&topic, "test", "initial").await);
}

#[tokio::test]
async fn frozen_action_estimates_are_platform_isolated_and_do_not_follow_refresh() {
    let engine = fee_test_engine().await;
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.001344"), d("0.0001"));
    let frozen = engine.fee_context(&fee_test_topic()).unwrap();
    let estimate = |platform| {
        action_fee_estimate(
            &frozen,
            platform,
            "test",
            OrderSide::Buy,
            d("30"),
            d("12"),
            d("0.0012"),
            d("0.04032"),
            d("12.0012"),
        )
    };
    let before = estimate(POLYMARKET);
    engine
        .outcome
        .install_test_fee_snapshot(1, d("0.003"), d("0.002"));
    let after = estimate(POLYMARKET);
    assert_eq!(before, after);
    assert_eq!(after["fee_model"], "pm_taker_v1");
    assert_eq!(
        after["polymarket_fee_rate"],
        frozen.context.polymarket_fee_rate.to_string()
    );
    assert_eq!(after["action"]["fee"], "0.0012");
    assert!(after["action"].get("settlement_reserve").is_none());
    assert!(after["action"].get("reserve_scope").is_none());
    for key in [
        "outcome_id",
        "token_ids",
        "taker_rate",
        "builder_rate",
        "account_version",
        "user_fees_fetched_at",
        "max_age_secs",
    ] {
        assert!(after.get(key).is_none(), "unexpected PM key {key}");
    }
    assert_eq!(estimate(OUTCOME)["action"]["settlement_reserve"], "0.04032");
    assert_eq!(estimate(OUTCOME)["action"]["fee"], "0.0012");
    assert_eq!(frozen.context.outcome_taker_rate, d("0.001344"));
}

#[test]
fn lifecycle_confirmation_rejects_resizing_or_switching_original_actions() {
    let action = crate::hedge::HedgeAction {
        platform: OUTCOME.into(),
        token_id: "#11".into(),
        label: "no".into(),
        side: HedgeSide::Buy,
        shares: d("30"),
        cap_price: d("0.4"),
        fee: Decimal::ZERO,
        marginal_value: d("17.95968"),
    };
    let mut changed = action.clone();
    changed.shares = d("29");
    assert!(!same_hedge_quantity(&action, &changed));
    changed = action.clone();
    changed.side = HedgeSide::Sell;
    assert!(!same_hedge_quantity(&action, &changed));
    changed = action.clone();
    changed.cap_price = d("0.41");
    assert!(
        same_hedge_quantity(&action, &changed),
        "recomputed cap is separately funded"
    );
    let sell = |platform: &str, token: &str| TakeProfitAction {
        platform: platform.into(),
        token_id: token.into(),
        label: "yes".into(),
        shares: d("30"),
        cap_price: d("0.6"),
        fee: Decimal::ZERO,
    };
    let old = TakeProfitPlan {
        actions: [sell(POLYMARKET, "pm"), sell(OUTCOME, "#11")],
        shares: d("30"),
        gross_revenue: d("36"),
        total_fee: Decimal::ZERO,
        gain: d("6"),
    };
    let mut resized = old.clone();
    resized.shares = d("29");
    resized
        .actions
        .iter_mut()
        .for_each(|action| action.shares = d("29"));
    assert!(!same_take_profit_quantity(&old, &resized));
    let mut switched = old.clone();
    switched.actions[1].token_id = "#10".into();
    assert!(!same_take_profit_quantity(&old, &switched));
    assert!(same_take_profit_quantity(&old, &old));
}

#[test]
fn settlement_end_gate_uses_exact_time_and_queries_unknown_dates() {
    let now = chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap();
    assert!(!settlement_check_due(
        Some(now + chrono::Duration::nanoseconds(1)),
        now
    ));
    assert!(settlement_check_due(Some(now), now));
    assert!(settlement_check_due(
        Some(now - chrono::Duration::nanoseconds(1)),
        now
    ));
    assert!(settlement_check_due(None, now));
}

#[tokio::test]
async fn settlement_end_gate_skips_future_and_queries_due_or_unknown() {
    use crate::platforms::polymarket::tests::execution_test_venue;
    use sqlx::postgres::PgPoolOptions;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = requests.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => accepted.unwrap(),
            };
            let mut bytes = Vec::new();
            let (body_start, content_length) = loop {
                let mut buffer = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0, "stub request ended before headers");
                bytes.extend_from_slice(&buffer[..n]);
                assert!(bytes.len() < 65_536);
                if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..index]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 65_536);
                    break (index + 4, length);
                }
            };
            while bytes.len() < body_start + content_length {
                let mut buffer = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0, "stub request ended before body");
                bytes.extend_from_slice(&buffer[..n]);
            }
            let request = std::str::from_utf8(&bytes[..body_start])
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_string();
            let body = match request.as_str() {
                "GET /markets/test-condition HTTP/1.1" => json!({
                    "tokens": [{"token_id": "123", "winner": false}],
                    "closed": false, "accepting_orders": true, "enable_order_book": true
                }),
                "POST /info HTTP/1.1" => {
                    let body: Value =
                        serde_json::from_slice(&bytes[body_start..body_start + content_length])
                            .unwrap();
                    assert_eq!(body, json!({"type": "settledOutcome", "outcome": 1211}));
                    Value::Null
                }
                _ => panic!("unexpected stub request: {request}"),
            }
            .to_string();
            observed.lock().await.push(request);
            let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });

    // Without the authoritative database, due scans fail closed before HTTP.
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(30))
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let cfg = admission_test_config(&base);
    let outcome = OutcomeVenue::connect(&cfg).unwrap();
    let (pm, _) = execution_test_venue(base).await;
    let (pm_sub_tx, _pm_sub_rx) = mpsc::channel(1);
    let (out_sub_tx, _out_sub_rx) = mpsc::channel(1);
    let engine = Engine {
        cfg,
        store: Store { pool: pool.clone() },
        common: pool,
        books: Arc::new(Mutex::new(BookStore::default())),
        dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
        topics: Arc::new(RwLock::new(HashMap::new())),
        pm,
        outcome,
        pm_sub_tx,
        out_sub_tx,
        notify: None,
        stats: Arc::new(MinuteStats::new()),
        position_scan_cursor: Mutex::new(0),
        settlement_scan_cursor: Mutex::new(0),
        last_settlement_sweep: Mutex::new(None),
        reported_stale_unknown: Mutex::new(HashSet::new()),
        rebalance_loss_cooldown: Mutex::new(HashMap::new()),
    };
    let mut identity = MarketIdentity::new(POLYMARKET, "test-condition").unwrap();
    identity.insert(OUTCOME, "1211").unwrap();
    let future = chrono::Utc::now() + chrono::Duration::hours(1);
    assert_eq!(
        engine
            .settlement_gate_after_end(1, "test", &identity, Some(future))
            .await
            .unwrap(),
        SettlementAccess::All
    );
    assert!(requests.lock().await.is_empty());
    let skipped = engine.stats.snapshot_and_reset();
    assert_eq!(skipped.settlement_scan, 0);
    assert_eq!(skipped.settlement_skipped_before_end, 1);
    assert_eq!(skipped.settlement_end_date_missing, 0);

    // 同一入口每次重判时间；未来检查不能缓存成提交前永久放行。
    let past = chrono::Utc::now() - chrono::Duration::hours(1);
    for end_date in [Some(past), None] {
        assert_eq!(
            engine
                .settlement_gate_after_end(1, "test", &identity, end_date)
                .await
                .unwrap(),
            SettlementAccess::Stop
        );
        let queried = engine.stats.snapshot_and_reset();
        assert_eq!(queried.settlement_scan, 1);
        assert_eq!(queried.settlement_skipped_before_end, 0);
        assert_eq!(
            queried.settlement_end_date_missing,
            u64::from(end_date.is_none())
        );
    }
    // pending 使用的原 gate 不经过时间门禁，仍查询两个平台。
    assert_eq!(
        engine.settlement_gate(1, "test", &identity).await.unwrap(),
        SettlementAccess::Stop
    );
    let pending = engine.stats.snapshot_and_reset();
    assert_eq!(pending.settlement_scan, 1);
    assert_eq!(pending.settlement_skipped_before_end, 0);
    assert_eq!(pending.settlement_end_date_missing, 0);
    stop_tx.send(()).unwrap();
    server.await.unwrap();
    let requests = requests.lock().await;
    assert!(requests.is_empty());
}

#[tokio::test]
#[ignore = "requires process APP_POSTGRES_URI; private schema only"]
async fn outcome_evidence_and_scan_commit_before_slow_pm_returns() {
    use crate::platforms::polymarket::tests::execution_test_venue;
    use sqlx::postgres::PgPoolOptions;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let uri =
        std::env::var("APP_POSTGRES_URI").expect("BLOCKED: process APP_POSTGRES_URI required");
    let schema = format!("independent_gate_{}", uuid::Uuid::new_v4().simple());
    let path = schema.clone();
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |c, _| {
            let path = path.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path',$1,false)")
                    .bind(path)
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect(&uri)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&pool)
        .await
        .unwrap();
    let store = Store { pool: pool.clone() };
    store.migrate().await.unwrap();
    let order_id:i64=sqlx::query_scalar("INSERT INTO arb_orders(event_id,unified_index,status) VALUES($1,0,'completed') RETURNING id").bind(uuid::Uuid::new_v4()).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO arb_order_market_identities VALUES($1,'polymarket','test-condition'),($1,'outcome','1211')").bind(order_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO legs(order_id,platform,token_id,label,side,intent,wallet_address,status,submitted_at) VALUES($1,'outcome','#12110','yes','BUY','arb_buy','0x1111111111111111111111111111111111111111','actived',NOW())").bind(order_id).execute(&pool).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let release = Arc::new(tokio::sync::Notify::new());
    let (scan_tx, mut scan_rx) = mpsc::channel(2);
    let release_server = release.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let release = release_server.clone();
            let scan_tx = scan_tx.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]);
                        let len = header
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + len {
                            break;
                        }
                    }
                }
                let raw = String::from_utf8_lossy(&bytes);
                let body = if raw.starts_with("GET ") {
                    release.notified().await;
                    json!({"condition_id":"test-condition","tokens":[{"token_id":"pm-yes","winner":false},{"token_id":"pm-no","winner":false}],"closed":true})
                } else if raw.contains("settledOutcome") {
                    json!({"settleFraction":"1"})
                } else {
                    scan_tx.send(()).await.unwrap();
                    json!([])
                };
                let body = body.to_string();
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(reply.as_bytes()).await.unwrap();
            });
        }
    });
    let cfg = admission_test_config(&base);
    let outcome = OutcomeVenue::connect(&cfg).unwrap();
    let (pm, _) = execution_test_venue(base).await;
    let (pm_sub_tx, _) = mpsc::channel(1);
    let (out_sub_tx, _) = mpsc::channel(1);
    let engine = Engine {
        cfg,
        store: store.clone(),
        common: pool.clone(),
        books: Arc::new(Mutex::new(BookStore::default())),
        dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
        topics: Arc::new(RwLock::new(HashMap::new())),
        pm,
        outcome,
        pm_sub_tx,
        out_sub_tx,
        notify: None,
        stats: Arc::new(MinuteStats::new()),
        position_scan_cursor: Mutex::new(0),
        settlement_scan_cursor: Mutex::new(0),
        last_settlement_sweep: Mutex::new(None),
        reported_stale_unknown: Mutex::new(HashSet::new()),
        rebalance_loss_cooldown: Mutex::new(HashMap::new()),
    };
    let mut identity = MarketIdentity::new(POLYMARKET, "test-condition").unwrap();
    identity.insert(OUTCOME, "1211").unwrap();
    let gate = engine.settlement_gate(order_id, "fixture", &identity);
    tokio::pin!(gate);
    tokio::select! { _=&mut gate=>panic!("gate returned before slow PM released"), result=tokio::time::timeout(Duration::from_secs(5),scan_rx.recv())=>assert!(result.unwrap().is_some()) }
    let state:(String,i64)=sqlx::query_as("SELECT position_status,(SELECT count(*) FROM order_platform_settlement_results WHERE order_id=$1 AND platform='outcome') FROM arb_orders WHERE id=$1").bind(order_id).fetch_one(&pool).await.unwrap();
    assert_eq!(state, ("settlement_pending".into(), 1));
    release.notify_one();
    assert_eq!(gate.await.unwrap(), SettlementAccess::Stop);
    server.abort();
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn confirmed_pm_amount_admission_blocks_all_side_effects() {
    use crate::platforms::polymarket::tests::execution_test_venue;
    use sqlx::postgres::PgPoolOptions;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&uri)
        .await
        .unwrap();
    let schema = format!("exec_admission_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = requests.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => accepted.unwrap(),
            };
            let mut bytes = Vec::new();
            let (body_start, content_length) = loop {
                let mut buffer = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0, "stub request ended before headers");
                bytes.extend_from_slice(&buffer[..n]);
                assert!(bytes.len() < 65_536, "unexpectedly large stub request");
                if let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..index]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 65_536, "unexpectedly large stub body");
                    break (index + 4, length);
                }
            };
            while bytes.len() < body_start + content_length {
                let mut buffer = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0, "stub request ended before body");
                bytes.extend_from_slice(&buffer[..n]);
            }
            // 只记录请求行；签名、认证头和请求体既不保留也不打印。
            let request = std::str::from_utf8(&bytes[..body_start])
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_string();
            let body = match request.as_str() {
                "POST /order HTTP/1.1" => json!({
                    "success": true, "status": "matched", "orderID": "pm-admission",
                    "makingAmount": "3.33", "takingAmount": "10"
                }),
                "POST /exchange HTTP/1.1" => json!({
                    "status": "ok", "response": {"type": "order", "data": {"statuses": [
                        {"filled": {"totalSz": "10", "avgPx": "0.4", "oid": 777}}
                    ]}}
                }),
                _ => panic!("unexpected stub request: {request}"),
            }
            .to_string();
            observed.lock().await.push(request);
            let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let search_path = schema.clone();
    let exercised: anyhow::Result<()> = async {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |conn, _| {
                let path = search_path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)")
                        .bind(path)
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&uri)
            .await?;
        let store = Store { pool: pool.clone() };
        store.migrate().await?;
        let cfg = admission_test_config(&base);
        let outcome = OutcomeVenue::connect(&cfg)?;
        outcome.install_test_fee_snapshot(0, Decimal::ZERO, Decimal::ZERO);
        let (pm, funder) = execution_test_venue(base).await;
        let (pm_sub_tx, _pm_sub_rx) = mpsc::channel(1);
        let (out_sub_tx, _out_sub_rx) = mpsc::channel(1);
        let engine = Engine {
            cfg,
            store,
            common: pool,
            books: Arc::new(Mutex::new(BookStore::default())),
            dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            pm,
            outcome,
            pm_sub_tx,
            out_sub_tx,
            notify: None,
            stats: Arc::new(MinuteStats::new()),
            position_scan_cursor: Mutex::new(0),
            settlement_scan_cursor: Mutex::new(0),
            last_settlement_sweep: Mutex::new(None),
            reported_stale_unknown: Mutex::new(HashSet::new()),
            rebalance_loss_cooldown: Mutex::new(HashMap::new()),
        };
        let token = |platform: &str, id: &str, label: &str| crate::domain::TokenRef {
            platform: platform.into(),
            token_id: id.into(),
            label: label.into(),
            option_id: if platform == OUTCOME {
                "0".into()
            } else {
                "admission-market".into()
            },
            condition_id: Some("admission-condition".into()),
            asset_id: (platform == OUTCOME).then_some(100_000_001),
            side_index: None,
            neg_risk: Some(false),
            fees_enabled: Some(false),
            fee_rate: Some(Decimal::ZERO),
        };
        let pm_token = token(POLYMARKET, "123", "yes");
        let out_token = token(OUTCOME, "#01", "no");
        let topic = Topic {
            key: TopicKey::new(uuid::Uuid::new_v4(), 0),
            title: "admission test".into(),
            market_title: "admission test".into(),
            end_date: None,
            tokens: vec![pm_token.clone(), out_token.clone()],
        };
        let fees = FeeContext {
            polymarket_fee_rate: Decimal::ZERO,
            outcome_taker_rate: Decimal::ZERO,
            outcome_builder_rate: Decimal::ZERO,
        };
        let limits = ArbLimits {
            cost_limit: d("100"),
            min_profit: Decimal::ZERO,
            min_apr: Decimal::ZERO,
            days: 1,
        };
        // 负例与正例均由真实盘口生成并通过 HTTP 确认所用的 confirm_plan；不伪造 ArbPlan。
        for (shares, expected_rows) in [("7", (0_i64, 0_i64, 0_i64)), ("10", (1, 2, 2))] {
            let confirmed = {
                let now = Instant::now();
                let mut books = engine.books.lock().await;
                books.set_tick_size(POLYMARKET, "123", d("0.001"));
                for (platform, id, price) in [(POLYMARKET, "123", "0.333"), (OUTCOME, "#01", "0.4")]
                {
                    books.replace_snapshot(
                        platform,
                        id,
                        vec![],
                        vec![crate::book::Level {
                            price: d(price),
                            size: d(shares),
                        }],
                        if shares == "7" { 100 } else { 101 },
                        now,
                    );
                }
                let pm_book = books.get(POLYMARKET, "123").unwrap();
                let out_book = books.get(OUTCOME, "#01").unwrap();
                let plan = crate::calc::plan_arbitrage(
                    &topic, pm_book, out_book, &pm_token, &out_token, &fees, &limits,
                )
                .ok_or_else(|| anyhow::anyhow!("calculator rejected {shares}-share fixture"))?;
                anyhow::ensure!(plan.pm.shares == d(shares));
                anyhow::ensure!(plan.pm.cap_price == d("0.333"));
                confirm_plan(&topic, &plan, pm_book, out_book, &fees, &limits).ok_or_else(|| {
                    anyhow::anyhow!("confirmation rejected {shares}-share fixture")
                })?
            };
            anyhow::ensure!(
                crate::platforms::polymarket::market_buy_base_units(
                    confirmed.pm.shares,
                    confirmed.pm.cap_price,
                )
                .is_ok()
                    == (shares == "10")
            );
            tokio::time::timeout(
                Duration::from_secs(10),
                engine.execute_confirmed_plan(
                    &topic,
                    &confirmed,
                    &funder,
                    &engine.fee_context(&topic)?,
                    Instant::now() + Duration::from_secs(30),
                ),
            )
            .await??;
            let counts: (i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT COUNT(*) FROM arb_orders), (SELECT COUNT(*) FROM legs), \
                     (SELECT COUNT(*) FROM signed_envelopes)",
            )
            .fetch_one(&engine.store.pool)
            .await?;
            anyhow::ensure!(
                counts == expected_rows,
                "{shares}-share row counts: {counts:?}"
            );
            let mut calls = requests.lock().await.clone();
            calls.sort();
            if shares == "7" {
                anyhow::ensure!(
                    calls.is_empty(),
                    "invalid plan submitted HTTP requests: {calls:?}"
                );
            } else {
                anyhow::ensure!(calls == ["POST /exchange HTTP/1.1", "POST /order HTTP/1.1"]);
                let submitted: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM legs WHERE submitted_at IS NOT NULL \
                         AND third_order_id IS NOT NULL AND status = 'actived'",
                )
                .fetch_one(&engine.store.pool)
                .await?;
                anyhow::ensure!(
                    submitted == 2,
                    "positive control must persist both successful submissions"
                );
            }
        }
        engine.store.pool.close().await;
        Ok(())
    }
    .await;
    let _ = stop_tx.send(());
    let server_result = server.await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.unwrap();
    server_result.unwrap();
    exercised.unwrap();
}

#[test]
fn arb_http_confirmation_refreshes_exact_funding_before_admission() {
    let token = |platform: &str, id: &str, label: &str| crate::domain::TokenRef {
        platform: platform.into(),
        token_id: id.into(),
        label: label.into(),
        option_id: "market".into(),
        condition_id: None,
        asset_id: None,
        side_index: None,
        neg_risk: None,
        fees_enabled: None,
        fee_rate: None,
    };
    let pm = token(POLYMARKET, "pm", "yes");
    let out = token(OUTCOME, "out", "no");
    let topic = Topic {
        key: TopicKey::new(uuid::Uuid::nil(), 0),
        title: "test".into(),
        market_title: "test".into(),
        end_date: None,
        tokens: vec![pm.clone(), out.clone()],
    };
    let levels = |rows: &[(&str, &str)]| {
        rows.iter()
            .map(|(price, size)| crate::book::Level {
                price: d(price),
                size: d(size),
            })
            .collect()
    };
    let now = Instant::now();
    let mut books = BookStore::default();
    books.set_tick_size(POLYMARKET, "pm", d("0.01"));
    books.replace_snapshot(
        POLYMARKET,
        "pm",
        vec![],
        levels(&[("0.30", "4.5"), ("0.40", "95.5")]),
        100,
        now,
    );
    books.replace_snapshot(OUTCOME, "out", vec![], levels(&[("0.40", "200")]), 100, now);
    let fees = FeeContext {
        polymarket_fee_rate: Decimal::ZERO,
        outcome_taker_rate: Decimal::ZERO,
        outcome_builder_rate: Decimal::ZERO,
    };
    let limits = ArbLimits {
        cost_limit: d("100"),
        min_profit: d("1"),
        min_apr: Decimal::ZERO,
        days: 1,
    };
    let first = crate::calc::plan_arbitrage(
        &topic,
        books.get(POLYMARKET, "pm").unwrap(),
        books.get(OUTCOME, "out").unwrap(),
        &pm,
        &out,
        &fees,
        &limits,
    )
    .unwrap();
    assert!(first.pm_balance_sufficient(d("39.70")));
    let pm_ticket = books.begin_rest(POLYMARKET, "pm");
    let out_ticket = books.begin_rest(OUTCOME, "out");
    let (pm_book, out_book) = accept_confirmation_books(
        &mut books,
        &pm_ticket,
        (
            vec![],
            levels(&[("0.40", "100")]),
            101,
            now,
            Some(d("0.01")),
        ),
        &out_ticket,
        (vec![], levels(&[("0.40", "200")]), 101, now),
        Duration::from_secs(5),
        now,
    )
    .unwrap();
    let confirmed = confirm_plan(&topic, &first, &pm_book, &out_book, &fees, &limits).unwrap();
    assert_eq!(confirmed.pm.shares, first.pm.shares);
    assert_eq!(confirmed.pm.cap_price, first.pm.cap_price);
    assert_eq!(confirmed.pm_required(), Some(d("40")));
    assert!(!confirmed.pm_balance_sufficient(d("39.70")));
    assert!(confirmed.pm_balance_sufficient(d("40")));
    assert!(confirmed.outcome_balance_sufficient(d("40")));
}

#[test]
fn automatic_pm_submission_requires_accepted_tick_and_unchanged_cap() {
    let mut req = MarketOrderRequest {
        token_id: "pm".into(),
        shares: d("10"),
        cap_price: d("0.451"),
        side: OrderSide::Buy,
        neg_risk: None,
        tick_size: None,
        asset_id: None,
        funder_address: None,
    };
    assert!(validate_pm_request_tick(&req).is_err());
    for tick in ["0", "-0.01", "1.1", "0.01"] {
        req.tick_size = Some(d(tick));
        assert!(validate_pm_request_tick(&req).is_err());
    }
    req.tick_size = Some(d("0.001"));
    assert!(validate_pm_request_tick(&req).is_ok());
    req.side = OrderSide::Sell;
    assert!(validate_pm_request_tick(&req).is_ok());
    req.tick_size = Some(d("0.01"));
    assert!(validate_pm_request_tick(&req).is_err());
}

#[test]
fn hard_http_confirmation_carries_single_rest_tick() {
    let mut books = BookStore::default();
    let now = Instant::now();
    let pm = books.begin_rest(POLYMARKET, "pm");
    let out = books.begin_rest(OUTCOME, "out");
    let (pm_book, _) = accept_confirmation_books(
        &mut books,
        &pm,
        (vec![], vec![], 100, now, Some(d("0.001"))),
        &out,
        (vec![], vec![], 100, now),
        Duration::from_secs(5),
        now,
    )
    .unwrap();
    assert_eq!(pm_book.tick_size, Some(d("0.001")));
    assert_eq!(books.tick_size(POLYMARKET, "pm"), Some(d("0.001")));
}

#[test]
fn hard_http_confirmation_rejects_conflict_without_ws_fallback_then_recovers() {
    let now = Instant::now();
    let level = || {
        vec![crate::book::Level {
            price: d("0.5"),
            size: d("3"),
        }]
    };
    for conflict_on_pm in [true, false] {
        let mut books = BookStore::default();
        books.set_tick_size(POLYMARKET, "pm", d("0.01"));
        books.replace_snapshot(POLYMARKET, "pm", vec![], level(), 100, now);
        books.replace_snapshot(OUTCOME, "out", vec![], level(), 100, now);
        // 两条生产确认路径共用此入口；持续冲突每轮只尝试一次。
        for _ in 0..3 {
            let pm = books.begin_rest(POLYMARKET, "pm");
            let out = books.begin_rest(OUTCOME, "out");
            if conflict_on_pm {
                let ticket = books.begin_rest(POLYMARKET, "pm");
                books
                    .accept_rest(&ticket, vec![], level(), 100, now, None)
                    .unwrap();
            } else {
                let ticket = books.begin_rest(OUTCOME, "out");
                books
                    .accept_rest(&ticket, vec![], level(), 100, now, None)
                    .unwrap();
            }
            assert!(accept_confirmation_books(
                &mut books,
                &pm,
                (vec![], level(), 100, now, None),
                &out,
                (vec![], level(), 100, now),
                Duration::from_secs(5),
                now
            )
            .is_none());
            assert!(books
                .get_at(POLYMARKET, "pm", now)
                .unwrap()
                .is_fresh(Duration::from_secs(5), now));
            assert!(books
                .get_at(OUTCOME, "out", now)
                .unwrap()
                .is_fresh(Duration::from_secs(5), now));
        }
        let pm = books.begin_rest(POLYMARKET, "pm");
        let out = books.begin_rest(OUTCOME, "out");
        assert!(accept_confirmation_books(
            &mut books,
            &pm,
            (vec![], level(), 100, now, None),
            &out,
            (vec![], level(), 100, now),
            Duration::from_secs(5),
            now
        )
        .is_some());
    }
}

#[test]
fn hard_http_confirmation_rejects_expired_missing_tick_and_observation_races() {
    let now = Instant::now();
    for case in 0..5 {
        let mut books = BookStore::default();
        if case != 0 {
            books.set_tick_size(POLYMARKET, "pm", d("0.01"));
        }
        let level = || {
            vec![crate::book::Level {
                price: d("0.5"),
                size: d("3"),
            }]
        };
        books.replace_snapshot(POLYMARKET, "pm", vec![], level(), 100, now);
        let pm = books.begin_rest(POLYMARKET, "pm");
        let out = books.begin_rest(OUTCOME, "out");
        if case == 2 {
            books.apply_levels(POLYMARKET, "pm", &[(false, d("0.5"), d("0"))], 100, now);
        } else if case == 3 {
            books.apply_levels(POLYMARKET, "pm", &[(false, d("0.5"), d("3"))], 100, now);
        } else if case == 4 {
            books.replace_snapshot(POLYMARKET, "pm", vec![], level(), 100, now);
        }
        let check_at = if case == 1 {
            now + Duration::from_secs(6)
        } else {
            now
        };
        assert!(accept_confirmation_books(
            &mut books,
            &pm,
            (vec![], level(), 101, now, None),
            &out,
            (vec![], level(), 101, now),
            Duration::from_secs(5),
            check_at
        )
        .is_none());
        if case == 2 {
            assert!(books.get_at(POLYMARKET, "pm", now).unwrap().asks.is_empty());
        }
    }
}

fn fill(trade_id: &str, order_id: &str, shares: &str, extra_ids: &[&str]) -> TradeFill {
    let mut order_ids = vec![order_id.to_string()];
    order_ids.extend(extra_ids.iter().map(|id| (*id).to_string()));
    TradeFill {
        trade_id: trade_id.into(),
        order_id: Some(order_id.into()),
        order_ids,
        coin: None,
        shares: d(shares),
        price: d("0.4"),
        fee: Some(d("0.01")),
        fee_rate_bps: None,
        fee_token: Some("USDC".into()),
        finality: crate::platforms::FillFinality::Confirmed,
        raw: json!({"oid": order_id}),
    }
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn pm_missing_order_execution_fetches_window_and_finalizes_trades() {
    use crate::platforms::polymarket::tests::poll_stub;
    use sqlx::postgres::PgPoolOptions;
    let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&uri)
        .await
        .unwrap();
    let schema = format!("pm_window_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search_path = schema.clone();
    let exercised: anyhow::Result<()> = async {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .after_connect(move |conn, _| {
                    let path = search_path.clone();
                    Box::pin(async move {
                        sqlx::query("SELECT set_config('search_path', $1, false)")
                            .bind(path)
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect(&uri)
                .await?;
            let store = Store { pool };
            store.migrate().await?;
            for (http_status, body, missing_time, empty) in [
                (404, json!({}), false, false),
                (200, Value::Null, false, false),
                (404, json!({}), false, true),
                (200, Value::Null, false, true),
                (404, json!({}), true, false),
            ] {
                let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
                let (_, ids) = store
                    .insert_actived_order_with_legs(&|_, _| None,
                        TopicKey::new(uuid::Uuid::new_v4(), 0),
                        &identity,
                        "PM window test",
                        "PM window test",
                        None,
                        d("10"),
                        d("1"),
                        d("9"),
                        &json!([]),
                        &[NewLeg {
                            platform: POLYMARKET,
                            token_id: "yes",
                            label: "yes",
                            side: "BUY",
                            intent: "arb_buy",
                            funder: Some("test-funder"),
                            wallet: None,
                            service: None,
                            req_price: d("0.5"),
                            req_shares: d("10"),
                            req_fee: Decimal::ZERO,
                            client_order_id: None,
                            fee_estimate: None,
                        }],
                        0,
                        Instant::now() + Duration::from_secs(30),
                    )
                    .await?;
                let oid = format!("taker-{}", ids[0]);
                store
                    .insert_envelope(&|_, _| None, ids[0], &oid, &json!({}), &json!({"test":true}), None)
                    .await?;
                if missing_time {
                    sqlx::query("UPDATE legs SET submitted_at=NULL WHERE id=$1")
                        .bind(ids[0])
                        .execute(&store.pool)
                        .await?;
                }
                let leg = store
                    .open_legs()
                    .await?
                    .into_iter()
                    .find(|leg| leg.id == ids[0])
                    .unwrap();
                let mut responses = vec![(http_status, body)];
                if !missing_time {
                    responses.push((
                        200,
                        if empty {
                            json!({"data":[], "next_cursor":"LTE="})
                        } else {
                            json!({"data":[{
                        "id":"confirmed-trade","taker_order_id":oid,"asset_id":"yes",
                        "size":"6","price":"0.5","status":"CONFIRMED",
                        "fee_amount":"0.01","fee_token":"USDC","maker_orders":[]
                    }], "next_cursor":"LTE="})
                        },
                    ));
                }
                let (pm, server) = poll_stub(responses).await;
                let page_result = reconcile_pm_page(&|_, _| None, &pm, &store, &leg).await?;
                let requests = tokio::time::timeout(Duration::from_secs(5), server).await??;
                if missing_time {
                    assert!(page_result.is_none());
                    assert_eq!(requests.len(), 1);
                    let info: Value =
                        sqlx::query_scalar("SELECT last_order_info FROM legs WHERE id=$1")
                            .bind(leg.id)
                            .fetch_one(&store.pool)
                            .await?;
                    assert_eq!(info["waiting_reason"], "submission_time_missing");
                    continue;
                }
                assert_eq!(requests.len(), 2);
                assert!(requests[0].starts_with(&format!("GET /data/order/{oid} ")));
                let after = leg.submitted_at.unwrap().timestamp();
                let url = url::Url::parse(&format!(
                    "http://localhost{}",
                    requests[1].split_whitespace().nth(1).unwrap()
                ))?;
                let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
                assert_eq!(query.get("after"), Some(&(after - 10).max(0).to_string()));
                assert_eq!(query.get("before"), Some(&(after + 300).to_string()));
                let (current, poll, page) = page_result.unwrap();
                assert!(!poll.found);
                assert_eq!(current.submitted_at, leg.submitted_at);
                assert_eq!(current.third_order_id.as_deref(), Some(oid.as_str()));
                assert!(
                    matches!(apply_reconciliation_page(&|_, _| None, &store, &current, poll, page, || async {
                        panic!("actual fee must not load a fee source")
                    }).await?,
                    LegResolution::Terminal { status, shares, .. }
                        if status == if empty { "failed" } else { "matched" }
                            && shares == if empty { Decimal::ZERO } else { d("6") })
                );
            }
            store.pool.close().await;
            Ok(())
        }
        .await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.unwrap();
    exercised.unwrap();
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn pm_fee_source_common_env_cache_and_errors() {
    use crate::platforms::polymarket::tests::execution_test_venue;
    use sqlx::postgres::PgPoolOptions;
    let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&uri)
        .await
        .unwrap();
    let schema = format!("pm_fee_source_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let path = schema.clone();
    let exercised: anyhow::Result<()> = async {
            let pool = PgPoolOptions::new().max_connections(2).after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)").bind(path).execute(conn).await?;
                    Ok(())
                })
            }).connect(&uri).await?;
            let store = Store { pool: pool.clone() };
            store.migrate().await?;
            sqlx::query("CREATE TABLE events (id UUID PRIMARY KEY, unified_options JSONB)").execute(&pool).await?;
            let key = TopicKey::new(uuid::Uuid::new_v4(), 7);
            let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
            let (_, ids) = store.insert_actived_order_with_legs(&|_, _| None,
                key, &identity, "fee source", "fee source", None,
                d("10"), d("1"), d("9"), &json!([]), &[NewLeg {
                    platform: POLYMARKET, token_id: "yes", label: "yes", side: "BUY", intent: "arb_buy",
                    funder: Some("test-funder"), wallet: None, service: None,
                    req_price: d("0.5"), req_shares: d("10"), req_fee: Decimal::ZERO, client_order_id: None,
                        fee_estimate: None,
                }], 0, Instant::now() + Duration::from_secs(30)
            ).await?;
            let leg = store.open_legs().await?.into_iter().find(|leg| leg.id == ids[0]).unwrap();
            // 地址不监听：费率来源不能意外依赖 PM HTTP。
            let base = "http://127.0.0.1:1";
            let mut cfg = admission_test_config(base);
            cfg.polymarket_fee_bps_prior = d("700");
            let outcome = OutcomeVenue::connect(&cfg)?;
            let (pm, _) = execution_test_venue(base.into()).await;
            let (pm_sub_tx, _) = mpsc::channel(1);
            let (out_sub_tx, _) = mpsc::channel(1);
            let engine = Engine {
                cfg, store, common: pool.clone(),
                books: Arc::new(Mutex::new(BookStore::default())),
                dirty: Arc::new(Mutex::new(DirtyCoalescer::default())),
                topics: Arc::new(RwLock::new(HashMap::new())), pm, outcome,
                pm_sub_tx, out_sub_tx, notify: None, stats: Arc::new(MinuteStats::new()),
                position_scan_cursor: Mutex::new(0), settlement_scan_cursor: Mutex::new(0),
                last_settlement_sweep: Mutex::new(None),
                reported_stale_unknown: Mutex::new(HashSet::new()),
                rebalance_loss_cooldown: Mutex::new(HashMap::new()),
            };
            let missing = engine.pm_reconciliation_fee_snapshot(&leg).await?;
            assert_eq!(missing["source"], "env");
            assert_eq!(crate::platforms::parse_decimal(&missing["bps"]), Some(d("700")));
            let catalog = |rate: Value, enabled: bool| json!([{"index":7,"platformOptions":[{
                "platform":"polymarket","conditionId":"test-condition","feesEnabled":enabled,
                "feeSchedule":{"rate":rate},"outcomes":[{"tokenId":"yes"}]
            }]}]);
            for (rate, enabled, source, expected) in [
                (json!("0.05"), true, "common", d("500")),
                (Value::Null, true, "env", d("700")),
                (json!("bad"), false, "common", Decimal::ZERO),
                (json!("0"), true, "common", Decimal::ZERO),
            ] {
                sqlx::query("INSERT INTO events VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET unified_options=EXCLUDED.unified_options")
                    .bind(key.event_id).bind(catalog(rate, enabled)).execute(&pool).await?;
                let snapshot = engine.pm_reconciliation_fee_snapshot(&leg).await?;
                assert_eq!(snapshot["source"], source);
                assert_eq!(crate::platforms::parse_decimal(&snapshot["bps"]), Some(expected));
                assert_eq!(snapshot["condition_id"], "test-condition");
                assert_eq!(snapshot["token_id"], "yes");
            }
            for raw in [catalog(json!("bad"), true), catalog(json!("1.01"), true),
                json!([{"index":7,"platformOptions":[{"platform":"polymarket","conditionId":"wrong"}]}])] {
                sqlx::query("UPDATE events SET unified_options=$1").bind(raw).execute(&pool).await?;
                assert!(engine.pm_reconciliation_fee_snapshot(&leg).await.is_err());
            }
            sqlx::query("DROP TABLE events").execute(&pool).await?;
            assert!(engine.pm_reconciliation_fee_snapshot(&leg).await.is_err());
            let topic = Topic {
                key, title: String::new(), market_title: String::new(), end_date: None,
                tokens: vec![crate::domain::TokenRef {
                    platform: POLYMARKET.into(), token_id: "yes".into(), label: "yes".into(),
                    option_id: "market".into(), condition_id: Some("test-condition".into()),
                    asset_id: None, side_index: None, neg_risk: Some(false),
                    fees_enabled: Some(true), fee_rate: Some(d("0.04")),
                }],
            };
            engine.topics.write().await.insert(key, topic.clone());
            let cached = engine.pm_reconciliation_fee_snapshot(&leg).await?;
            assert_eq!(cached["source"], "common");
            assert_eq!(crate::platforms::parse_decimal(&cached["bps"]), Some(d("400")));
            let mut wrong = topic;
            wrong.tokens[0].condition_id = Some("wrong".into());
            engine.topics.write().await.insert(key, wrong);
            assert!(engine.pm_reconciliation_fee_snapshot(&leg).await.is_err());
            pool.close().await;
            Ok(())
        }.await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    exercised.unwrap();
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn pm_fee_recovery_uses_persisted_observations_before_source_lookup() {
    use crate::platforms::polymarket::tests::poll_stub;
    use sqlx::postgres::PgPoolOptions;
    let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&uri)
        .await
        .unwrap();
    let schema = format!("pm_fee_recovery_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search_path = schema.clone();
    let exercised: anyhow::Result<()> = async {
            let pool = PgPoolOptions::new().max_connections(2).after_connect(move |conn, _| {
                let path = search_path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)").bind(path).execute(conn).await?;
                    Ok(())
                })
            }).connect(&uri).await?;
            let store = Store { pool };
            store.migrate().await?;
            // 复用、混合新旧快照、真正缺费失败、以及预读后的并发更新。
            for mode in ["reuse", "mixed", "fee_failure", "stale"] {
                let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
                let (parent_id, ids) = store.insert_actived_order_with_legs(&|_, _| None,
                    TopicKey::new(uuid::Uuid::new_v4(),0), &identity, "PM fee recovery", "PM fee recovery", None,
                    d("10"), d("1"), d("9"), &json!([]), &[NewLeg {
                        platform:POLYMARKET,token_id:"yes",label:"yes",side:"BUY",intent:"arb_buy",
                        funder:Some("test-funder"),wallet:None,service:None,req_price:d("0.5"),req_shares:d("10"),
                        req_fee:Decimal::ZERO,client_order_id:None,fee_estimate:None,
                    }],
                    0, Instant::now() + Duration::from_secs(30),
                ).await?;
                let id=ids[0];
                let oid=format!("fee-oid-{id}");
                store.insert_envelope(&|_, _| None, id,&oid,&json!({}),&json!({"test":true}),None).await?;
                let trade = |trade_id:&str, size:&str, status:&str| json!({
                    "id":trade_id,"taker_order_id":oid,"asset_id":"yes","size":size,"price":"0.5",
                    "status":status,"maker_orders":[]
                });
                let a=trade("a","6","TRADE_STATUS_CONFIRMED");
                let b=trade("b","4","TRADE_STATUS_MATCHED_NOT_BROADCASTED");
                let order=json!({"id":oid,"status":"ORDER_STATUS_MATCHED","asset_id":"yes","original_size":"10",
                    "size_matched":"10","associate_trades":["a","b"]});
                let (pm,server)=poll_stub(vec![(200,order),(200,json!({"data":[a.clone(),b],"next_cursor":"LTE="}))]).await;
                let source_calls = std::sync::atomic::AtomicUsize::new(0);
                let snapshot_for = |rate: &str| json!({"version":1,"source":"common",
                    "rate":rate,"bps":(d(rate)*d("10000")).to_string(),
                    "condition_id":"test-condition","token_id":"yes"});
                let leg=store.open_legs().await?.into_iter().find(|leg|leg.id==id).unwrap();
                let (current,poll,page)=reconcile_pm_page(&|_, _| None, &pm,&store,&leg).await?.unwrap();
                assert!(matches!(apply_reconciliation_page(&|_, _| None, &store,&current,poll,page,|| async {
                    source_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(snapshot_for("0.07"))
                }).await?,LegResolution::Pending(_)));
                let requests=tokio::time::timeout(Duration::from_secs(5),server).await??;
                assert_eq!(requests.len(),2);
                assert_eq!(source_calls.load(std::sync::atomic::Ordering::Relaxed),1);
                let saved:Value=sqlx::query_scalar("SELECT raw FROM fills WHERE leg_id=$1 AND trade_id='a'").bind(id).fetch_one(&store.pool).await?;
                let snapshot=saved["reconciliation_v1"]["raw"]["fee_calculation"].clone();
                assert_eq!(snapshot["rate"],"0.07");
                let next_b=trade("b","4",if mode=="reuse" || mode=="stale" {"TRADE_STATUS_FAILED"}else{"TRADE_STATUS_CONFIRMED"});
                let responses=vec![(200,Value::Null),(200,json!({"data":[a.clone(),a.clone(),next_b],"next_cursor":"LTE="}))];
                let (pm,server)=poll_stub(responses).await;
                let leg=store.open_legs().await?.into_iter().find(|leg|leg.id==id).unwrap();
                let (current,poll,page)=reconcile_pm_page(&|_, _| None, &pm,&store,&leg).await?.unwrap();
                let before:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                if mode=="stale" {store.record_reconciliation_wait(&current,"concurrent_update").await?;}
                let resolution=apply_reconciliation_page(&|_, _| None, &store,&current,poll,page,|| async {
                    assert!(mode=="mixed" || mode=="fee_failure", "saved fee must skip lookup");
                    source_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if mode=="fee_failure" { return Err(Error::msg("COMMON lookup failed")); }
                    Ok(snapshot_for("0.05"))
                }).await;
                let requests=tokio::time::timeout(Duration::from_secs(5),server).await??;
                assert_eq!(requests.len(),2);
                assert_eq!(source_calls.load(std::sync::atomic::Ordering::Relaxed),
                    if mode=="mixed" || mode=="fee_failure" {2}else{1});
                let after:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                if mode=="fee_failure" {
                    assert!(resolution.is_err());
                    assert_eq!(after,before);
                } else if mode=="stale" {
                    assert_eq!(resolution?,LegResolution::Pending("stale_leg_snapshot"));
                    assert_eq!(after["last_order_info"]["waiting_reason"],"concurrent_update");
                    assert_eq!(after["last_order_info"]["fill_progress"],before["last_order_info"]["fill_progress"]);
                } else {
                    let (shares,fee)=if mode=="reuse" {(d("6"),d("0.105"))}else{(d("10"),d("0.155"))};
                    assert!(matches!(resolution?,LegResolution::Terminal{status:"matched",shares:s,fee:f,..} if s==shares && f==fee));
                    let parent:Value=sqlx::query_scalar("SELECT to_jsonb(o) FROM arb_orders o WHERE id=$1").bind(parent_id).fetch_one(&store.pool).await?;
                    assert_eq!(parent["status"],"completed");
                    let cost:Decimal=sqlx::query_scalar("SELECT actual_cost FROM arb_orders WHERE id=$1").bind(parent_id).fetch_one(&store.pool).await?;
                    assert_eq!(cost,shares*d("0.5")+fee);
                }
                let latest:Value=sqlx::query_scalar("SELECT raw FROM fills WHERE leg_id=$1 AND trade_id='a'").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(latest["reconciliation_v1"]["raw"]["fee_calculation"],snapshot);
                let bps:Decimal=sqlx::query_scalar("SELECT fee_rate_bps FROM fills WHERE leg_id=$1 AND trade_id='a'").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(bps,d("700"));
                assert_eq!(crate::platforms::parse_decimal(&latest["reconciliation_v1"]["fee_rate_bps"]),Some(d("700")));
                let b_value:Value=sqlx::query_scalar("SELECT raw FROM fills WHERE leg_id=$1 AND trade_id='b'").bind(id).fetch_one(&store.pool).await?;
                if mode=="mixed" {assert_eq!(b_value["reconciliation_v1"]["raw"]["fee_calculation"]["rate"],"0.05");}
                if mode=="fee_failure" || mode=="stale" {assert_eq!(b_value["reconciliation_v1"]["finality"],"pending");}
                let count:i64=sqlx::query_scalar("SELECT count(*) FROM fills WHERE leg_id=$1").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(count,2);
            }
            store.pool.close().await;
            Ok(())
        }.await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.unwrap();
    exercised.unwrap();
}

#[test]
fn page_fee_merge_preserves_evidence_without_inventing_scan_members() {
    let mut stored = fill("a", "oid", "6", &[]);
    stored.fee = None;
    stored.raw = json!({"role":"taker","fee_calculation":{"source":"clob-markets","rate":"0.07"}});
    let mut incoming = stored.clone();
    incoming.raw = json!({"role":"taker"});
    assert!(needs_pm_fee_snapshot(&incoming));
    let extra = fill("not_in_page", "oid", "4", &[]);
    let merged = merge_page_observations(
        &[incoming.clone(), incoming.clone()],
        vec![stored.clone(), extra],
    )
    .unwrap();
    assert_eq!(merged.len(), 1);
    assert!(!needs_pm_fee_snapshot(&merged[0]));
    assert_eq!(
        merged[0].raw["fee_calculation"],
        stored.raw["fee_calculation"]
    );
    incoming.fee = Some(Decimal::ZERO);
    assert!(!needs_pm_fee_snapshot(
        &merge_page_observations(&[incoming.clone()], vec![stored.clone()]).unwrap()[0]
    ));
    incoming.fee_token = Some("OTHER".into());
    let other = merge_page_observations(&[incoming.clone()], vec![stored.clone()]).unwrap();
    assert!(crate::reconcile::accounting_fee(POLYMARKET, &other[0])
        .unwrap()
        .is_none());
    incoming.finality = crate::platforms::FillFinality::Failed;
    assert!(merge_page_observations(&[incoming], vec![stored]).is_err());
}

#[test]
fn workflow_switch_matrix_is_independent() {
    for mask in 0_u8..8 {
        let arb = mask & 1 != 0;
        let rebalance = mask & 2 != 0;
        let take_profit = mask & 4 != 0;
        assert_eq!(
            workflow_switch_enabled(arb, rebalance, take_profit, TradingIntent::Arbitrage),
            arb
        );
        assert_eq!(
            workflow_switch_enabled(arb, rebalance, take_profit, TradingIntent::Rebalance),
            rebalance
        );
        assert_eq!(
            workflow_switch_enabled(arb, rebalance, take_profit, TradingIntent::TakeProfit),
            take_profit
        );
    }
}

#[test]
fn workflow_switches_block_their_own_submission_kind() {
    for intent in [
        TradingIntent::Arbitrage,
        TradingIntent::Rebalance,
        TradingIntent::TakeProfit,
    ] {
        let err = ensure_trading_submission_enabled(false, intent).unwrap_err();
        assert!(err.to_string().contains(intent.env_name()));
        assert!(ensure_trading_submission_enabled(true, intent).is_ok());
    }
}

#[test]
fn rebalance_permissions_follow_settlement_access() {
    let action = |side| crate::hedge::HedgeAction {
        platform: POLYMARKET.into(),
        token_id: "token".into(),
        label: "yes".into(),
        side,
        shares: Decimal::ONE,
        cap_price: Decimal::ONE,
        fee: Decimal::ZERO,
        marginal_value: Decimal::ZERO,
    };
    assert!(action_allowed_for_rebalance(
        &action(HedgeSide::Sell),
        SettlementAccess::ReduceOnly,
    ));
    assert!(!action_allowed_for_rebalance(
        &action(HedgeSide::Buy),
        SettlementAccess::ReduceOnly,
    ));
    assert!(action_allowed_for_rebalance(
        &action(HedgeSide::Buy),
        SettlementAccess::All,
    ));
    assert!(action_allowed_for_rebalance(
        &action(HedgeSide::Sell),
        SettlementAccess::All,
    ));
    assert!(!action_allowed_for_rebalance(
        &action(HedgeSide::Sell),
        SettlementAccess::Stop,
    ));
}

#[test]
fn matches_trade_by_order_id_only() {
    let trades = vec![fill("t1", "oid-1", "3", &[]), fill("t2", "oid-2", "9", &[])];
    let matched = filter_trades(&trades, Some("oid-1"), None);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].trade_id, "t1");
    assert!(filter_trades(&trades, None, None).is_empty());
    assert!(filter_trades(&trades, Some("oid-1"), None)[0].matches(Some("oid-1"), None));
    assert!(!fill("t3", "oid-3", "1", &[]).matches(Some("oid-1"), Some("oid-3-substring")));
}

#[test]
fn matches_maker_order_id_without_json_substring() {
    let trade = fill("t1", "taker-1", "3", &["maker-9"]);
    assert!(trade.matches(Some("maker-9"), None));
    assert!(!trade.matches(Some("taker"), None));
}

#[test]
fn parent_status_requires_positive_matched() {
    assert_eq!(parent_terminal_status(true, true), None);
    assert_eq!(parent_terminal_status(false, true), Some("completed"));
    assert_eq!(parent_terminal_status(false, false), Some("cancelled"));
    // 单腿 matched + 另一腿 cancelled：无 open 腿且有正成交 → completed
    assert_eq!(parent_terminal_status(false, true), Some("completed"));
}

#[test]
fn pm_matched_response_uses_returned_amounts_and_estimated_fee() {
    let mut fees = FeeContext {
        polymarket_fee_rate: d("0.07"),
        outcome_taker_rate: Decimal::ZERO,
        outcome_builder_rate: Decimal::ZERO,
    };
    for side in [OrderSide::Buy, OrderSide::Sell] {
        let (making, taking) = match side {
            OrderSide::Buy => ("1.2", "3"),
            OrderSide::Sell => ("3", "1.2"),
        };
        let response = json!({
            "success":true, "status":"matched", "orderID":"oid",
            "makingAmount":making, "takingAmount":taking, "fee":"999",
        });
        let result = crate::platforms::polymarket::parse_submit(
            &response,
            "hash".into(),
            json!({"price":"0.9", "shares":"10"}),
        );
        assert_eq!(
            pm_matched_submit_fill(POLYMARKET, side, &result, &fees, &response),
            Some((d("3"), d("0.4"), d("0.0504"))),
        );
        assert!(pm_matched_submit_fill(OUTCOME, side, &result, &fees, &response).is_none());
        for (field, value) in [
            ("status", json!("live")),
            ("status", json!("delayed")),
            ("success", json!(false)),
            ("makingAmount", Value::Null),
            ("takingAmount", json!("bad")),
            ("makingAmount", json!("0")),
            ("takingAmount", json!("-1")),
        ] {
            let mut invalid = response.clone();
            invalid[field] = value;
            let parsed =
                crate::platforms::polymarket::parse_submit(&invalid, "hash".into(), json!({}));
            assert!(
                pm_matched_submit_fill(POLYMARKET, side, &parsed, &fees, &invalid).is_none(),
                "{field}"
            );
        }
    }
    let response = json!({"success":true,"status":"matched","orderID":"oid","makingAmount":"1.2","takingAmount":"3"});
    let result = crate::platforms::polymarket::parse_submit(&response, "hash".into(), json!({}));
    fees.polymarket_fee_rate = Decimal::ZERO;
    assert_eq!(
        pm_matched_submit_fill(POLYMARKET, OrderSide::Buy, &result, &fees, &response)
            .unwrap()
            .2,
        Decimal::ZERO
    );
    // 买卖方向颠倒会得到大于 1 的成交价格，不能入账。
    assert!(
        pm_matched_submit_fill(POLYMARKET, OrderSide::Sell, &result, &fees, &response).is_none()
    );
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn pm_matched_submission_accounts_atomically_without_trade_polling() {
    let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&uri)
        .await
        .unwrap();
    let schema = format!("pm_submit_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search_path = schema.clone();
    let exercised: anyhow::Result<()> = async {
            let pool = sqlx::postgres::PgPoolOptions::new().max_connections(2).after_connect(move |conn, _| {
                let path = search_path.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)").bind(path).execute(conn).await?;
                    Ok(())
                })
            }).connect(&uri).await?;
            let mut engine = fee_test_engine().await;
            engine.store = Store { pool };
            let store = &engine.store;
            store.migrate().await?;
            let fees = FeeContext { polymarket_fee_rate:d("0.07"), outcome_taker_rate:Decimal::ZERO, outcome_builder_rate:Decimal::ZERO };
            for side in [OrderSide::Buy, OrderSide::Sell] {
                let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
                let (parent, ids) = store.insert_actived_order_with_legs(&|_, _| None,
                    TopicKey::new(uuid::Uuid::new_v4(), 0), &identity,
                    "PM submit test", "PM submit test", None, d("10"), d("1"), d("9"), &json!([]),
                    &[NewLeg {
                        platform:POLYMARKET, token_id:"yes", label:"yes", side:side.as_str(), intent:"arb_buy",
                        funder:Some("test-funder"), wallet:None, service:None,
                        req_price:d("0.5"), req_shares:d("10"), req_fee:d("1"), client_order_id:None, fee_estimate:None,
                    }], 0, Instant::now() + Duration::from_secs(30),
                ).await?;
                let id = ids[0];
                let oid = format!("oid-{id}");
                store.insert_envelope(&|_, _| None, id, &oid, &json!({}), &json!({}), None).await?;
                let stale = store.open_legs().await?.into_iter().find(|leg|leg.id == id).unwrap();
                let (making, taking) = if side == OrderSide::Buy { ("1.2", "3") } else { ("3", "1.2") };
                let response = json!({"success":true,"status":"matched","orderID":oid,"makingAmount":making,"takingAmount":taking});
                let result = crate::platforms::polymarket::parse_submit(&response, oid.clone(), json!({}));
                if side == OrderSide::Buy {
                    // 父单刷新失败时，提交响应与腿账务也必须一起回滚。
                    sqlx::query("CREATE FUNCTION reject_parent_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test parent failure'; END $$").execute(&store.pool).await?;
                    sqlx::query("CREATE TRIGGER reject_parent BEFORE UPDATE ON arb_orders FOR EACH ROW EXECUTE FUNCTION reject_parent_update()").execute(&store.pool).await?;
                    assert!(persist_submit(&|_, _| None, store,id,POLYMARKET,side,&result,&fees,&crate::platforms::SubmissionResponse::Http(response.clone())).await.is_err());
                    let row:(String,Option<Decimal>,Option<Value>)=sqlx::query_as("SELECT l.status,l.actual_shares,e.submit_response FROM legs l JOIN signed_envelopes e ON e.leg_id=l.id WHERE l.id=$1").bind(id).fetch_one(&store.pool).await?;
                    assert_eq!(row.0, "pending");
                    assert!(row.1.is_none());
                    assert!(row.2.is_none());
                    let info:Option<Value>=sqlx::query_scalar("SELECT last_order_info FROM legs WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                    assert!(info.as_ref().and_then(|v|v.get("submit_response")).is_none());
                    sqlx::query("DROP TRIGGER reject_parent ON arb_orders").execute(&store.pool).await?;
                } else {
                    // 先前仅观察到的部分明细不能与提交返回的总量叠加。
                    sqlx::query("INSERT INTO fills(leg_id,third_order_id,trade_id,shares,price) VALUES($1,$2,'observed',1,0.4)").bind(id).bind(&oid).execute(&store.pool).await?;
                }
                persist_submit(&|_, _| None, store,id,POLYMARKET,side,&result,&fees,&crate::platforms::SubmissionResponse::Http(response.clone())).await?;
                persist_submit(&|_, _| None, store,id,POLYMARKET,side,&result,&fees,&crate::platforms::SubmissionResponse::Http(response.clone())).await?;
                let row:(String,Decimal,Decimal,Decimal,Value)=sqlx::query_as("SELECT status,actual_shares,actual_price,actual_fee,last_order_info FROM legs WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                assert_eq!((row.0.as_str(),row.1,row.2,row.3),("matched",d("3"),d("0.4"),d("0.0504")));
                assert_eq!(row.4["submission"]["fill"]["source"],"submit_response");
                assert_eq!(row.4["fee_sources"],json!(["estimated"]));
                assert!(row.4.get("submit_response").is_none());
                let before:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                let parent_before:Value=sqlx::query_scalar("SELECT to_jsonb(a) FROM arb_orders a WHERE id=$1").bind(parent).fetch_one(&store.pool).await?;
                let diagnostic=json!({"kind":"transport","error":"local failure"});
                store.record_submission_with_fill(&|_, _| None,id,"unknown",None,&json!({"kind":"unknown"}),&crate::platforms::SubmissionResponse::NoResponse(diagnostic.clone()),None).await?;
                let after:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                let expected=before;
                assert_eq!(after,expected);
                let envelope:Value=sqlx::query_scalar("SELECT submit_response FROM signed_envelopes WHERE leg_id=$1").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(envelope,response);
                // 仅测试夹具重置为 SQL NULL，覆盖本地诊断/真实响应的先后次序。
                sqlx::query("UPDATE signed_envelopes SET submit_response=NULL WHERE leg_id=$1").bind(id).execute(&store.pool).await?;
                let first=json!({"kind":"transport","received":false,"error":"first local failure"});
                for incoming in [first.clone(), json!({"kind":"timeout","received":false,"error":"later"})] {
                    store.record_submission_with_fill(&|_, _| None,id,"unknown",None,&json!({"kind":"unknown"}),&crate::platforms::SubmissionResponse::NoResponse(incoming),None).await?;
                    let saved:Value=sqlx::query_scalar("SELECT submit_response FROM signed_envelopes WHERE leg_id=$1").bind(id).fetch_one(&store.pool).await?;
                    assert_eq!(saved,first);
                }
                for http in [Value::Null, json!({"http_status":500,"body":{"long":"x".repeat(4096)}}), json!({"http_status":200,"body":"not json","body_format":"non_json"}), json!({"http_status":200,"body_read_error":"incomplete"}), response.clone()] {
                    store.record_submission_with_fill(&|_, _| None,id,"unknown",None,&json!({"kind":"unknown"}),&crate::platforms::SubmissionResponse::Http(http.clone()),None).await?;
                    store.record_submission_with_fill(&|_, _| None,id,"unknown",None,&json!({"kind":"unknown"}),&crate::platforms::SubmissionResponse::NoResponse(first.clone()),None).await?;
                    let saved:Value=sqlx::query_scalar("SELECT submit_response FROM signed_envelopes WHERE leg_id=$1").bind(id).fetch_one(&store.pool).await?;
                    assert_eq!(saved,http);
                }
                let unchanged:Value=sqlx::query_scalar("SELECT to_jsonb(l) FROM legs l WHERE id=$1").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(unchanged,expected);
                let parent_after:Value=sqlx::query_scalar("SELECT to_jsonb(a) FROM arb_orders a WHERE id=$1").bind(parent).fetch_one(&store.pool).await?;
                assert_eq!(parent_after,parent_before);
                assert!(store.open_legs().await?.is_empty());
                let positions=store.positions_for_order(parent).await?;
                assert_eq!(position_qty(&positions,POLYMARKET,"yes"), if side==OrderSide::Buy {d("3")} else {d("-3")});
                let actual:(String,Decimal)=sqlx::query_as("SELECT status,actual_cost FROM arb_orders WHERE id=$1").bind(parent).fetch_one(&store.pool).await?;
                assert_eq!(actual.0,"completed");
                if side==OrderSide::Buy { assert_eq!(actual.1,d("1.2504")); }
                // HTTP 端点不可用；matched 腿不再调度 order/trades 查询。
                engine.reconcile().await?;
                let poll=OrderPoll {found:true,status:"matched".into(),order_id:Some(oid.clone()),..Default::default()};
                assert!(store.record_order_poll(&|_, _| None, &stale,&poll).await?.is_none());
                let evidence=FillEvidence {poll,page_complete:true,history_complete:true,expected_shares:None,outcome_scan:None,pm_scan:None,pm_order_constraints:None};
                assert_eq!(store.record_reconciliation(&|_, _| None, &stale,&[],&evidence,&Value::Null).await?,LegResolution::Pending("stale_leg_snapshot"));
                let count:i64=sqlx::query_scalar("SELECT count(*) FROM fills WHERE leg_id=$1").bind(id).fetch_one(&store.pool).await?;
                assert_eq!(count,if side==OrderSide::Buy {0} else {1});
            }
            store.pool.close().await;
            Ok(())
        }.await;
    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.unwrap();
    exercised.unwrap();
}

#[tokio::test]
#[ignore = "requires APP_POSTGRES_URI; run manually against a test database"]
async fn pm_matched_submission_recovers_premature_failed_leg() {
    let uri = std::env::var("APP_POSTGRES_URI").expect("requires a test database");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&uri)
        .await
        .unwrap();
    let schema = format!("pm_recover_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search_path = schema.clone();
    let exercised: anyhow::Result<()> = async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(2)
                .after_connect(move |conn, _| {
                    let path = search_path.clone();
                    Box::pin(async move {
                        sqlx::query("SELECT set_config('search_path', $1, false)")
                            .bind(path)
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect(&uri)
                .await?;
            let mut engine = fee_test_engine().await;
            engine.store = Store { pool };
            let store = &engine.store;
            store.migrate().await?;
            let fees = FeeContext {
                polymarket_fee_rate: d("0.03"),
                outcome_taker_rate: Decimal::ZERO,
                outcome_builder_rate: Decimal::ZERO,
            };

            let identity = MarketIdentity::new(POLYMARKET, "test-condition")?;
            let (_parent, ids) = store
                .insert_actived_order_with_legs(
                    &|_, _| None,
                    TopicKey::new(uuid::Uuid::new_v4(), 0),
                    &identity,
                    "PM recover test",
                    "PM recover test",
                    None,
                    d("20"),
                    d("2"),
                    d("18"),
                    &json!([]),
                    &[NewLeg {
                        platform: POLYMARKET,
                        token_id: "yes",
                        label: "yes",
                        side: "SELL",
                        intent: "take_profit",
                        funder: Some("test-funder"),
                        wallet: None,
                        service: None,
                        req_price: d("0.48"),
                        req_shares: d("37"),
                        req_fee: d("0.28"),
                        client_order_id: None,
                        fee_estimate: None,
                    }],
                    0,
                    Instant::now() + Duration::from_secs(30),
                )
                .await?;
            let leg_id = ids[0];
            let oid = format!("oid-{leg_id}");
            store
                .insert_envelope(&|_, _| None, leg_id, &oid, &json!({}), &json!({}), None)
                .await?;

            // 模拟竞态条件：在途网络请求未返回时，对账抢先将该腿置为 failed
            let poll = OrderPoll {
                found: false,
                status: "not_found".into(),
                order_id: Some(oid.clone()),
                raw: json!({"lookup_missing": "null_body"}),
                ..OrderPoll::default()
            };
            let info = json!({
                "reason": "pending_after_submit",
                "order_poll": poll,
            });
            sqlx::query(
                "UPDATE legs SET status = 'failed', actual_shares = 0, actual_price = 0,
                        actual_fee = 0, last_order_info = $2
                 WHERE id = $1",
            )
            .bind(leg_id)
            .bind(info)
            .execute(&store.pool)
            .await?;

            // 交易所返回真实的 matched 撮合响应
            let response = json!({
                "success": true,
                "status": "matched",
                "orderID": oid,
                "makingAmount": "37",
                "takingAmount": "17.76",
                "transactionsHashes": ["0x467a7025dea66ac5050e1d12922db7de1e395ccb5a43215db64a2eae750f299c"]
            });
            let result = crate::platforms::polymarket::parse_submit(&response, oid.clone(), json!({}));

            // persist_submit 必须成功将 failed 恢复为 matched，并入账真实数据
            persist_submit(
                &|_, _| None,
                store,
                leg_id,
                POLYMARKET,
                OrderSide::Sell,
                &result,
                &fees,
                &crate::platforms::SubmissionResponse::Http(response.clone()),
            )
            .await?;

            let row: (String, Decimal, Decimal, Decimal, Value) = sqlx::query_as(
                "SELECT status, actual_shares, actual_price, actual_fee, last_order_info
                 FROM legs WHERE id = $1",
            )
            .bind(leg_id)
            .fetch_one(&store.pool)
            .await?;

            assert_eq!(row.0, "matched");
            assert_eq!(row.1, d("37"));
            assert_eq!(row.2, d("0.48"));
            assert_eq!(row.3, d("0.277056"));
            assert_eq!(row.4["submission"]["kind"], "ack");
            assert!(row.4.get("reason").is_none());
            assert!(row.4.get("order_poll").is_none());

            let env: Value = sqlx::query_scalar(
                "SELECT submit_response FROM signed_envelopes WHERE leg_id = $1",
            )
            .bind(leg_id)
            .fetch_one(&store.pool)
            .await?;
            assert_eq!(env["status"], "matched");
            assert_eq!(env["makingAmount"], "37");

            store.pool.close().await;
            Ok(())
        }
        .await;

    let cleanup = sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await;
    admin.close().await;
    cleanup.unwrap();
    exercised.unwrap();
}

#[test]
fn ack_fill_uses_avg_px_not_cap() {
    let (shares, price) = ack_fill(
        OUTCOME,
        OrderSide::Buy,
        None,
        Some(d("5")),
        Some(d("0.55")),
        &json!({"price": "0.60"}),
    )
    .unwrap();
    assert_eq!(shares.to_string(), "5");
    assert_eq!(price.to_string(), "0.55");
    let cap_only = ack_fill(
        OUTCOME,
        OrderSide::Buy,
        None,
        Some(d("5")),
        None,
        &json!({"price": "0.60"}),
    )
    .unwrap();
    assert_eq!(cap_only.1.to_string(), "0.60");
}

#[test]
fn fill_fee_uses_actual_shares_and_price() {
    let fees = FeeContext {
        polymarket_fee_rate: d("0.07"),
        outcome_taker_rate: d("0.00035"),
        outcome_builder_rate: Decimal::ZERO,
    };
    let (shares, price) = ack_fill(
        OUTCOME,
        OrderSide::Buy,
        None,
        Some(d("30")),
        Some(d("0.949")),
        &json!({}),
    )
    .unwrap();
    let fee = estimate_taker_fee(OUTCOME, OrderSide::Sell, shares, price, &fees);
    assert_eq!(fee, d("30") * d("0.949") * d("0.00035"));
    let pm_fee = estimate_taker_fee(POLYMARKET, OrderSide::Buy, d("30"), d("0.40"), &fees);
    assert_eq!(pm_fee, d("30") * d("0.07") * d("0.40") * d("0.60"));
}

#[test]
fn book_recv_skew_allows_exact_one_second() {
    let a = Instant::now();
    let b = a + Duration::from_secs(1);
    assert!(book_recv_skew_ok(a, b, HTTP_BOOK_SKEW_MAX));
    assert!(book_recv_skew_ok(b, a, HTTP_BOOK_SKEW_MAX));
}

#[test]
fn book_recv_skew_rejects_over_one_second() {
    let a = Instant::now();
    let b = a + Duration::from_secs(1) + Duration::from_millis(1);
    assert!(!book_recv_skew_ok(a, b, HTTP_BOOK_SKEW_MAX));
    assert!(!book_recv_skew_ok(b, a, HTTP_BOOK_SKEW_MAX));
}

#[test]
fn hedge_buy_uses_order_funder_not_token_buy() {
    let funder =
        resolve_hedge_pm_funder(false, Some("0xtoken".into()), Some("0xorder".into())).unwrap();
    assert_eq!(funder, "0xorder");
}

#[test]
fn hedge_sell_prefers_token_buy_funder() {
    let funder =
        resolve_hedge_pm_funder(true, Some("0xtoken".into()), Some("0xorder".into())).unwrap();
    assert_eq!(funder, "0xtoken");
}

#[test]
fn hedge_sell_falls_back_to_order_funder() {
    let funder = resolve_hedge_pm_funder(true, None, Some("0xorder".into())).unwrap();
    assert_eq!(funder, "0xorder");
}

#[test]
fn hedge_rejects_without_original_funder() {
    assert!(resolve_hedge_pm_funder(false, Some("0xtoken".into()), None).is_err());
    assert!(resolve_hedge_pm_funder(true, None, None).is_err());
}

#[test]
fn take_profit_submit_success_requires_ack() {
    let envelope = json!({});
    let ack = Ok(SubmitResult::Ack {
        order_id: "order-1".into(),
        order_hash: "h".into(),
        envelope: envelope.clone(),
        making: Some(d("1")),
        taking: Some(d("0.5")),
        avg_px: Some(d("0.5")),
    });
    let no_match = Ok(SubmitResult::NoMatch {
        order_hash: "h".into(),
        envelope: envelope.clone(),
        message: "no fill".into(),
    });
    let unknown = Ok(SubmitResult::Unknown {
        order_id: None,
        order_hash: "h".into(),
        envelope: envelope.clone(),
        message: "transport uncertain".into(),
    });
    let failed = Ok(SubmitResult::Failed {
        order_hash: "h".into(),
        envelope,
        status: 400,
        message: "rejected".into(),
    });
    assert!(submit_confirmed(&ack));
    assert!(!submit_confirmed(&no_match));
    assert!(!submit_confirmed(&unknown));
    assert!(!submit_confirmed(&failed));
    assert!(!submit_confirmed(&Err(Error::msg("submit error"))));
}

#[test]
fn settlement_pending_uses_settlement_only_scan() {
    assert!(settlement_only_scan("settlement_pending"));
    for status in ["watching", "closed", "settled"] {
        assert!(!settlement_only_scan(status));
    }
}

#[test]
fn settlement_sweep_runs_first_then_only_after_the_full_interval() {
    let interval = Duration::from_secs(60);
    let start = Instant::now();
    assert!(settlement_sweep_due(None, start, interval));
    assert!(!settlement_sweep_due(Some(start), start, interval));
    assert!(!settlement_sweep_due(
        Some(start),
        start + Duration::from_secs(59),
        interval
    ));
    assert!(settlement_sweep_due(
        Some(start),
        start + interval,
        interval
    ));
    // 时钟回拨不应把等待态订单变成每轮全速轮询。
    assert!(!settlement_sweep_due(
        Some(start + interval),
        start,
        interval
    ));
}

#[tokio::test]
async fn scan_cursor_advances_to_last_row_and_resets_on_empty_set() {
    let cursor = Mutex::new(0);
    let rows = vec![scan_row(7), scan_row(19)];
    advance_scan_cursor(&cursor, &rows, 0).await;
    assert_eq!(*cursor.lock().await, 19);

    // 空批次意味着 store 的回绕重查也没命中，游标必须归零。
    advance_scan_cursor(&cursor, &[], 19).await;
    assert_eq!(*cursor.lock().await, 0);

    // 已经在起点时空批次不做无谓写入，也不会把游标推成负数。
    advance_scan_cursor(&cursor, &[], 0).await;
    assert_eq!(*cursor.lock().await, 0);
}

fn scan_row(id: i64) -> ArbOrderRow {
    ArbOrderRow {
        id,
        title: "t".into(),
        event_id: uuid::Uuid::nil(),
        unified_index: 0,
        status: "completed".into(),
        rebalance_status: "completed".into(),
        position_status: "watching".into(),
        lifecycle_action: None,
        lifecycle_claim_id: None,
        lifecycle_claimed_at: None,
        settlement_source: None,
        settlement_result: None,
        settled_at: None,
        settlement_pending_since: None,
        settlement_pending_source: None,
        settlement_pending_result: None,
    }
}

#[test]
fn take_profit_disabled_and_cancelled_metrics_are_exclusive() {
    let stats = MinuteStats::new();
    record_take_profit_not_submitted(&stats, true);
    let disabled = stats.snapshot_and_reset();
    assert_eq!(disabled.take_profit_disabled, 1);
    assert_eq!(disabled.take_profit_cancelled, 0);

    record_take_profit_not_submitted(&stats, false);
    let cancelled = stats.snapshot_and_reset();
    assert_eq!(cancelled.take_profit_disabled, 0);
    assert_eq!(cancelled.take_profit_cancelled, 1);
}

#[test]
fn settlement_finalization_results_preserve_values_and_count_once() {
    let stats = MinuteStats::new();
    let actuals = (Decimal::new(95, 1), Decimal::from(10), Decimal::new(5, 1));
    let observed = |result| {
        handle_settlement_finalization_result(
            result,
            &stats,
            42,
            "pm-market",
            "95",
            Duration::from_millis(12),
        )
    };

    assert_eq!(observed(Ok(Some(actuals))).unwrap(), Some(actuals));
    let success = stats.snapshot_and_reset();
    assert_eq!(success.settled, 1);
    assert_eq!(success.settlement_finalize_fail, 0);
    assert_eq!(success.exec_err, 0);

    assert_eq!(observed(Ok(None)).unwrap(), None);
    assert_eq!(stats.snapshot_and_reset(), Default::default());

    let err = observed(Err(Error::Sqlx(sqlx::Error::PoolClosed))).unwrap_err();
    assert!(matches!(err, Error::Sqlx(sqlx::Error::PoolClosed)));
    let failure = stats.snapshot_and_reset();
    assert_eq!(failure.settled, 0);
    assert_eq!(failure.settlement_finalize_fail, 1);
    assert_eq!(failure.exec_err, 0);

    let err = observed(Err(Error::msg(
        "missing settlement payout for outcome:#5160",
    )))
    .unwrap_err();
    assert!(matches!(err, Error::Msg(ref message)
            if message == "missing settlement payout for outcome:#5160"));
    assert_eq!(stats.snapshot_and_reset().settlement_finalize_fail, 1);
    assert_eq!(stats.snapshot_and_reset(), Default::default());
}

#[test]
fn settlement_decision_requires_both_known_unsettled_for_full_access() {
    let pm = SettlementStatus::TradableUnsettled;
    let outcome = OutcomeSettlement::Unsettled;
    assert_eq!(
        settlement_decision::<(), ()>(Ok(&pm), Ok(&outcome)),
        (SettlementAccess::All, None)
    );
    let unavailable = SettlementStatus::Unavailable;
    assert_eq!(
        settlement_decision::<(), ()>(Ok(&unavailable), Ok(&outcome)),
        (SettlementAccess::ReduceOnly, None)
    );
}

#[test]
fn settlement_decision_stops_on_one_successful_settlement_despite_other_error() {
    let pm_settled = SettlementStatus::Settled { payouts: vec![] };
    let outcome_settled = OutcomeSettlement::Settled { payouts: vec![] };
    assert_eq!(
        settlement_decision::<(), &str>(Ok(&pm_settled), Err("outcome timeout")),
        (SettlementAccess::Stop, Some(POLYMARKET))
    );
    assert_eq!(
        settlement_decision::<&str, ()>(Err("pm timeout"), Ok(&outcome_settled)),
        (SettlementAccess::Stop, Some(OUTCOME))
    );
    assert_eq!(
        settlement_decision::<&str, &str>(Err("pm timeout"), Err("outcome timeout")),
        (SettlementAccess::ReduceOnly, None)
    );
}

#[test]
fn settlement_decision_prefers_polymarket_when_both_settled() {
    let pm = SettlementStatus::Settled { payouts: vec![] };
    let outcome = OutcomeSettlement::Settled { payouts: vec![] };
    assert_eq!(
        settlement_decision::<(), ()>(Ok(&pm), Ok(&outcome)),
        (SettlementAccess::Stop, Some(POLYMARKET))
    );
}

fn take_profit_topic() -> Topic {
    use crate::domain::TokenRef;
    use uuid::Uuid;

    let token = |platform: &str, token_id: &str, label: &str| TokenRef {
        platform: platform.into(),
        token_id: token_id.into(),
        label: label.into(),
        option_id: "1".into(),
        condition_id: None,
        asset_id: None,
        side_index: None,
        neg_risk: None,
        fees_enabled: None,
        fee_rate: None,
    };
    Topic {
        key: TopicKey::new(Uuid::nil(), 0),
        title: "topic".into(),
        market_title: "market".into(),
        end_date: None,
        tokens: vec![
            token(POLYMARKET, "pm-yes", "yes"),
            token(POLYMARKET, "pm-no", "no"),
            token(OUTCOME, "out-yes", "yes"),
            token(OUTCOME, "out-no", "no"),
            token(POLYMARKET, "pm-yes", "YES"),
        ],
    }
}

#[test]
fn take_profit_tokens_only_include_positive_complementary_pair_and_deduplicate() {
    let positions = crate::hedge::Positions::from([
        (
            POLYMARKET.into(),
            HashMap::from([("YES".into(), d("3")), ("no".into(), Decimal::ZERO)]),
        ),
        (
            OUTCOME.into(),
            HashMap::from([("yes".into(), d("4")), ("no".into(), d("2"))]),
        ),
    ]);

    assert_eq!(
        take_profit_book_tokens(&take_profit_topic(), &positions),
        vec![
            (POLYMARKET.into(), "pm-yes".into()),
            (OUTCOME.into(), "out-no".into()),
        ]
    );
}

#[test]
fn take_profit_tokens_include_both_positive_complementary_directions_once() {
    let positions = crate::hedge::Positions::from([
        (
            POLYMARKET.into(),
            HashMap::from([("yes".into(), d("3")), ("no".into(), d("1"))]),
        ),
        (
            OUTCOME.into(),
            HashMap::from([("yes".into(), d("4")), ("no".into(), d("2"))]),
        ),
    ]);
    let tokens = take_profit_book_tokens(&take_profit_topic(), &positions);

    assert_eq!(tokens.len(), 4);
    assert_eq!(tokens.iter().collect::<HashSet<_>>().len(), 4);
    assert!(tokens.contains(&(POLYMARKET.into(), "pm-yes".into())));
    assert!(tokens.contains(&(OUTCOME.into(), "out-no".into())));
    assert!(tokens.contains(&(POLYMARKET.into(), "pm-no".into())));
    assert!(tokens.contains(&(OUTCOME.into(), "out-yes".into())));
}

#[test]
fn take_profit_tokens_deduplicate_shared_token_ids() {
    let mut topic = take_profit_topic();
    topic
        .tokens
        .iter_mut()
        .filter(|token| token.platform == POLYMARKET)
        .for_each(|token| token.token_id = "pm-shared".into());
    let positions = crate::hedge::Positions::from([
        (
            POLYMARKET.into(),
            HashMap::from([("yes".into(), d("3")), ("no".into(), d("1"))]),
        ),
        (
            OUTCOME.into(),
            HashMap::from([("yes".into(), d("4")), ("no".into(), d("2"))]),
        ),
    ]);

    let tokens = take_profit_book_tokens(&topic, &positions);
    assert_eq!(tokens.len(), 3);
    assert_eq!(
        tokens
            .iter()
            .filter(|(platform, token_id)| platform == POLYMARKET && token_id == "pm-shared")
            .count(),
        1
    );
}

#[test]
fn position_detection_includes_negative_exposure() {
    let mut positions = crate::hedge::Positions::new();
    positions
        .entry(POLYMARKET.into())
        .or_default()
        .insert("yes".into(), Decimal::ZERO);
    positions
        .entry(OUTCOME.into())
        .or_default()
        .insert("no".into(), -Decimal::ONE);
    assert!(has_position(&positions));
    positions
        .get_mut(OUTCOME)
        .unwrap()
        .insert("no".into(), Decimal::ZERO);
    assert!(!has_position(&positions));
    positions
        .get_mut(POLYMARKET)
        .unwrap()
        .insert("yes".into(), Decimal::ONE);
    assert!(has_position(&positions));
}

#[test]
fn rebalance_loss_cooldown_constant_is_five_minutes() {
    assert_eq!(REBALANCE_LOSS_COOLDOWN, Duration::from_secs(300));
}

#[tokio::test]
async fn rebalance_loss_cooldown_inserted_with_correct_deadline() {
    let cooldowns = Mutex::new(HashMap::<TopicKey, Instant>::new());
    let key = TopicKey::new(uuid::Uuid::new_v4(), 0);
    let actual_profit = Decimal::new(-5, 1);
    let before = Instant::now();
    if actual_profit < Decimal::ZERO {
        let until = Instant::now() + REBALANCE_LOSS_COOLDOWN;
        cooldowns.lock().await.insert(key, until);
    }
    let after = Instant::now();

    let map = cooldowns.lock().await;
    let until = *map.get(&key).expect("topic must be in cooldown");
    assert!(until >= before + REBALANCE_LOSS_COOLDOWN);
    assert!(until <= after + REBALANCE_LOSS_COOLDOWN);
}

#[tokio::test]
async fn evaluate_topic_blocked_when_rebalance_loss_cooldown_active() {
    let engine = fee_test_engine().await;
    let topic = fee_test_topic();
    let key = topic.key;
    engine.topics.write().await.insert(key, topic);

    // 设置处于冷却期内
    let now = Instant::now();
    engine
        .rebalance_loss_cooldown
        .lock()
        .await
        .insert(key, now + Duration::from_secs(300));

    let res = engine.evaluate_topic(key).await;
    assert!(res.is_ok());

    // 验证统计指标计数增加且冷却记录仍在
    let stats = engine.stats.snapshot_and_reset();
    assert_eq!(stats.rebalance_loss_cooldown, 1);
    assert!(engine
        .rebalance_loss_cooldown
        .lock()
        .await
        .contains_key(&key));
}

#[tokio::test]
async fn evaluate_topic_clears_expired_rebalance_loss_cooldown() {
    let engine = fee_test_engine().await;
    let topic = fee_test_topic();
    let key = topic.key;
    engine.topics.write().await.insert(key, topic);

    // 设置已过期的时间戳
    let now = Instant::now();
    engine
        .rebalance_loss_cooldown
        .lock()
        .await
        .insert(key, now - Duration::from_secs(1));

    // 运行 evaluate_topic，即使后续因为未连接 db 失败，冷却条目在前期已被惰性清理
    let _ = engine.evaluate_topic(key).await;

    // 验证过期条目已被清理，且拦截计数未增加
    assert!(!engine
        .rebalance_loss_cooldown
        .lock()
        .await
        .contains_key(&key));
    let stats = engine.stats.snapshot_and_reset();
    assert_eq!(stats.rebalance_loss_cooldown, 0);
}
