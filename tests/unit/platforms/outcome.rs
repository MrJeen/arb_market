use super::*;

fn fee_user_reply() -> Value {
    json!({"userSpotCrossRate":"0.0007", "activeReferralDiscount":"0.04", "privateIgnoredField":"never log wallet response"})
}

fn fee_meta_reply() -> Value {
    json!({"feeScale":"1", "outcomes":[{"outcome":516,"venue":"out","quoteToken":"USDC","deployerFeeScale":"1","sideSpecs":[{"name":"Yes"},{"name":"No"}]}]})
}

#[tokio::test]
async fn fee_refresh_loopback_atomic_renewal_and_hot_path_without_http() {
    let (mut venue, server) = info_stub(vec![
        (200, fee_user_reply()),
        (200, fee_meta_reply()),
        (200, fee_user_reply()),
        (503, json!({"private":"not logged"})),
        (200, fee_user_reply()),
        (200, fee_meta_reply()),
    ]);
    venue.builder = Some(("test-builder".into(), 10));
    assert!(venue.fee_snapshot(516).is_err());
    venue.refresh_fees().await.unwrap();
    let first = venue.fee_snapshot(516).unwrap();
    assert_eq!(first.taker_rate, Decimal::new(1344, 6));
    assert_eq!(first.builder_rate, Decimal::new(1, 4));
    for _ in 0..100 {
        assert!(first.same_rules(&venue.fee_snapshot(516).unwrap()));
    }
    *venue.fee_last_attempt.lock().unwrap() = None;
    assert!(venue.refresh_fees().await.is_err());
    assert_eq!(
        first.estimate_json(),
        venue.fee_snapshot(516).unwrap().estimate_json()
    );
    *venue.fee_last_attempt.lock().unwrap() = None;
    venue.refresh_fees().await.unwrap();
    let renewed = venue.fee_snapshot(516).unwrap();
    assert!(first.same_rules(&renewed));
    assert_ne!(
        first.estimate_json()["user_fees_fetched_at"],
        renewed.estimate_json()["user_fees_fetched_at"]
    );
    assert!(!renewed
        .estimate_json()
        .to_string()
        .contains("privateIgnoredField"));
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 6);
    for pair in requests.chunks_exact(2) {
        assert_eq!(pair[0], json!({"type":"userFees","user":"0xtest"}));
        assert_eq!(pair[1], json!({"type":"outcomeMeta"}));
    }
}

#[test]
fn actuals_cache_resolver_rejects_foreign_wallet_and_noncanonical_token() {
    let mut venue = test_venue();
    venue.account = Some("0xTeSt".into());
    assert!(venue.latest_actuals_fee("0xtest", "#5160").is_none());
    venue.install_test_fee_snapshot(516, Decimal::ZERO, Decimal::ZERO);
    assert!(venue.latest_actuals_fee(" 0xTEST ", "#5160").is_some());
    for (wallet, token) in [
        ("other", "#5160"),
        ("0xtest", "#5170"),
        ("0xtest", "+5160"),
        ("0xtest", "#05160"),
        ("0xtest", "#5162"),
    ] {
        assert!(venue.latest_actuals_fee(wallet, token).is_none());
    }
    venue.expire_test_fee_snapshot(516);
    assert!(venue.latest_actuals_fee("0xtest", "#5160").is_none());
}

#[tokio::test]
async fn read_only_constructor_queries_only_info_without_signer() {
    for builder in [
        None,
        Some(crate::config::DEFAULT_OUTCOME_BUILDER.to_owned()),
    ] {
        let (stub, server) = info_stub(vec![(200, fee_user_reply()), (200, fee_meta_reply())]);
        let cfg = crate::config::RecomputeActualsConfig {
            app_postgres_uri: String::new(),
            hyperliquid_info_url: stub.info_url.clone(),
            outcome_account_address: Some("0xtest".into()),
            outcome_builder_address: builder.clone(),
            outcome_builder_fee: 10,
        };
        let venue = OutcomeVenue::connect_read_only(&cfg).unwrap();
        assert!(venue.signer.is_none());
        assert!(venue.exchange_url.is_empty());
        venue.refresh_fees().await.unwrap();
        assert_eq!(
            venue.fee_snapshot(516).unwrap().builder_rate,
            if builder.is_some() {
                Decimal::new(1, 4)
            } else {
                Decimal::ZERO
            }
        );
        assert!(venue.latest_actuals_fee("0xtest", "#5160").is_some());
        assert_eq!(
            server.join().unwrap(),
            vec![
                json!({"type":"userFees","user":"0xtest"}),
                json!({"type":"outcomeMeta"})
            ]
        );
    }
}

#[tokio::test]
async fn fee_refresh_missing_account_and_unset_builder() {
    let mut missing = test_venue();
    missing.account = None;
    assert!(missing.refresh_fees().await.is_err());
    assert!(missing.fee_snapshot(516).is_err());
    let (venue, server) = info_stub(vec![(200, fee_user_reply()), (200, fee_meta_reply())]);
    venue.refresh_fees().await.unwrap();
    assert_eq!(venue.fee_snapshot(516).unwrap().builder_rate, Decimal::ZERO);
    venue.expire_test_fee_snapshot(516);
    assert!(venue.fee_snapshot(516).is_err());
    server.join().unwrap();
}

#[tokio::test]
async fn fee_refresh_invalid_market_replaces_old_and_structural_failure_does_not_renew() {
    let mut invalid = fee_meta_reply();
    invalid["outcomes"][0]["venue"] = json!("unknown");
    let (venue, server) = info_stub(vec![
        (200, fee_user_reply()),
        (200, fee_meta_reply()),
        (200, fee_user_reply()),
        (200, json!({"outcomes":null})),
        (200, fee_user_reply()),
        (200, invalid),
    ]);
    venue.refresh_fees().await.unwrap();
    let first = venue.fee_snapshot(516).unwrap();
    *venue.fee_last_attempt.lock().unwrap() = None;
    assert!(venue.refresh_fees().await.is_err());
    assert_eq!(
        first.estimate_json(),
        venue.fee_snapshot(516).unwrap().estimate_json()
    );
    *venue.fee_last_attempt.lock().unwrap() = None;
    venue.refresh_fees().await.unwrap();
    assert!(venue.fee_snapshot(516).is_err());
    server.join().unwrap();
}

#[tokio::test]
async fn refresh_clones_share_single_flight_and_failed_attempt_cooldown() {
    let mut venue = test_venue();
    venue.account = None;
    let clone = venue.clone();
    venue.fee_refreshing.store(true, Ordering::Release);
    assert_eq!(
        clone.refresh_fees_coordinated().await.unwrap(),
        FeeRefreshOutcome::InFlight
    );
    venue.fee_refreshing.store(false, Ordering::Release);
    assert!(venue.refresh_fees_coordinated().await.is_err());
    assert_eq!(
        clone.refresh_fees_coordinated().await.unwrap(),
        FeeRefreshOutcome::Throttled
    );
    assert!(!clone.fee_refreshing.load(Ordering::Acquire));
    // 取消释放 guard，但开始尝试时间不能被清空。
    {
        venue.fee_refreshing.store(true, Ordering::Release);
        let _guard = FeeRefreshGuard(&venue.fee_refreshing);
    }
    assert_eq!(
        clone.refresh_fees_coordinated().await.unwrap(),
        FeeRefreshOutcome::Throttled
    );
}

#[tokio::test]
async fn cancelled_http_refresh_releases_single_flight_but_keeps_cooldown() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut venue = test_venue();
    venue.info_url = format!("http://{}", listener.local_addr().unwrap());
    venue.http = reqwest::Client::builder().no_proxy().build().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4096];
        stream.read(&mut buf).await.unwrap();
        sent.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let clone = venue.clone();
    let request = tokio::spawn(async move { clone.refresh_fees_coordinated().await });
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        venue.refresh_fees_coordinated().await.unwrap(),
        FeeRefreshOutcome::InFlight
    );
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(!venue.fee_refreshing.load(Ordering::Acquire));
    assert_eq!(
        venue.clone().refresh_fees_coordinated().await.unwrap(),
        FeeRefreshOutcome::Throttled
    );
    server.abort();
    let _ = server.await;
}

#[test]
fn fee_refresh_guard_releases_on_cancel() {
    let venue = test_venue();
    venue.fee_refreshing.store(true, Ordering::Release);
    {
        let _guard = FeeRefreshGuard(&venue.fee_refreshing);
    }
    assert!(!venue.fee_refreshing.load(Ordering::Acquire));
}

fn test_venue() -> OutcomeVenue {
    OutcomeVenue {
        http: reqwest::Client::new(),
        info_url: "http://127.0.0.1".into(),
        exchange_url: "http://127.0.0.1".into(),
        mainnet: true,
        signer: None,
        account: Some("0xtest".into()),
        builder: None,
        nonce: Arc::new(StdMutex::new(0)),
        fee_cache: Arc::new(StdRwLock::new(fees::FeeCache::default())),
        fee_refreshing: Arc::new(AtomicBool::new(false)),
        fee_last_attempt: Arc::new(StdMutex::new(None)),
    }
}

#[tokio::test]
async fn submit_http_evidence_preserves_json_and_distinguishes_missing_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let long = json!({"status":"ok","response":{"data":{"statuses":[{"resting":{"oid":123}}]}},"error":"real field", "unknown":[null,{"secret":"PRIVATE_RESPONSE".repeat(100)}]}).to_string();
    for (status, text, broken) in [
        (200, long.as_str(), false),
        (400, long.as_str(), false),
        (500, long.as_str(), false),
        (200, "null", false),
        (200, "", false),
        (200, "not JSON", false),
        (400, "not JSON", false),
        (200, "partial", true),
        (400, "partial", true),
        (0, "", false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let reply = format!(
            "HTTP/1.1 {status} Stub\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
            if broken { text.len() + 100 } else { text.len() }
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            socket.read(&mut buf).await.unwrap();
            if status != 0 {
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        let mut venue = test_venue();
        venue.exchange_url = format!("http://{addr}");
        venue.http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let (result, raw) = venue
            .post_prepared(PreparedOrder {
                order_hash: "hash".into(),
                envelope: json!({"cloid":"cloid"}),
                payload: json!({}),
                funder: None,
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert!(!format!("{raw:?}").contains("PRIVATE_RESPONSE"));
        if status == 0 {
            assert!(matches!(
                raw,
                super::super::SubmissionResponse::NoResponse(_)
            ));
            assert!(matches!(result, SubmitResult::Unknown { .. }));
        } else {
            assert!(matches!(raw, super::super::SubmissionResponse::Http(_)));
            if broken {
                assert_eq!(raw["body_error"], "response_body");
                assert!(raw.get("body").is_none());
            } else if let Ok(body) = serde_json::from_str::<Value>(text) {
                if status == 200 {
                    assert_eq!(*raw, body);
                } else {
                    assert_eq!(raw["body"], body);
                }
            } else {
                assert_eq!(raw["body"], text);
                assert_eq!(
                    raw["body_format"],
                    if text.is_empty() { "empty" } else { "non_json" }
                );
            }
            if status == 400 {
                assert!(matches!(result, SubmitResult::Failed { status: 400, .. }));
            } else if status == 200 && text == long {
                assert!(matches!(result, SubmitResult::Ack { .. }));
            } else {
                assert!(matches!(result, SubmitResult::Unknown { .. }));
            }
        }
    }
}

fn fill(tid: u64, time: u64, coin: &str) -> Value {
    json!({
        "tid": tid, "oid": 9007199254740993u64, "time": time,
        "coin": coin, "sz": "3", "px": "0.4", "fee": "0.01", "feeToken": "USDC"
    })
}

fn fill_state(start: i64) -> FillProgress {
    let mut state = FillProgress::load("#5160", start, &Value::Null).unwrap();
    state.end_time = 100_000;
    state
}

// 只绑定 loopback，直接构造 venue；不初始化配置、签名器或真实网络交易。
fn info_stub(replies: Vec<(u16, Value)>) -> (OutcomeVenue, std::thread::JoinHandle<Vec<Value>>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, reply) in replies {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "missing stub HTTP request");
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(err) => panic!("stub accept failed: {err}"),
                }
            };
            // macOS accept 继承 listener 的 O_NONBLOCK；线程内读写需显式改回阻塞。
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut data = Vec::new();
            let (body_start, content_length) = loop {
                let mut buf = [0; 4096];
                let len = stream.read(&mut buf).unwrap();
                assert!(len > 0, "request ended before headers");
                data.extend_from_slice(&buf[..len]);
                if let Some(index) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&data[..index]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (index + 4, length);
                }
            };
            while data.len() < body_start + content_length {
                let mut buf = [0; 4096];
                let len = stream.read(&mut buf).unwrap();
                assert!(len > 0, "request ended before body");
                data.extend_from_slice(&buf[..len]);
            }
            requests.push(
                serde_json::from_slice(&data[body_start..body_start + content_length]).unwrap(),
            );
            let body = reply.to_string();
            write!(stream,
                    "HTTP/1.1 {status} Stub\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).unwrap();
        }
        requests
    });
    let mut venue = test_venue();
    venue.info_url = format!("http://{addr}/info");
    venue.http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    (venue, server)
}

#[tokio::test]
async fn poll_order_serializes_exact_u64_and_cloid() {
    let ids = [
        "0",
        "00042",
        "9007199254740993",
        "18446744073709551615",
        "0x0123456789abcdefABCDEF0123456789",
    ];
    let (venue, server) = info_stub(
        ids.iter()
            .map(|_| (200, json!({"status": "unknownOid"})))
            .collect(),
    );
    for id in ids {
        assert!(!venue.poll_order(id, "#5160").await.unwrap().found);
    }
    let requests = server.join().unwrap();
    for (request, expected) in requests.iter().zip([0, 42, 9007199254740993, u64::MAX]) {
        assert_eq!(request["type"], "orderStatus");
        assert_eq!(request["oid"].as_u64(), Some(expected));
        assert!(!request["oid"].is_string());
    }
    assert_eq!(requests[4]["oid"].as_str(), Some(ids[4]));
}

#[tokio::test]
async fn invalid_order_ids_fail_before_account_or_http() {
    let mut venue = test_venue();
    venue.account = None;
    for id in [
        "",
        " 42",
        "+42",
        "-1",
        "1.0",
        "1e3",
        "18446744073709551616",
        "0xabc",
        "0x0123456789abcdef0123456789abcdeg",
    ] {
        let err = venue.poll_order(id, "#5160").await.unwrap_err();
        assert!(!err.to_string().contains("missing OUTCOME_ACCOUNT_ADDRESS"));
        assert!(order_query_id(id).is_err());
    }
}

#[test]
fn order_status_uses_official_nested_order_and_separate_sizes() {
    let cloid = "0x0123456789abcdef0123456789abcdef";
    let raw = json!({"status": "order", "order": {
        "status": "canceled", "statusTimestamp": 100,
        "order": {"oid": u64::MAX, "cloid": cloid, "coin": "#5160",
            "sz": "7", "origSz": "10", "limitPx": "0.4"}
    }});
    let order = parse_order_status(raw.clone(), cloid);
    assert!(order.found);
    assert_eq!(order.status, "canceled");
    assert_eq!(order.order_id.as_deref(), Some("18446744073709551615"));
    assert_eq!(order.client_order_id.as_deref(), Some(cloid));
    assert_eq!(order.coin.as_deref(), Some("#5160"));
    assert_eq!(order.original_shares, Some(Decimal::from(10)));
    assert_eq!(order.remaining_shares, Some(Decimal::from(7)));
    assert_eq!(order.price, Some("0.4".parse().unwrap()));
    assert_eq!(order.shares, None);
    assert_eq!(order.raw, raw);
}

#[test]
fn unknown_or_invalid_status_never_promotes_query_id_to_oid() {
    for raw in [
        Value::Null,
        json!({"status":"unknownOid"}),
        json!({}),
        json!({"status":"order", "order":{"status":"filled","order":{"oid":"0xabc"}}}),
        json!({"status":"order", "order":{"status":"filled","order":{"oid":1.0}}}),
    ] {
        let result = parse_order_status(raw, "0x0123456789abcdef0123456789abcdef");
        assert!(!result.found);
        assert!(result.order_id.is_none());
        assert!(result.shares.is_none());
    }
    assert_eq!(
        numeric_id(Some(&json!("18446744073709551615"))),
        Some(u64::MAX.to_string())
    );
    assert_eq!(numeric_id(Some(&json!("00042"))), Some("42".into()));
    assert!(numeric_id(Some(&json!("18446744073709551616"))).is_none());
}

#[test]
fn fills_preserve_official_fees_finality_and_optional_cloid() {
    let mut raw = fill(1, 10, "#5160");
    raw["fee"] = json!("-0.002");
    let fills = parse_user_fills(&json!([raw]));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id.as_deref(), Some("9007199254740993"));
    assert_eq!(fills[0].fee, Some("-0.002".parse().unwrap()));
    assert_eq!(fills[0].fee_token.as_deref(), Some("USDC"));
    assert_eq!(fills[0].finality, FillFinality::Confirmed);
    assert_eq!(fills[0].order_ids.len(), 1);
    let mut raw = fill(2, 10, "#5160");
    raw.as_object_mut().unwrap().remove("fee");
    raw.as_object_mut().unwrap().remove("feeToken");
    let parsed = parse_user_fill(&raw).unwrap();
    assert_eq!(parsed.fee, None);
    assert_eq!(parsed.fee_token, None);
    assert_eq!(parsed.fee_rate_bps, None);
}

#[test]
fn history_probe_requires_positive_lower_bound_evidence() {
    for (raw, covered) in [
        (json!([]), false),
        (json!([fill(1, 101, "#other")]), false),
        (json!([fill(1, 100, "#other")]), false),
        (json!([fill(1, 99, "#other")]), true),
    ] {
        let mut state = fill_state(100);
        apply_fill_page(&raw, &mut state, true).unwrap();
        assert!(!state.history_complete);
        apply_fill_page(&json!([]), &mut state, false).unwrap();
        assert!(!state.complete);
        assert_eq!(state.phase, FillPhase::FinalProbe);
        apply_fill_page(&raw, &mut state, true).unwrap();
        assert!(state.complete);
        assert_eq!(state.history_complete, covered);
    }
    let mut state = fill_state(100);
    state.history_complete = true;
    state.scanned_count = FILL_HISTORY_LIMIT - 1;
    apply_fill_page(&json!([fill(1, 100, "#5160")]), &mut state, false).unwrap();
    apply_fill_page(&json!([fill(0, 99, "#other")]), &mut state, true).unwrap();
    assert!(state.complete);
    assert!(!state.history_complete);
}

#[test]
fn malformed_relevant_fills_and_page_metadata_fail_closed() {
    for field in ["sz", "px", "fee", "feeToken", "coin", "tid", "oid", "time"] {
        let mut raw = fill(1, 100, "#5160");
        raw[field] = json!({"invalid": true});
        let mut state = fill_state(100);
        assert!(
            apply_fill_page(&json!([raw]), &mut state, false).is_err(),
            "{field}"
        );
        assert!(!state.complete);
    }
    for raw in [
        json!({"error": "unavailable"}),
        json!([fill(1, 99, "#5160")]),
        json!([fill(1, 101, "#5160"), fill(2, 100, "#5160")]),
    ] {
        assert!(apply_fill_page(&raw, &mut fill_state(100), false).is_err());
    }
}

#[test]
fn fill_pages_do_not_accept_balance_coin_aliases() {
    for coin in ["+5160", "100005160"] {
        let mut state = fill_state(100);
        assert!(apply_fill_page(&json!([fill(1, 100, coin)]), &mut state, false).is_err());
        assert!(!state.complete);
    }
}

#[tokio::test]
async fn time_pagination_uses_account_page_size_and_inclusive_boundary() {
    let first: Vec<_> = (1..=2000).map(|id| fill(id, id + 99, "#other")).collect();
    let last = first.last().unwrap().clone();
    let (venue, server) = info_stub(vec![
        (200, json!([fill(0, 99, "#other")])),
        (200, json!(first)),
        (
            200,
            json!([last, fill(2001, 2099, "#5160"), fill(2002, 2100, "#5160")]),
        ),
    ]);
    let probe = venue
        .poll_fill_page("#5160", 100, &Value::Null)
        .await
        .unwrap();
    assert!(!probe.complete);
    assert!(probe.fills.is_empty());
    assert_eq!(probe.progress["historyChecked"], true);
    let first = venue
        .poll_fill_page("#5160", 100, &probe.progress)
        .await
        .unwrap();
    assert!(!first.complete);
    assert!(first.fills.is_empty());
    assert_eq!(first.progress["cursor"], 2099);
    let page = venue
        .poll_fill_page("#5160", 100, &first.progress)
        .await
        .unwrap();
    assert!(!page.complete);
    assert!(!page.history_complete);
    assert_eq!(page.progress["phase"], "finalProbe");
    assert_eq!(
        page.fills
            .iter()
            .map(|f| f.trade_id.as_str())
            .collect::<Vec<_>>(),
        vec!["2001", "2002"]
    );
    let requests = server.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r["startTime"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 100, 2099]
    );
    assert!(requests
        .iter()
        .all(|r| r["endTime"] == requests[0]["endTime"]));
    assert!(requests.iter().all(|r| r["aggregateByTime"] == false));
}

#[tokio::test]
async fn fill_progress_continues_second_call_and_only_restarts_after_completion() {
    let first: Vec<_> = (1..=2000).map(|id| fill(id, id + 99, "#5160")).collect();
    let second: Vec<_> = (2000..4000).map(|id| fill(id, id + 99, "#5160")).collect();
    let (venue, server) = info_stub(vec![
        (200, json!([fill(0, 99, "#other")])),
        (200, json!(first)),
        (200, json!(second)),
        (
            200,
            json!([fill(3999, 4098, "#5160"), fill(4000, 4099, "#5160")]),
        ),
        (200, json!([fill(0, 99, "#other")])),
        (200, json!([fill(0, 99, "#other")])),
        (200, json!([])),
    ]);
    let probe = venue
        .poll_fill_page("#5160", 100, &json!({}))
        .await
        .unwrap();
    assert!(!probe.complete);
    let first = venue
        .poll_fill_page("#5160", 100, &probe.progress)
        .await
        .unwrap();
    assert!(!first.complete);
    assert_eq!(first.fills.len(), 2000);
    let saved: Value = serde_json::from_str(&first.progress.to_string()).unwrap();
    let second = venue.poll_fill_page("#5160", 100, &saved).await.unwrap();
    assert!(!second.complete);
    assert_eq!(second.fills.len(), 1999);
    let third = venue
        .poll_fill_page("#5160", 100, &second.progress)
        .await
        .unwrap();
    assert!(!third.complete);
    assert!(!third.history_complete);
    assert_eq!(third.fills.len(), 1);
    assert_eq!(third.fills[0].trade_id, "4000");
    assert_eq!(third.progress["endTime"], first.progress["endTime"]);
    assert_eq!(third.progress["historyLowerBound"], 99);
    let completed = venue
        .poll_fill_page("#5160", 100, &third.progress)
        .await
        .unwrap();
    assert!(completed.complete);
    assert!(completed.history_complete);
    let next_probe = venue
        .poll_fill_page("#5160", 100, &completed.progress)
        .await
        .unwrap();
    assert!(!next_probe.complete);
    let next_round = venue
        .poll_fill_page("#5160", 100, &next_probe.progress)
        .await
        .unwrap();
    assert!(!next_round.complete);
    assert_eq!(next_round.progress["phase"], "finalProbe");
    let requests = server.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r["startTime"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 100, 2099, 4098, 0, 0, 100]
    );
    assert!(requests[..4]
        .iter()
        .all(|r| r["endTime"] == requests[0]["endTime"]));
}

#[tokio::test]
async fn full_timestamp_page_never_skips_stalled_boundary() {
    let full: Vec<_> = (1..=2000).map(|id| fill(id, 100, "#5160")).collect();
    let (venue, server) = info_stub(vec![
        (200, json!([fill(0, 99, "#other")])),
        (200, json!(full)),
        (200, json!(full)),
        (200, json!(full)),
    ]);
    let probe = venue
        .poll_fill_page("#5160", 100, &Value::Null)
        .await
        .unwrap();
    assert!(!probe.complete);
    let first = venue
        .poll_fill_page("#5160", 100, &probe.progress)
        .await
        .unwrap();
    assert!(!first.complete);
    assert_eq!(first.fills.len(), 2000);
    assert_eq!(first.progress["cursor"], 100);
    let next = venue
        .poll_fill_page("#5160", 100, &first.progress)
        .await
        .unwrap();
    assert!(!next.complete);
    assert!(next.fills.is_empty());
    assert_eq!(next.progress["cursor"], 100);
    let retry = venue
        .poll_fill_page("#5160", 100, &next.progress)
        .await
        .unwrap();
    assert!(!retry.complete);
    assert!(retry.fills.is_empty());
    assert_eq!(retry.progress["cursor"], 100);
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[1..].iter().all(|r| r["startTime"] == 100));
}

#[tokio::test]
async fn empty_or_truncated_history_never_proves_zero_fills() {
    for probe in [json!([]), json!([fill(1, 101, "#other")])] {
        let (venue, server) = info_stub(vec![(200, probe.clone()), (200, json!([])), (200, probe)]);
        let probe = venue
            .poll_fill_page("#5160", 100, &Value::Null)
            .await
            .unwrap();
        assert!(!probe.complete);
        assert!(!probe.history_complete);
        let page = venue
            .poll_fill_page("#5160", 100, &probe.progress)
            .await
            .unwrap();
        assert!(!page.complete);
        assert!(!page.history_complete);
        assert!(page.fills.is_empty());
        let final_page = venue
            .poll_fill_page("#5160", 100, &page.progress)
            .await
            .unwrap();
        assert!(final_page.complete);
        assert!(!final_page.history_complete);
        assert_eq!(server.join().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn http_error_and_malformed_page_do_not_return_complete() {
    let (venue, server) = info_stub(vec![(503, json!({"error": "unavailable"}))]);
    assert!(venue
        .poll_fill_page("#5160", 100, &Value::Null)
        .await
        .is_err());
    assert_eq!(server.join().unwrap().len(), 1);
    let mut bad = fill(1, 100, "#5160");
    bad["fee"] = json!("not-a-fee");
    let (venue, server) = info_stub(vec![
        (200, json!([fill(0, 99, "#other")])),
        (200, json!([bad])),
    ]);
    let probe = venue
        .poll_fill_page("#5160", 100, &Value::Null)
        .await
        .unwrap();
    assert!(!probe.complete);
    assert!(venue
        .poll_fill_page("#5160", 100, &probe.progress)
        .await
        .is_err());
    assert_eq!(server.join().unwrap().len(), 2);
}

#[tokio::test]
async fn later_page_failures_retry_saved_cursor_instead_of_first_page() {
    let first: Vec<_> = (1..=2000).map(|id| fill(id, id + 99, "#5160")).collect();
    let mut bad = fill(2001, 2100, "#5160");
    bad["fee"] = json!("invalid");
    let (venue, server) = info_stub(vec![
        (200, json!([fill(0, 99, "#other")])),
        (200, json!(first)),
        (503, json!({"error": "temporary"})),
        (200, json!([bad])),
        (
            200,
            json!([fill(2000, 2099, "#5160"), fill(2001, 2100, "#5160")]),
        ),
    ]);
    let probe = venue
        .poll_fill_page("#5160", 100, &Value::Null)
        .await
        .unwrap();
    let first = venue
        .poll_fill_page("#5160", 100, &probe.progress)
        .await
        .unwrap();
    assert_eq!(first.fills.len(), 2000);
    assert!(!first.complete);
    let saved: Value = serde_json::from_str(&first.progress.to_string()).unwrap();
    assert_eq!(saved["cursor"], 2099);
    for _ in 0..2 {
        assert!(venue.poll_fill_page("#5160", 100, &saved).await.is_err());
    }
    let recovered = venue.poll_fill_page("#5160", 100, &saved).await.unwrap();
    assert!(!recovered.complete);
    assert!(!recovered.history_complete);
    assert_eq!(recovered.progress["phase"], "finalProbe");
    assert_eq!(recovered.fills.len(), 1);
    assert_eq!(recovered.fills[0].trade_id, "2001");
    let requests = server.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r["startTime"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 100, 2099, 2099, 2099]
    );
    assert!(requests
        .iter()
        .all(|r| r["endTime"] == requests[0]["endTime"]));
}

#[tokio::test]
async fn probes_keep_unique_fills_and_recheck_coverage_after_reload() {
    for final_probe in [json!([]), json!([fill(9, 101, "#other")])] {
        let (venue, server) = info_stub(vec![
            (200, json!([fill(0, 99, "#other"), fill(1, 100, "#5160")])),
            (200, json!([])),
            (200, final_probe),
        ]);
        let initial = venue
            .poll_fill_page_after_terminal("#5160", 100, &Value::Null, Some(200))
            .await
            .unwrap();
        assert_eq!(initial.fills.len(), 1);
        assert_eq!(initial.fills[0].trade_id, "1");
        assert!(!initial.history_complete);
        let saved: Value = serde_json::from_str(&initial.progress.to_string()).unwrap();
        let tail = venue
            .poll_fill_page_after_terminal("#5160", 100, &saved, Some(200))
            .await
            .unwrap();
        assert!(!tail.complete);
        assert_eq!(tail.progress["phase"], "finalProbe");
        let final_page = venue
            .poll_fill_page_after_terminal("#5160", 100, &tail.progress, Some(200))
            .await
            .unwrap();
        assert!(final_page.complete);
        assert!(!final_page.history_complete);
        let requests = server.join().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|r| r["startTime"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 100, 0]
        );
        assert!(requests
            .iter()
            .all(|r| r["endTime"] == requests[0]["endTime"]));
    }
}

#[tokio::test]
async fn final_probe_retries_saved_stage_and_returns_real_fills() {
    let (venue, server) = info_stub(vec![
        (200, json!([fill(0, 99, "#other")])),
        (200, json!([])),
        (503, json!({"error":"temporary"})),
        (200, json!([fill(0, 99, "#other"), fill(2, 150, "#5160")])),
    ]);
    let initial = venue
        .poll_fill_page_after_terminal("#5160", 100, &Value::Null, Some(200))
        .await
        .unwrap();
    let tail = venue
        .poll_fill_page_after_terminal("#5160", 100, &initial.progress, Some(200))
        .await
        .unwrap();
    let saved: Value = serde_json::from_str(&tail.progress.to_string()).unwrap();
    assert!(venue
        .poll_fill_page_after_terminal("#5160", 100, &saved, Some(200))
        .await
        .is_err());
    let final_page = venue
        .poll_fill_page_after_terminal("#5160", 100, &saved, Some(200))
        .await
        .unwrap();
    assert!(final_page.complete && final_page.history_complete);
    assert_eq!(final_page.fills[0].trade_id, "2");
    let proof: FillProgress = serde_json::from_value(final_page.progress).unwrap();
    assert!(proof.has_coverage());
    assert!(proof.follows_final_probe(&saved));
    let requests = server.join().unwrap();
    assert_eq!(requests[2], requests[3]);
}

#[test]
fn final_probe_cannot_clear_scan_limits_or_same_millisecond_ambiguity() {
    let full: Vec<_> = (1..=2000).map(|id| fill(id, 100, "#5160")).collect();
    let mut state = fill_state(100);
    apply_fill_page(&json!(full), &mut state, false).unwrap();
    assert!(apply_fill_page(&json!(full), &mut state, false).unwrap().1);
    apply_fill_page(&json!([]), &mut state, false).unwrap();
    apply_fill_page(&json!([fill(0, 99, "#other")]), &mut state, true).unwrap();
    assert!(!state.history_complete);
    let mut state = fill_state(100);
    state.phase = FillPhase::FinalProbe;
    let (fills, _) = apply_fill_page(&json!(full), &mut state, true).unwrap();
    assert_eq!(fills.len(), 2000);
    assert!(!state.history_complete);
}

#[tokio::test]
async fn first_terminal_poll_restarts_old_window_once_and_v1_keeps_safe_cursor() {
    let mut old = serde_json::to_value(fill_state(100)).unwrap();
    old["version"] = json!(1);
    old["historyChecked"] = json!(true);
    old["historyComplete"] = json!(true);
    old["cursor"] = json!(150);
    for field in ["phase", "account", "terminalObservedAtMs", "scanValid"] {
        old.as_object_mut().unwrap().remove(field);
    }
    let migrated = FillProgress::load("#5160", 100, &old).unwrap();
    assert_eq!(migrated.phase, FillPhase::Scan);
    assert_eq!(migrated.cursor, 150);
    assert_eq!(migrated.end_time, 100_000);
    assert!(!migrated.history_complete);
    old["complete"] = json!(true);
    assert_eq!(
        FillProgress::load("#5160", 100, &old).unwrap().phase,
        FillPhase::FinalProbe
    );
    let (venue, server) = info_stub(vec![(200, json!([])), (200, json!([]))]);
    let first = venue
        .poll_fill_page_after_terminal("#5160", 100, &old, Some(100_001))
        .await
        .unwrap();
    assert_eq!(first.progress["phase"], "scan");
    assert_eq!(first.progress["cursor"], 100);
    assert_eq!(first.progress["terminalObservedAtMs"], 100_001);
    let second = venue
        .poll_fill_page_after_terminal("#5160", 100, &first.progress, Some(100_001))
        .await
        .unwrap();
    assert_eq!(second.progress["phase"], "finalProbe");
    let requests = server.join().unwrap();
    assert_eq!(requests[0]["startTime"], 0);
    assert_eq!(requests[1]["startTime"], 100);
    assert_eq!(requests[0]["endTime"], requests[1]["endTime"]);
}

#[test]
fn fill_progress_rejects_other_queries_and_invalid_cursors() {
    let state = fill_state(100);
    let value = serde_json::to_value(state).unwrap();
    assert!(FillProgress::load("#5161", 100, &value).is_err());
    assert!(FillProgress::load("#5160", 101, &value).is_err());
    assert!(FillProgress::load("#5160", -1, &Value::Null).is_err());
    let mut bad = value;
    bad["cursor"] = json!(99);
    assert!(FillProgress::load("#5160", 100, &bad).is_err());
}

#[test]
fn submit_ack_without_valid_numeric_oid_stays_unknown() {
    let envelope = json!({"cloid": "0x0123456789abcdef0123456789abcdef"});
    for oid in [
        Value::Null,
        json!("0xabc"),
        json!(1.5),
        json!("18446744073709551616"),
    ] {
        let body = exchange_ok(json!({"filled": {"totalSz": "5", "avgPx": "0.4", "oid": oid}}));
        match parse_exchange_submit(&body, "hash".into(), envelope.clone(), "cloid") {
            SubmitResult::Unknown {
                order_id,
                envelope: saved,
                ..
            } => {
                assert!(order_id.is_none());
                assert_eq!(saved, envelope);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }
    let body = exchange_ok(json!({"filled": {"totalSz": "5", "avgPx": "0.4", "oid": u64::MAX}}));
    match parse_exchange_submit(&body, "hash".into(), envelope, "cloid") {
        SubmitResult::Ack { order_id, .. } => assert_eq!(order_id, u64::MAX.to_string()),
        other => panic!("expected Ack, got {other:?}"),
    }
}

#[tokio::test]
async fn user_state_fetches_each_balance_including_zero() {
    let (venue, server) = info_stub(
        [11, 0, 7]
            .into_iter()
            .map(|balance| {
                (
                    200,
                    json!({"balances": [
                        {"coin": "USDC", "total": balance.to_string(), "hold": "0"}
                    ]}),
                )
            })
            .collect(),
    );
    for balance in [11, 0, 7] {
        assert_eq!(venue.user_state().await.unwrap(), Decimal::from(balance));
    }
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests {
        assert_eq!(
            request,
            json!({"type": "spotClearinghouseState", "user": "0xtest"})
        );
    }
}

#[tokio::test]
async fn concurrent_user_state_calls_from_clones_each_request() {
    let reply = json!({"balances": [{"coin": "USDC", "total": "13", "hold": "0"}]});
    let (venue, server) = info_stub(vec![(200, reply.clone()), (200, reply)]);
    let cloned = venue.clone();
    let (first, second) = tokio::join!(venue.user_state(), cloned.user_state());
    assert_eq!(first.unwrap(), Decimal::from(13));
    assert_eq!(second.unwrap(), Decimal::from(13));
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(
            request,
            json!({"type": "spotClearinghouseState", "user": "0xtest"})
        );
    }
}

#[test]
fn l2_snapshot_restores_disconnected_book_at_same_timestamp() {
    let now = Instant::now();
    let later = now + Duration::from_secs(1);
    let mut books = BookStore::default();
    let mut raw = json!({"channel":"l2Book", "data":{
        "coin":"#5160", "time":10,
        "levels":[[{"px":"0.40","sz":"12","n":1}],[]]
    }});
    assert_eq!(apply_ws_book(&mut books, &raw, now), Some("#5160".into()));
    assert_eq!(apply_ws_book(&mut books, &raw, later), None);
    books.mark_platform_stale(OUTCOME);
    raw["data"]["time"] = json!(9);
    assert_eq!(apply_ws_book(&mut books, &raw, later), None);
    assert!(books.get(OUTCOME, "#5160").unwrap().stale);
    raw["data"]["time"] = json!(10);
    assert_eq!(apply_ws_book(&mut books, &raw, later), Some("#5160".into()));
    let book = books.get(OUTCOME, "#5160").unwrap();
    assert!(book.is_fresh(Duration::from_secs(5), later));
    assert_eq!(book.received_at, later);
}

#[test]
fn parses_l2_snapshot() {
    let raw = json!({
        "channel": "l2Book",
        "data": {
            "coin": "#5160",
            "time": 10,
            "levels": [
                [{"px": "0.40", "sz": "12", "n": 1}],
                [{"px": "0.45", "sz": "8", "n": 1}]
            ]
        }
    });
    let (bids, asks, ts) = parse_l2_book(&raw).unwrap();
    assert_eq!(ts, 10);
    assert_eq!(bids[0].price.to_string(), "0.40");
    assert_eq!(asks[0].size.to_string(), "8");
}

#[tokio::test]
async fn rest_book_rejects_wrong_identity_and_malformed_complete_payloads() {
    let valid = json!({"coin":"#5160","time":10,"levels":[[],[{"px":"0.4","sz":"2"}]]});
    for (field, value) in [
        ("coin", json!("#5161")),
        ("time", Value::Null),
        ("time", json!(-1)),
        ("time", json!("10")),
        ("levels", json!([[]])),
        ("levels", json!([[],[{"px":"0.4","sz":"bad"}]])),
        ("levels", json!([[],[{"px":"1.1","sz":"2"}]])),
    ] {
        let mut bad = valid.clone();
        bad[field] = value;
        let (venue, server) = info_stub(vec![(200, bad)]);
        assert!(venue.rest_book("#5160").await.is_err());
        assert_eq!(server.join().unwrap().len(), 1);
    }
}

#[test]
fn invalid_ws_book_cannot_refresh_existing_snapshot() {
    let now = Instant::now();
    let mut books = BookStore::default();
    let valid = json!({"coin":"#5160","time":10,"levels":[[],[{"px":"0.4","sz":"2"}]]});
    assert!(apply_ws_book(&mut books, &valid, now).is_some());
    let mut invalid = valid.clone();
    invalid["time"] = Value::Null;
    assert!(parse_l2_book(&invalid).is_err());
    apply_ws_book(&mut books, &invalid, now);
    assert!(!books
        .get(OUTCOME, "#5160")
        .unwrap()
        .is_fresh(Duration::from_secs(5), now));
}

#[test]
fn parse_user_fills_keeps_coin_and_ids() {
    let fills = parse_user_fills(&json!([{
        "tid": 1,
        "oid": 99,
        "cloid": "0xabc",
        "coin": "#5160",
        "sz": "3",
        "px": "0.4",
        "fee": "0.01"
    }, {
        "tid": 2,
        "oid": 100,
        "coin": "#5161",
        "sz": "4",
        "px": "0.5",
        "fee": "0"
    }]));
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].coin.as_deref(), Some("#5160"));
    assert!(fills[0].matches(Some("99"), Some("0xabc")));
    assert_eq!(fills[0].fee, Some("0.01".parse().unwrap()));
    assert_eq!(fills[1].fee, Some(Decimal::ZERO));
    assert!(!fills[1].matches(Some("99"), None));
}

#[test]
fn parse_coin_balance_reads_named_token() {
    let raw = json!({"balances": [
        {"coin": "USDC", "total": "10", "hold": "0"},
        {"coin": "+5160", "total": "7", "hold": "0"}
    ]});
    assert_eq!(parse_coin_balance(&raw, "#5160").unwrap(), Decimal::from(7));
    assert_eq!(parse_coin_balance(&raw, "+5160").unwrap(), Decimal::from(7));
    assert_eq!(parse_coin_balance(&raw, "usdc").unwrap(), Decimal::from(10));
    assert_eq!(parse_usdc_balance(&raw).unwrap(), Decimal::from(10));
}

#[test]
fn parse_coin_balance_subtracts_hold_and_clamps_to_zero() {
    for (total, hold, expected) in [
        (json!("7.5"), json!("2.25"), "5.25"),
        (json!(7.5), json!(2.25), "5.25"),
        (json!("7"), json!("7"), "0"),
        (json!("7"), json!("8"), "0"),
        (json!("0"), json!("0"), "0"),
    ] {
        let raw = json!({"balances": [{"coin": "+5160", "total": total, "hold": hold}]});
        assert_eq!(
            parse_coin_balance(&raw, "#5160").unwrap(),
            expected.parse::<Decimal>().unwrap()
        );
    }
}

#[test]
fn parse_coin_balance_rejects_missing_or_invalid_fields() {
    for coin in ["USDC", "+5160", "100012110"] {
        let want = match coin {
            "+5160" => "#5160",
            "100012110" => "#12110",
            _ => "USDC",
        };
        for field in ["total", "hold"] {
            let valid = json!({"coin": coin, "total": "10", "hold": "0"});
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(parse_coin_balance(&json!({"balances": [missing]}), want).is_err());
            for invalid in [
                Value::Null,
                json!(""),
                json!("bad"),
                json!("NaN"),
                json!("Infinity"),
                json!("79228162514264337593543950336"),
                json!("-1"),
                json!(-1),
                json!(true),
                json!([]),
                json!({}),
            ] {
                let mut item = valid.clone();
                item[field] = invalid;
                assert!(
                    parse_coin_balance(&json!({"balances": [item]}), want).is_err(),
                    "accepted invalid {field} for {want}"
                );
            }
        }
    }
}

#[test]
fn balance_parsers_distinguish_absent_coin_from_malformed_state() {
    for raw in [
        json!({"balances": []}),
        json!({"balances": [{"coin": "OTHER", "total": "1", "hold": "0"}]}),
    ] {
        assert_eq!(parse_coin_balance(&raw, "#5160").unwrap(), Decimal::ZERO);
        assert_eq!(parse_usdc_balance(&raw).unwrap(), Decimal::ZERO);
    }
    for raw in [
        Value::Null,
        json!([]),
        json!("bad"),
        json!({}),
        json!({"balances": null}),
        json!({"balances": {}}),
        json!({"balances": "bad"}),
    ] {
        assert!(parse_coin_balance(&raw, "#5160").is_err());
        assert!(parse_usdc_balance(&raw).is_err());
    }
}

#[test]
fn parse_usdc_balance_subtracts_hold_without_using_usdh() {
    for (hold, expected) in [("2", 8), ("10", 0), ("11", 0)] {
        let raw = json!({"balances": [
            {"coin": "usdc", "total": "10", "hold": hold},
            {"coin": "USDH", "total": "20", "hold": "3"}
        ]});
        assert_eq!(parse_usdc_balance(&raw).unwrap(), Decimal::from(expected));
    }
    // 未选中的 USDH 不参与余额校验。
    let raw = json!({"balances": [
        {"coin": "USDC", "total": "10", "hold": "10"},
        {"coin": "USDH", "total": "bad"}
    ]});
    assert_eq!(parse_usdc_balance(&raw).unwrap(), Decimal::ZERO);
}

#[test]
fn parse_usdc_balance_returns_zero_when_absent_or_zero_and_ignores_usdh() {
    for usdc in [
        None,
        Some(json!({"coin": "USDC", "total": "0", "hold": "0"})),
    ] {
        for usdh in [
            json!({"coin": "usdh", "total": "20", "hold": "3"}),
            json!({"coin": "USDH", "total": "20", "hold": "20"}),
            json!({"coin": "USDH", "total": "20", "hold": "21"}),
            json!({"coin": "USDH", "total": "20"}),
            json!({"coin": "USDH", "total": "bad", "hold": "bad"}),
        ] {
            let mut balances = vec![usdh];
            balances.extend(usdc.clone());
            assert_eq!(
                parse_usdc_balance(&json!({"balances": balances})).unwrap(),
                Decimal::ZERO
            );
        }
    }
    for usdc in [
        json!({"coin": "USDC", "hold": "0"}),
        json!({"coin": "USDC", "total": "bad", "hold": "0"}),
        json!({"coin": "USDC", "total": "-1", "hold": "0"}),
        json!({"coin": "USDC", "total": "0"}),
        json!({"coin": "USDC", "total": "0", "hold": "bad"}),
        json!({"coin": "USDC", "total": "10", "hold": "-1"}),
    ] {
        let raw = json!({"balances": [usdc, {"coin": "USDH", "total": "20", "hold": "0"}]});
        assert!(parse_usdc_balance(&raw).is_err());
    }
}

#[tokio::test]
async fn user_state_and_token_balance_return_available_balances() {
    let raw = json!({"balances": [
        {"coin": "USDC", "total": "10", "hold": "10"},
        {"coin": "USDH", "total": "20", "hold": "3"},
        {"coin": "+5160", "total": "7.5", "hold": "2.25"}
    ]});
    let usdh_only = json!({"balances": [{"coin": "USDH", "total": "20", "hold": "3"}]});
    let (venue, server) = info_stub(vec![
        (200, raw.clone()),
        (200, raw.clone()),
        (200, raw),
        (200, usdh_only.clone()),
        (200, usdh_only),
    ]);
    assert_eq!(venue.user_state().await.unwrap(), Decimal::ZERO);
    assert_eq!(venue.user_state().await.unwrap(), Decimal::ZERO);
    assert_eq!(
        venue.token_balance("#5160").await.unwrap(),
        "5.25".parse().unwrap()
    );
    assert_eq!(venue.user_state().await.unwrap(), Decimal::ZERO);
    assert_eq!(venue.user_state().await.unwrap(), Decimal::ZERO);
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 5);
    for request in requests {
        assert_eq!(
            request,
            json!({"type": "spotClearinghouseState", "user": "0xtest"})
        );
    }
}

#[tokio::test]
async fn user_state_errors_are_propagated_and_each_success_requests() {
    for (status, bad) in [
        (503, json!({"error": "unavailable"})),
        (200, json!({"balances": null})),
        (
            200,
            json!({"balances": [
                {"coin": "USDC", "total": "0"},
                {"coin": "USDH", "total": "20", "hold": "0"}
            ]}),
        ),
        (
            200,
            json!({"balances": [{"coin": "USDC", "total": "20", "hold": "bad"}]}),
        ),
    ] {
        let good = json!({"balances": [{"coin": "USDC", "total": "10", "hold": "3"}]});
        let (venue, server) = info_stub(vec![(status, bad), (200, good.clone()), (200, good)]);
        assert!(venue.user_state().await.is_err());
        assert_eq!(venue.user_state().await.unwrap(), Decimal::from(7));
        assert_eq!(venue.user_state().await.unwrap(), Decimal::from(7));
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests {
            assert_eq!(
                request,
                json!({"type": "spotClearinghouseState", "user": "0xtest"})
            );
        }
    }
}

#[tokio::test]
async fn token_balance_errors_are_propagated_and_retry_succeeds() {
    for (status, bad) in [
        (503, json!({"error": "unavailable"})),
        (200, json!({})),
        (200, json!({"balances": [{"coin": "+5160", "total": "7"}]})),
        (
            200,
            json!({"balances": [{"coin": "+5160", "total": "7", "hold": "-1"}]}),
        ),
    ] {
        let good = json!({"balances": [{"coin": "+5160", "total": "7", "hold": "2"}]});
        let (venue, server) = info_stub(vec![(status, bad), (200, good)]);
        assert!(venue.token_balance("#5160").await.is_err());
        assert_eq!(
            venue.token_balance("#5160").await.unwrap(),
            Decimal::from(5)
        );
        assert_eq!(server.join().unwrap().len(), 2);
    }
}

#[test]
fn parses_settlement_market_id() {
    assert_eq!(parse_settlement_market_id(" 516 ").unwrap(), 516);
    assert!(parse_settlement_market_id("not-a-number").is_err());
    assert!(parse_settlement_market_id("0").is_err());
}

#[test]
fn parse_coin_balance_matches_asset_id() {
    let raw = json!({"balances": [{"coin": "100012110", "total": "119", "hold": "0"}]});
    assert_eq!(
        parse_coin_balance(&raw, "#12110").unwrap(),
        Decimal::from(119)
    );
}

fn exchange_ok(status_item: Value) -> Value {
    json!({
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": [status_item]}}
    })
}

#[test]
fn parse_submit_ioc_unfilled_is_no_match() {
    let body = exchange_ok(json!({
        "error": "Order could not immediately match against any resting orders."
    }));
    match parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid") {
        SubmitResult::NoMatch { message, .. } => {
            assert!(message.contains("immediately match"));
        }
        other => panic!("expected NoMatch, got {other:?}"),
    }
}

#[test]
fn parse_submit_min_notional_is_no_match() {
    let body = exchange_ok(json!({"error": "Order must have minimum value of 1 USDC."}));
    assert!(matches!(
        parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid"),
        SubmitResult::NoMatch { .. }
    ));
}

#[test]
fn parse_submit_no_liquidity_is_no_match() {
    let body = exchange_ok(json!({"error": "No liquidity available for market order."}));
    assert!(matches!(
        parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid"),
        SubmitResult::NoMatch { .. }
    ));
}

#[test]
fn parse_submit_filled_reads_avg_px() {
    let body = exchange_ok(json!({
        "filled": {"totalSz": "5", "avgPx": "0.55", "oid": 777}
    }));
    match parse_exchange_submit(&body, "0x1".into(), json!({}), "cloid") {
        SubmitResult::Ack {
            order_id,
            taking,
            avg_px,
            ..
        } => {
            assert_eq!(order_id, "777");
            assert_eq!(taking.unwrap().to_string(), "5");
            assert_eq!(avg_px.unwrap().to_string(), "0.55");
        }
        other => panic!("expected Ack, got {other:?}"),
    }
}

#[test]
fn parse_submit_top_level_err_and_ok_share_classifier() {
    let err_body = json!({
        "status": "err",
        "response": "Order could not immediately match against any resting orders."
    });
    assert!(matches!(
        parse_exchange_submit(&err_body, "0x1".into(), json!({}), "cloid"),
        SubmitResult::NoMatch { .. }
    ));
    let ok_empty = json!({"status": "ok", "response": {"type": "order", "data": {"statuses": []}}});
    assert!(matches!(
        parse_exchange_submit(&ok_empty, "0x1".into(), json!({}), "cloid"),
        SubmitResult::Unknown { .. }
    ));
}

#[test]
fn explicit_reject_is_failed_but_ambiguous_http_statuses_stay_unknown() {
    let body = json!({"error": "unauthorized"});
    match classify_http_submit(400, &body, "0x1".into(), json!({}), "cloid") {
        SubmitResult::Failed { status, .. } => assert_eq!(status, 400),
        other => panic!("expected Failed, got {other:?}"),
    }
    // 5xx / 408 / 425 / 429 未证明订单没被撮合，记零成交会漏记真实持仓。
    for status in [408u16, 425, 429, 500, 502, 503, 504] {
        match classify_http_submit(status, &body, "0x1".into(), json!({}), "cloid") {
            SubmitResult::Unknown { message, .. } => assert!(
                message.contains(&status.to_string()),
                "status {status}: {message}"
            ),
            other => panic!("expected Unknown for {status}, got {other:?}"),
        }
    }
}

#[test]
fn transport_error_is_unknown() {
    match classify_submit_transport_error("connection timed out", "0x1".into(), json!({})) {
        SubmitResult::Unknown { message, .. } => {
            assert!(message.contains("timed out"));
        }
        other => panic!("expected Unknown, got {other:?}"),
    }
}

#[test]
fn http_200_ioc_error_stays_no_match() {
    let body = exchange_ok(json!({
        "error": "Order could not immediately match against any resting orders."
    }));
    assert!(matches!(
        classify_http_submit(200, &body, "0x1".into(), json!({}), "cloid"),
        SubmitResult::NoMatch { .. }
    ));
}
