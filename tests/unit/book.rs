use super::*;
use rust_decimal::prelude::FromStr;

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

#[test]
fn replaces_snapshot_and_rejects_older() {
    let mut store = BookStore::default();
    let now = Instant::now();
    assert!(store
        .replace_snapshot(
            POLYMARKET,
            "t1",
            vec![Level {
                price: d("0.4"),
                size: d("10"),
            }],
            vec![Level {
                price: d("0.5"),
                size: d("8"),
            }],
            100,
            now,
        )
        .is_applied());
    assert!(store
        .replace_snapshot(POLYMARKET, "t1", vec![], vec![], 100, now)
        .is_applied());
    assert!(!store
        .replace_snapshot(POLYMARKET, "t1", vec![], vec![], 90, now)
        .is_applied());
    let book = store.get(POLYMARKET, "t1").unwrap();
    assert!(book.bids.is_empty() && book.asks.is_empty());
    assert_eq!(book.exchange_ts_ms, 100);
    assert!(!book.stale);
    assert!(store
        .replace_snapshot(
            POLYMARKET,
            "t1",
            vec![],
            vec![Level {
                price: d("0.6"),
                size: d("4"),
            }],
            101,
            now,
        )
        .is_applied());
    assert_eq!(store.get(POLYMARKET, "t1").unwrap().asks[0].price, d("0.6"));
    assert!(store
        .apply_levels(POLYMARKET, "t1", &[(false, d("0.6"), d("1"))], 101, now)
        .is_applied());
    assert!(store
        .apply_levels(POLYMARKET, "t1", &[(false, d("0.61"), d("1"))], 102, now)
        .is_applied());
}

#[test]
fn empty_increment_batch_does_not_create_or_refresh_book() {
    let now = Instant::now();
    let later = now + Duration::from_secs(1);
    let mut store = BookStore::default();
    assert!(!store
        .apply_levels(POLYMARKET, "t", &[], 100, now)
        .is_applied());
    assert!(store.get(POLYMARKET, "t").is_none());
    store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 100, now);
    assert!(!store
        .apply_levels(POLYMARKET, "t", &[], 101, later)
        .is_applied());
    let book = store.get(POLYMARKET, "t").unwrap();
    assert_eq!(book.exchange_ts_ms, 100);
    assert_eq!(book.received_at, now);
}

#[test]
fn partial_books_remain_stale_until_snapshot() {
    let now = Instant::now();
    let mut store = BookStore::default();
    for (index, token) in ["empty", "tick-only"].into_iter().enumerate() {
        store.index_token(POLYMARKET, token, topic_key(index as i32));
        if token == "tick-only" {
            store.set_tick_size(POLYMARKET, token, d("0.01"));
        }
        assert!(store
            .apply_levels(POLYMARKET, token, &[(true, d("0.4"), d("10"))], 100, now)
            .is_applied());
        let book = store.get(POLYMARKET, token).unwrap();
        assert!(book.stale);
        assert!(!book.is_fresh(Duration::from_secs(5), now));
    }
    assert_eq!(
        store.stale_pm_tokens(Duration::from_secs(5), now, 10),
        vec!["empty", "tick-only"]
    );
    let later = now + Duration::from_secs(1);
    assert!(!store
        .replace_snapshot(POLYMARKET, "empty", vec![], vec![], 99, later)
        .is_applied());
    assert!(store.get(POLYMARKET, "empty").unwrap().stale);
    assert!(store
        .replace_snapshot(POLYMARKET, "empty", vec![], vec![], 101, later)
        .is_applied());
    assert!(store
        .get(POLYMARKET, "empty")
        .unwrap()
        .is_fresh(Duration::from_secs(5), later));
}

#[test]
fn complete_book_increment_renews_ttl_without_snapshot() {
    let now = Instant::now();
    let later = now + Duration::from_secs(10);
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 100, now);
    assert!(!store
        .get(POLYMARKET, "t")
        .unwrap()
        .is_fresh(Duration::from_secs(5), later));
    assert!(!store
        .replace_snapshot(POLYMARKET, "t", vec![], vec![], 100, later)
        .is_applied());
    assert!(store
        .apply_levels(POLYMARKET, "t", &[(true, d("0.4"), d("10"))], 101, later)
        .is_applied());
    assert!(store
        .get(POLYMARKET, "t")
        .unwrap()
        .is_fresh(Duration::from_secs(5), later));
}

#[test]
fn keeps_tick_size_across_snapshots() {
    let mut store = BookStore::default();
    let now = Instant::now();
    store.set_tick_size(POLYMARKET, "t1", d("0.001"));
    assert!(store
        .replace_snapshot(
            POLYMARKET,
            "t1",
            vec![Level {
                price: d("0.40"),
                size: d("10"),
            }],
            vec![],
            1,
            now,
        )
        .is_applied());
    assert_eq!(
        store.get(POLYMARKET, "t1").unwrap().tick_size,
        Some(d("0.001"))
    );
}

fn asks(size: &str) -> Vec<Level> {
    vec![Level {
        price: d("0.5"),
        size: d(size),
    }]
}

#[test]
fn tick_high_water_survives_disconnect_and_rejects_older_snapshots() {
    for disconnected in [false, true] {
        let now = Instant::now();
        let mut store = BookStore::default();
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
        assert!(store
            .set_tick_size_at(POLYMARKET, "t", d("0.001"), 200)
            .is_applied());
        if disconnected {
            store.mark_platform_stale(POLYMARKET);
        }
        for tick in [None, Some(d("0.01"))] {
            let ticket = store.begin_rest(POLYMARKET, "t");
            assert_eq!(
                store
                    .accept_rest(&ticket, vec![], asks("4"), 150, now, tick)
                    .unwrap_err(),
                BookReject::OlderTimestamp
            );
            assert_eq!(
                store.replace_snapshot_with_tick(
                    POLYMARKET,
                    "t",
                    vec![],
                    asks("4"),
                    150,
                    now,
                    tick
                ),
                BookUpdate::Rejected(BookReject::OlderTimestamp)
            );
        }
        assert_eq!(
            store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 150),
            BookUpdate::Rejected(BookReject::OlderTimestamp)
        );
        assert_eq!(store.tick_size(POLYMARKET, "t"), Some(d("0.001")));
        assert_eq!(store.get(POLYMARKET, "t").unwrap().exchange_ts_ms, 100);
    }
}

#[test]
fn older_depth_after_tick_invalidates_ws_without_reverting_tick() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    store.set_tick_size_at(POLYMARKET, "t", d("0.001"), 200);
    assert_eq!(
        store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("4"))], 150, now),
        BookUpdate::Rejected(BookReject::OlderTimestamp)
    );
    let book = store.get(POLYMARKET, "t").unwrap();
    assert!(book.stale);
    assert_eq!(book.asks, asks("3"));
    assert_eq!(book.exchange_ts_ms, 100);
    assert_eq!(book.tick_size, Some(d("0.001")));
}

#[test]
fn tick_observations_advance_revision_without_refreshing_or_completing_book() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 150);
    let old = store.begin_rest(POLYMARKET, "t");
    assert_eq!(
        store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 150),
        BookUpdate::VerifiedUnchanged
    );
    assert!(store.begin_rest(POLYMARKET, "t").revision > old.revision);
    assert!(store
        .set_tick_size_at(POLYMARKET, "t", d("0.01"), 200)
        .is_applied());
    assert_eq!(
        store
            .accept_rest(&old, vec![], asks("3"), 300, now, Some(d("0.001")))
            .unwrap_err(),
        BookReject::RevisionChanged
    );
    let later = now + Duration::from_secs(6);
    let book = store.get_at(POLYMARKET, "t", later).unwrap();
    assert_eq!(book.received_at, now);
    assert_eq!(book.exchange_ts_ms, 100);
    assert!(!book.is_fresh(Duration::from_secs(5), later));
    store.mark_platform_stale(POLYMARKET);
    store.set_tick_size_at(POLYMARKET, "t", d("0.001"), 300);
    assert!(store.get(POLYMARKET, "t").unwrap().stale);
}

#[test]
fn tick_compares_current_rest_baseline_high_water() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    let ticket = store.begin_rest(POLYMARKET, "t");
    store
        .accept_rest(&ticket, vec![], asks("4"), 300, now, None)
        .unwrap();
    assert_eq!(store.get(POLYMARKET, "t").unwrap().exchange_ts_ms, 300);
    assert_eq!(
        store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 200),
        BookUpdate::Rejected(BookReject::OlderTimestamp)
    );
    store.mark_platform_stale(POLYMARKET);
    assert_eq!(
        store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 200),
        BookUpdate::Rejected(BookReject::OlderTimestamp)
    );
}

#[test]
fn tick_conflict_cannot_be_seeded_or_fixed_by_tickless_snapshot() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 200);
    let prior = store.begin_rest(POLYMARKET, "t");
    assert_eq!(
        store.set_tick_size_at(POLYMARKET, "t", d("0.001"), 200),
        BookUpdate::Rejected(BookReject::TimestampConflict)
    );
    assert!(store.begin_rest(POLYMARKET, "t").revision > prior.revision);
    assert_eq!(store.tick_size(POLYMARKET, "t"), None);
    assert_eq!(store.get(POLYMARKET, "t").unwrap().tick_size, None);
    assert!(!store.get(POLYMARKET, "t").unwrap().stale);
    let ticket = store.begin_rest(POLYMARKET, "t");
    assert_eq!(store.seed_tick_size(&ticket, d("0.01")), None);
    assert_eq!(
        store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 200),
        BookUpdate::Rejected(BookReject::TimestampConflict)
    );
    let rest = store
        .accept_rest(&ticket, vec![], asks("4"), 300, now, None)
        .unwrap();
    assert_eq!(rest.tick_size, None);
    assert!(store
        .replace_snapshot(POLYMARKET, "t", vec![], asks("5"), 301, now)
        .is_applied());
    assert_eq!(store.tick_size(POLYMARKET, "t"), None);
    assert!(store
        .set_tick_size_at(POLYMARKET, "t", d("0.001"), 302)
        .is_applied());
    assert_eq!(store.tick_size(POLYMARKET, "t"), Some(d("0.001")));
    assert_eq!(
        store.get(POLYMARKET, "t").unwrap().tick_size,
        Some(d("0.001"))
    );
}

#[test]
fn snapshot_tick_is_prechecked_before_any_commit() {
    for rest in [false, true] {
        let now = Instant::now();
        let mut store = BookStore::default();
        store.replace_snapshot_with_tick(
            POLYMARKET,
            "t",
            vec![],
            asks("3"),
            100,
            now,
            Some(d("0.01")),
        );
        store.set_tick_size_at(POLYMARKET, "t", d("0.001"), 200);
        let ticket = store.begin_rest(POLYMARKET, "t");
        // 合法深度但同时间 tick 冲突：只撤销 tick 可信，不提交新盘口。
        if rest {
            assert_eq!(
                store
                    .accept_rest(&ticket, vec![], asks("4"), 200, now, Some(d("0.01")))
                    .unwrap_err(),
                BookReject::TimestampConflict
            );
        } else {
            assert_eq!(
                store.replace_snapshot_with_tick(
                    POLYMARKET,
                    "t",
                    vec![],
                    asks("4"),
                    200,
                    now,
                    Some(d("0.01"))
                ),
                BookUpdate::Rejected(BookReject::TimestampConflict)
            );
        }
        let book = store.get(POLYMARKET, "t").unwrap();
        assert_eq!(book.asks, asks("3"));
        assert_eq!(book.exchange_ts_ms, 100);
        assert_eq!(book.received_at, now);
        assert!(!book.stale);
        assert_eq!(book.tick_size, None);
        let ticket = store.begin_rest(POLYMARKET, "t");
        if rest {
            store
                .accept_rest(&ticket, vec![], asks("4"), 201, now, Some(d("0.001")))
                .unwrap();
        } else {
            assert!(store
                .replace_snapshot_with_tick(
                    POLYMARKET,
                    "t",
                    vec![],
                    asks("4"),
                    201,
                    now,
                    Some(d("0.001"))
                )
                .is_applied());
        }
        assert_eq!(store.tick_size(POLYMARKET, "t"), Some(d("0.001")));
    }
}

#[test]
fn rejected_rest_and_bad_ws_payload_do_not_commit_tick() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot_with_tick(
        POLYMARKET,
        "t",
        vec![],
        asks("3"),
        100,
        now,
        Some(d("0.01")),
    );
    let old = store.begin_rest(POLYMARKET, "t");
    store.set_tick_size_at(POLYMARKET, "t", d("0.001"), 200);
    let before = store.get(POLYMARKET, "t").unwrap().snapshot_json();
    assert_eq!(
        store
            .accept_rest(&old, vec![], asks("3"), 200, now, Some(d("0.01")))
            .unwrap_err(),
        BookReject::RevisionChanged
    );
    let ticket = store.begin_rest(POLYMARKET, "t");
    assert_eq!(
        store
            .accept_rest(&ticket, vec![], asks("-1"), 200, now, Some(d("0.01")))
            .unwrap_err(),
        BookReject::InvalidPayload
    );
    assert_eq!(store.get(POLYMARKET, "t").unwrap().snapshot_json(), before);
    assert_eq!(store.begin_rest(POLYMARKET, "t").revision, ticket.revision);
    assert_eq!(
        store.replace_snapshot_with_tick(
            POLYMARKET,
            "t",
            vec![],
            asks("-1"),
            300,
            now,
            Some(d("0.1"))
        ),
        BookUpdate::Rejected(BookReject::InvalidPayload)
    );
    assert_eq!(store.tick_size(POLYMARKET, "t"), Some(d("0.001")));
    // 无效 300 观察没有推进 tick 高水位。
    assert!(store
        .set_tick_size_at(POLYMARKET, "t", d("0.01"), 201)
        .is_applied());
}

#[test]
fn all_views_share_trusted_tick_and_seed_uses_ticket() {
    let now = Instant::now();
    let mut store = BookStore::default();
    let seed = store.begin_rest(POLYMARKET, "t");
    assert_eq!(store.seed_tick_size(&seed, d("0.01")), Some(d("0.01")));
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    let later = now + Duration::from_secs(4);
    let ticket = store.begin_rest(POLYMARKET, "t");
    store
        .accept_rest(&ticket, vec![], asks("4"), 200, later, Some(d("0.001")))
        .unwrap();
    for at in [now, now + Duration::from_secs(6)] {
        assert_eq!(
            store.get_at(POLYMARKET, "t", at).unwrap().tick_size,
            Some(d("0.001"))
        );
    }
    assert_eq!(store.seed_tick_size(&seed, d("0.1")), Some(d("0.001")));
    let missing = store.begin_rest(POLYMARKET, "missing");
    store.mark_platform_stale(POLYMARKET);
    assert_eq!(store.seed_tick_size(&missing, d("0.01")), None);
    let ticket = store.begin_rest(POLYMARKET, "missing");
    store.replace_snapshot(POLYMARKET, "missing", vec![], asks("3"), 100, now);
    assert_eq!(store.seed_tick_size(&ticket, d("0.01")), None);
}

#[test]
fn same_millisecond_deleted_level_requires_ws_full_snapshot_not_rest() {
    for stale in [false, true] {
        let now = Instant::now();
        let mut store = BookStore::default();
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
        assert!(store
            .apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("2"))], 100, now)
            .is_applied());
        let before = store.begin_rest(POLYMARKET, "t");
        assert!(store
            .apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 100, now)
            .is_applied());
        assert!(store.begin_rest(POLYMARKET, "t").revision > before.revision);
        if stale {
            store.mark_platform_stale(POLYMARKET);
            store.begin_platform_connection(POLYMARKET);
        }
        for ts in [99, 100] {
            if ts < 100 {
                assert_eq!(
                    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), ts, now),
                    BookUpdate::Rejected(BookReject::OlderTimestamp)
                );
            }
            let ticket = store.begin_rest(POLYMARKET, "t");
            assert!(store
                .accept_rest(&ticket, vec![], asks("3"), ts, now, None)
                .is_err());
            assert!(store.get_at(POLYMARKET, "t", now).unwrap().asks.is_empty());
        }
        assert!(store
            .replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now)
            .is_applied());
        assert!(!store.get_at(POLYMARKET, "t", now).unwrap().stale);
        store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 102, now);
        assert!(store.get_at(POLYMARKET, "t", now).unwrap().asks.is_empty());
    }
}

#[test]
fn pm_same_timestamp_full_snapshots_replace_both_sides_and_restore_stale() {
    for stale in [false, true] {
        let now = Instant::now();
        let later = now + Duration::from_secs(6);
        let mut store = BookStore::default();
        store.replace_snapshot(POLYMARKET, "t", asks("8"), asks("3"), 100, now);
        for (bids, incoming_asks) in [
            (asks("7"), asks("4")),
            (vec![], asks("4")),
            (vec![], vec![]),
        ] {
            if stale {
                store.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
            }
            let ticket = store.begin_rest(POLYMARKET, "t");
            assert_eq!(
                store.replace_snapshot(
                    POLYMARKET,
                    "t",
                    bids.clone(),
                    incoming_asks.clone(),
                    100,
                    later
                ),
                BookUpdate::Applied
            );
            let book = store.get(POLYMARKET, "t").unwrap();
            assert_eq!(book.bids, bids);
            assert_eq!(book.asks, incoming_asks);
            assert!(!book.stale);
            assert_eq!(book.received_at, later);
            assert_eq!(
                store
                    .accept_rest(&ticket, vec![], asks("9"), 200, later, None)
                    .unwrap_err(),
                BookReject::RevisionChanged
            );
            let ticket = store.begin_rest(POLYMARKET, "t");
            let duplicate_at = later + Duration::from_secs(6);
            assert_eq!(
                store.replace_snapshot(
                    POLYMARKET,
                    "t",
                    bids.clone(),
                    incoming_asks.clone(),
                    100,
                    duplicate_at
                ),
                BookUpdate::VerifiedUnchanged
            );
            assert_eq!(store.get(POLYMARKET, "t").unwrap().received_at, later);
            assert_eq!(
                store
                    .accept_rest(&ticket, vec![], asks("9"), 200, duplicate_at, None)
                    .unwrap_err(),
                BookReject::RevisionChanged
            );
            assert_eq!(
                store.replace_snapshot(POLYMARKET, "t", asks("9"), asks("9"), 99, duplicate_at),
                BookUpdate::Rejected(BookReject::OlderTimestamp)
            );
            assert_eq!(store.get(POLYMARKET, "t").unwrap().bids, bids);
            assert_eq!(store.get(POLYMARKET, "t").unwrap().asks, incoming_asks);
        }
    }
}

#[test]
fn pm_same_timestamp_ws_takeover_preserves_rest_delta_boundary() {
    for stale in [false, true] {
        let now = Instant::now();
        let mut store = BookStore::default();
        let ticket = store.begin_rest(POLYMARKET, "t");
        store
            .accept_rest(&ticket, asks("8"), asks("3"), 100, now, None)
            .unwrap();
        if stale {
            store.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
        }
        assert_eq!(
            store.replace_snapshot(POLYMARKET, "t", vec![], asks("4"), 100, now),
            BookUpdate::Applied
        );
        assert_eq!(
            store.get_with_source(POLYMARKET, "t").unwrap().1,
            BookSource::Ws
        );
        assert_eq!(
            store.sync.get(&ticket.key).unwrap().rest_boundary,
            Some(100)
        );
        let ticket = store.begin_rest(POLYMARKET, "t");
        assert_eq!(
            store
                .accept_rest(&ticket, vec![], asks("3"), 100, now, None)
                .unwrap_err(),
            BookReject::TimestampConflict
        );
        assert_eq!(
            store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("4"))], 100, now),
            BookUpdate::VerifiedUnchanged
        );
        assert_eq!(
            store.apply_levels(
                POLYMARKET,
                "t",
                &[(true, d("0.4"), d("2")), (false, d("0.5"), d("0"))],
                100,
                now
            ),
            BookUpdate::Rejected(BookReject::TimestampConflict)
        );
        let book = store.get(POLYMARKET, "t").unwrap();
        assert!(book.stale && book.bids.is_empty());
        assert_eq!(book.asks, asks("4"));
        assert_eq!(
            store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 101, now),
            BookUpdate::Applied
        );
        assert_eq!(store.sync.get(&ticket.key).unwrap().rest_boundary, None);
        assert_eq!(
            store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("2"))], 101, now),
            BookUpdate::Applied
        );
    }
}

#[test]
fn same_timestamp_ws_depth_replacement_still_prechecks_tick_atomically() {
    for stale in [false, true] {
        let now = Instant::now();
        let later = now + Duration::from_secs(6);
        let mut store = BookStore::default();
        store.replace_snapshot_with_tick(
            POLYMARKET,
            "t",
            asks("8"),
            asks("3"),
            100,
            now,
            Some(d("0.01")),
        );
        if stale {
            store.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
        }
        assert_eq!(
            store.replace_snapshot_with_tick(
                POLYMARKET,
                "t",
                vec![],
                asks("4"),
                100,
                later,
                Some(d("0.001"))
            ),
            BookUpdate::Rejected(BookReject::TimestampConflict)
        );
        let book = store.get(POLYMARKET, "t").unwrap();
        assert_eq!(book.bids, asks("8"));
        assert_eq!(book.asks, asks("3"));
        assert_eq!(book.received_at, now);
        assert_eq!(book.stale, stale);
        assert_eq!(book.tick_size, None);
        // 冲突 tick 的不可信状态不能被同时间戳全量夹带恢复。
        assert_eq!(
            store.replace_snapshot_with_tick(
                POLYMARKET,
                "t",
                vec![],
                asks("4"),
                100,
                later,
                Some(d("0.01"))
            ),
            BookUpdate::Rejected(BookReject::TimestampConflict)
        );
    }
}

#[test]
fn outcome_same_timestamp_depth_conflict_and_stale_recovery_rules_are_unchanged() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now);
    assert_eq!(
        store.replace_snapshot(OUTCOME, "t", vec![], asks("4"), 100, now),
        BookUpdate::Rejected(BookReject::TimestampConflict)
    );
    assert!(store.get(OUTCOME, "t").unwrap().stale);
    assert_eq!(store.get(OUTCOME, "t").unwrap().asks, asks("3"));
    assert_eq!(
        store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now),
        BookUpdate::Rejected(BookReject::TimestampConflict)
    );
    store.begin_platform_connection(OUTCOME);
    assert_eq!(
        store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now),
        BookUpdate::Applied
    );
}

#[test]
fn old_or_invalid_delta_requires_unambiguous_complete_snapshot() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    assert_eq!(
        store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("2"))], 99, now),
        BookUpdate::Rejected(BookReject::OlderTimestamp)
    );
    assert!(store.get_at(POLYMARKET, "t", now).unwrap().stale);
    assert!(store
        .replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now)
        .is_applied());
    assert!(store
        .replace_snapshot(POLYMARKET, "t", vec![], asks("2"), 101, now)
        .is_applied());
    assert_eq!(
        store.apply_levels(
            POLYMARKET,
            "t",
            &[(false, d("0.5"), d("-1"))],
            i64::MAX,
            now
        ),
        BookUpdate::Rejected(BookReject::InvalidPayload)
    );
    assert!(store
        .replace_snapshot(POLYMARKET, "t", vec![], asks("2"), 102, now)
        .is_applied());
}

#[test]
fn replayed_multi_update_batch_does_not_renew_ttl() {
    let now = Instant::now();
    let later = now + Duration::from_secs(6);
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    let updates = [(false, d("0.5"), d("0")), (false, d("0.5"), d("2"))];
    assert!(store
        .apply_levels(POLYMARKET, "t", &updates, 100, now)
        .is_applied());
    let revision = store.begin_rest(POLYMARKET, "t").revision;
    assert_eq!(
        store.apply_levels(POLYMARKET, "t", &updates, 100, later),
        BookUpdate::VerifiedUnchanged
    );
    assert!(store.begin_rest(POLYMARKET, "t").revision > revision);
    assert_eq!(
        store.get_at(POLYMARKET, "t", later).unwrap().received_at,
        now
    );
}

#[test]
fn rest_tickets_reject_all_local_competition_even_with_newer_timestamp() {
    for mutation in 0..6 {
        let now = Instant::now();
        let mut store = BookStore::default();
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
        let ticket = store.begin_rest(POLYMARKET, "t");
        match mutation {
            0 => {
                store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 100, now);
            }
            1 => {
                store.set_tick_size(POLYMARKET, "t", d("0.01"));
            }
            2 => store.mark_platform_stale(POLYMARKET),
            3 => store.begin_platform_connection(POLYMARKET),
            4 => {
                store
                    .accept_rest(&ticket, vec![], asks("3"), 100, now, None)
                    .unwrap();
            }
            _ => {
                store.replace_snapshot(POLYMARKET, "t", vec![], asks("4"), 101, now);
            }
        }
        let before = store.get_at(POLYMARKET, "t", now).unwrap().snapshot_json();
        assert!(
            store
                .accept_rest(&ticket, vec![], asks("99"), 1000, now, None)
                .is_err(),
            "mutation={mutation}"
        );
        assert_eq!(
            before,
            store.get_at(POLYMARKET, "t", now).unwrap().snapshot_json()
        );
    }
    for connect in [false, true] {
        let mut store = BookStore::default();
        let ticket = store.begin_rest(POLYMARKET, "unknown");
        if connect {
            store.begin_platform_connection(POLYMARKET);
        } else {
            store.mark_platform_stale(POLYMARKET);
        }
        assert_eq!(
            store
                .accept_rest(&ticket, vec![], asks("3"), 100, Instant::now(), None)
                .unwrap_err(),
            BookReject::EpochChanged
        );
    }
}

#[test]
fn pm_rest_recovers_and_continuous_deltas_keep_full_baseline() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", asks("8"), asks("9"), 90, now);
    store.mark_platform_stale(POLYMARKET);
    let ticket = store.begin_rest(POLYMARKET, "t");
    store
        .accept_rest(&ticket, vec![], asks("3"), 100, now, None)
        .unwrap();
    assert!(store.sync.get(&ticket.key).unwrap().rest.is_none());
    assert_eq!(
        store.get_with_source(POLYMARKET, "t").unwrap().1,
        BookSource::Rest
    );
    for ts in 101..105 {
        assert!(store
            .apply_levels(
                POLYMARKET,
                "t",
                &[(true, d("0.4"), Decimal::from(ts))],
                ts,
                now
            )
            .is_applied());
        let (book, source) = store.get_with_source(POLYMARKET, "t").unwrap();
        assert!(!book.stale);
        assert_eq!(book.asks, asks("3"));
        assert_eq!(book.bids.len(), 1);
        assert_eq!(source, BookSource::Ws);
    }
    // 旧 REST 副本不能否决已沿 WS 顺序演进后的同毫秒全量或 tick。
    let book = store.get(POLYMARKET, "t").unwrap().clone();
    assert_eq!(
        store.replace_snapshot(POLYMARKET, "t", book.bids, book.asks, 104, now),
        BookUpdate::VerifiedUnchanged
    );
    assert!(store
        .set_tick_size_at(POLYMARKET, "t", d("0.01"), 104)
        .is_applied());
    let ticket = store.begin_rest(POLYMARKET, "t");
    store
        .accept_rest(&ticket, vec![], vec![], 105, now, None)
        .unwrap();
    let book = store.get(POLYMARKET, "t").unwrap();
    assert!(!book.stale && book.bids.is_empty() && book.asks.is_empty());
    store.apply_levels(POLYMARKET, "t", &[(true, d("0.4"), d("1"))], 106, now);
    assert!(store.get(POLYMARKET, "t").unwrap().asks.is_empty());
}

#[test]
fn pm_noop_observations_reject_tickets_without_ttl_source_or_depth_changes() {
    let now = Instant::now();
    let later = now + Duration::from_secs(6);
    for observation in 0..4 {
        let mut store = BookStore::default();
        let initial = store.begin_rest(POLYMARKET, "t");
        store
            .accept_rest(&initial, vec![], asks("3"), 100, now, Some(d("0.01")))
            .unwrap();
        let ticket = store.begin_rest(POLYMARKET, "t");
        let update = match observation {
            0 => store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("3"))], 100, later),
            1 => store.apply_levels(POLYMARKET, "t", &[(true, d("0.4"), d("0"))], 100, later),
            2 => store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later),
            _ => store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 100),
        };
        assert_eq!(update, BookUpdate::VerifiedUnchanged);
        assert_eq!(
            store
                .accept_rest(&ticket, vec![], asks("4"), 200, later, None)
                .unwrap_err(),
            BookReject::RevisionChanged
        );
        let (book, source) = store.get_with_source_at(POLYMARKET, "t", later).unwrap();
        assert_eq!(source, BookSource::Rest);
        assert_eq!(book.received_at, now);
        assert!(!book.is_fresh(Duration::from_secs(5), later));
        assert_eq!(book.asks, asks("3"));
        // 任意 no-op 都不能解除 REST 的同毫秒跨源边界。
        assert_eq!(
            store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 100, later),
            BookUpdate::Rejected(BookReject::TimestampConflict)
        );
        assert!(store.get(POLYMARKET, "t").unwrap().stale);
        assert_eq!(store.get(POLYMARKET, "t").unwrap().asks, asks("3"));
        assert_eq!(
            store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later),
            BookUpdate::Applied
        );
        assert_eq!(store.get(POLYMARKET, "t").unwrap().received_at, later);
        assert_eq!(
            store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 100, later),
            BookUpdate::Rejected(BookReject::TimestampConflict)
        );
        store.begin_platform_connection(POLYMARKET);
        assert!(store
            .replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later)
            .is_applied());
    }
}

#[test]
fn pm_other_token_and_empty_delta_do_not_compete_but_old_delta_invalidates() {
    let now = Instant::now();
    let mut store = BookStore::default();
    let ticket = store.begin_rest(POLYMARKET, "t");
    store.replace_snapshot(POLYMARKET, "other", vec![], asks("2"), 200, now);
    store.apply_levels(POLYMARKET, "t", &[], 100, now);
    store
        .accept_rest(&ticket, vec![], asks("3"), 100, now, None)
        .unwrap();
    assert_eq!(
        store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("3"))], 99, now),
        BookUpdate::Rejected(BookReject::OlderTimestamp)
    );
    assert!(store.get(POLYMARKET, "t").unwrap().stale);
    assert_eq!(
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 99, now),
        BookUpdate::Rejected(BookReject::OlderTimestamp)
    );
}

#[test]
fn outcome_rest_is_independent_and_late_ws_cannot_use_it_as_baseline() {
    let now = Instant::now();
    let mut store = BookStore::default();
    let ticket = store.begin_rest(OUTCOME, "t");
    store
        .accept_rest(&ticket, vec![], asks("3"), 100, now, None)
        .unwrap();
    assert!(store
        .get_at(OUTCOME, "t", now)
        .unwrap()
        .is_fresh(Duration::from_secs(5), now));
    store.apply_levels(OUTCOME, "t", &[(true, d("0.4"), d("2"))], 100, now);
    let partial = store.get_at(OUTCOME, "t", now).unwrap();
    assert!(partial.stale);
    assert!(partial.asks.is_empty());
    assert!(!store
        .replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now)
        .is_applied());
    assert!(store
        .replace_snapshot(OUTCOME, "t", vec![], asks("4"), 101, now)
        .is_applied());
    assert!(!store.get_at(OUTCOME, "t", now).unwrap().stale);
}

#[test]
fn same_timestamp_delete_missing_from_ws_invalidates_rest_only_level() {
    let now = Instant::now();
    let mut store = BookStore::default();
    // WS 当前这一侧为空，REST 的未来观察包含新档。
    store.replace_snapshot(OUTCOME, "t", vec![], vec![], 100, now);
    let ticket = store.begin_rest(OUTCOME, "t");
    store
        .accept_rest(&ticket, vec![], asks("3"), 101, now, None)
        .unwrap();
    store.apply_levels(OUTCOME, "t", &[(false, d("0.5"), d("0"))], 101, now);
    assert!(store
        .get_at(OUTCOME, "t", now + Duration::from_secs(6))
        .unwrap()
        .asks
        .is_empty());
    let ticket = store.begin_rest(OUTCOME, "t");
    assert_eq!(
        store
            .accept_rest(&ticket, vec![], asks("3"), 101, now, None)
            .unwrap_err(),
        BookReject::TimestampConflict
    );
}

#[test]
fn fresh_ws_precedes_rest_then_rest_expires_and_changes_invalidate_it() {
    let now = Instant::now();
    let mut store = BookStore::default();
    store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now);
    let later = now + Duration::from_secs(4);
    let ticket = store.begin_rest(OUTCOME, "t");
    store
        .accept_rest(&ticket, vec![], asks("4"), 101, later, None)
        .unwrap();
    assert_eq!(store.get_at(OUTCOME, "t", later).unwrap().asks, asks("3"));
    assert_eq!(
        store
            .get_at(OUTCOME, "t", now + Duration::from_secs(6))
            .unwrap()
            .asks,
        asks("4")
    );
    assert!(!store
        .get_at(OUTCOME, "t", now + Duration::from_secs(10))
        .unwrap()
        .is_fresh(Duration::from_secs(5), now + Duration::from_secs(10)));
    store.apply_levels(OUTCOME, "t", &[(false, d("0.5"), d("0"))], 101, later);
    assert!(store.get_at(OUTCOME, "t", later).unwrap().asks.is_empty());
}

#[test]
fn source_selection_preserves_references_and_freshness_boundaries() {
    let now = Instant::now();
    let max_age = Duration::from_secs(5);
    let mut store = BookStore::new(max_age);
    let key = TokenBookKey::new(OUTCOME, "t");
    let assert_selected = |store: &BookStore, at, expected| {
        let (book, source) = store.get_with_source_at(OUTCOME, "t", at).unwrap();
        assert_eq!(source, expected);
        assert!(std::ptr::eq(book, store.get_at(OUTCOME, "t", at).unwrap()));
        let stored = match source {
            BookSource::Ws => store.books.get(&key).unwrap(),
            BookSource::Rest => store.sync.get(&key).unwrap().rest.as_ref().unwrap(),
        };
        assert!(std::ptr::eq(book, stored));
    };
    assert!(store.get_with_source_at(OUTCOME, "t", now).is_none());
    assert!(store.get_at(OUTCOME, "t", now).is_none());
    store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now);
    assert_selected(&store, now, BookSource::Ws);
    assert_selected(
        &store,
        now + max_age + Duration::from_nanos(1),
        BookSource::Ws,
    );
    let ticket = store.begin_rest(OUTCOME, "t");
    store
        .accept_rest(
            &ticket,
            vec![],
            asks("4"),
            101,
            now + Duration::from_secs(4),
            None,
        )
        .unwrap();
    for (age, source) in [
        (max_age, BookSource::Ws),
        (max_age + Duration::from_nanos(1), BookSource::Rest),
        (Duration::from_secs(9), BookSource::Rest),
        (
            Duration::from_secs(9) + Duration::from_nanos(1),
            BookSource::Rest,
        ),
    ] {
        assert_selected(&store, now + age, source);
    }
    // 增量推进版本后，旧 REST 不能遮住已过期的 WS。
    store.apply_levels(OUTCOME, "t", &[(false, d("0.5"), d("0"))], 101, now);
    assert_selected(&store, now + Duration::from_secs(10), BookSource::Ws);
    store.mark_platform_stale(OUTCOME);
    let ticket = store.begin_rest(OUTCOME, "t");
    store
        .accept_rest(&ticket, vec![], asks("4"), 102, now, None)
        .unwrap();
    assert_selected(&store, now, BookSource::Rest);
    store.begin_platform_connection(OUTCOME);
    assert_selected(&store, now, BookSource::Ws);
}

#[test]
fn source_getters_borrow_rest_only_and_invalid_ws_fallback() {
    let now = Instant::now();
    let mut store = BookStore::default();
    assert!(store.get_with_source(OUTCOME, "t").is_none());
    assert!(store.get(OUTCOME, "t").is_none());
    let ticket = store.begin_rest(OUTCOME, "t");
    store
        .accept_rest(
            &ticket,
            vec![],
            asks("3"),
            100,
            now - Duration::from_secs(60),
            None,
        )
        .unwrap();
    let (book, source) = store.get_with_source(OUTCOME, "t").unwrap();
    assert_eq!(source, BookSource::Rest);
    assert!(std::ptr::eq(book, store.get(OUTCOME, "t").unwrap()));
    assert!(!book.is_fresh(Duration::from_secs(5), now));
    store.mark_platform_stale(OUTCOME);
    assert!(store.get_with_source(OUTCOME, "t").is_none());
    store.replace_snapshot(OUTCOME, "t", vec![], asks("4"), 101, now);
    store.mark_platform_stale(OUTCOME);
    let (book, source) = store.get_with_source(OUTCOME, "t").unwrap();
    assert_eq!(source, BookSource::Ws);
    assert!(book.stale);
    assert!(std::ptr::eq(book, store.get(OUTCOME, "t").unwrap()));
    assert_eq!(BookSource::Ws.as_str(), "ws");
    assert_eq!(BookSource::Rest.as_str(), "rest");
}

#[test]
fn only_independent_rest_observations_or_new_epoch_initialization_renew_unchanged_ttl() {
    let now = Instant::now();
    let later = now + Duration::from_secs(6);
    let mut store = BookStore::default();
    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
    assert_eq!(
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later),
        BookUpdate::VerifiedUnchanged
    );
    assert_eq!(
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 101, later),
        BookUpdate::VerifiedUnchanged
    );
    store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("3"))], 102, later);
    assert_eq!(
        store.get_at(POLYMARKET, "t", later).unwrap().received_at,
        now
    );
    let ticket = store.begin_rest(POLYMARKET, "t");
    store
        .accept_rest(&ticket, vec![], asks("3"), 102, later, None)
        .unwrap();
    assert_eq!(
        store.get_at(POLYMARKET, "t", later).unwrap().received_at,
        later
    );
    store.mark_platform_stale(POLYMARKET);
    store.begin_platform_connection(POLYMARKET);
    assert!(store
        .replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 102, later)
        .is_applied());
}

#[test]
fn resync_rotates_attempts_despite_failures_missing_returns_and_subscription_changes() {
    let now = Instant::now();
    let mut store = BookStore::default();
    for i in 0..160 {
        store.index_token(POLYMARKET, &format!("{i:03}"), topic_key(i));
    }
    let first = store.stale_pm_tokens(Duration::from_secs(5), now, 80);
    // 第一轮全失败/无回包，下一轮也不能再次占据前半批。
    let second = store.stale_pm_tokens(Duration::from_secs(5), now + Duration::from_secs(10), 80);
    assert_eq!(first.len(), 80);
    assert_eq!(second.len(), 80);
    assert!(first.iter().all(|token| !second.contains(token)));
    assert_eq!(
        store.stale_pm_tokens(Duration::from_secs(5), now, 80),
        first
    );
    store.clear_topic_index();
    assert!(store
        .stale_pm_tokens(Duration::from_secs(5), now, 80)
        .is_empty());
    for (i, token) in ["001", "081", "161"].iter().enumerate() {
        store.index_token(POLYMARKET, token, topic_key(i as i32));
    }
    assert_eq!(
        store.stale_pm_tokens(Duration::from_secs(5), now, 2),
        vec!["081", "161"]
    );
    assert_eq!(
        store.stale_pm_tokens(Duration::from_secs(5), now, 2),
        vec!["001", "081"]
    );
    assert!(store
        .stale_pm_tokens(Duration::from_secs(5), now, 0)
        .is_empty());
}

#[test]
fn coalesces_dirty_topics() {
    let mut dirty = DirtyCoalescer::default();
    let topic = TopicKey {
        event_id: uuid::Uuid::nil(),
        unified_index: 0,
    };
    assert!(dirty.mark(topic).is_some());
    assert!(dirty.mark(topic).is_none());
    assert!(dirty.finish(topic).is_some());
    // finish 释放 computing，下一轮可以重新 mark 再算
    assert!(dirty.mark(topic).is_some());
    assert!(dirty.finish(topic).is_none());
}

#[test]
fn finish_without_pending_releases_lease() {
    let mut dirty = DirtyCoalescer::default();
    let topic = TopicKey {
        event_id: uuid::Uuid::nil(),
        unified_index: 0,
    };
    assert!(dirty.mark(topic).is_some());
    assert!(dirty.finish(topic).is_none());
    assert!(dirty.mark(topic).is_some());
}

fn topic_key(index: i32) -> TopicKey {
    TopicKey {
        event_id: uuid::Uuid::nil(),
        unified_index: index,
    }
}

#[test]
fn lists_stale_indexed_pm_tokens() {
    let mut store = BookStore::default();
    let now = Instant::now();
    store.index_token(POLYMARKET, "fresh", topic_key(0));
    store.index_token(POLYMARKET, "stale", topic_key(1));
    store.index_token(POLYMARKET, "missing", topic_key(2));
    store.index_token(OUTCOME, "#10", topic_key(0));
    assert!(store
        .replace_snapshot(
            POLYMARKET,
            "fresh",
            vec![],
            vec![Level {
                price: d("0.5"),
                size: d("8"),
            }],
            100,
            now,
        )
        .is_applied());
    assert!(store
        .replace_snapshot(
            POLYMARKET,
            "stale",
            vec![],
            vec![Level {
                price: d("0.4"),
                size: d("8"),
            }],
            100,
            now,
        )
        .is_applied());
    store.mark_platform_stale(POLYMARKET);
    // mark_platform_stale 把 fresh 也标过期了，重新写一份新鲜快照。
    assert!(store
        .replace_snapshot(
            POLYMARKET,
            "fresh",
            vec![],
            vec![Level {
                price: d("0.5"),
                size: d("8"),
            }],
            101,
            now,
        )
        .is_applied());
    let stale = store.stale_pm_tokens(Duration::from_secs(5), now, 80);
    assert_eq!(stale, vec!["missing".to_string(), "stale".to_string()]);
    let truncated = store.stale_pm_tokens(Duration::from_secs(5), now, 1);
    assert_eq!(truncated.len(), 1);
    assert_eq!(truncated[0], "missing");
}
