use super::*;
fn account() -> ParsedAccount {
    parse_account(&json!({"userSpotCrossRate":"0.0007", "activeReferralDiscount":"0.04"})).unwrap()
}
pub(super) fn meta() -> Value {
    json!({"feeScale":"1.0", "outcomes":[{"outcome":516,"venue":"out","quoteToken":"USDC","deployerFeeScale":"1.0","sideSpecs":[{"name":"Yes"},{"name":"No"}]}]})
}
fn publish(cache: &mut FeeCache, value: &Value) {
    let account = account();
    let markets = parse_markets(value, &account).unwrap();
    cache.publish(account, markets, Decimal::ZERO).unwrap();
}
#[test]
fn persisted_deadline_uses_both_sources_and_never_renews() {
    let mut cache = FeeCache::default();
    publish(&mut cache, &meta());
    let mut snapshot = cache.get(516).unwrap();
    let instant = Instant::now();
    let utc = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    for account_old in [true, false] {
        snapshot.account_time = SourceTime {
            monotonic: instant,
            utc,
        };
        snapshot.market_time = SourceTime {
            monotonic: instant,
            utc,
        };
        let old = if account_old {
            &mut snapshot.account_time
        } else {
            &mut snapshot.market_time
        };
        old.monotonic = instant - Duration::from_secs(100);
        old.utc = utc - chrono::Duration::seconds(100);
        let deadline = utc + chrono::Duration::seconds(800);
        assert_eq!(snapshot.valid_until_at(instant, utc), Some(deadline));
        assert_eq!(
            snapshot.valid_until_at(
                instant + Duration::from_secs(50),
                utc + chrono::Duration::seconds(50)
            ),
            Some(deadline)
        );
        assert_eq!(
            snapshot.valid_until_at(instant + Duration::from_secs(800), deadline),
            None
        );
        // Forward/backward wall-clock movement cannot extend monotonic TTL.
        assert!(
            snapshot
                .valid_until_at(instant, utc - chrono::Duration::seconds(50))
                .unwrap()
                <= deadline
        );
        assert_eq!(snapshot.valid_until_at(instant, deadline), None);
    }
}

#[test]
fn sample_scale_and_discount_boundaries() {
    for (scale, expected) in [("0", "0.000672"), ("1", "0.001344"), ("10", "0.01344")] {
        let mut value = meta();
        value["outcomes"][0]["deployerFeeScale"] = json!(scale);
        let mut cache = FeeCache::default();
        publish(&mut cache, &value);
        assert_eq!(
            cache.get(516).unwrap().taker_rate,
            Decimal::from_str_exact(expected).unwrap()
        );
    }
    for (discount, expected) in [("0", "0.0014"), ("1", "0")] {
        let account = parse_account(
            &json!({"userSpotCrossRate":"0.0007", "activeReferralDiscount":discount}),
        )
        .unwrap();
        let markets = parse_markets(&meta(), &account).unwrap();
        let mut cache = FeeCache::default();
        cache.publish(account, markets, Decimal::ZERO).unwrap();
        assert_eq!(
            cache.get(516).unwrap().taker_rate,
            Decimal::from_str_exact(expected).unwrap()
        );
    }
}
#[test]
fn invalid_account_and_market_inputs_fail_closed() {
    for field in ["userSpotCrossRate", "activeReferralDiscount"] {
        for bad in [
            json!(null),
            json!(true),
            json!({}),
            json!("NaN"),
            json!("-1"),
            json!("1e999"),
        ] {
            let mut value = json!({"userSpotCrossRate":"0.0007","activeReferralDiscount":"0.04"});
            value[field] = bad;
            assert!(parse_account(&value).is_err());
        }
    }
    assert!(
        parse_account(&json!({"userSpotCrossRate":"1", "activeReferralDiscount":"1.01"})).is_err()
    );
    for field in ["venue", "quoteToken", "deployerFeeScale", "sideSpecs"] {
        let mut value = meta();
        value["outcomes"][0][field] = Value::Null;
        let mut cache = FeeCache::default();
        publish(&mut cache, &value);
        assert!(cache.get(516).is_err());
    }
    for scale in [json!("-0.1"), json!("10.1"), json!(true), json!("NaN")] {
        let mut value = meta();
        value["outcomes"][0]["deployerFeeScale"] = scale;
        let mut cache = FeeCache::default();
        publish(&mut cache, &value);
        assert!(cache.get(516).is_err());
    }
    for top in [Value::Null, json!("2"), json!(true)] {
        let mut value = meta();
        value["feeScale"] = top;
        let mut cache = FeeCache::default();
        publish(&mut cache, &value);
        assert!(cache.get(516).is_err());
    }
    for base in ["1", "79228162514264337593543950335"] {
        let account =
            parse_account(&json!({"userSpotCrossRate":base,"activeReferralDiscount":"0"})).unwrap();
        assert!(parse_markets(&meta(), &account).unwrap().rules[&516].is_err());
    }
}
#[test]
fn identities_and_single_market_invalidation() {
    let mut value = meta();
    let mut second = value["outcomes"][0].clone();
    second["outcome"] = json!(517);
    value["outcomes"].as_array_mut().unwrap().push(second);
    let mut cache = FeeCache::default();
    publish(&mut cache, &value);
    let first = cache.get(516).unwrap();
    value["outcomes"][1]["sideSpecs"][0]["tokenId"] = json!("#5160");
    publish(&mut cache, &value);
    assert!(cache.get(517).is_err());
    assert!(first.same_rules(&cache.get(516).unwrap()));
    value["outcomes"][1]["outcome"] = json!(516);
    assert!(parse_markets(&value, &account()).is_err());
    value["outcomes"][1]["outcome"] = json!("517");
    assert!(parse_markets(&value, &account()).is_err());
}
#[test]
fn concurrent_readers_observe_one_atomic_generation() {
    let mut cache = FeeCache::default();
    publish(&mut cache, &meta());
    let cache = std::sync::Arc::new(std::sync::RwLock::new(cache));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let cache = cache.clone();
            std::thread::spawn(move || {
                for _ in 0..500 {
                    let snapshot = cache.read().unwrap().get(516).unwrap();
                    assert_eq!(
                        snapshot.taker_rate,
                        snapshot.account.base_rate
                            * (Decimal::ONE - snapshot.account.discount)
                            * (snapshot.market.scale + snapshot.market.scale.max(Decimal::ONE))
                    );
                    assert!(snapshot.is_fresh());
                }
            })
        })
        .collect();
    for i in 0..100 {
        let mut value = meta();
        value["outcomes"][0]["deployerFeeScale"] = json!(if i % 2 == 0 { "1" } else { "2" });
        let mut next = cache.read().unwrap().clone();
        publish(&mut next, &value);
        *cache.write().unwrap() = next;
    }
    for reader in readers {
        reader.join().unwrap();
    }
}

#[test]
fn account_rules_change_invalidates_even_when_derived_rate_is_same() {
    let mut cache = FeeCache::default();
    publish(&mut cache, &meta());
    let old = cache.get(516).unwrap();
    let changed =
        parse_account(&json!({"userSpotCrossRate":"0.001344", "activeReferralDiscount":"0.5"}))
            .unwrap();
    let markets = parse_markets(&meta(), &changed).unwrap();
    cache.publish(changed, markets, Decimal::ZERO).unwrap();
    let new = cache.get(516).unwrap();
    assert_eq!(old.taker_rate, new.taker_rate);
    assert!(!old.same_rules(&new));
    cache.expire_test();
    assert!(cache.get(516).is_err());
    publish(&mut cache, &meta());
    assert!(cache.get(516).unwrap().is_fresh());
}

#[test]
fn renewals_versions_and_both_source_expiry() {
    let mut cache = FeeCache::default();
    let mut value = meta();
    publish(&mut cache, &value);
    let old = cache.get(516).unwrap();
    publish(&mut cache, &value);
    let renewed = cache.get(516).unwrap();
    assert!(old.same_rules(&renewed));
    assert!(renewed.account_time.monotonic >= old.account_time.monotonic);
    value["outcomes"][0]["deployerFeeScale"] = json!("2");
    publish(&mut cache, &value);
    assert!(!old.same_rules(&cache.get(516).unwrap()));
    value["outcomes"][0]["deployerFeeScale"] = json!("1");
    publish(&mut cache, &value);
    assert!(!old.same_rules(&cache.get(516).unwrap()));
    let snapshot = cache.get(516).unwrap();
    for account_source in [true, false] {
        let mut stale = snapshot.clone();
        let source = if account_source {
            &mut stale.account_time
        } else {
            &mut stale.market_time
        };
        source.monotonic = Instant::now() - FEE_MAX_AGE;
        assert!(!stale.is_fresh());
    }
    let instant = Instant::now();
    let time = SourceTime {
        monotonic: instant,
        utc: Utc::now(),
    };
    assert!(time.fresh_at(instant + FEE_MAX_AGE - Duration::from_nanos(1)));
    assert!(!time.fresh_at(instant + FEE_MAX_AGE));
    assert_eq!(
        Decimal::from_str_exact(old.estimate_json()["taker_rate"].as_str().unwrap()).unwrap(),
        Decimal::new(1344, 6)
    );
}
