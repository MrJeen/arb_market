use super::*;

#[test]
fn escapes_telegram_markdown() {
    assert_eq!(escape_markdown(r"a_b*[c`](x)"), r"a\_b\*\[c\`](x)");
}

#[test]
fn place_notice_matches_success_and_failure_examples() {
    let mut notice = PlaceNotice {
        order_id: 8677,
        title: "Bitcoin Price Movement on September 17, 2026".into(),
        platforms: vec!["polymarket (rewards-45)".into(), "predictfun".into()],
        results: ["polymarket (rewards-45)", "predictfun"]
            .into_iter()
            .map(|platform| PlaceResult {
                platform: platform.into(),
                label: "yes".into(),
                market: "market".into(),
                status: PlaceStatus::Accepted,
                message: String::new(),
            })
            .collect(),
    };
    assert_eq!(
        format_place_notice("【crypto】", &notice),
        "🛒 【crypto】下单完成\n\
             📋 orderId: 8677\n\
             📋 title: Bitcoin Price Movement on September 17, 2026\n\
             🏪 polymarket (rewards-45), predictfun\n\
             ✅ 成功: 2"
    );

    notice.order_id = 353;
    notice.title = "Bitcoin price in September 2026".into();
    notice.platforms[1] = "outcome".into();
    notice.results[1] = PlaceResult {
        platform: "outcome".into(),
        label: "yes".into(),
        market: "#12170".into(),
        status: PlaceStatus::NoMatch,
        message: "Order could not immediately match against any resting orders. asset=100012170"
            .into(),
    };
    assert_eq!(
            format_place_notice("【new-crypto】", &notice),
            "🛒 【new-crypto】下单完成\n\
             📋 orderId: 353\n\
             📋 title: Bitcoin price in September 2026\n\
             🏪 polymarket (rewards-45), outcome\n\
             ✅ 成功: 1  ❌ 失败: 1\n\
             ❌ 失败详情:\n\
             - outcome label=yes market=#12170: Order could not immediately match against any resting orders. asset=100012170"
        );
}

#[test]
fn place_notice_escapes_title_and_errors() {
    let text = format_place_notice(
        "【market-arb】",
        &PlaceNotice {
            order_id: 27278,
            title: "La Liga: Real_Sociedad vs. Celta".into(),
            platforms: vec!["polymarket (rewards-11)".into(), "outcome".into()],
            results: vec![
                PlaceResult {
                    platform: "polymarket (rewards-11)".into(),
                    label: "yes".into(),
                    market: "111".into(),
                    status: PlaceStatus::Accepted,
                    message: String::new(),
                },
                PlaceResult {
                    platform: "outcome".into(),
                    label: "yes".into(),
                    market: "#12270".into(),
                    status: PlaceStatus::Rejected,
                    message: "HTTP 429 bad_request *reason* [detail] `code`".into(),
                },
            ],
        },
    );
    assert!(text.starts_with("🛒 【market-arb】下单完成"));
    assert!(text.contains("📋 orderId: 27278"));
    assert!(text.contains("Real\\_Sociedad"));
    assert!(text.contains("✅ 成功: 1  ❌ 失败: 1"));
    assert!(text.contains("❌ 失败详情:"));
    assert!(text.contains("polymarket (rewards-11)"));
    assert!(!text.contains("- polymarket"));
    assert!(text.contains("label=yes market=#12270: HTTP 429"));
    assert!(text.contains(r"bad\_request \*reason\* \[detail] \`code\`"));
    assert!(!text.contains("Real_Sociedad"));
}

#[test]
fn place_notice_blank_messages_have_category_fallbacks() {
    for (status, label) in [
        (PlaceStatus::Pending, "⏳ 待确认"),
        (PlaceStatus::NoMatch, "❌ 失败"),
        (PlaceStatus::Rejected, "❌ 失败"),
        (PlaceStatus::ExecutionError, "⚠️ 执行异常"),
    ] {
        for message in ["", " \n\t "] {
            let text = format_place_notice(
                "【cat】",
                &PlaceNotice {
                    order_id: 1,
                    title: "test".into(),
                    platforms: vec!["platform_name".into()],
                    results: vec![PlaceResult {
                        platform: "platform_name".into(),
                        label: "*yes*".into(),
                        market: "[market]".into(),
                        status,
                        message: message.into(),
                    }],
                },
            );
            assert!(text.contains(&format!("{label}: 1")));
            assert!(text.contains(status.fallback()));
            assert!(text.contains(r"platform\_name label=\*yes\* market=\[market]"));
        }
    }
}

#[test]
fn take_profit_notices_escape_title_and_include_amounts() {
    let trigger = format_take_profit_trigger_notice(
        "【cat】",
        &TakeProfitTriggerNotice {
            order_id: 42,
            title: "Team_A *wins*".into(),
            expected_gain: Decimal::new(125, 2),
        },
    );
    assert!(trigger.contains("止盈触发"));
    assert!(trigger.contains(r"Team\_A \*wins\*"));
    assert!(trigger.contains("expected gain: 1.25"));

    let completed = format_take_profit_completed_notice(
        "【cat】",
        &TakeProfitCompletedNotice {
            order_id: 42,
            title: "Team_A *wins*".into(),
            actual_profit: Decimal::new(11, 1),
            actual_cost: Decimal::new(25, 0),
        },
    );
    assert!(completed.contains("止盈完成"));
    assert!(completed.contains("actual profit: 1.1"));
    assert!(completed.contains("actual cost: 25"));
}

#[test]
fn settlement_notice_escapes_fields_and_supports_unavailable_profit() {
    let text = format_settlement_notice(
        "【cat】",
        &SettlementNotice {
            order_id: 7,
            title: "Event_[A]".into(),
            status: "settled_final".into(),
            actual_profit: Some(Decimal::new(-5, 1)),
        },
    );
    assert!(text.contains(r"Event\_\[A]"));
    assert!(text.contains(r"status: settled\_final"));
    assert!(text.contains("actual profit: -0.5"));

    let unavailable = format_settlement_notice(
        "【cat】",
        &SettlementNotice {
            order_id: 8,
            title: "Event B".into(),
            status: "unavailable".into(),
            actual_profit: None,
        },
    );
    assert!(!unavailable.contains("actual profit"));
}

#[test]
fn order_actuals_notice_contains_profit_and_cost() {
    assert_eq!(
        format_order_actuals_notice("【cat】", 8353, Decimal::new(-29, 2), Decimal::new(29, 2),),
        "【cat】订单 8353 入账收益 -0.29，入账成本 0.29（可能含标记的手续费估算）"
    );
}

#[test]
fn balance_insufficient_notice_contains_amounts_and_context() {
    let text = format_balance_insufficient_notice(
        "【market-arb】",
        "outcome",
        Decimal::new(125, 1),
        Decimal::new(20, 0),
        "arb topic=event_1:0",
    );
    assert!(text.contains("余额不足"));
    assert!(text.contains("platform: outcome"));
    assert!(text.contains(r"context: arb topic=event\_1:0"));
    assert!(text.contains("balance: 12.5"));
    assert!(text.contains("required: 20"));
}

#[test]
fn unknown_timeout_notice_lists_legs() {
    let text = format_unknown_timeout_notice(
        "【market-arb】",
        &[crate::store::ClosedLegRef {
            id: 9,
            order_id: 3,
            platform: "outcome".into(),
        }],
    );
    assert!(text.contains("交易腿超时未确认"));
    assert!(text.contains("orderId=3 legId=9 platform=outcome"));
}
