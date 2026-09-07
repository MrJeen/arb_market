use crate::config::Config;
use chrono::{FixedOffset, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BALANCE_ALERT_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct NatsNotifier {
    client: async_nats::Client,
    subject: String,
    channel: String,
    tag: String,
    balance_alerts: Arc<Mutex<HashMap<String, Instant>>>,
}

#[derive(Debug, Serialize)]
struct TgNotificationPayload {
    channel: String,
    text: String,
}

#[derive(Debug, Clone)]
pub struct PlaceNotice {
    pub order_id: i64,
    pub title: String,
    pub platforms: Vec<String>,
    pub results: Vec<PlaceResult>,
}

#[derive(Debug, Clone)]
pub struct PlaceResult {
    pub platform: String,
    pub label: String,
    pub market: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TakeProfitTriggerNotice {
    pub order_id: i64,
    pub title: String,
    pub expected_gain: Decimal,
}

#[derive(Debug, Clone)]
pub struct TakeProfitCompletedNotice {
    pub order_id: i64,
    pub title: String,
    pub actual_profit: Decimal,
    pub actual_cost: Decimal,
}

#[derive(Debug, Clone)]
pub struct SettlementNotice {
    pub order_id: i64,
    pub title: String,
    pub status: String,
    pub actual_profit: Option<Decimal>,
}

/// Telegram 旧版 Markdown（parse_mode=Markdown）只需转义 `_ * ` `[`。
/// `] ( ) \` 不是独立语法，转义后会显示成 `\(`，所以不转义。
pub fn escape_markdown(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '_' | '*' | '`' | '[') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

pub fn truncate_notify_line(value: &str, max_length: usize) -> String {
    let text: String = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.len() <= max_length {
        return text;
    }
    let keep = max_length.saturating_sub(3);
    let mut end = keep;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

pub fn format_notify_tag(cat: &str) -> String {
    let label = cat.trim();
    let label = if label.is_empty() {
        "market-arb"
    } else {
        label
    };
    format!("【{}】", escape_markdown(label))
}

pub fn format_place_notice(tag: &str, notice: &PlaceNotice) -> String {
    let success = notice.results.iter().filter(|r| r.error.is_none()).count();
    let fail = notice.results.len().saturating_sub(success);
    let result_line = [
        (success > 0).then(|| format!("✅ 成功: {success}")),
        (fail > 0).then(|| format!("❌ 失败: {fail}")),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("  ");
    let mut lines = vec![
        format!("🛒 {tag}下单完成"),
        format!("📋 orderId: {}", notice.order_id),
        format!("📋 title: {}", escape_markdown(&notice.title)),
        format!(
            "🏪 {}",
            notice
                .platforms
                .iter()
                .map(|p| escape_markdown(p))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    if !result_line.is_empty() {
        lines.push(result_line);
    }
    let failures: Vec<String> = notice
        .results
        .iter()
        .filter_map(|r| {
            let err = r.error.as_deref()?;
            Some(format!(
                "- {} label={} market={}: {}",
                escape_markdown(&r.platform),
                escape_markdown(&r.label),
                escape_markdown(&r.market),
                escape_markdown(&truncate_notify_line(err, 500))
            ))
        })
        .collect();
    if !failures.is_empty() {
        lines.push("❌ 失败详情:".into());
        lines.extend(failures);
    }
    lines.join("\n")
}

pub fn format_take_profit_trigger_notice(tag: &str, notice: &TakeProfitTriggerNotice) -> String {
    format!(
        "📈 {tag}止盈触发\n📋 orderId: {}\n📋 title: {}\n💰 expected gain: {}",
        notice.order_id,
        escape_markdown(&notice.title),
        notice.expected_gain.normalize(),
    )
}

pub fn format_take_profit_completed_notice(
    tag: &str,
    notice: &TakeProfitCompletedNotice,
) -> String {
    format!(
        "✅ {tag}止盈完成\n📋 orderId: {}\n📋 title: {}\n💰 actual profit: {}\n💰 actual cost: {}",
        notice.order_id,
        escape_markdown(&notice.title),
        notice.actual_profit.normalize(),
        notice.actual_cost.normalize(),
    )
}

pub fn format_settlement_notice(tag: &str, notice: &SettlementNotice) -> String {
    let mut lines = vec![
        format!("🏁 {tag}结算更新"),
        format!("📋 orderId: {}", notice.order_id),
        format!("📋 title: {}", escape_markdown(&notice.title)),
        format!("📋 status: {}", escape_markdown(&notice.status)),
    ];
    if let Some(actual_profit) = notice.actual_profit {
        lines.push(format!("💰 actual profit: {}", actual_profit.normalize()));
    }
    lines.join("\n")
}

pub fn format_order_actuals_notice(
    tag: &str,
    order_id: i64,
    actual_profit: Decimal,
    actual_cost: Decimal,
) -> String {
    format!(
        "{tag}订单 {order_id} 实际收益 {}，实际成本 {}",
        actual_profit.normalize(),
        actual_cost.normalize(),
    )
}

pub fn format_balance_insufficient_notice(
    tag: &str,
    platform: &str,
    balance: Decimal,
    required: Decimal,
    context: &str,
) -> String {
    format!(
        "⚠️ {tag}余额不足\n🏪 platform: {}\n📋 context: {}\n💰 balance: {}\n💰 required: {}",
        escape_markdown(platform),
        escape_markdown(context),
        balance.normalize(),
        required.normalize(),
    )
}

pub fn format_unknown_timeout_notice(tag: &str, legs: &[crate::store::ClosedLegRef]) -> String {
    let mut lines = vec![
        format!("⚠️ {tag}unknown 腿超时无成交，已标 cancelled"),
        format!("📋 count: {}", legs.len()),
    ];
    for leg in legs.iter().take(8) {
        lines.push(format!(
            "- orderId={} legId={} platform={}",
            leg.order_id,
            leg.id,
            escape_markdown(&leg.platform)
        ));
    }
    lines.join("\n")
}

pub fn format_platform_label(platform: &str, service: Option<&str>) -> String {
    match service.map(str::trim).filter(|s| !s.is_empty()) {
        Some(service) => format!("{platform} ({service})"),
        None => platform.to_string(),
    }
}

pub async fn connect(cfg: &Config) -> Option<NatsNotifier> {
    let url = cfg.nats_url.as_deref()?;
    let connect = async {
        if let Some(token) = cfg.nats_token.clone() {
            async_nats::ConnectOptions::with_token(token)
                .connect(url)
                .await
        } else {
            async_nats::connect(url).await
        }
    };
    match tokio::time::timeout(std::time::Duration::from_secs(5), connect).await {
        Ok(Ok(client)) => {
            tracing::info!(url, subject = %cfg.nats_subject, "nats connected");
            Some(NatsNotifier {
                client,
                subject: cfg.nats_subject.clone(),
                channel: cfg.nats_channel.clone(),
                tag: format_notify_tag(&cfg.cat),
                balance_alerts: Arc::new(Mutex::new(HashMap::new())),
            })
        }
        Ok(Err(err)) => {
            tracing::error!(error = %err, url, "nats connect failed");
            None
        }
        Err(_) => {
            tracing::error!(url, "nats connect timed out");
            None
        }
    }
}

impl NatsNotifier {
    pub fn publish_place(&self, notice: PlaceNotice) {
        self.publish(format_place_notice(&self.tag, &notice));
    }

    pub fn publish_alert(&self, body: String) {
        self.publish(body);
    }

    pub fn publish_take_profit_trigger(&self, notice: TakeProfitTriggerNotice) {
        self.publish(format_take_profit_trigger_notice(&self.tag, &notice));
    }

    pub fn publish_take_profit_completed(&self, notice: TakeProfitCompletedNotice) {
        self.publish(format_take_profit_completed_notice(&self.tag, &notice));
    }

    pub fn publish_settlement(&self, notice: SettlementNotice) {
        self.publish(format_settlement_notice(&self.tag, &notice));
    }

    pub fn publish_order_actuals(
        &self,
        order_id: i64,
        actual_profit: Decimal,
        actual_cost: Decimal,
    ) {
        self.publish(format_order_actuals_notice(
            &self.tag,
            order_id,
            actual_profit,
            actual_cost,
        ));
    }

    pub fn publish_balance_insufficient(
        &self,
        platform: &str,
        balance: Decimal,
        required: Decimal,
        context: &str,
    ) {
        let key = format!("{platform}:{context}");
        let now = Instant::now();
        let should_publish = {
            let mut alerts = self
                .balance_alerts
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            alerts.retain(|_, sent_at| {
                now.saturating_duration_since(*sent_at) < BALANCE_ALERT_COOLDOWN
            });
            match alerts.get(&key) {
                Some(sent_at)
                    if now.saturating_duration_since(*sent_at) < BALANCE_ALERT_COOLDOWN =>
                {
                    false
                }
                _ => {
                    alerts.insert(key, now);
                    true
                }
            }
        };
        if should_publish {
            self.publish(format_balance_insufficient_notice(
                &self.tag, platform, balance, required, context,
            ));
        }
    }

    fn publish(&self, body: String) {
        if body.is_empty() {
            return;
        }
        let utc8 = FixedOffset::east_opt(8 * 3600).expect("UTC+8 offset");
        let stamped = format!(
            "⏱ {}\n{body}",
            Utc::now()
                .with_timezone(&utc8)
                .format("%Y-%m-%d %H:%M:%S%.3f")
        );
        let payload = TgNotificationPayload {
            channel: self.channel.clone(),
            text: stamped,
        };
        let bytes = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(err) => {
                tracing::error!(error = %err, "tg notification json encode failed");
                return;
            }
        };
        let client = self.client.clone();
        let subject = self.subject.clone();
        let channel = payload.channel.clone();
        let payload_bytes = bytes.len();
        tokio::spawn(async move {
            if let Err(err) = client.publish(subject.clone(), bytes.into()).await {
                tracing::error!(
                    %subject,
                    %channel,
                    payload_bytes,
                    error = %err,
                    "tg notification nats publish failed"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_telegram_markdown() {
        assert_eq!(escape_markdown(r"a_b*[c`](x)"), r"a\_b\*\[c\`](x)");
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
                        error: None,
                    },
                    PlaceResult {
                        platform: "outcome".into(),
                        label: "yes".into(),
                        market: "#12270".into(),
                        error: Some("HTTP 429 ERRBADREQUEST { error: 'toomanyrequests' }".into()),
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
        assert!(text.contains("label=yes market=#12270: HTTP 429"));
        assert!(!text.contains("Real_Sociedad"));
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
            "【cat】订单 8353 实际收益 -0.29，实际成本 0.29"
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
        assert!(text.contains("unknown 腿超时无成交"));
        assert!(text.contains("orderId=3 legId=9 platform=outcome"));
    }
}
