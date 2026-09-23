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
    pub status: PlaceStatus,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceStatus {
    Accepted,
    Pending,
    NoMatch,
    Rejected,
    ExecutionError,
}

impl PlaceStatus {
    fn fallback(self) -> &'static str {
        match self {
            Self::Accepted => "提交已受理，不代表已成交",
            Self::Pending => "提交结果待确认",
            Self::NoMatch => "订单未匹配成交",
            Self::Rejected => "提交被明确拒绝",
            Self::ExecutionError => "执行异常，提交结果需核实",
        }
    }
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
    // 成功表示提交已受理；待确认和执行异常不能直接判定为下单失败。
    let groups: [(&str, &str, &[PlaceStatus]); 4] = [
        ("✅ 成功", "", &[PlaceStatus::Accepted]),
        (
            "❌ 失败",
            "❌ 失败详情:",
            &[PlaceStatus::NoMatch, PlaceStatus::Rejected],
        ),
        ("⏳ 待确认", "⏳ 待确认详情:", &[PlaceStatus::Pending]),
        (
            "⚠️ 执行异常",
            "⚠️ 执行异常详情:",
            &[PlaceStatus::ExecutionError],
        ),
    ];
    let result_line = groups
        .iter()
        .filter_map(|(label, _, statuses)| {
            let count = notice
                .results
                .iter()
                .filter(|r| statuses.contains(&r.status))
                .count();
            (count > 0).then(|| format!("{label}: {count}"))
        })
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
    for (_, heading, statuses) in groups.iter().skip(1) {
        let mut results = notice
            .results
            .iter()
            .filter(|r| statuses.contains(&r.status))
            .peekable();
        if results.peek().is_none() {
            continue;
        }
        lines.push((*heading).into());
        lines.extend(results.map(|r| {
            let message = if r.message.trim().is_empty() {
                r.status.fallback()
            } else {
                &r.message
            };
            format!(
                "- {} label={} market={}: {}",
                escape_markdown(&r.platform),
                escape_markdown(&r.label),
                escape_markdown(&r.market),
                escape_markdown(&truncate_notify_line(message, 500))
            )
        }));
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
        "✅ {tag}止盈完成\n📋 orderId: {}\n📋 title: {}\n💰 actual profit: {}\n💰 actual cost: {}\nℹ️ 入账口径可能含标记的手续费估算",
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
        lines.push("ℹ️ 入账口径可能含标记的手续费估算".into());
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
        "{tag}订单 {order_id} 入账收益 {}，入账成本 {}（可能含标记的手续费估算）",
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
        format!("⚠️ {tag}交易腿超时未确认，需人工核对远端成交（新套利已暂停）"),
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
#[path = "../tests/unit/notify.rs"]
mod tests;
