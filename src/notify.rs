use crate::config::Config;
use chrono::{FixedOffset, Utc};
use serde::Serialize;

#[derive(Clone)]
pub struct NatsNotifier {
    client: async_nats::Client,
    subject: String,
    channel: String,
    tag: String,
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
}
