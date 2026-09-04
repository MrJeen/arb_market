use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::TopicKey;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenBookKey {
    pub platform: String,
    pub token_id: String,
}

impl TokenBookKey {
    pub fn new(platform: impl Into<String>, token_id: impl Into<String>) -> Self {
        Self {
            platform: platform.into(),
            token_id: token_id.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub platform: String,
    pub token_id: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub exchange_ts_ms: i64,
    pub received_at: Instant,
    pub stale: bool,
    /// Polymarket 最小价格档位。来自盘口字段、`tick_size_change`，或缺失时一次 REST `/tick-size`。
    pub tick_size: Option<Decimal>,
}

impl OrderBook {
    pub fn empty(platform: impl Into<String>, token_id: impl Into<String>) -> Self {
        Self {
            platform: platform.into(),
            token_id: token_id.into(),
            bids: Vec::new(),
            asks: Vec::new(),
            exchange_ts_ms: 0,
            received_at: Instant::now(),
            stale: true,
            tick_size: None,
        }
    }

    pub fn is_fresh(&self, max_age: Duration, now: Instant) -> bool {
        !self.stale && now.duration_since(self.received_at) <= max_age
    }
}

#[derive(Debug, Default)]
pub struct BookStore {
    books: HashMap<TokenBookKey, OrderBook>,
    token_topics: HashMap<TokenBookKey, HashSet<TopicKey>>,
}

impl BookStore {
    pub fn replace_snapshot(
        &mut self,
        platform: &str,
        token_id: &str,
        bids: Vec<Level>,
        asks: Vec<Level>,
        exchange_ts_ms: i64,
        now: Instant,
    ) -> bool {
        let key = TokenBookKey::new(platform, token_id);
        let prev_tick = self.books.get(&key).and_then(|book| book.tick_size);
        if let Some(existing) = self.books.get(&key) {
            if exchange_ts_ms < existing.exchange_ts_ms {
                return false;
            }
        }
        let mut bids = bids;
        let mut asks = asks;
        bids.sort_by(|a, b| b.price.cmp(&a.price));
        asks.sort_by(|a, b| a.price.cmp(&b.price));
        self.books.insert(
            key,
            OrderBook {
                platform: platform.to_string(),
                token_id: token_id.to_string(),
                bids,
                asks,
                exchange_ts_ms,
                received_at: now,
                stale: false,
                tick_size: prev_tick,
            },
        );
        true
    }

    pub fn apply_level(
        &mut self,
        platform: &str,
        token_id: &str,
        is_bid: bool,
        price: Decimal,
        size: Decimal,
        exchange_ts_ms: i64,
        now: Instant,
    ) -> bool {
        let key = TokenBookKey::new(platform, token_id);
        let book = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        if exchange_ts_ms < book.exchange_ts_ms {
            return false;
        }
        let levels = if is_bid {
            &mut book.bids
        } else {
            &mut book.asks
        };
        if let Some(idx) = levels.iter().position(|l| l.price == price) {
            if size.is_zero() {
                levels.remove(idx);
            } else {
                levels[idx].size = size;
            }
        } else if !size.is_zero() {
            levels.push(Level { price, size });
        }
        if is_bid {
            book.bids.sort_by(|a, b| b.price.cmp(&a.price));
        } else {
            book.asks.sort_by(|a, b| a.price.cmp(&b.price));
        }
        book.exchange_ts_ms = exchange_ts_ms;
        book.received_at = now;
        book.stale = false;
        true
    }

    pub fn set_tick_size(&mut self, platform: &str, token_id: &str, tick_size: Decimal) {
        if tick_size <= Decimal::ZERO {
            return;
        }
        let book = self
            .books
            .entry(TokenBookKey::new(platform, token_id))
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        book.tick_size = Some(tick_size);
    }

    pub fn mark_platform_stale(&mut self, platform: &str) {
        for book in self.books.values_mut() {
            if book.platform == platform {
                book.stale = true;
            }
        }
    }

    pub fn get(&self, platform: &str, token_id: &str) -> Option<&OrderBook> {
        self.books.get(&TokenBookKey::new(platform, token_id))
    }

    pub fn index_token(&mut self, platform: &str, token_id: &str, topic: TopicKey) {
        self.token_topics
            .entry(TokenBookKey::new(platform, token_id))
            .or_default()
            .insert(topic);
    }

    pub fn clear_topic_index(&mut self) {
        self.token_topics.clear();
    }

    pub fn topics_for(&self, platform: &str, token_id: &str) -> Vec<TopicKey> {
        self.token_topics
            .get(&TokenBookKey::new(platform, token_id))
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn desired_tokens(&self) -> (Vec<String>, Vec<String>) {
        let mut pm = Vec::new();
        let mut outcome = Vec::new();
        for key in self.token_topics.keys() {
            if key.platform == POLYMARKET {
                pm.push(key.token_id.clone());
            } else if key.platform == OUTCOME {
                outcome.push(key.token_id.clone());
            }
        }
        pm.sort();
        pm.dedup();
        outcome.sort();
        outcome.dedup();
        (pm, outcome)
    }

    pub fn stale_pm_tokens(&self, max_age: Duration, now: Instant, limit: usize) -> Vec<String> {
        let mut tokens: Vec<String> = self
            .token_topics
            .keys()
            .filter(|key| key.platform == POLYMARKET)
            .filter(|key| match self.books.get(*key) {
                Some(book) => !book.is_fresh(max_age, now),
                None => true,
            })
            .map(|key| key.token_id.clone())
            .collect();
        tokens.sort();
        tokens.dedup();
        tokens.truncate(limit);
        tokens
    }
}

#[derive(Debug, Default)]
pub struct DirtyCoalescer {
    computing: HashSet<TopicKey>,
    pending: HashSet<TopicKey>,
}

impl DirtyCoalescer {
    pub fn mark(&mut self, topic: TopicKey) -> Option<TopicKey> {
        if self.computing.contains(&topic) {
            self.pending.insert(topic);
            None
        } else {
            self.computing.insert(topic);
            Some(topic)
        }
    }

    pub fn finish(&mut self, topic: TopicKey) -> Option<TopicKey> {
        self.computing.remove(&topic);
        if self.pending.remove(&topic) {
            self.computing.insert(topic);
            Some(topic)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn replaces_snapshot_and_rejects_older() {
        let mut store = BookStore::default();
        let now = Instant::now();
        assert!(store.replace_snapshot(
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
        ));
        assert!(!store.replace_snapshot(POLYMARKET, "t1", vec![], vec![], 90, now,));
        let book = store.get(POLYMARKET, "t1").unwrap();
        assert_eq!(book.asks[0].price, d("0.5"));
        assert!(!book.stale);
    }

    #[test]
    fn keeps_tick_size_across_snapshots() {
        let mut store = BookStore::default();
        let now = Instant::now();
        store.set_tick_size(POLYMARKET, "t1", d("0.001"));
        assert!(store.replace_snapshot(
            POLYMARKET,
            "t1",
            vec![Level {
                price: d("0.40"),
                size: d("10"),
            }],
            vec![],
            1,
            now,
        ));
        assert_eq!(
            store.get(POLYMARKET, "t1").unwrap().tick_size,
            Some(d("0.001"))
        );
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
        assert!(dirty.finish(topic).is_none());
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
        assert!(store.replace_snapshot(
            POLYMARKET,
            "fresh",
            vec![],
            vec![Level {
                price: d("0.5"),
                size: d("8"),
            }],
            100,
            now,
        ));
        assert!(store.replace_snapshot(
            POLYMARKET,
            "stale",
            vec![],
            vec![Level {
                price: d("0.4"),
                size: d("8"),
            }],
            100,
            now,
        ));
        store.mark_platform_stale(POLYMARKET);
        // mark_platform_stale 把 fresh 也标过期了，重新写一份新鲜快照。
        assert!(store.replace_snapshot(
            POLYMARKET,
            "fresh",
            vec![],
            vec![Level {
                price: d("0.5"),
                size: d("8"),
            }],
            101,
            now,
        ));
        let stale = store.stale_pm_tokens(Duration::from_secs(5), now, 80);
        assert_eq!(stale, vec!["missing".to_string(), "stale".to_string()]);
        let truncated = store.stale_pm_tokens(Duration::from_secs(5), now, 1);
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0], "missing");
    }
}
