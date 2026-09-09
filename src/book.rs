use crate::config::{OUTCOME, POLYMARKET};
use crate::domain::TopicKey;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn best_bid(&self) -> Option<Decimal> {
        best_bid_px(&self.bids)
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        best_ask_px(&self.asks)
    }

    pub fn snapshot_json(&self) -> Value {
        serde_json::json!({
            "platform": self.platform,
            "token_id": self.token_id,
            "bids": self.bids,
            "asks": self.asks,
            "exchange_ts_ms": self.exchange_ts_ms,
            "tick_size": self.tick_size,
            "stale": self.stale,
        })
    }
}

pub fn best_bid_px(bids: &[Level]) -> Option<Decimal> {
    bids.iter()
        .filter(|level| level.price > Decimal::ZERO)
        .map(|level| level.price)
        .max()
}

pub fn best_ask_px(asks: &[Level]) -> Option<Decimal> {
    asks.iter()
        .filter(|level| level.price > Decimal::ZERO)
        .map(|level| level.price)
        .min()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookReject {
    InvalidPayload,
    OlderTimestamp,
    TimestampConflict,
    EpochChanged,
    RevisionChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookUpdate {
    Applied,
    VerifiedUnchanged,
    Rejected(BookReject),
}

impl BookUpdate {
    pub fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

#[derive(Debug, Clone)]
pub struct RestTicket {
    pub key: TokenBookKey,
    pub epoch: u64,
    pub revision: u64,
}

#[derive(Debug, Default)]
struct SyncState {
    revision: u64,
    ws_epoch: Option<u64>,
    // 候选失效后仍保留最后全量高水位，防止断线或删除后旧档复活。
    rest: Option<OrderBook>,
    rest_version: Option<(u64, u64)>,
}

#[derive(Debug)]
pub struct BookStore {
    // 这里只存 WS 基线；REST 永不成为后续 WS 增量的底本。
    books: HashMap<TokenBookKey, OrderBook>,
    sync: HashMap<TokenBookKey, SyncState>,
    epochs: HashMap<String, u64>,
    token_topics: HashMap<TokenBookKey, HashSet<TopicKey>>,
    max_age: Duration,
    resync_cursor: Option<String>,
}

impl Default for BookStore {
    fn default() -> Self {
        Self::new(Duration::from_secs(5))
    }
}

impl BookStore {
    pub fn new(max_age: Duration) -> Self {
        Self {
            books: HashMap::new(),
            sync: HashMap::new(),
            epochs: HashMap::new(),
            token_topics: HashMap::new(),
            max_age,
            resync_cursor: None,
        }
    }

    pub fn begin_rest(&self, platform: &str, token_id: &str) -> RestTicket {
        let key = TokenBookKey::new(platform, token_id);
        RestTicket {
            epoch: self.epochs.get(platform).copied().unwrap_or(0),
            revision: self.sync.get(&key).map_or(0, |state| state.revision),
            key,
        }
    }

    fn changed(&mut self, key: &TokenBookKey) {
        let state = self.sync.entry(key.clone()).or_default();
        state.revision += 1;
        state.rest_version = None;
    }

    fn snapshot_conflict(
        &self,
        key: &TokenBookKey,
        bids: &[Level],
        asks: &[Level],
        ts: i64,
    ) -> Result<(), BookReject> {
        for prior in self
            .books
            .get(key)
            .into_iter()
            .chain(self.sync.get(key).and_then(|state| state.rest.as_ref()))
        {
            if ts < prior.exchange_ts_ms {
                return Err(BookReject::OlderTimestamp);
            }
            if ts == prior.exchange_ts_ms && (bids != prior.bids || asks != prior.asks) {
                return Err(BookReject::TimestampConflict);
            }
        }
        Ok(())
    }

    pub fn replace_snapshot(
        &mut self,
        platform: &str,
        token_id: &str,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        exchange_ts_ms: i64,
        now: Instant,
    ) -> BookUpdate {
        let key = TokenBookKey::new(platform, token_id);
        if token_id.is_empty()
            || exchange_ts_ms <= 0
            || !valid_levels(&bids)
            || !valid_levels(&asks)
        {
            return self.invalidate_ws(platform, token_id, BookReject::InvalidPayload);
        }
        sort_levels(&mut bids, &mut asks);
        if let Err(reason) = self.snapshot_conflict(&key, &bids, &asks, exchange_ts_ms) {
            // 同毫秒不同内容无法证明哪本完整；保留删除但撤销 WS 完整性。
            // REST CAS 拒绝不走此入口，不能损坏正常 WS。
            if reason == BookReject::TimestampConflict {
                return self.invalidate_ws(platform, token_id, reason);
            }
            return BookUpdate::Rejected(reason);
        }
        let old = self.books.get(&key);
        let epoch = self.epochs.get(platform).copied().unwrap_or(0);
        if old.is_some_and(|book| book.stale && book.exchange_ts_ms == exchange_ts_ms)
            && self
                .sync
                .get(&key)
                .is_some_and(|state| state.ws_epoch == Some(epoch))
        {
            return BookUpdate::Rejected(BookReject::TimestampConflict);
        }
        if old.is_some_and(|book| !book.stale && book.bids == bids && book.asks == asks) {
            if old.unwrap().exchange_ts_ms != exchange_ts_ms {
                self.books.get_mut(&key).unwrap().exchange_ts_ms = exchange_ts_ms;
                self.changed(&key);
            }
            return BookUpdate::VerifiedUnchanged;
        }
        let restored = old.is_none_or(|book| book.stale);
        let tick_size = self.get(platform, token_id).and_then(|book| book.tick_size);
        self.books.insert(
            key.clone(),
            OrderBook {
                platform: platform.into(),
                token_id: token_id.into(),
                bids,
                asks,
                exchange_ts_ms,
                received_at: now,
                stale: false,
                tick_size,
            },
        );
        self.changed(&key);
        let state = self.sync.get_mut(&key).unwrap();
        state.ws_epoch = Some(epoch);
        if restored {
            tracing::info!(
                platform,
                token = token_id,
                epoch,
                revision = state.revision,
                "book WS completeness restored"
            );
        }
        BookUpdate::Applied
    }

    /// 同 token 批内顺序保留；毫秒时间戳不是事件 ID。旧消息使基线失去完整性。
    pub fn apply_levels(
        &mut self,
        platform: &str,
        token_id: &str,
        updates: &[(bool, Decimal, Decimal)],
        exchange_ts_ms: i64,
        now: Instant,
    ) -> BookUpdate {
        if updates.is_empty() {
            return BookUpdate::VerifiedUnchanged;
        }
        if token_id.is_empty()
            || exchange_ts_ms <= 0
            || updates.iter().any(|(_, price, size)| {
                *price <= Decimal::ZERO || *price > Decimal::ONE || *size < Decimal::ZERO
            })
        {
            return self.invalidate_ws(platform, token_id, BookReject::InvalidPayload);
        }
        let key = TokenBookKey::new(platform, token_id);
        let high = self
            .books
            .get(&key)
            .into_iter()
            .chain(self.sync.get(&key).and_then(|state| state.rest.as_ref()))
            .map(|book| book.exchange_ts_ms)
            .max()
            .unwrap_or(0);
        if exchange_ts_ms < high {
            return self.invalidate_ws(platform, token_id, BookReject::OlderTimestamp);
        }
        // 即便删除的价位不在 WS 基线中，也可能仍在独立 REST 候选中。
        // 此观察不能续 WS TTL，但必须使候选和在途 REST 票据失效。
        let rest_observed = self
            .sync
            .get(&key)
            .is_some_and(|state| state.rest_version.is_some());
        let book = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        let mut changed = false;
        // 同价位只提交批内最后值；保持接收顺序语义，同时重复整批不因中间值续 TTL。
        let mut final_updates = HashMap::new();
        for &(is_bid, price, size) in updates {
            final_updates.insert((is_bid, price), size);
        }
        for ((is_bid, price), size) in final_updates {
            let levels = if is_bid {
                &mut book.bids
            } else {
                &mut book.asks
            };
            if let Some(idx) = levels.iter().position(|level| level.price == price) {
                if size.is_zero() {
                    levels.remove(idx);
                    changed = true;
                } else if levels[idx].size != size {
                    levels[idx].size = size;
                    changed = true;
                }
            } else if !size.is_zero() {
                levels.push(Level { price, size });
                changed = true;
            }
        }
        // 无变化包不续 TTL；但严格更高的已观察时间仍须保护后续全量覆盖。
        let advanced = exchange_ts_ms > book.exchange_ts_ms;
        book.exchange_ts_ms = exchange_ts_ms;
        if changed {
            sort_levels(&mut book.bids, &mut book.asks);
            book.received_at = now;
        }
        if changed || advanced || rest_observed {
            self.changed(&key);
        }
        if changed {
            BookUpdate::Applied
        } else {
            BookUpdate::VerifiedUnchanged
        }
    }

    pub fn set_tick_size(
        &mut self,
        platform: &str,
        token_id: &str,
        tick_size: Decimal,
    ) -> BookUpdate {
        if tick_size <= Decimal::ZERO || tick_size > Decimal::ONE {
            return BookUpdate::Rejected(BookReject::InvalidPayload);
        }
        let key = TokenBookKey::new(platform, token_id);
        let book = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        if book.tick_size == Some(tick_size) {
            return BookUpdate::VerifiedUnchanged;
        }
        book.tick_size = Some(tick_size);
        self.changed(&key);
        BookUpdate::Applied
    }

    pub fn invalidate_ws(
        &mut self,
        platform: &str,
        token_id: &str,
        reason: BookReject,
    ) -> BookUpdate {
        let key = TokenBookKey::new(platform, token_id);
        let book = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        if !book.stale {
            tracing::warn!(
                platform,
                token = token_id,
                ?reason,
                "book WS completeness lost"
            );
        }
        book.stale = true;
        self.changed(&key);
        BookUpdate::Rejected(reason)
    }

    pub fn begin_platform_connection(&mut self, platform: &str) {
        self.mark_platform_stale(platform);
    }

    pub fn mark_platform_stale(&mut self, platform: &str) {
        let epoch = self.epochs.entry(platform.into()).or_default();
        *epoch += 1;
        tracing::info!(platform, epoch = *epoch, "book connection epoch changed");
        for (key, book) in &mut self.books {
            if key.platform == platform {
                book.stale = true;
            }
        }
        for (key, state) in &mut self.sync {
            if key.platform == platform {
                state.revision += 1;
                state.rest_version = None;
            }
        }
    }

    pub fn accept_rest(
        &mut self,
        ticket: &RestTicket,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        exchange_ts_ms: i64,
        received_at: Instant,
        tick: Option<Decimal>,
    ) -> Result<OrderBook, BookReject> {
        let result = (|| {
            let current = self.begin_rest(&ticket.key.platform, &ticket.key.token_id);
            if current.epoch != ticket.epoch {
                return Err(BookReject::EpochChanged);
            }
            if current.revision != ticket.revision {
                return Err(BookReject::RevisionChanged);
            }
            if ticket.key.token_id.is_empty()
                || exchange_ts_ms <= 0
                || !valid_levels(&bids)
                || !valid_levels(&asks)
                || tick.is_some_and(|v| v <= Decimal::ZERO || v > Decimal::ONE)
            {
                return Err(BookReject::InvalidPayload);
            }
            sort_levels(&mut bids, &mut asks);
            self.snapshot_conflict(&ticket.key, &bids, &asks, exchange_ts_ms)?;
            let tick_size = tick.or_else(|| {
                self.get(&ticket.key.platform, &ticket.key.token_id)
                    .and_then(|b| b.tick_size)
            });
            let book = OrderBook {
                platform: ticket.key.platform.clone(),
                token_id: ticket.key.token_id.clone(),
                bids,
                asks,
                exchange_ts_ms,
                received_at,
                stale: false,
                tick_size,
            };
            if let Some(tick) = tick {
                if let Some(ws) = self.books.get_mut(&ticket.key) {
                    ws.tick_size = Some(tick);
                }
            }
            self.changed(&ticket.key);
            let state = self.sync.get_mut(&ticket.key).unwrap();
            state.rest = Some(book.clone());
            state.rest_version = Some((ticket.epoch, state.revision));
            Ok(book)
        })();
        if let Err(reason) = result {
            tracing::debug!(platform = %ticket.key.platform, token = %ticket.key.token_id,
                epoch = ticket.epoch, revision = ticket.revision, ?reason, "REST book rejected");
        }
        result
    }

    pub fn get(&self, platform: &str, token_id: &str) -> Option<&OrderBook> {
        self.get_at(platform, token_id, Instant::now())
    }

    pub fn get_at(&self, platform: &str, token_id: &str, now: Instant) -> Option<&OrderBook> {
        let key = TokenBookKey::new(platform, token_id);
        let ws = self.books.get(&key);
        if ws.is_some_and(|book| book.is_fresh(self.max_age, now)) {
            return ws;
        }
        let epoch = self.epochs.get(platform).copied().unwrap_or(0);
        let rest = self.sync.get(&key).and_then(|state| {
            (state.rest_version == Some((epoch, state.revision)))
                .then_some(state.rest.as_ref())
                .flatten()
        });
        rest.or(ws)
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

    pub fn stale_pm_tokens(
        &mut self,
        max_age: Duration,
        now: Instant,
        limit: usize,
    ) -> Vec<String> {
        let mut tokens: Vec<String> = self
            .token_topics
            .keys()
            .filter(|key| key.platform == POLYMARKET)
            .filter(|key| match self.get_at(&key.platform, &key.token_id, now) {
                Some(book) => !book.is_fresh(max_age, now),
                None => true,
            })
            .map(|key| key.token_id.clone())
            .collect();
        tokens.sort();
        tokens.dedup();
        if tokens.is_empty() || limit == 0 {
            return Vec::new();
        }
        let start = self
            .resync_cursor
            .as_ref()
            .map_or(0, |cursor| tokens.partition_point(|t| t <= cursor));
        let count = limit.min(500).min(tokens.len());
        let selected: Vec<_> = (0..count)
            .map(|i| tokens[(start + i) % tokens.len()].clone())
            .collect();
        self.resync_cursor = selected.last().cloned();
        selected
    }
}

fn valid_levels(levels: &[Level]) -> bool {
    let mut prices = HashSet::new();
    levels.iter().all(|level| {
        level.price > Decimal::ZERO
            && level.price <= Decimal::ONE
            && level.size > Decimal::ZERO
            && prices.insert(level.price)
    })
}

fn sort_levels(bids: &mut [Level], asks: &mut [Level]) {
    bids.sort_by(|a, b| b.price.cmp(&a.price));
    asks.sort_by(|a, b| a.price.cmp(&b.price));
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

    /// 结束本轮计算。若期间又有标记，返回 `Some` 且不占住 computing，调用方应重新 `mark` 再算。
    pub fn finish(&mut self, topic: TopicKey) -> Option<TopicKey> {
        self.computing.remove(&topic);
        if self.pending.remove(&topic) {
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
        assert!(!store
            .replace_snapshot(POLYMARKET, "t1", vec![], vec![], 100, now)
            .is_applied());
        assert!(!store
            .replace_snapshot(POLYMARKET, "t1", vec![], vec![], 90, now)
            .is_applied());
        let book = store.get(POLYMARKET, "t1").unwrap();
        assert_eq!(book.asks[0].price, d("0.5"));
        assert!(book.stale);
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
    fn same_millisecond_changes_and_delete_cannot_be_resurrected() {
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
                assert!(matches!(
                    store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), ts, now),
                    BookUpdate::Rejected(_)
                ));
                let ticket = store.begin_rest(POLYMARKET, "t");
                assert!(store
                    .accept_rest(&ticket, vec![], asks("3"), ts, now, None)
                    .is_err());
                assert!(store.get_at(POLYMARKET, "t", now).unwrap().asks.is_empty());
            }
            assert!(store
                .replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 101, now)
                .is_applied());
            assert!(!store.get_at(POLYMARKET, "t", now).unwrap().stale);
            store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 102, now);
            assert!(store.get_at(POLYMARKET, "t", now).unwrap().asks.is_empty());
        }
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
        assert!(!store
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
        assert_eq!(store.begin_rest(POLYMARKET, "t").revision, revision);
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
    fn rest_is_independent_and_late_ws_cannot_use_it_as_baseline() {
        let now = Instant::now();
        let mut store = BookStore::default();
        let ticket = store.begin_rest(POLYMARKET, "t");
        store
            .accept_rest(&ticket, vec![], asks("3"), 100, now, None)
            .unwrap();
        assert!(store
            .get_at(POLYMARKET, "t", now)
            .unwrap()
            .is_fresh(Duration::from_secs(5), now));
        store.apply_levels(POLYMARKET, "t", &[(true, d("0.4"), d("2"))], 100, now);
        let partial = store.get_at(POLYMARKET, "t", now).unwrap();
        assert!(partial.stale);
        assert!(partial.asks.is_empty());
        assert!(!store
            .replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now)
            .is_applied());
        assert!(store
            .replace_snapshot(POLYMARKET, "t", vec![], asks("4"), 101, now)
            .is_applied());
        assert!(!store.get_at(POLYMARKET, "t", now).unwrap().stale);
    }

    #[test]
    fn same_timestamp_delete_missing_from_ws_invalidates_rest_only_level() {
        let now = Instant::now();
        let mut store = BookStore::default();
        // WS 当前这一侧为空，REST 的未来观察包含新档。
        store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 100, now);
        let ticket = store.begin_rest(POLYMARKET, "t");
        store
            .accept_rest(&ticket, vec![], asks("3"), 101, now, None)
            .unwrap();
        store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 101, now);
        assert!(store
            .get_at(POLYMARKET, "t", now + Duration::from_secs(6))
            .unwrap()
            .asks
            .is_empty());
        let ticket = store.begin_rest(POLYMARKET, "t");
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
        store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, now);
        let later = now + Duration::from_secs(4);
        let ticket = store.begin_rest(POLYMARKET, "t");
        store
            .accept_rest(&ticket, vec![], asks("4"), 101, later, None)
            .unwrap();
        assert_eq!(
            store.get_at(POLYMARKET, "t", later).unwrap().asks,
            asks("3")
        );
        assert_eq!(
            store
                .get_at(POLYMARKET, "t", now + Duration::from_secs(6))
                .unwrap()
                .asks,
            asks("4")
        );
        assert!(!store
            .get_at(POLYMARKET, "t", now + Duration::from_secs(10))
            .unwrap()
            .is_fresh(Duration::from_secs(5), now + Duration::from_secs(10)));
        store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 101, later);
        assert!(store
            .get_at(POLYMARKET, "t", later)
            .unwrap()
            .asks
            .is_empty());
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
        let second =
            store.stale_pm_tokens(Duration::from_secs(5), now + Duration::from_secs(10), 80);
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
}
