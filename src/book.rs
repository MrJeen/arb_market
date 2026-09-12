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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookSource {
    Ws,
    Rest,
}

impl BookSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ws => "ws",
            Self::Rest => "rest",
        }
    }
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

// 两侧已排序；首个不同索引同时保留价格和数量，涵盖插入/删除及重复价位。
fn first_level_diff<'a>(current: &'a [Level], incoming: &'a [Level]) -> Option<(usize, Option<&'a Level>, Option<&'a Level>)> {
    (0..current.len().max(incoming.len())).find_map(|index| {
        let (current, incoming) = (current.get(index), incoming.get(index));
        (current != incoming).then_some((index, current, incoming))
    })
}

#[derive(Debug, Clone)]
pub struct RestTicket {
    pub key: TokenBookKey,
    pub epoch: u64,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy)]
struct TickObservation {
    value: Decimal,
    exchange_ts_ms: Option<i64>,
    trusted: bool,
}

#[derive(Debug, Default)]
struct SyncState {
    tick: Option<TickObservation>,
    revision: u64,
    ws_epoch: Option<u64>,
    // PM 只保留当前基线；全量边界独立于 no-op 观察，避免同毫秒跨源歧义。
    source: Option<BookSource>,
    rest_boundary: Option<i64>,
    // Outcome 保持独立 REST 候选及其高水位。
    rest: Option<OrderBook>,
    rest_version: Option<(u64, u64)>,
}

#[derive(Debug)]
pub struct BookStore {
    // PM 的单一完整基线；Outcome 保持 WS 基线。
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
        event: &'static str,
    ) -> Result<(), BookReject> {
        if self
            .sync
            .get(key)
            .and_then(|state| state.tick)
            .and_then(|tick| tick.exchange_ts_ms)
            .is_some_and(|high| ts < high)
        {
            let state = self.sync.get(key).unwrap();
            tracing::debug!(platform = %key.platform, token = %key.token_id,
                event, source = if event == "rest_snapshot" { "rest" } else { "ws" },
                reason = ?BookReject::OlderTimestamp, conflict = "tick_high_water",
                current_exchange_ts_ms = ?self.books.get(key).map(|b| b.exchange_ts_ms),
                tick_exchange_ts_ms = ?state.tick.and_then(|t| t.exchange_ts_ms),
                incoming_exchange_ts_ms = ts, rest_boundary = ?state.rest_boundary,
                epoch = self.epochs.get(&key.platform).copied().unwrap_or(0), revision = state.revision,
                "book snapshot rejected");
            return Err(BookReject::OlderTimestamp);
        }
        for (compared_book, prior) in self
            .books
            .get(key)
            .map(|book| ("current", book))
            .into_iter()
            .chain(self.sync.get(key).and_then(|state| state.rest.as_ref())
                .map(|book| ("rest_candidate", book)))
        {
            if ts < prior.exchange_ts_ms {
                return Err(BookReject::OlderTimestamp);
            }
            // PM WS 全量按接收顺序整体覆盖；REST 与 Outcome 仍拒绝同毫秒深度冲突。
            if ts == prior.exchange_ts_ms && (bids != prior.bids || asks != prior.asks)
                && !(key.platform == POLYMARKET && event == "ws_snapshot")
            {
                // 只在 debug 开启时扫描到首个差异；不序列化完整盘口。
                if tracing::enabled!(tracing::Level::DEBUG) {
                    let diff = first_level_diff(&prior.bids, bids)
                        .map(|diff| ("bid", diff))
                        .or_else(|| first_level_diff(&prior.asks, asks).map(|diff| ("ask", diff)));
                    let state = self.sync.get(key);
                    tracing::debug!(platform = %key.platform, token = %key.token_id,
                        event, source = if event == "rest_snapshot" { "rest" } else { "ws" },
                        reason = ?BookReject::TimestampConflict, conflict = "snapshot_depth",
                        compared_book, current_source = ?state.and_then(|s| s.source),
                        current_exchange_ts_ms = prior.exchange_ts_ms, incoming_exchange_ts_ms = ts,
                        rest_boundary = ?state.and_then(|s| s.rest_boundary),
                        epoch = self.epochs.get(&key.platform).copied().unwrap_or(0),
                        revision = state.map_or(0, |s| s.revision),
                        current_bid_count = prior.bids.len(), incoming_bid_count = bids.len(),
                        current_ask_count = prior.asks.len(), incoming_ask_count = asks.len(),
                        first_diff = ?diff, "book timestamp conflict");
                }
                return Err(BookReject::TimestampConflict);
            }
        }
        Ok(())
    }

    pub fn replace_snapshot(
        &mut self,
        platform: &str,
        token_id: &str,
        bids: Vec<Level>,
        asks: Vec<Level>,
        exchange_ts_ms: i64,
        now: Instant,
    ) -> BookUpdate {
        self.replace_snapshot_with_tick(platform, token_id, bids, asks, exchange_ts_ms, now, None)
    }

    pub fn replace_snapshot_with_tick(
        &mut self,
        platform: &str,
        token_id: &str,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        exchange_ts_ms: i64,
        now: Instant,
        tick: Option<Decimal>,
    ) -> BookUpdate {
        let key = TokenBookKey::new(platform, token_id);
        if token_id.is_empty()
            || exchange_ts_ms <= 0
            || !valid_levels(&bids)
            || !valid_levels(&asks)
            || tick.is_some_and(|value| value <= Decimal::ZERO || value > Decimal::ONE)
        {
            return self.invalidate_ws(platform, token_id, BookReject::InvalidPayload);
        }
        sort_levels(&mut bids, &mut asks);
        if let Err(reason) = self.snapshot_conflict(&key, &bids, &asks, exchange_ts_ms, "ws_snapshot") {
            // 盘口与 tick 均预检完才提交；坏快照不能借携带的新 tick 改写观察高水位。
            if reason == BookReject::TimestampConflict {
                return self.invalidate_ws_event(platform, token_id, reason, "ws_snapshot", Some(exchange_ts_ms));
            }
            return BookUpdate::Rejected(reason);
        }
        let epoch = self.epochs.get(platform).copied().unwrap_or(0);
        if platform != POLYMARKET && self
            .books
            .get(&key)
            .is_some_and(|book| book.stale && book.exchange_ts_ms == exchange_ts_ms)
            && self
                .sync
                .get(&key)
                .is_some_and(|state| state.ws_epoch == Some(epoch))
        {
            let state = self.sync.get(&key).unwrap();
            tracing::debug!(platform, token = token_id, event = "ws_snapshot", source = "ws",
                reason = ?BookReject::TimestampConflict, conflict = "stale_same_epoch",
                current_exchange_ts_ms = exchange_ts_ms, incoming_exchange_ts_ms = exchange_ts_ms,
                rest_boundary = ?state.rest_boundary, epoch, revision = state.revision,
                "book timestamp conflict");
            return BookUpdate::Rejected(BookReject::TimestampConflict);
        }
        let tick_changed = match self.check_snapshot_tick(&key, tick, exchange_ts_ms, "ws_snapshot") {
            Ok(changed) => changed,
            Err(reason) => return BookUpdate::Rejected(reason),
        };
        let unchanged = self
            .books
            .get(&key)
            .is_some_and(|book| !book.stale && book.bids == bids && book.asks == asks);
        let restored = self.books.get(&key).is_none_or(|book| book.stale);
        if tick_changed {
            self.commit_tick(&key, tick.unwrap(), Some(exchange_ts_ms));
        }
        if unchanged {
            let book = self.books.get_mut(&key).unwrap();
            let advanced = book.exchange_ts_ms != exchange_ts_ms;
            book.exchange_ts_ms = exchange_ts_ms;
            if platform == POLYMARKET || advanced || tick_changed {
                self.changed(&key);
            }
            return if tick_changed {
                BookUpdate::Applied
            } else {
                BookUpdate::VerifiedUnchanged
            };
        }
        let tick_size = self.tick_size(platform, token_id);
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
        state.source = Some(BookSource::Ws);
        // 同毫秒 WS 全量可接管基线，但不能消除 REST 与后续 delta 的排序歧义。
        // 仅严格更新的全量清除边界；no-op 路径继续保留边界和来源。
        if state.rest_boundary.is_some_and(|boundary| exchange_ts_ms > boundary) {
            state.rest_boundary = None;
        }
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
            .chain(
                self.sync
                    .get(&key)
                    .and_then(|state| state.tick)
                    .and_then(|tick| tick.exchange_ts_ms),
            )
            .max()
            .unwrap_or(0);
        if exchange_ts_ms < high {
            return self.invalidate_ws_event(platform, token_id, BookReject::OlderTimestamp, "ws_delta", Some(exchange_ts_ms));
        }
        let rest_observed = self.sync.get(&key).is_some_and(|state| state.rest_version.is_some());
        // 先归并再预检，拒绝歧义批次时不能局部提交或借 no-op 消除 REST 边界。
        let mut final_updates = HashMap::new();
        for &(is_bid, price, size) in updates {
            final_updates.insert((is_bid, price), size);
        }
        if platform == POLYMARKET
            && self.sync.get(&key).is_some_and(|state| state.rest_boundary == Some(exchange_ts_ms))
            && self.books.get(&key).is_some_and(|book| {
                final_updates.iter().any(|(&(is_bid, price), &size)| {
                    let levels = if is_bid { &book.bids } else { &book.asks };
                    levels.iter().find(|level| level.price == price)
                        .map_or(!size.is_zero(), |level| level.size != size)
                })
            })
        {
            if tracing::enabled!(tracing::Level::DEBUG) {
                let book = self.books.get(&key).unwrap();
                let state = self.sync.get(&key).unwrap();
                let diff = final_updates.iter().find_map(|(&(is_bid, price), &size)| {
                    let levels = if is_bid { &book.bids } else { &book.asks };
                    let current = levels.iter().find(|level| level.price == price)
                        .map_or(Decimal::ZERO, |level| level.size);
                    (current != size).then_some((if is_bid { "bid" } else { "ask" }, price, current, size))
                });
                if let Some((side, price, current_size, incoming_size)) = diff {
                    tracing::debug!(platform, token = token_id, event = "ws_delta", source = "ws",
                        reason = ?BookReject::TimestampConflict, conflict = "rest_boundary",
                        current_source = ?state.source, current_exchange_ts_ms = book.exchange_ts_ms,
                        incoming_exchange_ts_ms = exchange_ts_ms, rest_boundary = ?state.rest_boundary,
                        epoch = self.epochs.get(platform).copied().unwrap_or(0), revision = state.revision,
                        side, %price, %current_size, %incoming_size, update_count = final_updates.len(),
                        "book timestamp conflict");
                }
            }
            return self.invalidate_ws_event(platform, token_id, BookReject::TimestampConflict, "ws_delta", Some(exchange_ts_ms));
        }
        let book = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        let mut changed = false;
        // 同价位只提交批内最后值；保持接收顺序语义，同时重复整批不因中间值续 TTL。
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
        if platform == POLYMARKET || changed || advanced || rest_observed {
            self.changed(&key);
        }
        if changed {
            self.sync.get_mut(&key).unwrap().source = Some(BookSource::Ws);
            BookUpdate::Applied
        } else {
            BookUpdate::VerifiedUnchanged
        }
    }

    pub fn tick_size(&self, platform: &str, token_id: &str) -> Option<Decimal> {
        self.sync
            .get(&TokenBookKey::new(platform, token_id))
            .and_then(|state| state.tick)
            .filter(|tick| tick.trusted)
            .map(|tick| tick.value)
    }

    /// 无时间戳 REST 只初始化从未观察过的 tick；失效票据不能填补冲突。
    pub fn seed_tick_size(&mut self, ticket: &RestTicket, tick: Decimal) -> Option<Decimal> {
        let current = self.begin_rest(&ticket.key.platform, &ticket.key.token_id);
        let accepted = self.tick_size(&ticket.key.platform, &ticket.key.token_id);
        if accepted.is_some() {
            return accepted;
        }
        if current.epoch != ticket.epoch
            || current.revision != ticket.revision
            || self
                .sync
                .get(&ticket.key)
                .is_some_and(|state| state.tick.is_some())
            || ticket.key.token_id.is_empty()
            || tick <= Decimal::ZERO
            || tick > Decimal::ONE
        {
            tracing::debug!(platform = %ticket.key.platform, token = %ticket.key.token_id,
                epoch = current.epoch, revision = current.revision,
                reason = "ticket changed, tick already observed or invalid seed", "tick seed rejected");
            return None;
        }
        self.commit_tick(&ticket.key, tick, None);
        self.changed(&ticket.key);
        Some(tick)
    }

    fn check_tick(&self, key: &TokenBookKey, tick: Decimal, ts: i64) -> Result<bool, BookReject> {
        if key.token_id.is_empty() || ts <= 0 || tick <= Decimal::ZERO || tick > Decimal::ONE {
            return Err(BookReject::InvalidPayload);
        }
        let high = self
            .books
            .get(key)
            .into_iter()
            .chain(self.sync.get(key).and_then(|state| state.rest.as_ref()))
            .map(|book| book.exchange_ts_ms)
            .max()
            .unwrap_or(0);
        let prior = self.sync.get(key).and_then(|state| state.tick);
        if ts < high
            || prior
                .and_then(|tick| tick.exchange_ts_ms)
                .is_some_and(|high| ts < high)
        {
            return Err(BookReject::OlderTimestamp);
        }
        if let Some(prior) = prior {
            if prior.exchange_ts_ms == Some(ts) {
                return if prior.trusted && prior.value == tick {
                    Ok(false)
                } else {
                    Err(BookReject::TimestampConflict)
                };
            }
        }
        Ok(true)
    }

    fn check_snapshot_tick(
        &mut self,
        key: &TokenBookKey,
        tick: Option<Decimal>,
        ts: i64,
        event: &'static str,
    ) -> Result<bool, BookReject> {
        let Some(tick) = tick else {
            return Ok(false);
        };
        let result = self.check_tick(key, tick, ts);
        if let Err(reason) = result {
            let state = self.sync.get(key);
            let prior = state.and_then(|s| s.tick);
            tracing::debug!(platform = %key.platform, token = %key.token_id,
                event, source = if event == "rest_snapshot" { "rest" } else { "ws" },
                ?reason, conflict = "tick_observation",
                current_exchange_ts_ms = ?self.books.get(key).map(|b| b.exchange_ts_ms),
                rest_exchange_ts_ms = ?state.and_then(|s| s.rest.as_ref()).map(|b| b.exchange_ts_ms),
                tick_exchange_ts_ms = ?prior.and_then(|t| t.exchange_ts_ms), incoming_exchange_ts_ms = ts,
                current_tick = ?prior.map(|t| t.value), incoming_tick = %tick,
                tick_trusted = ?prior.map(|t| t.trusted), rest_boundary = ?state.and_then(|s| s.rest_boundary),
                epoch = self.epochs.get(&key.platform).copied().unwrap_or(0),
                revision = state.map_or(0, |s| s.revision), "tick observation rejected");
        }
        if result == Err(BookReject::TimestampConflict) {
            self.conflict_tick(key, event, ts, tick);
        }
        result
    }

    fn sync_tick_views(&mut self, key: &TokenBookKey) {
        let tick = self.tick_size(&key.platform, &key.token_id);
        if let Some(book) = self.books.get_mut(key) {
            book.tick_size = tick;
        }
        if let Some(book) = self.sync.get_mut(key).and_then(|state| state.rest.as_mut()) {
            book.tick_size = tick;
        }
    }

    fn conflict_tick(&mut self, key: &TokenBookKey, event: &'static str, incoming_exchange_ts_ms: i64, incoming_tick: Decimal) {
        let Some(tick) = self.sync.get_mut(key).and_then(|state| state.tick.as_mut()) else {
            return;
        };
        if !tick.trusted {
            return;
        }
        tick.trusted = false;
        let ts = tick.exchange_ts_ms;
        let current_tick = tick.value;
        self.sync_tick_views(key);
        self.changed(key);
        let current = self.begin_rest(&key.platform, &key.token_id);
        tracing::warn!(platform = %key.platform, token = %key.token_id, timestamp = ?ts,
            event, source = if event == "rest_snapshot" { "rest" } else { "ws" },
            current_exchange_ts_ms = ?ts, incoming_exchange_ts_ms, %current_tick, %incoming_tick,
            rest_boundary = ?self.sync.get(key).and_then(|s| s.rest_boundary),
            epoch = current.epoch, revision = current.revision,
            reason = "same timestamp has conflicting tick values", "tick trust lost");
    }

    fn commit_tick(&mut self, key: &TokenBookKey, value: Decimal, exchange_ts_ms: Option<i64>) {
        let state = self.sync.entry(key.clone()).or_default();
        let recovered = state.tick.is_some_and(|tick| !tick.trusted);
        state.tick = Some(TickObservation {
            value,
            exchange_ts_ms,
            trusted: true,
        });
        // tick-only 观察保留一个 stale WS 壳，但不提供完整盘口，也不刷新 TTL。
        self.books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(&key.platform, &key.token_id));
        self.sync_tick_views(key);
        if recovered {
            let current = self.begin_rest(&key.platform, &key.token_id);
            tracing::info!(platform = %key.platform, token = %key.token_id, timestamp = ?exchange_ts_ms,
                epoch = current.epoch, revision = current.revision + 1, "tick trust restored");
        }
    }

    pub fn set_tick_size_at(
        &mut self,
        platform: &str,
        token_id: &str,
        tick: Decimal,
        ts: i64,
    ) -> BookUpdate {
        let key = TokenBookKey::new(platform, token_id);
        match self.check_snapshot_tick(&key, Some(tick), ts, "ws_tick") {
            Ok(true) => {
                self.commit_tick(&key, tick, Some(ts));
                self.changed(&key);
                BookUpdate::Applied
            }
            Ok(false) => {
                if platform == POLYMARKET {
                    self.changed(&key);
                }
                BookUpdate::VerifiedUnchanged
            },
            Err(reason) => BookUpdate::Rejected(reason),
        }
    }

    /// 仅测试夹具使用；生产无时间戳初始化必须持有 REST 票据。
    #[cfg(test)]
    pub fn set_tick_size(&mut self, platform: &str, token_id: &str, tick: Decimal) -> BookUpdate {
        if tick <= Decimal::ZERO || tick > Decimal::ONE {
            return BookUpdate::Rejected(BookReject::InvalidPayload);
        }
        if self.tick_size(platform, token_id) == Some(tick) {
            return BookUpdate::VerifiedUnchanged;
        }
        let key = TokenBookKey::new(platform, token_id);
        self.commit_tick(&key, tick, None);
        self.changed(&key);
        BookUpdate::Applied
    }

    pub fn invalidate_ws(
        &mut self,
        platform: &str,
        token_id: &str,
        reason: BookReject,
    ) -> BookUpdate {
        self.invalidate_ws_event(platform, token_id, reason, "ws_invalid", None)
    }

    fn invalidate_ws_event(
        &mut self, platform: &str, token_id: &str, reason: BookReject,
        event: &'static str, incoming_exchange_ts_ms: Option<i64>,
    ) -> BookUpdate {
        let key = TokenBookKey::new(platform, token_id);
        let state = self.sync.get(&key);
        let book = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::empty(platform, token_id));
        if !book.stale {
            tracing::warn!(
                platform,
                token = token_id,
                ?reason, event, source = "ws",
                current_exchange_ts_ms = book.exchange_ts_ms, ?incoming_exchange_ts_ms,
                rest_boundary = ?state.and_then(|s| s.rest_boundary),
                tick_exchange_ts_ms = ?state.and_then(|s| s.tick).and_then(|t| t.exchange_ts_ms),
                epoch = self.epochs.get(platform).copied().unwrap_or(0),
                revision = state.map_or(0, |s| s.revision),
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
            self.snapshot_conflict(&ticket.key, &bids, &asks, exchange_ts_ms, "rest_snapshot")?;
            let tick_changed = self.check_snapshot_tick(&ticket.key, tick, exchange_ts_ms, "rest_snapshot")?;
            if tick_changed {
                self.commit_tick(&ticket.key, tick.unwrap(), Some(exchange_ts_ms));
            }
            let tick_size = self.tick_size(&ticket.key.platform, &ticket.key.token_id);
            let restored = self.books.get(&ticket.key).is_none_or(|book| book.stale);
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
            self.changed(&ticket.key);
            let state = self.sync.get_mut(&ticket.key).unwrap();
            if ticket.key.platform == POLYMARKET {
                state.source = Some(BookSource::Rest);
                state.rest_boundary = Some(exchange_ts_ms);
                state.ws_epoch = Some(ticket.epoch);
                self.books.insert(ticket.key.clone(), book.clone());
                if restored {
                    tracing::info!(platform = %ticket.key.platform, token = %ticket.key.token_id,
                        epoch = ticket.epoch, revision = state.revision, timestamp = exchange_ts_ms,
                        "book REST completeness restored");
                }
            } else {
                state.rest = Some(book.clone());
                state.rest_version = Some((ticket.epoch, state.revision));
            }
            Ok(book)
        })();
        if let Err(reason) = result {
            let state = self.sync.get(&ticket.key);
            tracing::debug!(platform = %ticket.key.platform, token = %ticket.key.token_id,
                event = "rest_snapshot", source = "rest", ?reason,
                epoch = self.epochs.get(&ticket.key.platform).copied().unwrap_or(0),
                revision = state.map_or(0, |s| s.revision),
                ticket_epoch = ticket.epoch, ticket_revision = ticket.revision,
                current_exchange_ts_ms = ?self.books.get(&ticket.key).map(|b| b.exchange_ts_ms),
                incoming_exchange_ts_ms = exchange_ts_ms,
                rest_boundary = ?state.and_then(|s| s.rest_boundary), "REST book rejected");
        }
        result
    }

    pub fn get(&self, platform: &str, token_id: &str) -> Option<&OrderBook> {
        self.get_with_source(platform, token_id)
            .map(|(book, _)| book)
    }

    pub fn get_at(&self, platform: &str, token_id: &str, now: Instant) -> Option<&OrderBook> {
        self.get_with_source_at(platform, token_id, now)
            .map(|(book, _)| book)
    }

    pub fn get_with_source(
        &self,
        platform: &str,
        token_id: &str,
    ) -> Option<(&OrderBook, BookSource)> {
        self.get_with_source_at(platform, token_id, Instant::now())
    }

    pub fn get_with_source_at(
        &self,
        platform: &str,
        token_id: &str,
        now: Instant,
    ) -> Option<(&OrderBook, BookSource)> {
        let key = TokenBookKey::new(platform, token_id);
        let ws = self.books.get(&key);
        if platform == POLYMARKET {
            let source = self.sync.get(&key).and_then(|state| state.source).unwrap_or(BookSource::Ws);
            return ws.map(|book| (book, source));
        }
        if ws.is_some_and(|book| book.is_fresh(self.max_age, now)) {
            return ws.map(|book| (book, BookSource::Ws));
        }
        let epoch = self.epochs.get(platform).copied().unwrap_or(0);
        let rest = self.sync.get(&key).and_then(|state| {
            (state.rest_version == Some((epoch, state.revision)))
                .then_some(state.rest.as_ref())
                .flatten()
        });
        rest.map(|book| (book, BookSource::Rest))
            .or_else(|| ws.map(|book| (book, BookSource::Ws)))
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
                    assert_eq!(store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), ts, now),
                        BookUpdate::Rejected(BookReject::OlderTimestamp));
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
            for (bids, incoming_asks) in [(asks("7"), asks("4")), (vec![], asks("4")), (vec![], vec![])] {
                if stale {
                    store.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
                }
                let ticket = store.begin_rest(POLYMARKET, "t");
                assert_eq!(store.replace_snapshot(POLYMARKET, "t", bids.clone(), incoming_asks.clone(), 100, later), BookUpdate::Applied);
                let book = store.get(POLYMARKET, "t").unwrap();
                assert_eq!(book.bids, bids);
                assert_eq!(book.asks, incoming_asks);
                assert!(!book.stale);
                assert_eq!(book.received_at, later);
                assert_eq!(store.accept_rest(&ticket, vec![], asks("9"), 200, later, None).unwrap_err(), BookReject::RevisionChanged);
                let ticket = store.begin_rest(POLYMARKET, "t");
                let duplicate_at = later + Duration::from_secs(6);
                assert_eq!(store.replace_snapshot(POLYMARKET, "t", bids.clone(), incoming_asks.clone(), 100, duplicate_at), BookUpdate::VerifiedUnchanged);
                assert_eq!(store.get(POLYMARKET, "t").unwrap().received_at, later);
                assert_eq!(store.accept_rest(&ticket, vec![], asks("9"), 200, duplicate_at, None).unwrap_err(), BookReject::RevisionChanged);
                assert_eq!(store.replace_snapshot(POLYMARKET, "t", asks("9"), asks("9"), 99, duplicate_at), BookUpdate::Rejected(BookReject::OlderTimestamp));
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
            store.accept_rest(&ticket, asks("8"), asks("3"), 100, now, None).unwrap();
            if stale {
                store.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
            }
            assert_eq!(store.replace_snapshot(POLYMARKET, "t", vec![], asks("4"), 100, now), BookUpdate::Applied);
            assert_eq!(store.get_with_source(POLYMARKET, "t").unwrap().1, BookSource::Ws);
            assert_eq!(store.sync.get(&ticket.key).unwrap().rest_boundary, Some(100));
            let ticket = store.begin_rest(POLYMARKET, "t");
            assert_eq!(store.accept_rest(&ticket, vec![], asks("3"), 100, now, None).unwrap_err(), BookReject::TimestampConflict);
            assert_eq!(store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("4"))], 100, now), BookUpdate::VerifiedUnchanged);
            assert_eq!(store.apply_levels(POLYMARKET, "t", &[(true, d("0.4"), d("2")), (false, d("0.5"), d("0"))], 100, now), BookUpdate::Rejected(BookReject::TimestampConflict));
            let book = store.get(POLYMARKET, "t").unwrap();
            assert!(book.stale && book.bids.is_empty());
            assert_eq!(book.asks, asks("4"));
            assert_eq!(store.replace_snapshot(POLYMARKET, "t", vec![], vec![], 101, now), BookUpdate::Applied);
            assert_eq!(store.sync.get(&ticket.key).unwrap().rest_boundary, None);
            assert_eq!(store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("2"))], 101, now), BookUpdate::Applied);
        }
    }

    #[test]
    fn same_timestamp_ws_depth_replacement_still_prechecks_tick_atomically() {
        for stale in [false, true] {
            let now = Instant::now();
            let later = now + Duration::from_secs(6);
            let mut store = BookStore::default();
            store.replace_snapshot_with_tick(POLYMARKET, "t", asks("8"), asks("3"), 100, now, Some(d("0.01")));
            if stale {
                store.invalidate_ws(POLYMARKET, "t", BookReject::InvalidPayload);
            }
            assert_eq!(store.replace_snapshot_with_tick(POLYMARKET, "t", vec![], asks("4"), 100, later, Some(d("0.001"))), BookUpdate::Rejected(BookReject::TimestampConflict));
            let book = store.get(POLYMARKET, "t").unwrap();
            assert_eq!(book.bids, asks("8"));
            assert_eq!(book.asks, asks("3"));
            assert_eq!(book.received_at, now);
            assert_eq!(book.stale, stale);
            assert_eq!(book.tick_size, None);
            // 冲突 tick 的不可信状态不能被同时间戳全量夹带恢复。
            assert_eq!(store.replace_snapshot_with_tick(POLYMARKET, "t", vec![], asks("4"), 100, later, Some(d("0.01"))), BookUpdate::Rejected(BookReject::TimestampConflict));
        }
    }

    #[test]
    fn outcome_same_timestamp_depth_conflict_and_stale_recovery_rules_are_unchanged() {
        let now = Instant::now();
        let mut store = BookStore::default();
        store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now);
        assert_eq!(store.replace_snapshot(OUTCOME, "t", vec![], asks("4"), 100, now), BookUpdate::Rejected(BookReject::TimestampConflict));
        assert!(store.get(OUTCOME, "t").unwrap().stale);
        assert_eq!(store.get(OUTCOME, "t").unwrap().asks, asks("3"));
        assert_eq!(store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now), BookUpdate::Rejected(BookReject::TimestampConflict));
        store.begin_platform_connection(OUTCOME);
        assert_eq!(store.replace_snapshot(OUTCOME, "t", vec![], asks("3"), 100, now), BookUpdate::Applied);
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
        store.accept_rest(&ticket, vec![], asks("3"), 100, now, None).unwrap();
        assert!(store.sync.get(&ticket.key).unwrap().rest.is_none());
        assert_eq!(store.get_with_source(POLYMARKET, "t").unwrap().1, BookSource::Rest);
        for ts in 101..105 {
            assert!(store.apply_levels(POLYMARKET, "t", &[(true, d("0.4"), Decimal::from(ts))], ts, now).is_applied());
            let (book, source) = store.get_with_source(POLYMARKET, "t").unwrap();
            assert!(!book.stale);
            assert_eq!(book.asks, asks("3"));
            assert_eq!(book.bids.len(), 1);
            assert_eq!(source, BookSource::Ws);
        }
        // 旧 REST 副本不能否决已沿 WS 顺序演进后的同毫秒全量或 tick。
        let book = store.get(POLYMARKET, "t").unwrap().clone();
        assert_eq!(store.replace_snapshot(POLYMARKET, "t", book.bids, book.asks, 104, now), BookUpdate::VerifiedUnchanged);
        assert!(store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 104).is_applied());
        let ticket = store.begin_rest(POLYMARKET, "t");
        store.accept_rest(&ticket, vec![], vec![], 105, now, None).unwrap();
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
            store.accept_rest(&initial, vec![], asks("3"), 100, now, Some(d("0.01"))).unwrap();
            let ticket = store.begin_rest(POLYMARKET, "t");
            let update = match observation {
                0 => store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("3"))], 100, later),
                1 => store.apply_levels(POLYMARKET, "t", &[(true, d("0.4"), d("0"))], 100, later),
                2 => store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later),
                _ => store.set_tick_size_at(POLYMARKET, "t", d("0.01"), 100),
            };
            assert_eq!(update, BookUpdate::VerifiedUnchanged);
            assert_eq!(store.accept_rest(&ticket, vec![], asks("4"), 200, later, None).unwrap_err(), BookReject::RevisionChanged);
            let (book, source) = store.get_with_source_at(POLYMARKET, "t", later).unwrap();
            assert_eq!(source, BookSource::Rest);
            assert_eq!(book.received_at, now);
            assert!(!book.is_fresh(Duration::from_secs(5), later));
            assert_eq!(book.asks, asks("3"));
            // 任意 no-op 都不能解除 REST 的同毫秒跨源边界。
            assert_eq!(store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 100, later), BookUpdate::Rejected(BookReject::TimestampConflict));
            assert!(store.get(POLYMARKET, "t").unwrap().stale);
            assert_eq!(store.get(POLYMARKET, "t").unwrap().asks, asks("3"));
            assert_eq!(store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later), BookUpdate::Applied);
            assert_eq!(store.get(POLYMARKET, "t").unwrap().received_at, later);
            assert_eq!(store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("0"))], 100, later), BookUpdate::Rejected(BookReject::TimestampConflict));
            store.begin_platform_connection(POLYMARKET);
            assert!(store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 100, later).is_applied());
        }
    }

    #[test]
    fn pm_other_token_and_empty_delta_do_not_compete_but_old_delta_invalidates() {
        let now = Instant::now();
        let mut store = BookStore::default();
        let ticket = store.begin_rest(POLYMARKET, "t");
        store.replace_snapshot(POLYMARKET, "other", vec![], asks("2"), 200, now);
        store.apply_levels(POLYMARKET, "t", &[], 100, now);
        store.accept_rest(&ticket, vec![], asks("3"), 100, now, None).unwrap();
        assert_eq!(store.apply_levels(POLYMARKET, "t", &[(false, d("0.5"), d("3"))], 99, now), BookUpdate::Rejected(BookReject::OlderTimestamp));
        assert!(store.get(POLYMARKET, "t").unwrap().stale);
        assert_eq!(store.replace_snapshot(POLYMARKET, "t", vec![], asks("3"), 99, now), BookUpdate::Rejected(BookReject::OlderTimestamp));
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
        assert_eq!(
            store.get_at(OUTCOME, "t", later).unwrap().asks,
            asks("3")
        );
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
        assert!(store
            .get_at(OUTCOME, "t", later)
            .unwrap()
            .asks
            .is_empty());
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
            assert!(std::ptr::eq(
                book,
                store.get_at(OUTCOME, "t", at).unwrap()
            ));
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
