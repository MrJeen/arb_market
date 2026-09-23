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
fn first_level_diff<'a>(
    current: &'a [Level],
    incoming: &'a [Level],
) -> Option<(usize, Option<&'a Level>, Option<&'a Level>)> {
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
            .chain(
                self.sync
                    .get(key)
                    .and_then(|state| state.rest.as_ref())
                    .map(|book| ("rest_candidate", book)),
            )
        {
            if ts < prior.exchange_ts_ms {
                return Err(BookReject::OlderTimestamp);
            }
            // PM WS 全量按接收顺序整体覆盖；REST 与 Outcome 仍拒绝同毫秒深度冲突。
            if ts == prior.exchange_ts_ms
                && (bids != prior.bids || asks != prior.asks)
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
        if let Err(reason) =
            self.snapshot_conflict(&key, &bids, &asks, exchange_ts_ms, "ws_snapshot")
        {
            // 盘口与 tick 均预检完才提交；坏快照不能借携带的新 tick 改写观察高水位。
            if reason == BookReject::TimestampConflict {
                return self.invalidate_ws_event(
                    platform,
                    token_id,
                    reason,
                    "ws_snapshot",
                    Some(exchange_ts_ms),
                );
            }
            return BookUpdate::Rejected(reason);
        }
        let epoch = self.epochs.get(platform).copied().unwrap_or(0);
        if platform != POLYMARKET
            && self
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
        let tick_changed = match self.check_snapshot_tick(&key, tick, exchange_ts_ms, "ws_snapshot")
        {
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
        if state
            .rest_boundary
            .is_some_and(|boundary| exchange_ts_ms > boundary)
        {
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
            return self.invalidate_ws_event(
                platform,
                token_id,
                BookReject::OlderTimestamp,
                "ws_delta",
                Some(exchange_ts_ms),
            );
        }
        let rest_observed = self
            .sync
            .get(&key)
            .is_some_and(|state| state.rest_version.is_some());
        // 先归并再预检，拒绝歧义批次时不能局部提交或借 no-op 消除 REST 边界。
        let mut final_updates = HashMap::new();
        for &(is_bid, price, size) in updates {
            final_updates.insert((is_bid, price), size);
        }
        if platform == POLYMARKET
            && self
                .sync
                .get(&key)
                .is_some_and(|state| state.rest_boundary == Some(exchange_ts_ms))
            && self.books.get(&key).is_some_and(|book| {
                final_updates.iter().any(|(&(is_bid, price), &size)| {
                    let levels = if is_bid { &book.bids } else { &book.asks };
                    levels
                        .iter()
                        .find(|level| level.price == price)
                        .map_or(!size.is_zero(), |level| level.size != size)
                })
            })
        {
            if tracing::enabled!(tracing::Level::DEBUG) {
                let book = self.books.get(&key).unwrap();
                let state = self.sync.get(&key).unwrap();
                let diff = final_updates.iter().find_map(|(&(is_bid, price), &size)| {
                    let levels = if is_bid { &book.bids } else { &book.asks };
                    let current = levels
                        .iter()
                        .find(|level| level.price == price)
                        .map_or(Decimal::ZERO, |level| level.size);
                    (current != size).then_some((
                        if is_bid { "bid" } else { "ask" },
                        price,
                        current,
                        size,
                    ))
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
            return self.invalidate_ws_event(
                platform,
                token_id,
                BookReject::TimestampConflict,
                "ws_delta",
                Some(exchange_ts_ms),
            );
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

    fn conflict_tick(
        &mut self,
        key: &TokenBookKey,
        event: &'static str,
        incoming_exchange_ts_ms: i64,
        incoming_tick: Decimal,
    ) {
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
            }
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
        &mut self,
        platform: &str,
        token_id: &str,
        reason: BookReject,
        event: &'static str,
        incoming_exchange_ts_ms: Option<i64>,
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
            let tick_changed =
                self.check_snapshot_tick(&ticket.key, tick, exchange_ts_ms, "rest_snapshot")?;
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
            let source = self
                .sync
                .get(&key)
                .and_then(|state| state.source)
                .unwrap_or(BookSource::Ws);
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
#[path = "../tests/unit/book.rs"]
mod tests;
