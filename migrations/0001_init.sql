CREATE TABLE IF NOT EXISTS arb_orders (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    unified_index   INTEGER NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    market_title    TEXT NOT NULL DEFAULT '',
    end_date        TIMESTAMPTZ,
    estimated_rev   NUMERIC(20, 8) NOT NULL DEFAULT 0,
    estimated_profit NUMERIC(20, 8) NOT NULL DEFAULT 0,
    estimated_cost  NUMERIC(20, 8) NOT NULL DEFAULT 0,
    actual_rev      NUMERIC(20, 8) NOT NULL DEFAULT 0,
    actual_profit   NUMERIC(20, 8) NOT NULL DEFAULT 0,
    actual_cost     NUMERIC(20, 8) NOT NULL DEFAULT 0,
    fills           JSONB NOT NULL DEFAULT '[]'::jsonb,
    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
    rebalance_status VARCHAR(16) NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    rebalanced_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_arb_orders_topic_status
    ON arb_orders (event_id, unified_index, status);
CREATE INDEX IF NOT EXISTS idx_arb_orders_rebalance
    ON arb_orders (status, rebalance_status, id);

CREATE TABLE IF NOT EXISTS legs (
    id               BIGSERIAL PRIMARY KEY,
    order_id         BIGINT NOT NULL REFERENCES arb_orders (id),
    platform         VARCHAR(32) NOT NULL,
    token_id         TEXT NOT NULL,
    label            TEXT NOT NULL,
    side             VARCHAR(8) NOT NULL,
    intent           VARCHAR(32) NOT NULL DEFAULT 'arb_buy',
    funder_address   TEXT,
    wallet_address   TEXT,
    req_price        NUMERIC(20, 8),
    req_shares       NUMERIC(20, 8),
    req_fee          NUMERIC(20, 8),
    actual_price     NUMERIC(20, 8),
    actual_shares    NUMERIC(20, 8),
    actual_fee       NUMERIC(20, 8),
    client_order_id  TEXT,
    third_order_id   TEXT,
    status           VARCHAR(16) NOT NULL DEFAULT 'pending',
    last_order_info  JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_legs_order ON legs (order_id);
CREATE INDEX IF NOT EXISTS idx_legs_status ON legs (status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_legs_client_order_id
    ON legs (client_order_id)
    WHERE client_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS signed_envelopes (
    id          BIGSERIAL PRIMARY KEY,
    leg_id      BIGINT NOT NULL REFERENCES legs (id),
    order_hash  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signed_envelopes_leg ON signed_envelopes (leg_id);
CREATE INDEX IF NOT EXISTS idx_signed_envelopes_hash ON signed_envelopes (order_hash);

CREATE TABLE IF NOT EXISTS fills (
    id              BIGSERIAL PRIMARY KEY,
    leg_id          BIGINT NOT NULL REFERENCES legs (id),
    third_order_id  TEXT NOT NULL DEFAULT '',
    trade_id        TEXT NOT NULL DEFAULT '',
    shares          NUMERIC(20, 8),
    price           NUMERIC(20, 8),
    fee             NUMERIC(20, 8),
    fee_rate_bps    NUMERIC(20, 8),
    raw             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_trade
    ON fills (leg_id, trade_id, third_order_id);

CREATE TABLE IF NOT EXISTS fee_estimates (
    platform    VARCHAR(32) NOT NULL,
    token_id    TEXT NOT NULL DEFAULT '',
    ema_bps     NUMERIC(20, 8) NOT NULL,
    samples     INTEGER NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (platform, token_id)
);
