CREATE TABLE order_platform_settlement_results (
    order_id BIGINT NOT NULL REFERENCES arb_orders(id) ON DELETE CASCADE,
    platform TEXT NOT NULL CHECK (platform IN ('polymarket','outcome')),
    market_id TEXT NOT NULL CHECK (btrim(market_id) <> ''),
    endpoint TEXT NOT NULL CHECK (btrim(endpoint) <> ''),
    source TEXT NOT NULL,
    payouts JSONB NOT NULL,
    evidence JSONB NOT NULL,
    evidence_version INTEGER NOT NULL CHECK (evidence_version = 1),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, platform)
);
