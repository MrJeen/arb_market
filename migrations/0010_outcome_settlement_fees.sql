-- 独立证据，不改写普通交易 fills；NUMERIC 不指定 scale，避免 PostgreSQL 静默舍入。
CREATE TABLE outcome_settlement_fee_groups (
    id BIGSERIAL PRIMARY KEY,
    network TEXT NOT NULL CHECK (network <> ''),
    wallet TEXT NOT NULL CHECK (wallet <> '' AND wallet = lower(wallet)),
    token TEXT NOT NULL CHECK (token <> ''),
    sealed BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot JSONB,
    evidence JSONB,
    payout NUMERIC CHECK (payout >= 0 AND payout <= 1),
    total_quantity NUMERIC CHECK (total_quantity >= 0),
    total_fee NUMERIC CHECK (total_fee >= 0),
    scan_progress JSONB,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (network, wallet, token),
    CHECK (NOT sealed OR (snapshot IS NOT NULL AND evidence IS NOT NULL AND payout IS NOT NULL AND total_quantity IS NOT NULL AND total_fee IS NOT NULL))
);
CREATE TABLE outcome_settlement_fee_events (
    id BIGSERIAL PRIMARY KEY,
    group_id BIGINT NOT NULL REFERENCES outcome_settlement_fee_groups(id),
    network TEXT NOT NULL,
    wallet TEXT NOT NULL,
    tid TEXT NOT NULL CHECK (tid <> ''),
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    payout NUMERIC NOT NULL CHECK (payout >= 0 AND payout <= 1),
    fee NUMERIC NOT NULL CHECK (fee >= 0),
    fee_token TEXT NOT NULL CHECK (fee_token <> ''),
    evidence JSONB NOT NULL,
    UNIQUE (network, wallet, tid)
);
CREATE TABLE outcome_settlement_fee_allocations (
    group_id BIGINT NOT NULL REFERENCES outcome_settlement_fee_groups(id),
    order_id BIGINT NOT NULL REFERENCES arb_orders(id),
    quantity NUMERIC NOT NULL CHECK (quantity >= 0),
    fee NUMERIC NOT NULL CHECK (fee >= 0),
    status TEXT NOT NULL CHECK (status IN ('verified','not_applicable')),
    applied_at TIMESTAMPTZ,
    application_audit JSONB,
    PRIMARY KEY (group_id, order_id),
    CHECK (status <> 'not_applicable' OR (quantity = 0 AND fee = 0)),
    CHECK ((applied_at IS NULL) = (application_audit IS NULL))
);
CREATE INDEX outcome_settlement_fee_allocations_order_idx ON outcome_settlement_fee_allocations(order_id);
-- 老列的固定 scale 会将新费用静默舍入；扩大精度不改变已有数值。
ALTER TABLE arb_orders ALTER COLUMN actual_rev TYPE NUMERIC;
ALTER TABLE arb_orders ALTER COLUMN actual_profit TYPE NUMERIC;
