ALTER TABLE arb_orders
    ADD COLUMN polymarket_condition_id TEXT,
    ADD COLUMN outcome_id BIGINT,
    ADD COLUMN position_status VARCHAR(16) NOT NULL DEFAULT 'watching',
    ADD COLUMN lifecycle_action VARCHAR(32),
    ADD COLUMN lifecycle_claim_id UUID,
    ADD COLUMN lifecycle_claimed_at TIMESTAMPTZ,
    ADD COLUMN settlement_source TEXT,
    ADD COLUMN settlement_result JSONB,
    ADD COLUMN settled_at TIMESTAMPTZ;

ALTER TABLE arb_orders
    ADD CONSTRAINT arb_orders_market_identity_complete
        CHECK ((polymarket_condition_id IS NULL) = (outcome_id IS NULL)),
    ADD CONSTRAINT arb_orders_outcome_id_positive
        CHECK (outcome_id IS NULL OR outcome_id > 0),
    ADD CONSTRAINT arb_orders_position_status_valid
        CHECK (position_status IN ('watching', 'closed', 'settled')),
    ADD CONSTRAINT arb_orders_lifecycle_action_valid
        CHECK (lifecycle_action IS NULL OR lifecycle_action IN ('take_profit', 'rebalance')),
    ADD CONSTRAINT arb_orders_lifecycle_claim_complete
        CHECK (
            (lifecycle_action IS NULL AND lifecycle_claim_id IS NULL AND lifecycle_claimed_at IS NULL)
            OR
            (lifecycle_action IS NOT NULL AND lifecycle_claim_id IS NOT NULL AND lifecycle_claimed_at IS NOT NULL)
        ),
    ADD CONSTRAINT arb_orders_settlement_complete
        CHECK (
            (settled_at IS NULL AND settlement_source IS NULL AND settlement_result IS NULL)
            OR
            (settled_at IS NOT NULL AND settlement_source IS NOT NULL AND settlement_result IS NOT NULL)
        );

ALTER TABLE legs
    ADD COLUMN lifecycle_claim_id UUID;

CREATE INDEX idx_legs_lifecycle_claim
    ON legs (order_id, lifecycle_claim_id)
    WHERE lifecycle_claim_id IS NOT NULL;

CREATE INDEX idx_arb_orders_position_lifecycle
    ON arb_orders (position_status, id)
    WHERE status = 'completed' AND position_status = 'watching';

CREATE INDEX idx_arb_orders_market_identity
    ON arb_orders (polymarket_condition_id, outcome_id)
    WHERE polymarket_condition_id IS NOT NULL;
