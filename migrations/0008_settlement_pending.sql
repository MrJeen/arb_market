ALTER TABLE arb_orders
    DROP CONSTRAINT arb_orders_position_status_valid,
    ADD COLUMN settlement_pending_since TIMESTAMPTZ,
    ADD COLUMN settlement_pending_source TEXT,
    ADD COLUMN settlement_pending_result JSONB;

ALTER TABLE arb_orders
    ADD CONSTRAINT arb_orders_position_status_valid
        CHECK (position_status IN ('watching', 'settlement_pending', 'closed', 'settled')),
    ADD CONSTRAINT arb_orders_settlement_pending_complete
        CHECK (
            (settlement_pending_since IS NULL
             AND settlement_pending_source IS NULL
             AND settlement_pending_result IS NULL)
            OR
            (settlement_pending_since IS NOT NULL
             AND settlement_pending_source IS NOT NULL
             AND settlement_pending_result IS NOT NULL)
        ),
    ADD CONSTRAINT arb_orders_settlement_pending_state
        CHECK (
            (position_status = 'settlement_pending'
             AND settlement_pending_since IS NOT NULL)
            OR
            (position_status IN ('watching', 'closed')
             AND settlement_pending_since IS NULL)
            OR
            position_status = 'settled'
        );

DROP INDEX idx_arb_orders_position_lifecycle;
CREATE INDEX idx_arb_orders_position_lifecycle
    ON arb_orders (position_status, id)
    WHERE status = 'completed'
      AND position_status IN ('watching', 'settlement_pending');
