CREATE UNIQUE INDEX IF NOT EXISTS idx_arb_orders_active_topic
    ON arb_orders (event_id, unified_index)
    WHERE status IN ('pending', 'actived');
