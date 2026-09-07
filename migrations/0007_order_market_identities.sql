CREATE TABLE arb_order_market_identities (
    order_id BIGINT NOT NULL REFERENCES arb_orders(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    market_id TEXT NOT NULL,
    CONSTRAINT arb_order_market_identities_pkey PRIMARY KEY (order_id, platform),
    CONSTRAINT arb_order_market_identities_platform_nonempty CHECK (btrim(platform) <> ''),
    CONSTRAINT arb_order_market_identities_market_id_nonempty CHECK (btrim(market_id) <> '')
);

INSERT INTO arb_order_market_identities (order_id, platform, market_id)
SELECT id, 'polymarket', polymarket_condition_id
FROM arb_orders
WHERE polymarket_condition_id IS NOT NULL;

INSERT INTO arb_order_market_identities (order_id, platform, market_id)
SELECT id, 'outcome', outcome_id::TEXT
FROM arb_orders
WHERE outcome_id IS NOT NULL;

CREATE INDEX idx_arb_order_market_identities_lookup
    ON arb_order_market_identities (platform, market_id);

DROP INDEX idx_arb_orders_market_identity;

ALTER TABLE arb_orders
    DROP CONSTRAINT arb_orders_market_identity_complete,
    DROP CONSTRAINT arb_orders_outcome_id_positive,
    DROP COLUMN polymarket_condition_id,
    DROP COLUMN outcome_id;
