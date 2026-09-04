ALTER TABLE legs ADD COLUMN IF NOT EXISTS service TEXT;
CREATE INDEX IF NOT EXISTS idx_legs_service ON legs (service) WHERE service IS NOT NULL;
