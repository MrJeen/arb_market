ALTER TABLE signed_envelopes ADD COLUMN IF NOT EXISTS submit_response JSONB;
ALTER TABLE signed_envelopes ADD COLUMN IF NOT EXISTS book_snapshot JSONB;
