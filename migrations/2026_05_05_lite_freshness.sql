ALTER TABLE normalized_records
ADD COLUMN IF NOT EXISTS source_url_text TEXT,
ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS content_hash CHAR(64),
ADD COLUMN IF NOT EXISTS confidence_score NUMERIC(5,4),
ADD COLUMN IF NOT EXISTS extraction_method TEXT,
ADD COLUMN IF NOT EXISTS freshness_status TEXT;

CREATE INDEX IF NOT EXISTS idx_normalized_records_last_crawled_at ON normalized_records(last_crawled_at);
CREATE INDEX IF NOT EXISTS idx_normalized_records_freshness_status ON normalized_records(freshness_status);
