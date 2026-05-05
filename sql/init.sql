CREATE TABLE IF NOT EXISTS source_registry (
    id BIGSERIAL PRIMARY KEY,
    seed_url TEXT NOT NULL,
    crawl_frequency_minutes INTEGER NOT NULL DEFAULT 60,
    is_active BOOLEAN NOT NULL DEFAULT true,
    last_dispatched_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS page_state (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    url_hash CHAR(64) NOT NULL UNIQUE,
    content_hash CHAR(64),
    s3_key TEXT,
    http_status INTEGER,
    etag TEXT,
    last_modified TEXT,
    last_crawled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_page_state_last_crawled_at ON page_state(last_crawled_at);

CREATE TABLE IF NOT EXISTS crawl_logs (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT,
    url TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_logs_event_ts ON crawl_logs(event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_crawl_logs_source_id ON crawl_logs(source_id);

CREATE TABLE IF NOT EXISTS parsed_college_data (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    url_hash CHAR(64) NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_parsed_college_data_extracted_at ON parsed_college_data(extracted_at DESC);

CREATE TABLE IF NOT EXISTS normalized_records (
    id BIGSERIAL PRIMARY KEY,
    source_url TEXT,
    category TEXT NOT NULL,
    record_hash CHAR(64) NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    mapped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_normalized_records_category ON normalized_records(category);
CREATE INDEX IF NOT EXISTS idx_normalized_records_mapped_at ON normalized_records(mapped_at DESC);

CREATE TABLE IF NOT EXISTS enriched_records (
    id BIGSERIAL PRIMARY KEY,
    source_url TEXT,
    category TEXT,
    record_hash CHAR(64) NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    enriched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enriched_records_category ON enriched_records(category);
CREATE INDEX IF NOT EXISTS idx_enriched_records_enriched_at ON enriched_records(enriched_at DESC);

CREATE TABLE IF NOT EXISTS crawl_trigger_events (
    id BIGSERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    trigger_reason TEXT NOT NULL,
    metadata JSONB,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_trigger_events_triggered_at ON crawl_trigger_events(triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_crawl_trigger_events_type ON crawl_trigger_events(trigger_type);

ALTER TABLE source_registry
ADD COLUMN IF NOT EXISTS category TEXT;

CREATE TABLE IF NOT EXISTS user_registration_events (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    profile_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    crawl_requested BOOLEAN NOT NULL DEFAULT false,
    crawl_requested_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_user_registration_events_unprocessed
    ON user_registration_events(crawl_requested, created_at);

CREATE TABLE IF NOT EXISTS airflow_pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value BIGINT NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_airflow_pipeline_metrics_name_time
    ON airflow_pipeline_metrics(metric_name, measured_at DESC);

-- =========================================================
-- Domain schema for high-scale content entities (80M+ users)
-- =========================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Institutes: canonical organization root entity.
CREATE TABLE IF NOT EXISTS institutes (
    id BIGSERIAL PRIMARY KEY,
    institute_code TEXT UNIQUE,
    name TEXT NOT NULL,
    normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED,
    institute_type TEXT, -- university, college-group, training-center, etc.
    country_code CHAR(2) DEFAULT 'US',
    state_code TEXT,
    city TEXT,
    ranking_score NUMERIC(8, 3),
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_doc TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(city, '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(state_code, '')), 'B')
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_institutes_name_trgm ON institutes USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_institutes_country_state_city ON institutes(country_code, state_code, city);
CREATE INDEX IF NOT EXISTS idx_institutes_attrs_gin ON institutes USING GIN (attrs jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_institutes_search_doc ON institutes USING GIN (search_doc);

-- Colleges: leaf-level educational entities under institutes.
CREATE TABLE IF NOT EXISTS colleges (
    id BIGSERIAL PRIMARY KEY,
    institute_id BIGINT NOT NULL REFERENCES institutes(id) ON DELETE CASCADE,
    college_code TEXT UNIQUE,
    name TEXT NOT NULL,
    normalized_name TEXT GENERATED ALWAYS AS (lower(name)) STORED,
    campus_city TEXT,
    campus_state TEXT,
    accreditation TEXT,
    ownership_type TEXT,
    established_year SMALLINT,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_doc TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(campus_city, '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(campus_state, '')), 'B')
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_colleges_institute_id ON colleges(institute_id);
CREATE INDEX IF NOT EXISTS idx_colleges_name_trgm ON colleges USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_colleges_location ON colleges(campus_state, campus_city);
CREATE INDEX IF NOT EXISTS idx_colleges_attrs_gin ON colleges USING GIN (attrs jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_colleges_search_doc ON colleges USING GIN (search_doc);

-- Admissions: program-level or cycle-level admission windows/requirements.
CREATE TABLE IF NOT EXISTS admissions (
    id BIGSERIAL PRIMARY KEY,
    college_id BIGINT NOT NULL REFERENCES colleges(id) ON DELETE CASCADE,
    academic_cycle TEXT NOT NULL, -- e.g. 2026-fall
    program_name TEXT NOT NULL,
    degree_level TEXT,
    application_start_date DATE,
    application_end_date DATE,
    tuition_min NUMERIC(14, 2),
    tuition_max NUMERIC(14, 2),
    currency CHAR(3) DEFAULT 'USD',
    admission_url TEXT,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (college_id, academic_cycle, program_name)
);

CREATE INDEX IF NOT EXISTS idx_admissions_college_cycle ON admissions(college_id, academic_cycle);
CREATE INDEX IF NOT EXISTS idx_admissions_program_trgm ON admissions USING GIN (program_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_admissions_date_window ON admissions(application_start_date, application_end_date);
CREATE INDEX IF NOT EXISTS idx_admissions_attrs_gin ON admissions USING GIN (attrs jsonb_path_ops);

-- Jobs: campus placements and external job opportunities.
CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    college_id BIGINT REFERENCES colleges(id) ON DELETE SET NULL,
    institute_id BIGINT REFERENCES institutes(id) ON DELETE SET NULL,
    external_job_id TEXT,
    title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    job_type TEXT,
    work_mode TEXT,
    location_city TEXT,
    location_state TEXT,
    salary_min NUMERIC(14, 2),
    salary_max NUMERIC(14, 2),
    currency CHAR(3) DEFAULT 'USD',
    posted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    apply_url TEXT,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_doc TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(company_name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(location_city, '')), 'B')
    ) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_external_job_id ON jobs(external_job_id) WHERE external_job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_college_posted_at ON jobs(college_id, posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_company_trgm ON jobs USING GIN (company_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_jobs_title_trgm ON jobs USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_jobs_attrs_gin ON jobs USING GIN (attrs jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_jobs_search_doc ON jobs USING GIN (search_doc);

-- Scholarships: offer-level scholarship metadata.
CREATE TABLE IF NOT EXISTS scholarships (
    id BIGSERIAL PRIMARY KEY,
    institute_id BIGINT REFERENCES institutes(id) ON DELETE SET NULL,
    college_id BIGINT REFERENCES colleges(id) ON DELETE SET NULL,
    scholarship_code TEXT UNIQUE,
    title TEXT NOT NULL,
    sponsor_name TEXT,
    amount_min NUMERIC(14, 2),
    amount_max NUMERIC(14, 2),
    currency CHAR(3) DEFAULT 'USD',
    eligibility_summary TEXT,
    application_deadline DATE,
    application_url TEXT,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    search_doc TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(sponsor_name, '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(eligibility_summary, '')), 'C')
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scholarships_deadline ON scholarships(application_deadline);
CREATE INDEX IF NOT EXISTS idx_scholarships_title_trgm ON scholarships USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_scholarships_attrs_gin ON scholarships USING GIN (attrs jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_scholarships_search_doc ON scholarships USING GIN (search_doc);

-- Reviews: high-volume, user-generated table partitioned for 80M+ users scale.
CREATE TABLE IF NOT EXISTS reviews (
    id BIGSERIAL NOT NULL,
    user_id BIGINT NOT NULL,
    target_type TEXT NOT NULL CHECK (target_type IN ('college', 'institute', 'job', 'scholarship')),
    target_id BIGINT NOT NULL,
    rating SMALLINT CHECK (rating BETWEEN 1 AND 5),
    review_title TEXT,
    review_text TEXT,
    sentiment_label TEXT,
    sentiment_score NUMERIC(6, 5),
    is_verified BOOLEAN NOT NULL DEFAULT false,
    attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Rolling partitions (example windows, add future partitions via maintenance job).
CREATE TABLE IF NOT EXISTS reviews_2026_q1 PARTITION OF reviews
    FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS reviews_2026_q2 PARTITION OF reviews
    FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS reviews_2026_q3 PARTITION OF reviews
    FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS reviews_2026_q4 PARTITION OF reviews
    FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');

CREATE INDEX IF NOT EXISTS idx_reviews_user_id_created_at ON reviews(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reviews_target ON reviews(target_type, target_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_attrs_gin ON reviews USING GIN (attrs jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_reviews_text_trgm ON reviews USING GIN (review_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_reviews_created_at_brin ON reviews USING BRIN (created_at);

-- =========================================================
-- Audit logs (partitioned by month for write-heavy workloads)
-- =========================================================
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL NOT NULL,
    entity_name TEXT NOT NULL,
    entity_pk TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_by TEXT,
    request_id TEXT,
    before_data JSONB,
    after_data JSONB,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, changed_at)
) PARTITION BY RANGE (changed_at);

CREATE TABLE IF NOT EXISTS audit_logs_2026_04 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS audit_logs_2026_05 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE INDEX IF NOT EXISTS idx_audit_logs_entity_time ON audit_logs(entity_name, entity_pk, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_after_gin ON audit_logs USING GIN (after_data jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_audit_logs_before_gin ON audit_logs USING GIN (before_data jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_audit_logs_changed_at_brin ON audit_logs USING BRIN (changed_at);

CREATE OR REPLACE FUNCTION audit_row_change() RETURNS trigger AS $$
DECLARE
    effective_request_id TEXT;
    effective_actor TEXT;
BEGIN
    effective_request_id := current_setting('app.request_id', true);
    effective_actor := current_setting('app.user_id', true);

    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_logs(entity_name, entity_pk, action, changed_by, request_id, before_data, after_data, changed_at)
        VALUES (TG_TABLE_NAME, NEW.id::text, TG_OP, effective_actor, effective_request_id, NULL, to_jsonb(NEW), NOW());
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_logs(entity_name, entity_pk, action, changed_by, request_id, before_data, after_data, changed_at)
        VALUES (TG_TABLE_NAME, NEW.id::text, TG_OP, effective_actor, effective_request_id, to_jsonb(OLD), to_jsonb(NEW), NOW());
        RETURN NEW;
    ELSE
        INSERT INTO audit_logs(entity_name, entity_pk, action, changed_by, request_id, before_data, after_data, changed_at)
        VALUES (TG_TABLE_NAME, OLD.id::text, TG_OP, effective_actor, effective_request_id, to_jsonb(OLD), NULL, NOW());
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_institutes ON institutes;
CREATE TRIGGER trg_audit_institutes AFTER INSERT OR UPDATE OR DELETE ON institutes
FOR EACH ROW EXECUTE FUNCTION audit_row_change();

DROP TRIGGER IF EXISTS trg_audit_colleges ON colleges;
CREATE TRIGGER trg_audit_colleges AFTER INSERT OR UPDATE OR DELETE ON colleges
FOR EACH ROW EXECUTE FUNCTION audit_row_change();

DROP TRIGGER IF EXISTS trg_audit_admissions ON admissions;
CREATE TRIGGER trg_audit_admissions AFTER INSERT OR UPDATE OR DELETE ON admissions
FOR EACH ROW EXECUTE FUNCTION audit_row_change();

DROP TRIGGER IF EXISTS trg_audit_jobs ON jobs;
CREATE TRIGGER trg_audit_jobs AFTER INSERT OR UPDATE OR DELETE ON jobs
FOR EACH ROW EXECUTE FUNCTION audit_row_change();

DROP TRIGGER IF EXISTS trg_audit_scholarships ON scholarships;
CREATE TRIGGER trg_audit_scholarships AFTER INSERT OR UPDATE OR DELETE ON scholarships
FOR EACH ROW EXECUTE FUNCTION audit_row_change();

DROP TRIGGER IF EXISTS trg_audit_reviews ON reviews;
CREATE TRIGGER trg_audit_reviews AFTER INSERT OR UPDATE OR DELETE ON reviews
FOR EACH ROW EXECUTE FUNCTION audit_row_change();

-- =========================================================
-- Event-driven college page backend + chatbot sync schema
-- =========================================================

CREATE TABLE IF NOT EXISTS college_pages (
    id BIGSERIAL PRIMARY KEY,
    college_id BIGINT,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_college_pages_college_id ON college_pages(college_id);
CREATE INDEX IF NOT EXISTS idx_college_pages_status ON college_pages(status);
CREATE INDEX IF NOT EXISTS idx_college_pages_source_payload_gin ON college_pages USING GIN (source_payload jsonb_path_ops);

CREATE TABLE IF NOT EXISTS college_page_sections (
    id BIGSERIAL PRIMARY KEY,
    page_id BIGINT NOT NULL REFERENCES college_pages(id) ON DELETE CASCADE,
    section_key TEXT NOT NULL CHECK (section_key IN ('info', 'courses', 'faculty', 'hostel', 'placement')),
    section_title TEXT NOT NULL,
    body JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash CHAR(64) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(page_id, section_key)
);

CREATE INDEX IF NOT EXISTS idx_college_page_sections_page_id ON college_page_sections(page_id);
CREATE INDEX IF NOT EXISTS idx_college_page_sections_body_gin ON college_page_sections USING GIN (body jsonb_path_ops);

CREATE TABLE IF NOT EXISTS college_page_sync_events (
    id BIGSERIAL PRIMARY KEY,
    page_id BIGINT NOT NULL REFERENCES college_pages(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    sync_status TEXT NOT NULL DEFAULT 'pending',
    sync_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_college_page_sync_events_status_created_at
    ON college_page_sync_events(sync_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_college_page_sync_events_payload_gin
    ON college_page_sync_events USING GIN (event_payload jsonb_path_ops);

-- Mirror table for chatbot DB sync target (for local/dev fallback).
CREATE TABLE IF NOT EXISTS chatbot_college_pages (
    id BIGSERIAL PRIMARY KEY,
    page_slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chatbot_college_pages_payload_gin
    ON chatbot_college_pages USING GIN (payload jsonb_path_ops);

-- Review ingestion performance indexes.
CREATE INDEX IF NOT EXISTS idx_reviews_source ON reviews(((attrs->>'source')));
CREATE INDEX IF NOT EXISTS idx_reviews_fake_prob ON reviews(((attrs->>'fake_probability')));
