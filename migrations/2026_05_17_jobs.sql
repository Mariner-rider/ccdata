CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    organization TEXT NOT NULL,
    job_type TEXT NOT NULL CHECK (job_type IN ('govt','private','internship','contract')),
    category TEXT NOT NULL CHECK (category IN ('tech','banking','defence','railway','teaching','other')),
    vacancies INTEGER,
    eligibility_text TEXT,
    age_limit TEXT,
    pay_scale TEXT,
    location TEXT,
    application_start_date DATE,
    application_end_date DATE,
    application_link TEXT NOT NULL,
    official_notification_pdf_url TEXT,
    exam_date DATE,
    result_date DATE,
    source_site TEXT,
    country TEXT NOT NULL DEFAULT 'India',
    state TEXT,
    status TEXT NOT NULL CHECK (status IN ('upcoming','ongoing','closed')),
    requires_login BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT uq_jobs_title_org_end_constraint UNIQUE(title, organization, application_end_date),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_title_org_end
ON jobs(title, organization, COALESCE(application_end_date, DATE '0001-01-01'));

CREATE INDEX IF NOT EXISTS idx_jobs_type_category_state_status ON jobs(job_type, category, state, status);
CREATE INDEX IF NOT EXISTS idx_jobs_internships ON jobs(status, location) WHERE job_type = 'internship';
CREATE INDEX IF NOT EXISTS idx_jobs_search_text ON jobs USING GIN (to_tsvector('english', title || ' ' || organization || ' ' || category || ' ' || COALESCE(location,'') || ' ' || COALESCE(eligibility_text,'')));
