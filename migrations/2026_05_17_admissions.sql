CREATE TABLE IF NOT EXISTS institutions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source_url TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admissions (
    id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT REFERENCES institutions(id) ON DELETE SET NULL,
    entity_name TEXT NOT NULL,
    admission_type TEXT NOT NULL CHECK (admission_type IN ('UG','PG','PhD','Diploma','Certificate')),
    program_name TEXT NOT NULL,
    intake_year INTEGER NOT NULL,
    application_start_date DATE,
    application_end_date DATE,
    exam_date DATE,
    result_date DATE,
    application_link TEXT NOT NULL,
    eligibility_text TEXT,
    fee_inr INTEGER,
    mode TEXT NOT NULL DEFAULT 'online' CHECK (mode IN ('online','offline','both')),
    status TEXT NOT NULL DEFAULT 'upcoming' CHECK (status IN ('upcoming','ongoing','closed')),
    country TEXT NOT NULL DEFAULT 'India',
    state TEXT,
    source_url TEXT,
    source_name TEXT,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_admissions_entity_program_year UNIQUE(entity_id, program_name, intake_year)
);

CREATE INDEX IF NOT EXISTS idx_admissions_status_state_type ON admissions(status, state, admission_type);
CREATE INDEX IF NOT EXISTS idx_admissions_upcoming_dates ON admissions(status, application_start_date, application_end_date, exam_date);
CREATE INDEX IF NOT EXISTS idx_admissions_country ON admissions(country);
