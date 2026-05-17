CREATE TABLE IF NOT EXISTS institutions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source_url TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research_items (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    authors TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    abstract TEXT,
    type TEXT NOT NULL CHECK (type IN ('paper','thesis','ongoing_project','patent')),
    field TEXT NOT NULL CHECK (field IN ('engineering','medicine','arts','commerce','science','law','other')),
    subfield TEXT,
    keywords TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    institution_id BIGINT REFERENCES institutions(id) ON DELETE SET NULL,
    institution_name TEXT,
    published_date DATE,
    doi TEXT,
    arxiv_id TEXT,
    pdf_url TEXT,
    source_url TEXT,
    citation_count INTEGER,
    status TEXT NOT NULL CHECK (status IN ('published','preprint','ongoing')),
    title_author_hash TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_doi ON research_items(doi) WHERE doi IS NOT NULL AND doi <> '';
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_arxiv_id ON research_items(arxiv_id) WHERE arxiv_id IS NOT NULL AND arxiv_id <> '';
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_title_author_hash ON research_items(title_author_hash);
CREATE INDEX IF NOT EXISTS idx_research_field_type_year ON research_items(field, type, published_date DESC);
CREATE INDEX IF NOT EXISTS idx_research_institution ON research_items(institution_id);
CREATE INDEX IF NOT EXISTS idx_research_keywords ON research_items USING GIN (keywords);
CREATE INDEX IF NOT EXISTS idx_research_search ON research_items USING GIN (to_tsvector('english', title || ' ' || COALESCE(abstract,'') || ' ' || COALESCE(institution_name,'') || ' ' || field || ' ' || COALESCE(subfield,'')));
