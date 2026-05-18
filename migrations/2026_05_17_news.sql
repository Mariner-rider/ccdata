CREATE TABLE IF NOT EXISTS institutions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source_url TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_articles (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    content_url TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('admission_update','exam_notification','result','scholarship','welfare_scheme','policy','campus_news','ranking','abroad')),
    tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    published_at DATE,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    related_entity_ids INT[] NOT NULL DEFAULT ARRAY[]::INT[],
    image_url TEXT,
    is_featured BOOLEAN NOT NULL DEFAULT FALSE,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_news_title_source_published UNIQUE(title, source_name, published_at)
);

CREATE TABLE IF NOT EXISTS news_article_entities (
    article_id BIGINT NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    entity_id BIGINT NOT NULL REFERENCES institutions(id) ON DELETE CASCADE,
    PRIMARY KEY(article_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_news_category_published ON news_articles(category, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_featured ON news_articles(is_featured, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_related_entities ON news_articles USING GIN (related_entity_ids);
CREATE INDEX IF NOT EXISTS idx_news_tags ON news_articles USING GIN (tags);
