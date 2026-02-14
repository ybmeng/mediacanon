-- MediaCanon Schema (minimal)

CREATE TABLE IF NOT EXISTS titles (
    id SERIAL PRIMARY KEY,
    type VARCHAR(20) NOT NULL CHECK (type IN ('movie', 'show')),
    display_name VARCHAR(500) NOT NULL,
    year INTEGER,
    imdb_id VARCHAR(20) UNIQUE,
    image_url TEXT,
    tmdb_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    num_votes INTEGER,
    average_rating REAL,
    original_title VARCHAR(500),
    original_language VARCHAR(10),
    release_date DATE
);

CREATE TABLE IF NOT EXISTS movies (
    id SERIAL PRIMARY KEY,
    title_id INTEGER NOT NULL UNIQUE REFERENCES titles(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS shows (
    id SERIAL PRIMARY KEY,
    title_id INTEGER NOT NULL UNIQUE REFERENCES titles(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS show_seasons (
    id SERIAL PRIMARY KEY,
    show_id INTEGER NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    season INTEGER NOT NULL,
    UNIQUE(show_id, season)
);

CREATE TABLE IF NOT EXISTS show_episodes (
    id SERIAL PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES show_seasons(id) ON DELETE CASCADE,
    episode INTEGER NOT NULL,
    display_name VARCHAR(500),
    synopsis TEXT,
    UNIQUE(season_id, episode)
);

-- Genres
CREATE TABLE IF NOT EXISTS genres (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_genres (
    title_id INTEGER NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    genre_id INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (title_id, genre_id)
);

-- View tracking (rolling 7-day window)
CREATE TABLE IF NOT EXISTS title_views (
    id BIGSERIAL PRIMARY KEY,
    title_id INTEGER NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    viewed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Collections (YAML-defined, loaded at server startup)
CREATE TABLE IF NOT EXISTS collections (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    slug VARCHAR(200) NOT NULL UNIQUE,
    description TEXT,
    strategy VARCHAR(20) NOT NULL DEFAULT 'filter',
    filter_params JSONB,
    languages TEXT[],
    regions TEXT[],
    pinned BOOLEAN DEFAULT FALSE,
    active BOOLEAN DEFAULT TRUE,
    engagement_count REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Static/LLM collection title memberships
CREATE TABLE IF NOT EXISTS collection_titles (
    collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    title_id INTEGER NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
    rank INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (collection_id, title_id)
);

-- Collection click tracking (rolling 7-day window)
CREATE TABLE IF NOT EXISTS collection_clicks (
    id BIGSERIAL PRIMARY KEY,
    collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    clicked_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- New columns on titles
ALTER TABLE titles ADD COLUMN IF NOT EXISTS tmdb_popularity REAL;
ALTER TABLE titles ADD COLUMN IF NOT EXISTS runtime_minutes INTEGER;
ALTER TABLE titles ADD COLUMN IF NOT EXISTS origin_country VARCHAR(10);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_titles_type ON titles(type);
CREATE INDEX IF NOT EXISTS idx_titles_display_name ON titles(display_name);
CREATE INDEX IF NOT EXISTS idx_titles_imdb_id ON titles(imdb_id);
CREATE INDEX IF NOT EXISTS idx_show_seasons_show ON show_seasons(show_id);
CREATE INDEX IF NOT EXISTS idx_show_episodes_season ON show_episodes(season_id);
CREATE INDEX IF NOT EXISTS idx_titles_num_votes ON titles(num_votes);
CREATE INDEX IF NOT EXISTS idx_title_genres_title ON title_genres(title_id);
CREATE INDEX IF NOT EXISTS idx_title_genres_genre ON title_genres(genre_id);
CREATE INDEX IF NOT EXISTS idx_title_views_title ON title_views(title_id);
CREATE INDEX IF NOT EXISTS idx_title_views_viewed_at ON title_views(viewed_at);
CREATE INDEX IF NOT EXISTS idx_titles_popularity ON titles(tmdb_popularity);
CREATE INDEX IF NOT EXISTS idx_titles_rating ON titles(average_rating);
CREATE INDEX IF NOT EXISTS idx_titles_year ON titles(year);
CREATE INDEX IF NOT EXISTS idx_collection_clicks_collection ON collection_clicks(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_clicks_clicked_at ON collection_clicks(clicked_at);
CREATE INDEX IF NOT EXISTS idx_collections_active ON collections(active);
CREATE INDEX IF NOT EXISTS idx_collection_titles_collection ON collection_titles(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_titles_rank ON collection_titles(collection_id, rank);

-- Custom genres
ALTER TABLE genres ADD COLUMN IF NOT EXISTS is_custom BOOLEAN DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_titles_original_language ON titles(original_language);

CREATE TABLE IF NOT EXISTS custom_genre_reviews (
    title_id INTEGER PRIMARY KEY REFERENCES titles(id) ON DELETE CASCADE,
    reviewed_at TIMESTAMP DEFAULT NOW()
);

-- TMDB backfill flag (set TRUE to re-fetch metadata on next visit)
ALTER TABLE titles ADD COLUMN IF NOT EXISTS needs_backfill_tmdb BOOLEAN DEFAULT TRUE;
CREATE INDEX IF NOT EXISTS idx_titles_needs_backfill ON titles(needs_backfill_tmdb) WHERE needs_backfill_tmdb = TRUE;

-- Sync state (track file hashes to skip unchanged IMDb imports)
CREATE TABLE IF NOT EXISTS sync_state (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Source attribution for title views
ALTER TABLE title_views ADD COLUMN IF NOT EXISTS source VARCHAR(100);
CREATE INDEX IF NOT EXISTS idx_title_views_source ON title_views(source);

-- Track when episodes were last checked against TMDB (for daily lazy re-fetch)
ALTER TABLE titles ADD COLUMN IF NOT EXISTS episodes_checked_at TIMESTAMP;
