-- Migration 001: Unify title IDs
--
-- Eliminates the movies and shows tables by moving all relevant data
-- directly onto titles and show_seasons. After this migration:
--   - titles.is_series_finished holds the old shows.is_series_finished value
--   - show_seasons.title_id points directly to titles.id (was shows.id)
--   - movies and shows tables are dropped
--
-- Old schema:
--   movies(id, title_id FK titles.id)
--   shows(id, title_id FK titles.id, is_series_finished)
--   show_seasons(id, show_id FK shows.id, season, UNIQUE(show_id, season))
--
-- New schema:
--   titles(..., is_series_finished BOOLEAN)
--   show_seasons(id, title_id FK titles.id, season, UNIQUE(title_id, season))

BEGIN;

-- ============================================================
-- Step 1: Add is_series_finished to titles
-- New column — no backfill needed (shows table didn't have it).
-- ============================================================
ALTER TABLE titles ADD COLUMN IF NOT EXISTS is_series_finished BOOLEAN;

-- ============================================================
-- Step 2: Add title_id column to show_seasons (nullable for now)
-- We need it nullable so existing rows aren't rejected before backfill.
-- ============================================================
ALTER TABLE show_seasons ADD COLUMN title_id INTEGER;

-- ============================================================
-- Step 3: Backfill show_seasons.title_id from shows.title_id
-- Each show_seasons row pointed at shows.id; we resolve through
-- shows to get the real titles.id.
-- ============================================================
UPDATE show_seasons ss
   SET title_id = s.title_id
  FROM shows s
 WHERE ss.show_id = s.id;

-- ============================================================
-- Step 4: Make show_seasons.title_id NOT NULL
-- All rows should now have a value from the backfill above.
-- ============================================================
ALTER TABLE show_seasons ALTER COLUMN title_id SET NOT NULL;

-- ============================================================
-- Step 5: Drop the old unique constraint and index on show_id
-- Must happen before we drop the show_id column.
-- ============================================================
ALTER TABLE show_seasons DROP CONSTRAINT IF EXISTS show_seasons_show_id_season_key;

DROP INDEX IF EXISTS idx_show_seasons_show;

-- ============================================================
-- Step 6: Drop the old show_id FK constraint and column
-- ============================================================
ALTER TABLE show_seasons DROP CONSTRAINT IF EXISTS show_seasons_show_id_fkey;
ALTER TABLE show_seasons DROP COLUMN show_id;

-- ============================================================
-- Step 7: Add new unique constraint and FK on title_id
-- ============================================================
ALTER TABLE show_seasons ADD CONSTRAINT show_seasons_title_id_season_key UNIQUE (title_id, season);

ALTER TABLE show_seasons
    ADD CONSTRAINT show_seasons_title_id_fkey
    FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE;

-- ============================================================
-- Step 8: Create the new index (replaces old idx_show_seasons_show)
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_show_seasons_title ON show_seasons(title_id);

-- ============================================================
-- Step 9: Drop movies table
-- No data needs migrating — movies had only (id, title_id) and
-- everything useful already lives on titles.
-- ============================================================
DROP TABLE IF EXISTS movies;

-- ============================================================
-- Step 10: Drop shows table
-- All data (is_series_finished, title_id linkage) has been migrated.
-- ============================================================
DROP TABLE IF EXISTS shows;

COMMIT;
