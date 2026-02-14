# PRD: Data Model & Search Ranking

## Problem

Searching for a common title like "Nobody" returns results in alphabetical order. There's no signal to surface the most relevant result — an obscure 1970s title ranks the same as a major 2025 release.

## Data We Present

Every field we store, where it comes from, and how it gets into the database.

### Titles

| Field | DB Column | Source | Sync Mechanism |
|-------|-----------|--------|----------------|
| Display name | `display_name` | IMDb `title.basics.tsv.gz` → `primaryTitle` | IMDb batch sync |
| Type | `type` | IMDb `title.basics.tsv.gz` → `titleType` | IMDb batch sync |
| Start Year | `start_year` | IMDb `title.basics.tsv.gz` → `startYear` | IMDb batch sync |
| End Year | `end_year` | IMDb `title.basics.tsv.gz` → `endYear` | IMDb batch sync |
| IMDb ID | `imdb_id` | IMDb `title.basics.tsv.gz` → `tconst` | IMDb batch sync |
| Poster image | `image_url` | TMDB Find API → `poster_path` | TMDB batch sync (shows) / on-demand lazy fetch (all) |
| TMDB ID | `tmdb_id` | TMDB Find API → `id` | TMDB batch sync / on-demand lazy fetch |

### Derived Fields (computed in Go, not stored in DB)

| Field | JSON Key | Logic |
|-------|----------|-------|
| Series finished | `is_series_finished` | `true` if `end_year` is set (non-null) |
| Season finished | `is_season_finished` | If series finished → all seasons `true`. If ongoing → `true` for seasons below the max season number |

These flags are computed in `getShowByID()` after loading seasons and returned on the `Show` and `Season` JSON objects respectively.

### Episodes

| Field | DB Column | Source | Sync Mechanism |
|-------|-----------|--------|----------------|
| Season/episode number | `season`, `episode` | IMDb `title.episode.tsv.gz` | IMDb batch sync |
| Display name | `display_name` | IMDb `title.basics.tsv.gz` → `primaryTitle` (cross-referenced by episode tconst) | IMDb batch sync |
| Synopsis | `synopsis` | TMDB Episode API → `overview` | On-demand lazy fetch |
| Still image | `image_url` | TMDB Episode API → `still_path` | TMDB batch sync / on-demand lazy fetch |
| Air date | `air_date` | TMDB Episode API → `air_date` | TMDB batch sync / on-demand lazy fetch |
| Runtime | `runtime_minutes` | TMDB Episode API → `runtime` | TMDB batch sync / on-demand lazy fetch |

### Now Stored (previously proposed)

These fields have been implemented since the original PRD:

| Field | DB Column | Source | Sync Mechanism |
|-------|-----------|--------|----------------|
| Num votes | `num_votes` | IMDb `title.ratings.tsv.gz` → `numVotes` | IMDb batch sync. Used for search ranking. |
| Average rating | `average_rating` | IMDb `title.ratings.tsv.gz` → `averageRating` | IMDb batch sync |
| Genres | `title_genres` table | IMDb `title.basics.tsv.gz` → `genres` | IMDb batch sync |
| Runtime (titles) | `runtime_minutes` | IMDb `title.basics.tsv.gz` → `runtimeMinutes` | IMDb batch sync |
| Original title | `original_title` | IMDb `title.basics.tsv.gz` → `originalTitle` | IMDb batch sync |
| Origin country | `origin_country` | TMDB API → `origin_country` | TMDB backfill / on-demand lazy fetch |
| Original language | `original_language` | TMDB API → `original_language` | TMDB backfill / on-demand lazy fetch |
| TMDB popularity | `tmdb_popularity` | TMDB API → `popularity` | TMDB backfill / on-demand lazy fetch |

### Not Yet Stored (Available)

| Field | Source | Notes |
|-------|--------|-------|
| TMDB vote count | TMDB API → `vote_count` | Similar to IMDb numVotes but smaller dataset |
| People (cast/crew) | IMDb `title.principals.tsv.gz`, `name.basics.tsv.gz` | Not downloaded at all yet |
| Alt titles by region | IMDb `title.akas.tsv.gz` | Not downloaded. Has region/language per alt title. |

## Sync Mechanisms

### 1. IMDb Batch Sync

**Code:** `cmd/sync/main.go`
**Trigger:** Manual CLI run
**Freshness:** IMDb updates dataset files daily. We run this on-demand — could be scheduled (e.g. weekly cron).

Downloads TSV files from `datasets.imdbws.com`, diffs against existing DB rows, and does batched inserts/updates. Uses parallel workers (default 8) with configurable batch size (default 5000).

**Files currently downloaded:**
- `title.basics.tsv.gz` — all titles (~10M rows, we filter to movies + shows + episodes). Parses `startYear` (col 5) and `endYear` (col 6).
- `title.episode.tsv.gz` — episode-to-parent-show mapping
- `title.ratings.tsv.gz` — `numVotes` and `averageRating` per title

**Pipeline:**
1. Download `.tsv.gz` files (skips if unchanged via `If-Modified-Since` / hash check)
2. Load all existing titles/movies/shows from DB into memory maps
3. Scan `title.basics.tsv.gz`, diff against existing, collect inserts/updates (including `start_year`, `end_year`, `runtime_minutes`, `original_title`, genres)
4. Batched `INSERT` / `UPDATE` with parallel workers
5. Sync genre associations (`title_genres`)
6. Same for episodes (two-pass: seasons first, then episodes)
7. Import ratings from `title.ratings.tsv.gz` (batched update of `num_votes`, `average_rating`)

### 2. TMDB Batch Sync

**Code:** `cmd/sync-images/main.go`
**Trigger:** Manual CLI run (requires `-key` API key)
**Freshness:** One-time per title. Skips titles that already have `image_url` (configurable with `--skip-synced`).

Iterates over shows missing images. For each show:
1. TMDB Find API (look up by IMDb ID) → get poster + TMDB ID
2. For each episode → TMDB Episode API → get still image, air date, runtime
3. Rate-limited at ~40 req/sec with retry on 429s

Currently **only processes shows**, not movies. Movies get images via on-demand lazy fetch.

### 3. On-Demand Lazy Fetch

**Code:** `main.go` — `maybeFetchImage()`, `maybeFetchEpisodes()`
**Trigger:** User visits a detail page (movie or show)
**Freshness:** Fetches once, then cached in DB permanently. Uses sentinel value `"none"` / `"TMDB_NOT_FOUND_DO_NOT_RETRY"` to avoid re-fetching titles not found on TMDB.

When a user views a title detail page:
1. If `TMDB_API_KEY` is set and `image_url` is null → fetch poster from TMDB Find API, store it
2. For shows: if episodes are missing stills/air dates → batch fetch from TMDB Episode API

This is the only mechanism that handles **movies** — the TMDB batch sync only covers shows.

## Implemented: numVotes for Search Ranking

IMDb's `numVotes` is used as the primary search ranking signal. All title search and browse queries order by `num_votes DESC NULLS LAST`. This was chosen because:
- A major release has 300k+ votes; an obscure same-name title has <500
- Free bulk download from the same IMDb datasets pipeline
- No API key needed
- Stable signal (vote counts only go up) unlike TMDB `popularity` which fluctuates daily

```sql
ORDER BY t.num_votes DESC NULLS LAST, t.display_name LIMIT 100
```
