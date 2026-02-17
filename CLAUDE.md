# Project: MediaCanon

Open movie and TV metadata database (https://mediacanon.org) powered by IMDb and TMDB data. Runs as a macOS menu bar app with a web dashboard, JSON API, and PostgreSQL backend.

## Architecture

Single-binary Go monolith with PostgreSQL. Server-rendered HTML frontend (Go templates), JSON API, and macOS menu bar app (systray). Collections loaded from embedded YAML files at startup. Carousel data cached in-memory for fast discover page rendering.

```
┌──────────────┐     ┌─────────────────────────────────────────────┐
│ IMDb Datasets│────▶│ cmd/sync/main.go (batch import + TMDB       │
│ (.tsv.gz)    │     │ backfill if TMDB_API_KEY set)               │
└──────────────┘     └──────────────┬────────────────────────────────┘
┌──────────────┐     ┌──────────────▼────────────────────────────────┐
│ TMDB API     │────▶│ PostgreSQL (titles, movies, shows,            │
└──────────────┘     │ show_seasons, show_episodes, genres,          │
       ▲             │ title_genres, collections, title_views, etc)  │
       │             └──────────────┬────────────────────────────────┘
       │                            │
       │             ┌──────────────▼────────────────────────────────┐
       └─────────────│ main.go (~2990 lines, web server + API)       │
    (on-demand       │  - HTML pages (Go templates)                  │
     image fetch,    │  - JSON API (/api/*)                          │
     TMDB backfill)  │  - Discover page + carousel cache             │
                     │  - Collections system                         │
                     │  - macOS systray                              │
                     └───────────────────────────────────────────────┘

cmd/sync-images/main.go — legacy batch TMDB image sync (standalone CLI)
```

## Entry Points

- **`backend/main.go`** (~2990 lines) — Web server + API. Entry via `systray.Run(onReady, onExit)`.
  - Env vars: `PORT` (default `8080`), `DATABASE_URL` (default `postgres://localhost/mediacanon?sslmode=disable`), `TMDB_API_KEY` (optional, enables on-demand image/metadata fetching)
  - On startup: connects to DB (25 max conns, 5 idle), parses templates, loads collections from embedded YAML, builds carousel cache, starts daily cleanup goroutine for old views/clicks

- **`backend/cmd/sync/main.go`** (~1910 lines) — Combined IMDb dataset importer + TMDB backfill. Three modes:
  - Default: IMDb import (download TSVs, sync titles/genres/episodes/ratings) + TMDB backfill for titles with `needs_backfill_tmdb=true`
  - `-genres-export FILE`: Export unreviewed titles for manual genre review
  - `-genres-import FILE`: Import genre assignments from reviewed file
  - CLI flags: `-db` (DSN), `-dir` (download dir, default `./imdb_data`), `-batch` (5000), `-workers` (8), `-force` (re-import unchanged files), `-genres-limit`, `-genres-filter`
  - Env: `TMDB_API_KEY` (enables Section 2: TMDB backfill batch)

- **`backend/cmd/sync-images/main.go`** (~334 lines) — Legacy TMDB image/metadata batch sync for shows. Iterates shows missing images, calls TMDB Find API for poster + metadata, then TMDB Episode API for stills/air dates/runtime. Rate-limited ~40 req/sec.
  - CLI flags: `-key` (TMDB API key, required), `-db`, `-limit`, `-skip-synced`

## HTTP Routes

### Web Pages

| Path | Handler | Description |
|------|---------|-------------|
| `GET /` | `handleHome` | Homepage with movie/show counts |
| `GET /discover` | `handleDiscoverPage` | Discover page: carousel sections, genre/country/type filter chips, collection detail |
| `GET /titles` | `handleTitlesList` | Browse/search with `?q=`, `?type=`, `?lang=`, `?page=` filters, paginated (100/page) |
| `GET /movies/{id}` | `handleMoviePage` | Movie detail page (triggers TMDB image fetch + backfill, logs engagement) |
| `GET /shows/{id}` | `handleShowPage` | Show detail page (triggers TMDB image fetch + backfill + episode fetch, logs engagement) |
| `GET /add` | `handleAddPage` | Add title form |
| `GET /api`, `GET /api/` | `handleAPIPage` | API documentation page |

### JSON API

| Path | Methods | Handler | Description |
|------|---------|---------|-------------|
| `/api/titles` | GET, POST | `handleAPITitles` | Search titles (`?q=`, `?type=`, `?lang=`, `?page=`, `?per_page=` max 100). Response includes `languages` distribution. POST creates title. |
| `/api/titles/{id}` | GET, PUT, DELETE | `handleAPITitle` | Single title CRUD. GET logs engagement with `?source=`. |
| `/api/movies` | POST | `handleAPIMoviesCreate` | Create movie (creates title + movie record in transaction) |
| `/api/movies/{id}` | GET, PUT, DELETE | `handleAPIMovie` | Movie with embedded title. GET triggers TMDB image fetch + logs engagement. |
| `/api/shows` | POST | `handleAPIShowsCreate` | Create show (creates title + show record in transaction) |
| `/api/shows/{id}` | GET, PUT, DELETE | `handleAPIShow` | Show with seasons + episodes. GET triggers TMDB image + episode fetch + logs engagement. |
| `/api/shows/{id}/seasons` | GET, POST | `handleShowSeasons` | List/create seasons for a show |
| `/api/seasons/{id}` | GET, DELETE | `handleAPISeason` | Season with episodes |
| `/api/seasons/{id}/episodes` | GET, POST | `handleSeasonEpisodes` | List/create episodes for a season |
| `/api/episodes/{id}` | GET, PUT, DELETE | `handleAPIEpisode` | Single episode CRUD |
| `/api/discover` | GET | `handleAPIDiscover` | Discover titles with filters: `?genre=`, `?type=`, `?lang=`, `?sort=`, `?country=`, `?year_min=`, `?rating_min=`, `?min_votes=`, `?limit=`, `?page=` |
| `/api/discover/carousels` | GET | `handleAPIDiscoverCarousels` | Paginated carousel sections (interleaved show/movie genre rows). `?page=`, `?per_page=` (max 50, default 10). Returns cached data. |
| `/api/collections` | GET | `handleAPICollections` | List active collections (ordered by pinned DESC, engagement DESC) |
| `/api/collections/{slug_or_id}` | GET | `handleAPICollection` | Collection detail with titles. Tries slug first, then numeric ID. Logs click. |

### Sort Options (for discover/collection filters)

`most_rated` (default, by num_votes), `trending` (tmdb_popularity), `popular` (title_views count), `top_rated` (average_rating, requires 1000+ votes unless min_votes specified), `newest` (start_year + release_date), `hidden_gems` (rating >= 7.5, votes 100-10000), `a-z` (alphabetical)

## Database Schema

Source of truth: `backend/schema.sql`. Run with `psql -d mediacanon -f backend/schema.sql`.

### Core Tables

```
titles (id, type[movie|show], display_name, start_year, end_year, imdb_id UNIQUE,
        image_url, tmdb_id, num_votes, average_rating, original_title,
        original_language, release_date, tmdb_popularity, runtime_minutes,
        origin_country, needs_backfill_tmdb, episodes_checked_at,
        created_at, updated_at)
  ├── movies (id, title_id UNIQUE FK→titles ON DELETE CASCADE)
  └── shows  (id, title_id UNIQUE FK→titles ON DELETE CASCADE)
       └── show_seasons (id, show_id FK→shows, season, UNIQUE(show_id, season))
            └── show_episodes (id, season_id FK→show_seasons, episode, display_name,
                               synopsis, image_url, air_date, runtime_minutes,
                               UNIQUE(season_id, episode))
```

### Genre Tables

```
genres (id, name UNIQUE, is_custom BOOLEAN DEFAULT FALSE)
title_genres (title_id FK→titles, genre_id FK→genres, PK(title_id, genre_id))
custom_genre_reviews (title_id PK FK→titles, reviewed_at)
```

### Collections Tables

```
collections (id, name, slug UNIQUE, description, strategy[filter|static|llm],
             filter_params JSONB, languages TEXT[], regions TEXT[],
             pinned BOOLEAN, active BOOLEAN, engagement_count REAL,
             created_at, updated_at)
collection_titles (collection_id FK, title_id FK, rank, PK(collection_id, title_id))
collection_clicks (id BIGSERIAL, collection_id FK, clicked_at)
```

### Tracking Tables

```
title_views (id BIGSERIAL, title_id FK, source VARCHAR(100), viewed_at)
sync_state (key VARCHAR(100) PK, value TEXT, updated_at)
```

### Key Indexes

`idx_titles_type`, `idx_titles_display_name`, `idx_titles_imdb_id`, `idx_titles_num_votes`, `idx_titles_popularity`, `idx_titles_rating`, `idx_titles_original_language`, `idx_titles_start_year`, `idx_titles_needs_backfill` (partial: WHERE true), `idx_show_seasons_show`, `idx_show_episodes_season`, `idx_title_genres_title`, `idx_title_genres_genre`, `idx_title_views_title`, `idx_title_views_viewed_at`, `idx_title_views_source`, `idx_collection_clicks_collection`, `idx_collection_clicks_clicked_at`, `idx_collections_active`, `idx_collection_titles_collection`, `idx_collection_titles_rank`

## Three Sync Workflows

### 1. Batch IMDb Import (`cmd/sync`, default mode)

**Trigger:** Run `./sync-mediacanon` (or `cd backend && go build -o sync-mediacanon ./cmd/sync/ && ./sync-mediacanon`)

**Data flow:**
1. Download 3 TSV.gz files from IMDb: `title.basics.tsv.gz`, `title.episode.tsv.gz`, `title.ratings.tsv.gz`
   - Uses `If-Modified-Since` header to skip unchanged files
   - Sets file mtime to `Last-Modified` from server
2. Compute SHA-256 hash of all 3 files combined → compare with `sync_state.imdb_files_hash`
3. If unchanged (and no `-force`), skip import stages
4. **Sync titles** (step 1.3): Load existing titles into memory map (imdb_id → ExistingTitle). Scan TSV line-by-line. Type mapping: `movie`/`tvMovie` → `movie`, `tvSeries`/`tvMiniSeries` → `show`, `tvEpisode` → collected for later display_name lookup, everything else → ignored. Diff against existing (check display_name, start_year, end_year, original_title, runtime_minutes). Batched INSERT/UPDATE with parallel workers.
5. **Sync genres** (step 1.4): Collect genre data from titles TSV (field 8, comma-separated). Upsert genre names. Batch insert title_genres associations (ON CONFLICT DO NOTHING).
6. **Sync episodes** (step 1.5): Two-pass. First pass: scan for new seasons, batch insert. Second pass: scan for new/changed episodes. Episode display_name comes from cross-referencing episode tconst in `title.basics.tsv.gz` (collected during title scan via `episodeTitles` sync.Map).
7. **Sync ratings** (step 1.6): Load existing ratings into memory map. Diff against TSV. Batched UPDATE with CASE statements.
8. Save new hash to `sync_state.imdb_files_hash`

### 2. TMDB Backfill Batch (`cmd/sync`, Section 2)

**Trigger:** Runs automatically after IMDb import when `TMDB_API_KEY` is set.

**Data flow:**
1. Query titles with `needs_backfill_tmdb = true`, ordered by num_votes DESC
2. For each title (in batches of 100): resolve TMDB ID via Find API (if missing) → call TMDB Details API → store: tmdb_id, image_url, original_language, release_date, tmdb_popularity, origin_country, runtime_minutes → set `needs_backfill_tmdb = false`
3. Rate-limited: ~40 req/sec (25ms ticker), handles 429 with 5s sleep

**Sentinel values:** After backfill, `needs_backfill_tmdb` is set to `false`. Use `./reset-tmdb-backfill` to re-flag all titles for re-fetch.

### 3. Lazy On-Demand TMDB Fetch (main.go, at request time)

**Trigger:** User visits a title detail page or API endpoint.

**Three lazy fetch functions:**
- **`maybeFetchImage(title)`**: If title has no image_url and has an imdb_id → call TMDB Find API → store poster URL, tmdb_id, original_language, release_date, tmdb_popularity, origin_country. If no poster found, stores `"none"` as sentinel to avoid re-fetching.
- **`maybeTMDBBackfill(title)`**: If `needs_backfill_tmdb = true` → call TMDB Details API → store full metadata including origin_country and runtime_minutes → clear flag.
- **`maybeFetchEpisodes(show)`**: If episodes are missing data (image, air_date, synopsis, runtime) and haven't been checked in 24 hours (`episodes_checked_at` cooldown) → fetch from TMDB Episode API concurrently (semaphore of 5). **Omniseason fallback**: if all episodes in a season return 404 from TMDB, retry using absolute episode numbers mapped to TMDB Season 1 (handles shows where TMDB uses flat season structure).

**TMDB sentinel values:**
- Title `image_url = "none"`: No poster found, don't re-fetch
- Title `image_url = "TMDB_NOT_FOUND_DO_NOT_RETRY"`: Not used for title images, but set on episode images
- Episode `image_url = "TMDB_NOT_FOUND_DO_NOT_RETRY"`: No still found, will retry after 24h cooldown
- Sentinels are stripped from in-memory structs before rendering (via `stripEpisodeSentinels()`)

## Collections System

### YAML Definition Files

Located in `backend/collections/*.yaml`, embedded via `//go:embed collections/*.yaml`. Loaded at server startup via `loadCollections()` — upserts into `collections` table (preserves `engagement_count` and `active` from DB).

Currently ~42 collection files, all using the `filter` strategy. Example:

```yaml
slug: top-action-shows
name: Top Action Shows
description: |
  The highest-rated action TV series...
strategy: filter
pinned: false
languages: [en]
regions: [global]
filter:
  type: show
  genre: Action
  sort: top_rated
  min_votes: 5000
  limit: 100
```

### Strategies

- **`filter`**: Dynamic — applies filter params (type, lang, genre, sort, min_votes, limit) against titles table at query time via `fetchDiscoverTitles()`. Used for all current collections.
- **`static`**: Fixed list — `titles` field contains IMDb IDs, resolved to title_ids at load time, stored in `collection_titles` with rank ordering.
- **`llm`**: Same behavior as `static` (uses `collection_titles` table), but generated by LLM. Currently no collections use this.

### Engagement Tracking

- `logEngagement(titleID, source)`: Called on every title detail view. Logs to `title_views`. If source starts with `"collection-"`, also logs a `collection_clicks` entry.
- `cleanupOldViews()`: Daily goroutine deletes `title_views` and `collection_clicks` older than 7 days. Updates `collections.engagement_count` from rolling window.
- Collections sorted by `pinned DESC, engagement_count DESC`.

### Carousel Cache

`buildCarouselCache()` runs at startup. One SQL query (CTE with ROW_NUMBER) gets top 30 titles per (type, genre) combination, ranked by average_rating + num_votes. Results stored in `carouselCache` map keyed by `"type:genre"`. Served by discover page and `/api/discover/carousels`.

## Derived Fields (computed in Go, not stored)

- `Show.IsSeriesFinished`: `true` if `end_year` is set (non-null)
- `Season.IsSeasonFinished`: If series finished → all seasons `true`. If ongoing → `true` for seasons below the max season number
- Computed in `getShowByID()` after loading seasons

## Models

**Go structs (main.go):**
- `Title` — Full title with all fields including `NeedsBackfillTMDB`, `EpisodesCheckedAt`, `Genres` (JSON-excluded internal fields)
- `DiscoverTitle` — Lightweight struct for poster grids: TitleID, Type, DisplayName, Years, ImageURL, MovieID/ShowID, Rating, Votes, Popularity, Genres, EngagementCount
- `TitleSearchResult` — Search result with ShowID/MovieID for client navigation
- `TitleListItem` — Internal struct for titles list page
- `Movie` — MovieID, TitleID, embedded Title
- `Show` — ShowID, TitleID, embedded Title, Seasons, IsSeriesFinished
- `Season` — SeasonID, ShowID, SeasonNumber, Episodes, IsSeasonFinished
- `Episode` — EpisodeID, SeasonID, EpisodeNumber, DisplayName, ImageURL, AirDate, RuntimeMinutes, Synopsis
- `Collection` — ID, Name, Slug, Description, Strategy, Pinned, Active, EngagementCount, Languages, Regions
- `CollectionDef` — YAML structure for collection definition files (with Filter sub-struct and Titles list)

## Consumer Interface

**This is the API contract that anywatch and other consumers use.** The base URL is `https://mediacanon.org`.

### Search

```
GET /api/titles?q={query}&type={movie|show}&lang={iso_code}&page={n}&per_page={n}
→ { titles: TitleSearchResult[], total, page, per_page, total_pages, languages: [{code, count}] }
```

Each `TitleSearchResult` includes `show_id` or `movie_id` for direct navigation to show/movie endpoints.

### Title Detail

```
GET /api/titles/{title_id}?source={source}
→ Title (with genres[])
```

### Movie Detail

```
GET /api/movies/{movie_id}?source={source}
→ Movie { movie_id, title_id, title: Title }
```

Triggers on-demand TMDB image fetch if missing. Title includes genres.

### Show Detail (primary consumer endpoint)

```
GET /api/shows/{show_id}?source={source}
→ Show { show_id, title_id, title: Title, seasons: Season[], is_series_finished }
  Season { season_id, show_id, season_number, episodes: Episode[], is_season_finished }
  Episode { episode_id, season_id, episode_number, display_name?, image_url?, air_date?, runtime_minutes?, synopsis? }
```

Triggers on-demand TMDB image fetch + episode data fetch if missing. Title includes genres.

### Seasons & Episodes

```
GET /api/shows/{show_id}/seasons → Season[]
GET /api/seasons/{season_id} → Season { ..., episodes: Episode[] }
GET /api/seasons/{season_id}/episodes → Episode[]
GET /api/episodes/{episode_id} → Episode
```

### Discover & Collections

```
GET /api/discover?genre=&type=&lang=&sort=&country=&year_min=&rating_min=&min_votes=&limit=&page=
→ { titles: DiscoverTitle[], total, page, per_page }

GET /api/discover/carousels?page=&per_page=
→ { carousels: [{ name, slug, description, collection_id, engagement_count, total_count, titles: DiscoverTitle[] }], total, page, per_page }

GET /api/collections → Collection[]
GET /api/collections/{slug_or_id} → { collection: Collection, titles: DiscoverTitle[], total }
```

### Write Endpoints

All write endpoints are enabled (no authentication):
- `POST /api/titles` — Create title
- `PUT /api/titles/{id}` — Update title
- `DELETE /api/titles/{id}` — Delete title (cascades to movie/show)
- `POST /api/movies` — Create movie (title + movie in transaction)
- `PUT /api/movies/{id}` — Update movie title fields
- `DELETE /api/movies/{id}` — Delete movie + title
- `POST /api/shows` — Create show (title + show in transaction)
- `PUT /api/shows/{id}` — Update show title fields
- `DELETE /api/shows/{id}` — Delete show + title
- `POST /api/shows/{id}/seasons` — Create season
- `DELETE /api/seasons/{id}` — Delete season
- `POST /api/seasons/{id}/episodes` — Create episode
- `PUT /api/episodes/{id}` — Update episode
- `DELETE /api/episodes/{id}` — Delete episode

### Engagement Source Tracking

Pass `?source=collection-{slug}` on detail endpoints to track which collection drove the click. This feeds into `collection_clicks` and `engagement_count` for collection ranking.

## Key Modules

- **`backend/main.go`** — Monolith server. Models, TMDB fetch logic, page handlers, API handlers, collection loading, carousel cache, discover helpers, utility functions.
- **`backend/cmd/sync/main.go`** — IMDb dataset importer + TMDB backfill + custom genre export/import. Downloads TSVs, loads DB state into memory maps, diffs, batched insert/update with configurable batch size and parallel workers.
- **`backend/cmd/sync-images/main.go`** — Legacy standalone TMDB image sync for shows. Iterates shows missing images, fetches poster + episode stills from TMDB.
- **`backend/templates/`** — `base.html` (layout), `home.html`, `titles.html`, `movie.html`, `show.html`, `add.html`, `api.html`, `search.html`, `discover.html`
- **`backend/static/`** — `style.css` (CSS vars, dark mode via `prefers-color-scheme`), `app.js`, `icon.png`, `icon.svg`
- **`backend/collections/`** — ~42 YAML files defining genre-based collections (all filter strategy)
- **`backend/schema.sql`** — DDL: CREATE TABLEs + ALTER TABLEs for incremental columns + indexes

## Build & Run

```bash
# Server — build and run (CGO required for systray)
cd backend
CGO_ENABLED=1 go build -o mediacanon .
PORT=8081 TMDB_API_KEY=xxx ./mediacanon

# Quick dev iteration — rebuild, restart launchd service + cloudflared tunnel
./update

# IMDb sync — download datasets, diff & import, then TMDB backfill
./sync-mediacanon                                  # normal run
TMDB_API_KEY=xxx ./sync-mediacanon                 # with TMDB backfill
./sync-mediacanon -force                           # force re-import
./sync-mediacanon -genres-export review.txt         # export for genre review
./sync-mediacanon -genres-import review.txt         # import genre assignments

# Legacy TMDB image sync (standalone)
cd backend && go build -o sync-images ./cmd/sync-images/
./sync-images -key $TMDB_API_KEY

# Reset TMDB backfill flag (re-fetch all metadata on next visit/sync)
./reset-tmdb-backfill

# Schema setup
psql -d mediacanon -f backend/schema.sql
```

### Deployment

Server runs as a macOS LaunchAgent (`com.mediacanon.server`) with a Cloudflare tunnel (`com.mediacanon.tunnel`) for public access at mediacanon.org. The `./update` script handles: stop services → build → restart services.

## Conventions

- Single-file monolith pattern — all server code in `main.go` (~2990 lines)
- Embedded static assets via `//go:embed` (templates, static files, collections YAML, icon)
- Templates: `{{define "body"}}` content + `{{template "base" .}}` layout
- TMDB sentinel values: title `image_url = "none"` (no poster), episode `image_url = "TMDB_NOT_FOUND_DO_NOT_RETRY"` (no still, retryable after 24h)
- `readOnly()` is a no-op — all writes are allowed, no authentication
- `noCache()` middleware on all non-static routes (Cache-Control: no-store)
- URL routing: `http.ServeMux` with `strings.TrimPrefix` for ID extraction
- JSON responses via `jsonResponse()`/`jsonError()` helpers
- DB connection pool: 25 max open, 5 max idle, 5min max lifetime, 1min max idle time
- Logging: stdlib `log` to both stdout and `mediacanon.log` (next to binary)
- Signal handling: ignores SIGHUP (survives sleep/wake), graceful shutdown on SIGINT/SIGTERM
- When adding features, always update: JSON API handlers, API docs template (`templates/api.html`), and schema to keep them in sync
- Custom genres: `Dating`, `Cooking` — arbitrary thematic tags assigned via export/import review workflow

## Dependencies

- `github.com/lib/pq` v1.10.9 — PostgreSQL driver
- `fyne.io/systray` v1.12.0 — macOS menu bar integration (requires `CGO_ENABLED=1`)
- `gopkg.in/yaml.v3` v3.0.1 — YAML parsing for collection definitions

**Go version:** 1.24.5 | Module path: `mediacanon.org/backend`

**External services:** IMDb datasets (datasets.imdbws.com), TMDB API (api.themoviedb.org), Cloudflare tunnel (public access)
