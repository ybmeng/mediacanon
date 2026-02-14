# Project: MediaCanon

Open movie and TV metadata database (https://mediacanon.org) powered by IMDb and TMDB data.

## Architecture

Single-binary Go monolith with PostgreSQL. Server-rendered HTML frontend, JSON API, and macOS menu bar app (systray).

```
┌──────────────┐     ┌─────────────────────────────────────┐
│ IMDb Datasets│────▶│ cmd/sync/main.go (batch import)     │
│ (.tsv.gz)    │     └──────────────┬──────────────────────┘
└──────────────┘                    │
┌──────────────┐     ┌──────────────▼──────────────────────┐
│ TMDB API     │────▶│ PostgreSQL (titles, movies, shows,  │
└──────────────┘     │ show_seasons, show_episodes)        │
       ▲             └──────────────┬──────────────────────┘
       │                            │
       │             ┌──────────────▼──────────────────────┐
       └─────────────│ main.go (web server + API)          │
    (on-demand       │  - HTML pages (Go templates)        │
     image fetch)    │  - JSON API (/api/*)                │
                     │  - macOS systray                    │
                     └─────────────────────────────────────┘
```

## Entry Points

- **`backend/main.go`** — Web server + API (~1640 lines). Entry via `systray.Run(onReady, onExit)`.
  - `PORT` (default `8080`), `DATABASE_URL` (default `postgres://localhost/mediacanon?sslmode=disable`), `TMDB_API_KEY` (optional)
- **`backend/cmd/sync/main.go`** — IMDb dataset importer (~980 lines). CLI flags: `-db`, `-dir`, `-batch` (5000), `-workers` (8)
- **`backend/cmd/sync-images/main.go`** — TMDB image/metadata sync (~315 lines). CLI flags: `-key` (required), `-db`, `-limit`, `-skip-synced`

### HTTP Routes

| Path | Handler | Description |
|------|---------|-------------|
| `GET /` | `handleHome` | Homepage with movie/show counts |
| `GET /titles` | `handleTitlesList` | Browse/search with `?q=`, `?type=`, `?lang=` filters |
| `GET /movies/{id}` | `handleMoviePage` | Movie detail (triggers TMDB image fetch) |
| `GET /shows/{id}` | `handleShowPage` | Show detail (triggers TMDB image + episode fetch) |
| `GET /add` | `handleAddPage` | Add title form |
| `GET /api`, `GET /api/` | `handleAPIPage` | API documentation page |
| `GET /api/titles` | `handleAPITitles` | Search titles (`?q=`, `?type=`, `?lang=`, `?page=`, `?per_page=`, limit 100) |
| `GET /api/titles/{id}` | `handleAPITitle` | Single title |
| `GET /api/movies/{id}` | `handleAPIMovie` | Movie with embedded title |
| `GET /api/shows/{id}` | `handleAPIShow` | Show with seasons + episodes |
| `GET /api/shows/{id}/seasons` | `handleShowSeasons` | List seasons |
| `GET /api/seasons/{id}` | `handleAPISeason` | Season with episodes |
| `GET /api/seasons/{id}/episodes` | `handleSeasonEpisodes` | List episodes |
| `GET /api/episodes/{id}` | `handleAPIEpisode` | Single episode |

Write endpoints (POST/PUT/DELETE) are enabled.

## Key Modules

- **`backend/main.go`** — Monolith server. Models: `Title`, `TitleSearchResult`, `TitleListItem`, `Movie`, `Show`, `Season`, `Episode`, `TMDBFindResponse`, `TMDBEpisodeResponse`. Key helpers: `getTitleByID()`, `getMovieByID()`, `getShowByID()`, `maybeFetchImage()`, `maybeFetchEpisodes()`, `fetchAndStoreTMDBImage()`, `langDisplay()`, `fmtRating()`, `setupLogging()`, `getLocalIP()`.
- **`backend/cmd/sync/main.go`** — Downloads `title.basics.tsv.gz`, `title.episode.tsv.gz`, `title.ratings.tsv.gz` from IMDb. Loads existing DB state into memory maps, diffs, batched insert/update with parallel workers. Type mapping: `tvSeries`/`tvMiniSeries` → show, `movie`/`tvMovie` → movie.
- **`backend/cmd/sync-images/main.go`** — Iterates shows missing images, calls TMDB Find API (by IMDb ID) for poster + metadata, then TMDB Episode API for stills/air dates/runtime. Rate-limited ~40 req/sec. Only processes shows (movies use on-demand fetch).
- **`backend/templates/`** — `base.html` (layout), `home.html`, `titles.html`, `movie.html`, `show.html`, `add.html`, `api.html`, `search.html`
- **`backend/static/`** — `style.css` (606 lines, CSS vars, dark mode via `prefers-color-scheme`), `app.js` (21 lines, season filter toggle), `icon.png`, `icon.svg`

## Data Flow

**Search:** User query → `handleTitlesList`/`handleAPITitles` → SQL `ILIKE` with optional type/lang filters → `ORDER BY num_votes DESC NULLS LAST` → return top 100

**Detail page:** User visits `/movies/{id}` or `/shows/{id}` → fetch from DB → `maybeFetchImage()` checks if image missing, calls TMDB Find API, stores result → for shows, `maybeFetchEpisodes()` concurrently fetches missing episode data from TMDB → render template

**IMDb sync:** Download TSV → load existing DB rows into memory maps → scan TSV line-by-line → diff (insert new, update changed) → batched SQL with parallel workers → same for episodes (two-pass: seasons first, then episodes) → ratings pass

**TMDB sync:** Query shows missing images → for each: TMDB Find API → store poster/language/date → fetch all episodes via TMDB Episode API → store stills/air dates/runtime

## Database

```bash
psql -d mediacanon -f backend/schema.sql
```

**Tables:** `titles` (id, type, display_name, start_year, end_year, imdb_id, image_url, tmdb_id, num_votes, average_rating, original_title, original_language, release_date, created_at, updated_at) → `movies` (id, title_id) / `shows` (id, title_id) → `show_seasons` (id, show_id, season) → `show_episodes` (id, season_id, episode, display_name, synopsis, image_url, air_date, runtime_minutes)

Note: `show_episodes.image_url`, `air_date`, `runtime_minutes` are added via ALTER TABLE in code/migration, not in the base `schema.sql`.

## Build & Run

```bash
cd backend
CGO_ENABLED=1 go build -o mediacanon .    # CGO required for systray
PORT=8081 ./mediacanon

# IMDb sync
cd backend && go build -o sync-imdb ./cmd/sync/ && ./sync-imdb

# TMDB image sync
cd backend && go build -o sync-images ./cmd/sync-images/ && ./sync-images -key $TMDB_API_KEY
```

Root scripts: `./sync-imdb` (builds + runs sync + restarts server), `./update` (rebuilds + restarts server + cloudflared tunnel)

## Conventions

- Single-file monolith pattern — all server code in `main.go`
- Embedded static assets via `//go:embed`
- Templates: `{{define "body"}}` content + `{{template "base" .}}` layout
- TMDB sentinel values: `"none"` and `"TMDB_NOT_FOUND_DO_NOT_RETRY"` to avoid re-fetching misses
- `readOnly()` is a no-op — writes are allowed
- `noCache()` middleware on all non-static routes
- No authentication, no rate limiting on API
- URL routing: `http.ServeMux` with `strings.TrimPrefix` for ID extraction
- JSON responses via `jsonResponse()`/`jsonError()` helpers
- When adding features, always update the JSON API (`handleAPI*` handlers), the API docs template (`templates/api.html`), and the schemas to keep them complete and in sync

## Dependencies

- `github.com/lib/pq` v1.10.9 — PostgreSQL driver
- `fyne.io/systray` v1.12.0 — macOS menu bar integration (requires `CGO_ENABLED=1`)
