# MediaCanon

Open movie and TV metadata database. No API keys. No rate limits.

https://mediacanon.org

## Run Locally

```bash
# Build and start server
cd /Users/ybmeng/mediacanon/backend
go build -o mediacanon .
PORT=8081 ./mediacanon &

# Start Cloudflare tunnel (for public access)
cloudflared tunnel run mediacanon &
```

## Database

Requires PostgreSQL with database `mediacanon`. Schema in `schema.sql`.

```bash
psql -d mediacanon -f schema.sql
```

## IMDb Sync

Import data from IMDb datasets:

```bash
go build -o imdb-sync ./cmd/sync

# First run (downloads data)
./imdb-sync

# Subsequent runs (skip download)
./imdb-sync -skip-download
```

Options:
- `-db` - Database URL (default: `postgres://localhost/mediacanon?sslmode=disable`)
- `-dir` - Download directory (default: `./imdb_data`)
- `-batch` - Batch size for inserts (default: 5000)
- `-workers` - Parallel workers (default: 8)

## API

See https://mediacanon.org/api for full documentation.

```bash
# Search titles
curl https://mediacanon.org/api/titles?q=matrix

# Get a show with all episodes
curl https://mediacanon.org/api/shows/47214

# Get a movie
curl https://mediacanon.org/api/movies/12345
```
