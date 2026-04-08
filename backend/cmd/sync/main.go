package main

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

const (
	basicsURL   = "https://datasets.imdbws.com/title.basics.tsv.gz"
	episodesURL = "https://datasets.imdbws.com/title.episode.tsv.gz"
	ratingsURL  = "https://datasets.imdbws.com/title.ratings.tsv.gz"

	upsertBatchSize = 500 // rows per DB batch operation
)

var (
	db         *sql.DB
	batchSize  int // legacy flag, kept for CLI compat but upsertBatchSize used internally
	workers    int
	tmdbAPIKey string
)

type seasonKey struct {
	titleID int
	season  int
}

type TitleRecord struct {
	ImdbID         string
	Type           string
	DisplayName    string
	StartYear      *int
	EndYear        *int
	OriginalTitle  string
	RuntimeMinutes *int
	Genres         []string
}

type RatingRecord struct {
	ImdbID        string
	NumVotes      int
	AverageRating float64
}

func main() {
	dsn := flag.String("db", "postgres://localhost/mediacanon?sslmode=disable", "Database URL")
	downloadDir := flag.String("dir", "./imdb_data", "Directory to store downloaded files")
	forceImdb := flag.Bool("force", false, "Force IMDb import even if files unchanged")
	genresExport := flag.String("genres-export", "", "Export unreviewed titles to file for genre review")
	genresImport := flag.String("genres-import", "", "Import genre assignments from reviewed file")
	genresLimit := flag.Int("genres-limit", 100, "Number of titles to export for genre review")
	genresFilter := flag.String("genres-filter", "", "Only export titles with these IMDb genres (comma-separated, e.g. 'Reality-TV,Game-Show')")
	flag.IntVar(&batchSize, "batch", 5000, "Batch size for inserts (legacy, internal uses 100)")
	flag.IntVar(&workers, "workers", 8, "Number of parallel workers")
	flag.Parse()

	tmdbAPIKey = os.Getenv("TMDB_API_KEY")

	var err error
	db, err = sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(workers + 5)
	db.SetMaxIdleConns(workers + 5)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("Cannot connect to database:", err)
	}

	// Ensure sync_state table exists
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS sync_state (
		key VARCHAR(100) PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at TIMESTAMP DEFAULT NOW()
	)`)
	if err != nil {
		log.Fatal("create sync_state table:", err)
	}

	start := time.Now()

	// ── Genre export/import (early exit) ──────────────────────────────
	if *genresExport != "" {
		var filter []string
		if *genresFilter != "" {
			for _, g := range strings.Split(*genresFilter, ",") {
				if s := strings.TrimSpace(g); s != "" {
					filter = append(filter, s)
				}
			}
		}
		if err := exportGenreReview(*genresExport, *genresLimit, filter); err != nil {
			log.Fatal(err)
		}
		log.Printf("Done in %v", time.Since(start))
		return
	}
	if *genresImport != "" {
		if err := importGenreReview(*genresImport); err != nil {
			log.Fatal(err)
		}
		log.Printf("Done in %v", time.Since(start))
		return
	}

	// ── Section 1: IMDb Import ────────────────────────────────────────
	// Acquire file lock to prevent concurrent sync runs
	lockFile, err := os.OpenFile("/tmp/sync-imdb.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatal("open lock file:", err)
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		log.Fatal("Another sync-imdb process is running. Exiting.")
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	log.Println("File lock acquired (no other sync running)")

	os.MkdirAll(*downloadDir, 0755)

	basicsFile := *downloadDir + "/title.basics.tsv.gz"
	episodesFile := *downloadDir + "/title.episode.tsv.gz"
	ratingsFile := *downloadDir + "/title.ratings.tsv.gz"

	log.Println("━━━ IMDb Import ━━━")

	log.Println("[1.1] Downloading IMDb datasets...")
	if err := downloadFile(basicsURL, basicsFile); err != nil {
		log.Fatal(err)
	}
	if err := downloadFile(episodesURL, episodesFile); err != nil {
		log.Fatal(err)
	}
	if err := downloadFile(ratingsURL, ratingsFile); err != nil {
		log.Fatal(err)
	}

	// Compute combined hash of all 3 files
	log.Println("[1.2] Checking file hashes...")
	currentHash, err := hashFiles(basicsFile, episodesFile, ratingsFile)
	if err != nil {
		log.Fatal(err)
	}

	previousHash := getSyncState("imdb_files_hash")
	imdbChanged := *forceImdb || currentHash != previousHash

	if !imdbChanged {
		log.Printf("IMDb files unchanged (hash: %s…), skipping import stages", currentHash[:12])
	} else {
		if previousHash == "" {
			log.Println("First run (no previous hash), importing...")
		} else if *forceImdb {
			log.Println("Force flag set, importing...")
		} else {
			log.Printf("Files changed (%s… → %s…), importing...", previousHash[:12], currentHash[:12])
		}

		log.Println("[1.2.1] Creating genres staging table...")
		if err := createGenresStagingTable(); err != nil {
			log.Fatal(err)
		}

		log.Println("[1.3] Syncing titles (streaming)...")
		if err := syncTitles(basicsFile); err != nil {
			log.Fatal(err)
		}

		log.Println("[1.4] Syncing genres (from staging)...")
		if err := syncGenres(); err != nil {
			log.Fatal(err)
		}

		// Drop genres staging table (no longer needed)
		db.Exec(`DROP TABLE IF EXISTS _title_genres_staging`)

		log.Println("[1.5] Syncing episodes (streaming, merge-join with basics)...")
		if err := syncEpisodes(episodesFile, basicsFile); err != nil {
			log.Fatal(err)
		}

		log.Println("[1.6] Syncing ratings (streaming)...")
		if err := syncRatings(ratingsFile); err != nil {
			log.Fatal(err)
		}

		// Store hash after successful import
		setSyncState("imdb_files_hash", currentHash)
		log.Println("Import complete, hash saved")
	}

	// ── Section 2: TMDB Backfill ─────────────────────────────────────
	if tmdbAPIKey == "" {
		log.Println("━━━ TMDB Backfill ━━━")
		log.Println("Skipping: TMDB_API_KEY not set")
	} else {
		log.Println("━━━ TMDB Backfill ━━━")
		tmdbBackfillBatch()
	}

	log.Printf("All done in %v", time.Since(start))
}

// createGenresStagingTable creates a staging table for genres (small, ~3M rows).
// Episode titles no longer use staging — they use merge-join on sorted TSV files.
func createGenresStagingTable() error {
	_, err := db.Exec(`DROP TABLE IF EXISTS _title_genres_staging`)
	if err != nil {
		return fmt.Errorf("drop old _title_genres_staging: %w", err)
	}
	_, err = db.Exec(`CREATE UNLOGGED TABLE _title_genres_staging (
		imdb_id TEXT NOT NULL,
		genre_name TEXT NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("create _title_genres_staging: %w", err)
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_tgs_imdb ON _title_genres_staging(imdb_id)`)
	if err != nil {
		return fmt.Errorf("create index on _title_genres_staging: %w", err)
	}
	return nil
}

// hashFiles computes a single SHA-256 over the contents of all files (sorted by name for stability).
func hashFiles(paths ...string) (string, error) {
	sorted := make([]string, len(paths))
	copy(sorted, paths)
	sort.Strings(sorted)

	h := sha256.New()
	for _, p := range sorted {
		f, err := os.Open(p)
		if err != nil {
			return "", fmt.Errorf("hash %s: %w", p, err)
		}
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			return "", fmt.Errorf("hash %s: %w", p, err)
		}
		f.Close()
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func getSyncState(key string) string {
	var value string
	err := db.QueryRow(`SELECT value FROM sync_state WHERE key = $1`, key).Scan(&value)
	if err != nil {
		return ""
	}
	return value
}

func setSyncState(key, value string) {
	_, err := db.Exec(`INSERT INTO sync_state (key, value, updated_at) VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`, key, value)
	if err != nil {
		log.Printf("WARNING: failed to save sync state %q: %v", key, err)
	}
}

func downloadFile(url, dest string) error {
	name := filepath.Base(dest)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	// If local file exists, send If-Modified-Since to skip unchanged files
	if info, err := os.Stat(dest); err == nil {
		req.Header.Set("If-Modified-Since", info.ModTime().UTC().Format(http.TimeFormat))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 304 {
		log.Printf("%s: not modified, skipping download", name)
		return nil
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: server returned %d", name, resp.StatusCode)
	}

	log.Printf("Downloading %s...", name)
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	// Set file mtime to Last-Modified from server so future runs can compare
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			os.Chtimes(dest, t, t)
		}
	}

	log.Printf("%s: downloaded", name)
	return nil
}

// syncTitles streams the basics TSV, UPSERTs titles in batches of 100,
// and writes episode titles + genre associations to staging tables.
// No large in-memory maps. ~O(batch_size) memory.
func syncTitles(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	scanner.Scan() // Skip header

	var (
		titleBatch    []TitleRecord
		genreBatch    []struct{ imdbID, genre string }
		scanned       int64
		inserted      int64
		updated       int64
		unchanged     int64
		ignored       int64
		batchNum      int64
		totalBatches  int64 = 12600 // ~1.26M titles / 500
		genreBatchNum int64
	)

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) < 9 {
			continue
		}

		imdbID := fields[0]
		titleType := fields[1]
		displayName := fields[2]
		originalTitle := fields[3]
		yearStr := fields[5]

		// Skip episodes — handled in syncEpisodes via merge-join
		if titleType == "tvEpisode" {
			continue
		}

		// Only process movies and shows
		var ourType string
		switch titleType {
		case "movie", "tvMovie":
			ourType = "movie"
		case "tvSeries", "tvMiniSeries":
			ourType = "show"
		default:
			ignored++
			continue
		}

		var startYear *int
		if yearStr != "\\N" {
			if y, err := strconv.Atoi(yearStr); err == nil {
				startYear = &y
			}
		}

		endYearStr := fields[6]
		var endYear *int
		if endYearStr != "\\N" {
			if y, err := strconv.Atoi(endYearStr); err == nil {
				endYear = &y
			}
		}

		var runtimeMinutes *int
		if fields[7] != "\\N" {
			if rt, err := strconv.Atoi(fields[7]); err == nil {
				runtimeMinutes = &rt
			}
		}

		// Parse genres → staging table (replaces titleGenres map)
		if fields[8] != "\\N" {
			genres := strings.Split(fields[8], ",")
			for _, g := range genres {
				genreBatch = append(genreBatch, struct{ imdbID, genre string }{imdbID, g})
			}
			if len(genreBatch) >= upsertBatchSize {
				if err := flushGenresStaging(genreBatch); err != nil {
					return err
				}
				genreBatchNum++
				genreBatch = genreBatch[:0]
			}
		}

		scanned++

		titleBatch = append(titleBatch, TitleRecord{
			ImdbID:         imdbID,
			Type:           ourType,
			DisplayName:    displayName,
			StartYear:      startYear,
			EndYear:        endYear,
			OriginalTitle:  originalTitle,
			RuntimeMinutes: runtimeMinutes,
		})

		if len(titleBatch) >= upsertBatchSize {
			ins, upd, unch, err := upsertTitleBatch(titleBatch)
			if err != nil {
				return err
			}
			inserted += ins
			updated += upd
			unchanged += unch
			titleBatch = titleBatch[:0]
			batchNum++

			if batchNum%150 == 0 {
				log.Printf("[titles] %d/%d batches (%d rows) | %d inserted, %d updated, %d unchanged",
					batchNum, totalBatches, scanned, inserted, updated, unchanged)
			}
		}
	}

	// Flush remaining batches
	if len(titleBatch) > 0 {
		ins, upd, unch, err := upsertTitleBatch(titleBatch)
		if err != nil {
			return err
		}
		inserted += ins
		updated += upd
		unchanged += unch
		batchNum++
	}
	if len(genreBatch) > 0 {
		if err := flushGenresStaging(genreBatch); err != nil {
			return err
		}
	}

	log.Printf("[titles] complete: %d batches, %d rows | %d inserted, %d updated, %d unchanged, %d ignored",
		batchNum, scanned, inserted, updated, unchanged, ignored)

	return scanner.Err()
}

// upsertTitleBatch UPSERTs a batch of titles.
// Returns (inserted, updated, unchanged) counts.
func upsertTitleBatch(batch []TitleRecord) (int64, int64, int64, error) {
	if len(batch) == 0 {
		return 0, 0, 0, nil
	}

	// Build UPSERT with RETURNING to know which were inserted vs updated
	values := make([]string, len(batch))
	args := make([]any, len(batch)*7)
	for j, r := range batch {
		base := j * 7
		values[j] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7)
		args[base] = r.ImdbID
		args[base+1] = r.Type
		args[base+2] = r.DisplayName
		args[base+3] = r.StartYear
		args[base+4] = r.EndYear
		args[base+5] = r.OriginalTitle
		args[base+6] = r.RuntimeMinutes
	}

	// UPSERT: INSERT ... ON CONFLICT DO UPDATE ... WHERE something IS DISTINCT FROM
	// RETURNING id, type, (xmax = 0) as was_inserted
	// xmax = 0 means the row was freshly inserted (no previous version)
	query := fmt.Sprintf(`
		INSERT INTO titles (imdb_id, type, display_name, start_year, end_year, original_title, runtime_minutes)
		VALUES %s
		ON CONFLICT (imdb_id) DO UPDATE SET
			display_name = EXCLUDED.display_name,
			start_year = EXCLUDED.start_year,
			end_year = EXCLUDED.end_year,
			original_title = EXCLUDED.original_title,
			runtime_minutes = EXCLUDED.runtime_minutes,
			updated_at = NOW()
		WHERE titles.display_name IS DISTINCT FROM EXCLUDED.display_name
			OR titles.start_year IS DISTINCT FROM EXCLUDED.start_year
			OR titles.end_year IS DISTINCT FROM EXCLUDED.end_year
			OR titles.original_title IS DISTINCT FROM EXCLUDED.original_title
			OR titles.runtime_minutes IS DISTINCT FROM EXCLUDED.runtime_minutes
		RETURNING id, type, (xmax = 0) as was_inserted
	`, strings.Join(values, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("title upsert: %w", err)
	}

	// Collect returned IDs (only inserted or updated rows are returned)
	var insertCount, updateCount int64

	for rows.Next() {
		var id int
		var titleType string
		var wasInserted bool
		if err := rows.Scan(&id, &titleType, &wasInserted); err != nil {
			rows.Close()
			return 0, 0, 0, fmt.Errorf("title upsert scan: %w", err)
		}
		if wasInserted {
			insertCount++
		} else {
			updateCount++
		}
	}
	rows.Close()

	unchangedCount := int64(len(batch)) - insertCount - updateCount

	return insertCount, updateCount, unchangedCount, nil
}

// flushGenresStaging batch-inserts genre associations to the staging table
func flushGenresStaging(batch []struct{ imdbID, genre string }) error {
	if len(batch) == 0 {
		return nil
	}
	values := make([]string, len(batch))
	args := make([]any, len(batch)*2)
	for j, g := range batch {
		base := j * 2
		values[j] = fmt.Sprintf("($%d, $%d)", base+1, base+2)
		args[base] = g.imdbID
		args[base+1] = g.genre
	}
	_, err := db.Exec(fmt.Sprintf(`
		INSERT INTO _title_genres_staging (imdb_id, genre_name)
		VALUES %s
	`, strings.Join(values, ",")), args...)
	if err != nil {
		return fmt.Errorf("genre staging insert: %w", err)
	}
	return nil
}

// syncGenres reads from _title_genres_staging and bulk-inserts into genres + title_genres.
// No in-memory maps needed.
func syncGenres() error {
	// Check if staging has data
	var stagingCount int
	db.QueryRow(`SELECT COUNT(*) FROM _title_genres_staging`).Scan(&stagingCount)
	if stagingCount == 0 {
		log.Println("No genre data in staging, skipping genre sync")
		return nil
	}
	log.Printf("[genres] %d genre associations in staging", stagingCount)

	// Step 1: Insert all unique genre names from staging
	_, err := db.Exec(`
		INSERT INTO genres (name)
		SELECT DISTINCT genre_name FROM _title_genres_staging
		ON CONFLICT (name) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("genre name insert: %w", err)
	}

	var genreCount int
	db.QueryRow(`SELECT COUNT(*) FROM genres`).Scan(&genreCount)
	log.Printf("[genres] %d genres in database", genreCount)

	// Step 2: Bulk-insert title_genres by joining staging with titles and genres
	result, err := db.Exec(`
		INSERT INTO title_genres (title_id, genre_id)
		SELECT t.id, g.id
		FROM _title_genres_staging s
		JOIN titles t ON t.imdb_id = s.imdb_id
		JOIN genres g ON g.name = s.genre_name
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("title_genres insert from staging: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("[genres] complete: %d new title_genre associations inserted", rowsAffected)

	return nil
}

type episodeReady struct {
	seasonID    int
	episode     int
	displayName string
}

// syncEpisodes streams the episode TSV and merge-joins with the basics TSV
// (both sorted by tconst) to get display names without staging tables.
func syncEpisodes(filepath string, basicsFile string) error {
	// Build show imdb_id -> title_id cache (small: ~250K shows max)
	showCache := make(map[string]int)
	rows, err := db.Query(`SELECT imdb_id, id FROM titles WHERE type = 'show' AND imdb_id IS NOT NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var imdbID string
		var titleID int
		rows.Scan(&imdbID, &titleID)
		showCache[imdbID] = titleID
	}
	rows.Close()
	log.Printf("[episodes] loaded %d shows into cache", len(showCache))

	// Load existing seasons (small: ~100K)
	existingSeasons := make(map[seasonKey]int)
	rows, err = db.Query(`SELECT id, title_id, season FROM show_seasons`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, titleID, season int
		rows.Scan(&id, &titleID, &season)
		existingSeasons[seasonKey{titleID, season}] = id
	}
	rows.Close()
	log.Printf("[episodes] loaded %d existing seasons", len(existingSeasons))

	// First pass: collect seasons to insert
	log.Println("[episodes] scanning for new seasons...")
	seasonsToInsert := make(map[seasonKey]bool)

	f, err := os.Open(filepath)
	if err != nil {
		return err
	}

	gz, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return err
	}

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	scanner.Scan() // Skip header

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 4 {
			continue
		}
		parentImdbID := fields[1]
		seasonStr := fields[2]

		if seasonStr == "\\N" {
			continue
		}
		season, err := strconv.Atoi(seasonStr)
		if err != nil {
			continue
		}

		titleID, ok := showCache[parentImdbID]
		if !ok {
			continue
		}

		key := seasonKey{titleID, season}
		if _, exists := existingSeasons[key]; !exists {
			seasonsToInsert[key] = true
		}
	}
	gz.Close()
	f.Close()

	// Insert new seasons
	if len(seasonsToInsert) > 0 {
		log.Printf("[episodes] inserting %d new seasons...", len(seasonsToInsert))
		seasonList := make([]seasonKey, 0, len(seasonsToInsert))
		for k := range seasonsToInsert {
			seasonList = append(seasonList, k)
		}

		for i := 0; i < len(seasonList); i += upsertBatchSize {
			end := i + upsertBatchSize
			if end > len(seasonList) {
				end = len(seasonList)
			}
			batch := seasonList[i:end]

			values := make([]string, len(batch))
			args := make([]any, len(batch)*2)
			for j, k := range batch {
				values[j] = fmt.Sprintf("($%d, $%d)", j*2+1, j*2+2)
				args[j*2] = k.titleID
				args[j*2+1] = k.season
			}

			rows, err := db.Query(fmt.Sprintf(`
				INSERT INTO show_seasons (title_id, season)
				VALUES %s
				ON CONFLICT (title_id, season) DO NOTHING
				RETURNING id, title_id, season
			`, strings.Join(values, ",")), args...)
			if err != nil {
				return fmt.Errorf("season insert: %w", err)
			}

			for rows.Next() {
				var id, titleID, season int
				rows.Scan(&id, &titleID, &season)
				existingSeasons[seasonKey{titleID, season}] = id
			}
			rows.Close()
		}
		log.Printf("[episodes] seasons inserted, total seasons: %d", len(existingSeasons))
	}

	// Second pass: stream episodes, merge-join with basics file for display names
	log.Println("[episodes] streaming episode upserts (merge-join with basics)...")

	// Open episodes file
	f, err = os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err = gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	epScanner := bufio.NewScanner(gz)
	epScanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	epScanner.Scan() // Skip header

	// Open basics file for merge-join (sorted by tconst)
	bf, err := os.Open(basicsFile)
	if err != nil {
		return err
	}
	defer bf.Close()

	bgz, err := gzip.NewReader(bf)
	if err != nil {
		return err
	}
	defer bgz.Close()

	basicsScanner := bufio.NewScanner(bgz)
	basicsScanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	basicsScanner.Scan() // Skip header

	// Basics file cursor state for merge-join
	var basicsCurID, basicsCurName string
	var basicsEOF bool
	advanceBasics := func() {
		for basicsScanner.Scan() {
			bfields := strings.Split(basicsScanner.Text(), "\t")
			if len(bfields) >= 3 && bfields[1] == "tvEpisode" {
				basicsCurID = bfields[0]
				basicsCurName = bfields[2]
				return
			}
		}
		basicsEOF = true
	}
	advanceBasics() // position at first tvEpisode

	// lookupDisplayName advances the basics cursor to find the display name
	// for a given episode IMDb ID. Works because both files are sorted by tconst.
	lookupDisplayName := func(imdbID string) string {
		for !basicsEOF && basicsCurID < imdbID {
			advanceBasics()
		}
		if !basicsEOF && basicsCurID == imdbID {
			return basicsCurName
		}
		return ""
	}

	var (
		pendingBatch []episodeReady
		scanned      int64
		inserted     int64
		updated      int64
		unchangedEp  int64
		skipped      int64
		batchNum     int64
		totalBatches int64 = 14600 // ~7.3M episodes / 500
	)

	for epScanner.Scan() {
		fields := strings.Split(epScanner.Text(), "\t")
		if len(fields) < 4 {
			continue
		}

		episodeImdbID := fields[0]
		parentImdbID := fields[1]
		seasonStr := fields[2]
		episodeStr := fields[3]

		if seasonStr == "\\N" || episodeStr == "\\N" {
			skipped++
			continue
		}

		season, err := strconv.Atoi(seasonStr)
		if err != nil {
			continue
		}
		episode, err := strconv.Atoi(episodeStr)
		if err != nil {
			continue
		}

		titleID, ok := showCache[parentImdbID]
		if !ok {
			continue
		}

		seasonID, ok := existingSeasons[seasonKey{titleID, season}]
		if !ok {
			continue
		}

		displayName := lookupDisplayName(episodeImdbID)

		scanned++
		pendingBatch = append(pendingBatch, episodeReady{
			seasonID:    seasonID,
			episode:     episode,
			displayName: displayName,
		})

		if len(pendingBatch) >= upsertBatchSize {
			ins, upd, unch, err := upsertEpisodeBatchDirect(pendingBatch)
			if err != nil {
				return err
			}
			inserted += ins
			updated += upd
			unchangedEp += unch
			pendingBatch = pendingBatch[:0]
			batchNum++

			if batchNum%500 == 0 {
				log.Printf("[episodes] %d/%d batches (%d rows) | %d inserted, %d updated, %d unchanged",
					batchNum, totalBatches, scanned, inserted, updated, unchangedEp)
			}
		}
	}

	// Flush remaining
	if len(pendingBatch) > 0 {
		ins, upd, unch, err := upsertEpisodeBatchDirect(pendingBatch)
		if err != nil {
			return err
		}
		inserted += ins
		updated += upd
		unchangedEp += unch
		batchNum++
	}

	log.Printf("[episodes] complete: %d batches, %d rows | %d inserted, %d updated, %d unchanged, %d skipped",
		batchNum, scanned, inserted, updated, unchangedEp, skipped)

	return epScanner.Err()
}

// upsertEpisodeBatchDirect UPSERTs episodes with display names already resolved.
func upsertEpisodeBatchDirect(batch []episodeReady) (int64, int64, int64, error) {
	if len(batch) == 0 {
		return 0, 0, 0, nil
	}

	values := make([]string, len(batch))
	args := make([]any, len(batch)*3)
	for j, ep := range batch {
		base := j * 3
		values[j] = fmt.Sprintf("($%d, $%d, $%d)", base+1, base+2, base+3)
		args[base] = ep.seasonID
		args[base+1] = ep.episode
		if ep.displayName == "" {
			args[base+2] = nil
		} else {
			args[base+2] = ep.displayName
		}
	}

	query := fmt.Sprintf(`
		INSERT INTO show_episodes (season_id, episode, display_name)
		VALUES %s
		ON CONFLICT (season_id, episode) DO UPDATE SET
			display_name = EXCLUDED.display_name
		WHERE show_episodes.display_name IS DISTINCT FROM EXCLUDED.display_name
		RETURNING id, (xmax = 0) as was_inserted
	`, strings.Join(values, ","))

	resultRows, err := db.Query(query, args...)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("episode upsert: %w", err)
	}

	var insertCount, updateCount int64
	for resultRows.Next() {
		var id int
		var wasInserted bool
		resultRows.Scan(&id, &wasInserted)
		if wasInserted {
			insertCount++
		} else {
			updateCount++
		}
	}
	resultRows.Close()

	unchangedCount := int64(len(batch)) - insertCount - updateCount
	return insertCount, updateCount, unchangedCount, nil
}

// syncRatings streams the ratings TSV and batch-UPDATEs in batches of 100.
// No pre-loaded map of existing ratings.
func syncRatings(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	scanner.Scan() // Skip header: tconst, averageRating, numVotes

	var (
		batch        []RatingRecord
		scanned      int64
		updated      int64
		batchNum     int64
		totalBatches int64 = 14000 // ~1.4M ratings / 100
	)

	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 3 {
			continue
		}

		imdbID := fields[0]
		averageRating, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		numVotes, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}

		scanned++
		batch = append(batch, RatingRecord{imdbID, numVotes, averageRating})

		if len(batch) >= upsertBatchSize {
			n, err := updateRatingsBatchStreaming(batch)
			if err != nil {
				return err
			}
			updated += n
			batch = batch[:0]
			batchNum++

			if batchNum%150 == 0 {
				log.Printf("[ratings] %d/%d batches (%d rows) | %d updated",
					batchNum, totalBatches, scanned, updated)
			}
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		n, err := updateRatingsBatchStreaming(batch)
		if err != nil {
			return err
		}
		updated += n
		batchNum++
	}

	log.Printf("[ratings] complete: %d batches, %d rows scanned, %d updated",
		batchNum, scanned, updated)
	return scanner.Err()
}

// updateRatingsBatchStreaming updates ratings using WHERE IS DISTINCT FROM
// to skip unchanged rows. No pre-loaded map needed.
func updateRatingsBatchStreaming(records []RatingRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(records)*3)
	votesCases := make([]string, len(records))
	ratingCases := make([]string, len(records))
	idPlaceholders := make([]string, len(records))

	for j, r := range records {
		base := j * 3
		idPlaceholders[j] = fmt.Sprintf("$%d", base+1)
		votesCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d::integer", base+1, base+2)
		ratingCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d::real", base+1, base+3)
		args = append(args, r.ImdbID, r.NumVotes, r.AverageRating)
	}

	// Use subquery with IS DISTINCT FROM to only update changed rows
	result, err := db.Exec(fmt.Sprintf(`
		UPDATE titles SET
			num_votes = CASE %s END,
			average_rating = CASE %s END
		WHERE imdb_id IN (%s)
		AND (
			num_votes IS DISTINCT FROM (CASE %s END)
			OR average_rating IS DISTINCT FROM (CASE %s END)
		)
	`, strings.Join(votesCases, " "), strings.Join(ratingCases, " "),
		strings.Join(idPlaceholders, ","),
		strings.Join(votesCases, " "), strings.Join(ratingCases, " ")), args...)
	if err != nil {
		return 0, fmt.Errorf("ratings update: %w", err)
	}

	n, _ := result.RowsAffected()
	return n, nil
}

// Custom genre names (arbitrary thematic tags assigned during review)
var customGenreNames = []string{"Dating", "Cooking"}

// tmdbBackfill fetches origin_country (and other metadata) from TMDB for a title missing it.
// Returns the origin_country code, or "" if unavailable.
func tmdbBackfill(titleID int, imdbID, titleType string) string {
	if tmdbAPIKey == "" || imdbID == "" {
		return ""
	}

	url := fmt.Sprintf("https://api.themoviedb.org/3/find/%s?api_key=%s&external_source=imdb_id", imdbID, tmdbAPIKey)
	resp, err := http.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return ""
	}

	var result struct {
		TVResults []struct {
			ID               int      `json:"id"`
			PosterPath       string   `json:"poster_path"`
			OriginalLanguage string   `json:"original_language"`
			FirstAirDate     string   `json:"first_air_date"`
			Popularity       float64  `json:"popularity"`
			OriginCountry    []string `json:"origin_country"`
		} `json:"tv_results"`
		MovieResults []struct {
			ID               int      `json:"id"`
			PosterPath       string   `json:"poster_path"`
			OriginalLanguage string   `json:"original_language"`
			ReleaseDate      string   `json:"release_date"`
			Popularity       float64  `json:"popularity"`
			OriginCountry    []string `json:"origin_country"`
		} `json:"movie_results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}

	var originCountry, origLang, releaseDate, posterPath string
	var tmdbID int
	var popularity float64

	if titleType == "show" && len(result.TVResults) > 0 {
		tv := result.TVResults[0]
		tmdbID = tv.ID
		origLang = tv.OriginalLanguage
		releaseDate = tv.FirstAirDate
		popularity = tv.Popularity
		posterPath = tv.PosterPath
		if len(tv.OriginCountry) > 0 {
			originCountry = tv.OriginCountry[0]
		}
	} else if titleType == "movie" && len(result.MovieResults) > 0 {
		mv := result.MovieResults[0]
		tmdbID = mv.ID
		origLang = mv.OriginalLanguage
		releaseDate = mv.ReleaseDate
		popularity = mv.Popularity
		posterPath = mv.PosterPath
		if len(mv.OriginCountry) > 0 {
			originCountry = mv.OriginCountry[0]
		}
	}

	if tmdbID == 0 {
		return ""
	}

	// Find API doesn't return origin_country for movies — fetch from movie details
	if originCountry == "" && tmdbID != 0 {
		detailURL := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?api_key=%s", tmdbID, tmdbAPIKey)
		if titleType == "show" {
			detailURL = fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?api_key=%s", tmdbID, tmdbAPIKey)
		}
		if dresp, err := http.Get(detailURL); err == nil {
			defer dresp.Body.Close()
			if dresp.StatusCode == 200 {
				var detail struct {
					OriginCountry       []string `json:"origin_country"`
					ProductionCountries []struct {
						ISO string `json:"iso_3166_1"`
					} `json:"production_countries"`
				}
				if json.NewDecoder(dresp.Body).Decode(&detail) == nil {
					if len(detail.OriginCountry) > 0 {
						originCountry = detail.OriginCountry[0]
					} else if len(detail.ProductionCountries) > 0 {
						originCountry = detail.ProductionCountries[0].ISO
					}
				}
			}
		}
	}

	imageURL := ""
	if posterPath != "" {
		imageURL = "https://image.tmdb.org/t/p/w500" + posterPath
	}

	db.Exec(`UPDATE titles SET
		tmdb_id = COALESCE(tmdb_id, $1),
		image_url = COALESCE(NULLIF(image_url, ''), NULLIF($2, '')),
		original_language = COALESCE(NULLIF($3, ''), original_language),
		release_date = CASE WHEN $4 = '' THEN release_date ELSE COALESCE(release_date, $4::date) END,
		tmdb_popularity = COALESCE(tmdb_popularity, $5),
		origin_country = COALESCE(NULLIF($6, ''), origin_country)
		WHERE id = $7`,
		tmdbID, imageURL, origLang, releaseDate, popularity, originCountry, titleID)

	return originCountry
}

// tmdbBackfillBatch processes all titles with needs_backfill_tmdb=true in batches.
// For each title, calls TMDB Details API to fill origin_country, image, popularity, etc.
func tmdbBackfillBatch() {
	const batchLimit = 100
	// ~40 req/sec to stay under TMDB rate limit
	rateLimiter := time.NewTicker(25 * time.Millisecond)
	defer rateLimiter.Stop()

	total := 0
	db.QueryRow(`SELECT COUNT(*) FROM titles WHERE needs_backfill_tmdb = true`).Scan(&total)
	if total == 0 {
		log.Println("No titles need TMDB backfill")
		return
	}
	log.Printf("[2.1] %d titles need TMDB backfill, processing in batches of %d...", total, batchLimit)

	processed := 0
	updated := 0
	batchNum := 0

	for {
		batchNum++
		rows, err := db.Query(`
			SELECT id, type, imdb_id, tmdb_id
			FROM titles
			WHERE needs_backfill_tmdb = true
			ORDER BY num_votes DESC NULLS LAST
			LIMIT $1`, batchLimit)
		if err != nil {
			log.Printf("Batch query error: %v", err)
			break
		}

		type backfillRow struct {
			ID     int
			Type   string
			ImdbID *string
			TmdbID *int
		}
		var batch []backfillRow
		for rows.Next() {
			var r backfillRow
			rows.Scan(&r.ID, &r.Type, &r.ImdbID, &r.TmdbID)
			batch = append(batch, r)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		log.Printf("  Batch %d: %d titles (processed %d/%d so far, %d updated)", batchNum, len(batch), processed, total, updated)

		for _, r := range batch {
			if r.ImdbID == nil || *r.ImdbID == "" {
				db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, r.ID)
				processed++
				continue
			}

			tmdbID := 0
			if r.TmdbID != nil {
				tmdbID = *r.TmdbID
			}

			// Resolve TMDB ID via Find API if needed
			if tmdbID == 0 {
				<-rateLimiter.C
				findURL := fmt.Sprintf("https://api.themoviedb.org/3/find/%s?api_key=%s&external_source=imdb_id", *r.ImdbID, tmdbAPIKey)
				if resp, err := http.Get(findURL); err == nil {
					if resp.StatusCode == 200 {
						var result struct {
							TVResults    []struct{ ID int `json:"id"` } `json:"tv_results"`
							MovieResults []struct{ ID int `json:"id"` } `json:"movie_results"`
						}
						if json.NewDecoder(resp.Body).Decode(&result) == nil {
							if r.Type == "show" && len(result.TVResults) > 0 {
								tmdbID = result.TVResults[0].ID
							} else if r.Type == "movie" && len(result.MovieResults) > 0 {
								tmdbID = result.MovieResults[0].ID
							} else if len(result.MovieResults) > 0 {
								tmdbID = result.MovieResults[0].ID
							} else if len(result.TVResults) > 0 {
								tmdbID = result.TVResults[0].ID
							}
						}
					}
					resp.Body.Close()
				}
			}

			if tmdbID == 0 {
				db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, r.ID)
				processed++
				continue
			}

			// Call TMDB Details API
			<-rateLimiter.C
			detailURL := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?api_key=%s&append_to_response=watch/providers", tmdbID, tmdbAPIKey)
			if r.Type == "show" {
				detailURL = fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?api_key=%s&append_to_response=watch/providers", tmdbID, tmdbAPIKey)
			}

			dresp, err := http.Get(detailURL)
			if err != nil {
				log.Printf("    TMDB details fetch error for %d: %v", r.ID, err)
				db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, r.ID)
				processed++
				continue
			}

			if dresp.StatusCode != 200 {
				if dresp.StatusCode == 429 {
					log.Printf("    TMDB rate limited, sleeping 5s...")
					dresp.Body.Close()
					time.Sleep(5 * time.Second)
					processed++
					continue // don't clear flag, retry next batch
				}
				dresp.Body.Close()
				db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, r.ID)
				processed++
				continue
			}

			var detail struct {
				PosterPath          string   `json:"poster_path"`
				OriginalLanguage    string   `json:"original_language"`
				ReleaseDate         string   `json:"release_date"`
				FirstAirDate        string   `json:"first_air_date"`
				Popularity          float64  `json:"popularity"`
				OriginCountry       []string `json:"origin_country"`
				ProductionCountries []struct {
					ISO string `json:"iso_3166_1"`
				} `json:"production_countries"`
				Runtime             float64         `json:"runtime"`
				Networks            json.RawMessage `json:"networks"`
				ProductionCompanies json.RawMessage `json:"production_companies"`
				WatchProviders      struct {
					Results json.RawMessage `json:"results"`
				} `json:"watch/providers"`
			}
			json.NewDecoder(dresp.Body).Decode(&detail)
			dresp.Body.Close()

			originCountry := ""
			if len(detail.OriginCountry) > 0 {
				originCountry = detail.OriginCountry[0]
			} else if len(detail.ProductionCountries) > 0 {
				originCountry = detail.ProductionCountries[0].ISO
			}

			releaseDate := detail.ReleaseDate
			if releaseDate == "" {
				releaseDate = detail.FirstAirDate
			}

			imageURL := ""
			if detail.PosterPath != "" {
				imageURL = "https://image.tmdb.org/t/p/w500" + detail.PosterPath
			}

			_, err = db.Exec(`UPDATE titles SET
				tmdb_id = $1,
				image_url = CASE WHEN $2 = '' THEN image_url ELSE COALESCE(NULLIF($2, ''), image_url) END,
				original_language = COALESCE(NULLIF($3, ''), original_language),
				release_date = CASE WHEN $4 = '' THEN release_date ELSE $4::date END,
				tmdb_popularity = CASE WHEN $5::real = 0 THEN tmdb_popularity ELSE $5::real END,
				origin_country = COALESCE(NULLIF($6, ''), origin_country),
				runtime_minutes = CASE WHEN $7::int = 0 THEN runtime_minutes ELSE $7::int END,
				needs_backfill_tmdb = false,
				networks = $9,
				production_companies = $10,
				watch_providers = $11,
				watch_providers_checked_at = NOW()
				WHERE id = $8`,
				tmdbID, imageURL, detail.OriginalLanguage, releaseDate,
				detail.Popularity, originCountry, int(detail.Runtime), r.ID,
				detail.Networks, detail.ProductionCompanies, detail.WatchProviders.Results)

			if err != nil {
				log.Printf("    DB update error for %d: %v", r.ID, err)
			} else {
				updated++
			}
			processed++
		}
	}

	log.Printf("[2.1] TMDB backfill complete: %d processed, %d updated", processed, updated)
}

func ensureCustomGenreSchema() error {
	_, err := db.Exec(`ALTER TABLE genres ADD COLUMN IF NOT EXISTS is_custom BOOLEAN DEFAULT FALSE`)
	if err != nil {
		return fmt.Errorf("alter genres table: %w", err)
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_titles_original_language ON titles(original_language)`)
	if err != nil {
		return fmt.Errorf("create language index: %w", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS custom_genre_reviews (
		title_id INTEGER PRIMARY KEY REFERENCES titles(id) ON DELETE CASCADE,
		reviewed_at TIMESTAMP DEFAULT NOW()
	)`)
	if err != nil {
		return fmt.Errorf("create custom_genre_reviews table: %w", err)
	}
	for _, name := range customGenreNames {
		_, err := db.Exec(`INSERT INTO genres (name, is_custom) VALUES ($1, true) ON CONFLICT (name) DO UPDATE SET is_custom = true`, name)
		if err != nil {
			return fmt.Errorf("insert custom genre %q: %w", name, err)
		}
	}
	return nil
}

func exportGenreReview(filename string, limit int, filterGenres []string) error {
	if err := ensureCustomGenreSchema(); err != nil {
		return err
	}

	// Load custom genre names from DB
	var customNames []string
	rows, err := db.Query(`SELECT name FROM genres WHERE is_custom = true ORDER BY name`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		customNames = append(customNames, name)
	}
	rows.Close()

	// Query unreviewed titles, optionally filtered by IMDb genre
	var query string
	var args []any

	if len(filterGenres) > 0 {
		placeholders := make([]string, len(filterGenres))
		for i, g := range filterGenres {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, g)
		}
		args = append(args, limit)
		query = fmt.Sprintf(`SELECT t.id, t.display_name, t.start_year, t.type, COALESCE(t.num_votes, 0), COALESCE(t.average_rating, 0),
			COALESCE(t.original_language, ''), COALESCE(t.origin_country, ''), COALESCE(t.imdb_id, ''),
			(SELECT string_agg(g.name, ', ' ORDER BY g.name) FROM title_genres tg JOIN genres g ON g.id = tg.genre_id WHERE tg.title_id = t.id) as genres
			FROM titles t
			WHERE NOT EXISTS (SELECT 1 FROM custom_genre_reviews cr WHERE cr.title_id = t.id)
			AND EXISTS (SELECT 1 FROM title_genres tg2 JOIN genres g2 ON g2.id = tg2.genre_id WHERE tg2.title_id = t.id AND g2.name IN (%s))
			ORDER BY t.num_votes DESC NULLS LAST
			LIMIT $%d`, strings.Join(placeholders, ","), len(filterGenres)+1)
		log.Printf("Filtering by IMDb genres: %s", strings.Join(filterGenres, ", "))
	} else {
		query = `SELECT t.id, t.display_name, t.start_year, t.type, COALESCE(t.num_votes, 0), COALESCE(t.average_rating, 0),
			COALESCE(t.original_language, ''), COALESCE(t.origin_country, ''), COALESCE(t.imdb_id, ''),
			(SELECT string_agg(g.name, ', ' ORDER BY g.name) FROM title_genres tg JOIN genres g ON g.id = tg.genre_id WHERE tg.title_id = t.id) as genres
			FROM titles t
			WHERE NOT EXISTS (SELECT 1 FROM custom_genre_reviews cr WHERE cr.title_id = t.id)
			ORDER BY t.num_votes DESC NULLS LAST
			LIMIT $1`
		args = append(args, limit)
	}

	rows, err = db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("query candidates: %w", err)
	}

	type candidate struct {
		ID        int
		Name      string
		StartYear *int
		Type      string
		Votes     int
		Rating    float64
		Lang      string
		Country   string
		ImdbID    string
		Genres    *string
	}

	var candidates []candidate
	for rows.Next() {
		var c candidate
		rows.Scan(&c.ID, &c.Name, &c.StartYear, &c.Type, &c.Votes, &c.Rating, &c.Lang, &c.Country, &c.ImdbID, &c.Genres)
		candidates = append(candidates, c)
	}
	rows.Close()

	if len(candidates) == 0 {
		log.Println("No unreviewed titles found")
		return nil
	}

	// TMDB backfill for titles missing origin_country
	backfilled := 0
	for i := range candidates {
		c := &candidates[i]
		if c.Country == "" && c.ImdbID != "" {
			if oc := tmdbBackfill(c.ID, c.ImdbID, c.Type); oc != "" {
				c.Country = oc
				backfilled++
			}
		}
	}
	if backfilled > 0 {
		log.Printf("Backfilled origin_country for %d titles via TMDB", backfilled)
	}

	// Write file
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "# MediaCanon Custom Genre Review\n")
	fmt.Fprintf(w, "# Generated: %s | %d titles | Custom genres: %s\n",
		time.Now().Format("2006-01-02"), len(candidates), strings.Join(customNames, ", "))
	fmt.Fprintf(w, "# Edit GENRES lines. Use \"none\" or leave empty to skip.\n")
	fmt.Fprintf(w, "# Import: ./sync-mediacanon -genres-import %s\n", filename)
	fmt.Fprintf(w, "\n")

	for _, c := range candidates {
		yearStr := "????"
		if c.StartYear != nil {
			yearStr = strconv.Itoa(*c.StartYear)
		}
		langCountry := c.Lang
		if c.Country != "" {
			langCountry = c.Lang + "/" + c.Country
		}
		genresStr := ""
		if c.Genres != nil && *c.Genres != "" {
			genresStr = " | " + *c.Genres
		}

		fmt.Fprintf(w, "[%d] %s (%s) | %s | %s votes | %.1f | %s%s\n",
			c.ID, c.Name, yearStr, c.Type, formatVotes(c.Votes), c.Rating, langCountry, genresStr)
		fmt.Fprintf(w, "GENRES:\n")
		fmt.Fprintf(w, "\n")
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	log.Printf("Exported %d titles to %s", len(candidates), filename)
	return nil
}

func importGenreReview(filename string) error {
	if err := ensureCustomGenreSchema(); err != nil {
		return err
	}

	// Load custom genre ID cache
	genreCache := make(map[string]int)
	rows, err := db.Query(`SELECT id, name FROM genres WHERE is_custom = true`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		genreCache[name] = id
	}
	rows.Close()

	// Also build a case-insensitive lookup
	genreCacheLC := make(map[string]int)
	for name, id := range genreCache {
		genreCacheLC[strings.ToLower(name)] = id
	}

	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	var currentTitleID int
	var titlesProcessed, genresAssigned, skipped int

	for scanner.Scan() {
		line := scanner.Text()

		// Skip comments and blank lines
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// Title line: [ID] ...
		if strings.HasPrefix(trimmed, "[") {
			closeBracket := strings.Index(trimmed, "]")
			if closeBracket == -1 {
				continue
			}
			idStr := trimmed[1:closeBracket]
			id, err := strconv.Atoi(idStr)
			if err != nil {
				log.Printf("WARNING: invalid title ID %q, skipping", idStr)
				currentTitleID = 0
				continue
			}
			currentTitleID = id
			continue
		}

		// GENRES: line
		if strings.HasPrefix(trimmed, "GENRES:") && currentTitleID != 0 {
			titleID := currentTitleID
			currentTitleID = 0 // consume

			genreStr := strings.TrimSpace(strings.TrimPrefix(trimmed, "GENRES:"))

			// Mark as reviewed regardless
			_, err := db.Exec(`INSERT INTO custom_genre_reviews (title_id) VALUES ($1) ON CONFLICT DO NOTHING`, titleID)
			if err != nil {
				log.Printf("WARNING: failed to mark title %d as reviewed: %v", titleID, err)
			}
			titlesProcessed++

			if genreStr == "" || strings.EqualFold(genreStr, "none") {
				skipped++
				continue
			}

			// Parse comma-separated genres
			names := strings.Split(genreStr, ",")
			for _, raw := range names {
				name := strings.TrimSpace(raw)
				if name == "" {
					continue
				}
				genreID, ok := genreCache[name]
				if !ok {
					// Try case-insensitive
					genreID, ok = genreCacheLC[strings.ToLower(name)]
				}
				if !ok {
					log.Printf("WARNING: unknown genre %q for title %d, skipping", name, titleID)
					continue
				}
				_, err := db.Exec(`INSERT INTO title_genres (title_id, genre_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, titleID, genreID)
				if err != nil {
					log.Printf("WARNING: failed to assign genre %q to title %d: %v", name, titleID, err)
					continue
				}
				genresAssigned++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	log.Printf("Import complete: %d titles processed, %d genre assignments, %d skipped (none/empty)", titlesProcessed, genresAssigned, skipped)
	return nil
}

func formatVotes(n int) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.0fK", float64(n)/1000)
	}
	return strconv.Itoa(n)
}

func init() {
	fmt.Println("MediaCanon IMDb Sync")
	fmt.Println()
	fmt.Println("Modes:")
	fmt.Println("  (default)           IMDb import (download, titles, genres, episodes, ratings)")
	fmt.Println("  -genres-export FILE Export unreviewed titles for genre review")
	fmt.Println("  -genres-import FILE Import genre assignments from reviewed file")
	fmt.Println()
	fmt.Println("Flags: -force            re-import even if files unchanged")
	fmt.Println("       -genres-limit N   number of titles to export (default 100)")
	fmt.Println("       -genres-filter X  only export titles with these IMDb genres (comma-separated)")
	fmt.Println()
}
