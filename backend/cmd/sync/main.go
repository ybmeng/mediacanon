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
	"sync"
	"time"

	_ "github.com/lib/pq"
)

const (
	basicsURL   = "https://datasets.imdbws.com/title.basics.tsv.gz"
	episodesURL = "https://datasets.imdbws.com/title.episode.tsv.gz"
	ratingsURL  = "https://datasets.imdbws.com/title.ratings.tsv.gz"
)

var (
	db            *sql.DB
	episodeTitles sync.Map // Concurrent map for episode titles
	batchSize     int
	workers       int
	titleGenres   = make(map[string][]string) // imdb_id -> genre names, populated during title scan
	tmdbAPIKey    string
)

// Existing data caches
type ExistingTitle struct {
	ID             int
	Type           string
	DisplayName    string
	StartYear      *int
	EndYear        *int
	OriginalTitle  string
	RuntimeMinutes *int
}

type ExistingEpisode struct {
	ID          int
	DisplayName string
}

type seasonKey struct {
	showID int
	season int
}

type episodeKey struct {
	seasonID int
	episode  int
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

type EpisodeRecord struct {
	ImdbID       string
	ParentImdbID string
	Season       int
	Episode      int
	DisplayName  string
}

func main() {
	dsn := flag.String("db", "postgres://localhost/mediacanon?sslmode=disable", "Database URL")
	downloadDir := flag.String("dir", "./imdb_data", "Directory to store downloaded files")
	forceImdb := flag.Bool("force", false, "Force IMDb import even if files unchanged")
	genresExport := flag.String("genres-export", "", "Export unreviewed titles to file for genre review")
	genresImport := flag.String("genres-import", "", "Import genre assignments from reviewed file")
	genresLimit := flag.Int("genres-limit", 100, "Number of titles to export for genre review")
	genresFilter := flag.String("genres-filter", "", "Only export titles with these IMDb genres (comma-separated, e.g. 'Reality-TV,Game-Show')")
	flag.IntVar(&batchSize, "batch", 5000, "Batch size for inserts")
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

		log.Println("[1.3] Syncing titles...")
		if err := syncTitles(basicsFile); err != nil {
			log.Fatal(err)
		}

		log.Println("[1.4] Syncing genres...")
		if err := syncGenres(); err != nil {
			log.Fatal(err)
		}

		log.Println("[1.5] Syncing episodes...")
		if err := syncEpisodes(episodesFile); err != nil {
			log.Fatal(err)
		}

		log.Println("[1.6] Syncing ratings...")
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

func syncTitles(filepath string) error {
	// Load existing titles from DB
	log.Println("Loading existing titles from database...")
	existingTitles := make(map[string]ExistingTitle)
	rows, err := db.Query(`SELECT id, imdb_id, type, display_name, start_year, end_year, COALESCE(original_title, ''), runtime_minutes FROM titles`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var t ExistingTitle
		var imdbID string
		rows.Scan(&t.ID, &imdbID, &t.Type, &t.DisplayName, &t.StartYear, &t.EndYear, &t.OriginalTitle, &t.RuntimeMinutes)
		existingTitles[imdbID] = t
	}
	rows.Close()
	log.Printf("Loaded %d existing titles", len(existingTitles))

	// Load existing movies and shows
	existingMovies := make(map[int]bool)
	rows, err = db.Query(`SELECT title_id FROM movies`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int
		rows.Scan(&id)
		existingMovies[id] = true
	}
	rows.Close()

	existingShows := make(map[int]bool)
	rows, err = db.Query(`SELECT title_id FROM shows`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int
		rows.Scan(&id)
		existingShows[id] = true
	}
	rows.Close()
	log.Printf("Loaded %d existing movies, %d existing shows", len(existingMovies), len(existingShows))

	// Read file and diff
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

	var toInsert []TitleRecord
	var toUpdate []TitleRecord
	var newMovieTitleIDs, newShowTitleIDs []int

	var scanned, unchanged, ignored, episodeCount int64

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

		// Collect episode titles for later
		if titleType == "tvEpisode" {
			episodeTitles.Store(imdbID, displayName)
			episodeCount++
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

		// Parse runtime (fields[7])
		var runtimeMinutes *int
		if fields[7] != "\\N" {
			if rt, err := strconv.Atoi(fields[7]); err == nil {
				runtimeMinutes = &rt
			}
		}

		// Parse genres (fields[8], comma-separated)
		var genres []string
		if fields[8] != "\\N" {
			genres = strings.Split(fields[8], ",")
		}

		scanned++

		record := TitleRecord{
			ImdbID:         imdbID,
			Type:           ourType,
			DisplayName:    displayName,
			StartYear:      startYear,
			EndYear:        endYear,
			OriginalTitle:  originalTitle,
			RuntimeMinutes: runtimeMinutes,
			Genres:         genres,
		}

		// Store genres for later sync
		if len(genres) > 0 {
			titleGenres[imdbID] = genres
		}

		// Check if exists and needs update
		if existing, ok := existingTitles[imdbID]; ok {
			needsUpdate := existing.DisplayName != displayName ||
				!intsEqual(existing.StartYear, startYear) ||
				!intsEqual(existing.EndYear, endYear) ||
				existing.OriginalTitle != originalTitle ||
				!intsEqual(existing.RuntimeMinutes, runtimeMinutes)

			if needsUpdate {
				toUpdate = append(toUpdate, record)
			} else {
				unchanged++
			}

			// Check if movie/show record exists
			if ourType == "movie" && !existingMovies[existing.ID] {
				newMovieTitleIDs = append(newMovieTitleIDs, existing.ID)
			} else if ourType == "show" && !existingShows[existing.ID] {
				newShowTitleIDs = append(newShowTitleIDs, existing.ID)
			}
		} else {
			toInsert = append(toInsert, record)
		}

		if scanned%100000 == 0 {
			log.Printf("Scanned %d titles: %d unchanged, %d to insert, %d to update, %d ignored, %d episode titles...",
				scanned, unchanged, len(toInsert), len(toUpdate), ignored, episodeCount)
		}
	}

	log.Printf("Scan complete: %d movies/shows (%d to insert, %d to update, %d unchanged), %d ignored (shorts/games/etc), %d episode titles",
		scanned, len(toInsert), len(toUpdate), unchanged, ignored, episodeCount)

	// Insert new titles in parallel batches
	if len(toInsert) > 0 {
		log.Printf("Inserting %d new titles...", len(toInsert))
		newIDs, err := insertTitlesBatched(toInsert)
		if err != nil {
			return err
		}

		// Collect new movie/show IDs
		for i, r := range toInsert {
			if r.Type == "movie" {
				newMovieTitleIDs = append(newMovieTitleIDs, newIDs[i])
			} else {
				newShowTitleIDs = append(newShowTitleIDs, newIDs[i])
			}
		}
	}

	// Update changed titles
	if len(toUpdate) > 0 {
		log.Printf("Updating %d changed titles...", len(toUpdate))
		if err := updateTitlesBatched(toUpdate); err != nil {
			return err
		}
	}

	// Insert new movie/show records
	if len(newMovieTitleIDs) > 0 {
		log.Printf("Inserting %d new movie records...", len(newMovieTitleIDs))
		if err := insertMoviesBatched(newMovieTitleIDs); err != nil {
			return err
		}
	}

	if len(newShowTitleIDs) > 0 {
		log.Printf("Inserting %d new show records...", len(newShowTitleIDs))
		if err := insertShowsBatched(newShowTitleIDs); err != nil {
			return err
		}
	}

	log.Printf("Titles done: %d inserted, %d updated, %d unchanged, %d ignored",
		len(toInsert), len(toUpdate), unchanged, ignored)

	return scanner.Err()
}

func intsEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func insertTitlesBatched(records []TitleRecord) ([]int, error) {
	ids := make([]int, len(records))
	idIdx := 0

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		values := make([]string, len(batch))
		args := make([]any, len(batch)*7)
		for j, r := range batch {
			base := j * 7
			values[j] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4, base+5, base+6, base+7)
			args[base] = r.ImdbID
			args[base+1] = r.Type
			args[base+2] = r.DisplayName
			args[base+3] = r.StartYear
			args[base+4] = r.EndYear
			args[base+5] = r.OriginalTitle
			args[base+6] = r.RuntimeMinutes
		}

		rows, err := db.Query(fmt.Sprintf(`
			INSERT INTO titles (imdb_id, type, display_name, start_year, end_year, original_title, runtime_minutes)
			VALUES %s
			RETURNING id
		`, strings.Join(values, ",")), args...)
		if err != nil {
			return nil, fmt.Errorf("title insert: %w", err)
		}

		for rows.Next() {
			rows.Scan(&ids[idIdx])
			idIdx++
		}
		rows.Close()

		if (i/batchSize+1)%20 == 0 || end >= len(records) {
			log.Printf("  inserted %d/%d titles...", end, len(records))
		}
	}

	return ids, nil
}

func updateTitlesBatched(records []TitleRecord) error {
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]

		// Use a single UPDATE with CASE statements for efficiency
		var imdbIDs []string
		displayNames := make(map[string]string)
		startYears := make(map[string]*int)
		endYears := make(map[string]*int)
		originalTitles := make(map[string]string)
		runtimes := make(map[string]*int)

		for _, r := range batch {
			imdbIDs = append(imdbIDs, r.ImdbID)
			displayNames[r.ImdbID] = r.DisplayName
			startYears[r.ImdbID] = r.StartYear
			endYears[r.ImdbID] = r.EndYear
			originalTitles[r.ImdbID] = r.OriginalTitle
			runtimes[r.ImdbID] = r.RuntimeMinutes
		}

		// Build UPDATE query
		args := make([]any, 0, len(batch)*6)
		displayCases := make([]string, len(batch))
		startYearCases := make([]string, len(batch))
		endYearCases := make([]string, len(batch))
		origTitleCases := make([]string, len(batch))
		runtimeCases := make([]string, len(batch))
		idPlaceholders := make([]string, len(batch))

		for j, id := range imdbIDs {
			base := j * 6
			idPlaceholders[j] = fmt.Sprintf("$%d", base+1)
			displayCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d", base+1, base+2)
			startYearCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d::integer", base+1, base+3)
			endYearCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d::integer", base+1, base+4)
			origTitleCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d", base+1, base+5)
			runtimeCases[j] = fmt.Sprintf("WHEN imdb_id = $%d THEN $%d::integer", base+1, base+6)
			args = append(args, id, displayNames[id], startYears[id], endYears[id], originalTitles[id], runtimes[id])
		}

		_, err := db.Exec(fmt.Sprintf(`
			UPDATE titles SET
				display_name = CASE %s END,
				start_year = CASE %s END,
				end_year = CASE %s END,
				original_title = CASE %s END,
				runtime_minutes = CASE %s END,
				updated_at = NOW()
			WHERE imdb_id IN (%s)
		`, strings.Join(displayCases, " "), strings.Join(startYearCases, " "), strings.Join(endYearCases, " "), strings.Join(origTitleCases, " "), strings.Join(runtimeCases, " "), strings.Join(idPlaceholders, ",")), args...)
		if err != nil {
			return fmt.Errorf("title update: %w", err)
		}

		if (i/batchSize+1)%20 == 0 || end >= len(records) {
			log.Printf("  updated %d/%d titles...", end, len(records))
		}
	}
	return nil
}

func insertMoviesBatched(titleIDs []int) error {
	for i := 0; i < len(titleIDs); i += batchSize {
		end := i + batchSize
		if end > len(titleIDs) {
			end = len(titleIDs)
		}
		batch := titleIDs[i:end]

		values := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, id := range batch {
			values[j] = fmt.Sprintf("($%d)", j+1)
			args[j] = id
		}

		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO movies (title_id) VALUES %s
			ON CONFLICT (title_id) DO NOTHING
		`, strings.Join(values, ",")), args...)
		if err != nil {
			return fmt.Errorf("movie insert: %w", err)
		}
	}
	return nil
}

func insertShowsBatched(titleIDs []int) error {
	for i := 0; i < len(titleIDs); i += batchSize {
		end := i + batchSize
		if end > len(titleIDs) {
			end = len(titleIDs)
		}
		batch := titleIDs[i:end]

		values := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, id := range batch {
			values[j] = fmt.Sprintf("($%d)", j+1)
			args[j] = id
		}

		_, err := db.Exec(fmt.Sprintf(`
			INSERT INTO shows (title_id) VALUES %s
			ON CONFLICT (title_id) DO NOTHING
		`, strings.Join(values, ",")), args...)
		if err != nil {
			return fmt.Errorf("show insert: %w", err)
		}
	}
	return nil
}

func syncGenres() error {
	if len(titleGenres) == 0 {
		log.Println("No genre data collected, skipping genre sync")
		return nil
	}

	// Collect all unique genre names
	genreSet := make(map[string]bool)
	for _, genres := range titleGenres {
		for _, g := range genres {
			genreSet[g] = true
		}
	}
	log.Printf("Found %d unique genres across %d titles", len(genreSet), len(titleGenres))

	// Insert genres (ON CONFLICT DO NOTHING)
	genreNames := make([]string, 0, len(genreSet))
	for g := range genreSet {
		genreNames = append(genreNames, g)
	}
	for i := 0; i < len(genreNames); i += batchSize {
		end := i + batchSize
		if end > len(genreNames) {
			end = len(genreNames)
		}
		batch := genreNames[i:end]
		values := make([]string, len(batch))
		args := make([]any, len(batch))
		for j, name := range batch {
			values[j] = fmt.Sprintf("($%d)", j+1)
			args[j] = name
		}
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO genres (name) VALUES %s ON CONFLICT (name) DO NOTHING`, strings.Join(values, ",")), args...)
		if err != nil {
			return fmt.Errorf("genre insert: %w", err)
		}
	}

	// Load genre ID cache
	genreIDCache := make(map[string]int)
	rows, err := db.Query(`SELECT id, name FROM genres`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		genreIDCache[name] = id
	}
	rows.Close()
	log.Printf("Loaded %d genres from database", len(genreIDCache))

	// Load imdb_id -> title_id cache
	imdbToTitleID := make(map[string]int)
	rows, err = db.Query(`SELECT imdb_id, id FROM titles WHERE imdb_id IS NOT NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var imdbID string
		var titleID int
		rows.Scan(&imdbID, &titleID)
		imdbToTitleID[imdbID] = titleID
	}
	rows.Close()
	log.Printf("Loaded %d imdb->title mappings", len(imdbToTitleID))

	// Load existing title_genres
	existingTG := make(map[[2]int]bool)
	rows, err = db.Query(`SELECT title_id, genre_id FROM title_genres`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var titleID, genreID int
		rows.Scan(&titleID, &genreID)
		existingTG[[2]int{titleID, genreID}] = true
	}
	rows.Close()
	log.Printf("Loaded %d existing title_genre associations", len(existingTG))

	// Collect new associations
	type tgPair struct {
		titleID int
		genreID int
	}
	var toInsert []tgPair
	for imdbID, genres := range titleGenres {
		titleID, ok := imdbToTitleID[imdbID]
		if !ok {
			continue
		}
		for _, g := range genres {
			genreID, ok := genreIDCache[g]
			if !ok {
				continue
			}
			if !existingTG[[2]int{titleID, genreID}] {
				toInsert = append(toInsert, tgPair{titleID, genreID})
			}
		}
	}
	log.Printf("Found %d new title_genre associations to insert", len(toInsert))

	// Batch insert
	for i := 0; i < len(toInsert); i += batchSize {
		end := i + batchSize
		if end > len(toInsert) {
			end = len(toInsert)
		}
		batch := toInsert[i:end]
		values := make([]string, len(batch))
		args := make([]any, len(batch)*2)
		for j, pair := range batch {
			base := j * 2
			values[j] = fmt.Sprintf("($%d, $%d)", base+1, base+2)
			args[base] = pair.titleID
			args[base+1] = pair.genreID
		}
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO title_genres (title_id, genre_id) VALUES %s ON CONFLICT DO NOTHING`, strings.Join(values, ",")), args...)
		if err != nil {
			return fmt.Errorf("title_genre insert: %w", err)
		}
		if (i/batchSize+1)%20 == 0 || end >= len(toInsert) {
			log.Printf("  inserted %d/%d genre associations...", end, len(toInsert))
		}
	}

	log.Printf("Genre sync complete: %d genres, %d new associations", len(genreIDCache), len(toInsert))
	return nil
}

func syncEpisodes(filepath string) error {
	// Build show imdb_id -> show_id cache
	showCache := make(map[string]int)
	rows, err := db.Query(`SELECT t.imdb_id, s.id FROM shows s JOIN titles t ON s.title_id = t.id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var imdbID string
		var showID int
		rows.Scan(&imdbID, &showID)
		showCache[imdbID] = showID
	}
	rows.Close()
	log.Printf("Loaded %d shows into cache", len(showCache))

	// Load existing seasons
	log.Println("Loading existing seasons...")
	existingSeasons := make(map[seasonKey]int)
	rows, err = db.Query(`SELECT id, show_id, season FROM show_seasons`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, showID, season int
		rows.Scan(&id, &showID, &season)
		existingSeasons[seasonKey{showID, season}] = id
	}
	rows.Close()
	log.Printf("Loaded %d existing seasons", len(existingSeasons))

	// Load existing episodes
	log.Println("Loading existing episodes...")
	existingEpisodes := make(map[episodeKey]ExistingEpisode)
	rows, err = db.Query(`SELECT id, season_id, episode, display_name FROM show_episodes`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, seasonID, episode int
		var displayName sql.NullString
		rows.Scan(&id, &seasonID, &episode, &displayName)
		existingEpisodes[episodeKey{seasonID, episode}] = ExistingEpisode{
			ID:          id,
			DisplayName: displayName.String,
		}
	}
	rows.Close()
	log.Printf("Loaded %d existing episodes", len(existingEpisodes))

	// First pass: collect seasons to insert
	log.Println("Scanning for new seasons...")
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

		showID, ok := showCache[parentImdbID]
		if !ok {
			continue
		}

		key := seasonKey{showID, season}
		if _, exists := existingSeasons[key]; !exists {
			seasonsToInsert[key] = true
		}
	}
	gz.Close()
	f.Close()

	// Insert new seasons
	if len(seasonsToInsert) > 0 {
		log.Printf("Inserting %d new seasons...", len(seasonsToInsert))
		seasonList := make([]seasonKey, 0, len(seasonsToInsert))
		for k := range seasonsToInsert {
			seasonList = append(seasonList, k)
		}

		for i := 0; i < len(seasonList); i += batchSize {
			end := i + batchSize
			if end > len(seasonList) {
				end = len(seasonList)
			}
			batch := seasonList[i:end]

			values := make([]string, len(batch))
			args := make([]any, len(batch)*2)
			for j, k := range batch {
				values[j] = fmt.Sprintf("($%d, $%d)", j*2+1, j*2+2)
				args[j*2] = k.showID
				args[j*2+1] = k.season
			}

			rows, err := db.Query(fmt.Sprintf(`
				INSERT INTO show_seasons (show_id, season)
				VALUES %s
				RETURNING id, show_id, season
			`, strings.Join(values, ",")), args...)
			if err != nil {
				return fmt.Errorf("season insert: %w", err)
			}

			for rows.Next() {
				var id, showID, season int
				rows.Scan(&id, &showID, &season)
				existingSeasons[seasonKey{showID, season}] = id
			}
			rows.Close()

			if (i/batchSize+1)%20 == 0 || end >= len(seasonList) {
				log.Printf("  inserted %d/%d seasons...", end, len(seasonList))
			}
		}
	}

	// Second pass: collect episodes to insert/update
	log.Println("Scanning for episode changes...")

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

	scanner = bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	scanner.Scan() // Skip header

	type EpisodeInsert struct {
		SeasonID    int
		Episode     int
		DisplayName *string
	}
	type EpisodeUpdate struct {
		ID          int
		DisplayName string
	}

	var toInsert []EpisodeInsert
	var toUpdate []EpisodeUpdate
	var scanned, unchanged, skipped int64

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
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

		showID, ok := showCache[parentImdbID]
		if !ok {
			continue
		}

		seasonID, ok := existingSeasons[seasonKey{showID, season}]
		if !ok {
			continue
		}

		// Get display name from collected episode titles
		displayName := ""
		if title, ok := episodeTitles.Load(episodeImdbID); ok {
			displayName = title.(string)
		}

		scanned++

		key := episodeKey{seasonID, episode}
		if existing, ok := existingEpisodes[key]; ok {
			// Episode exists - check if display_name needs update
			if displayName != "" && existing.DisplayName != displayName {
				toUpdate = append(toUpdate, EpisodeUpdate{
					ID:          existing.ID,
					DisplayName: displayName,
				})
			} else {
				unchanged++
			}
		} else {
			// New episode
			var dn *string
			if displayName != "" {
				dn = &displayName
			}
			toInsert = append(toInsert, EpisodeInsert{
				SeasonID:    seasonID,
				Episode:     episode,
				DisplayName: dn,
			})
		}

		if scanned%100000 == 0 {
			log.Printf("Scanned %d episodes: %d unchanged, %d to insert, %d to update, %d skipped...",
				scanned, unchanged, len(toInsert), len(toUpdate), skipped)
		}
	}

	log.Printf("Scan complete: %d episodes, %d unchanged, %d to insert, %d to update, %d skipped",
		scanned, unchanged, len(toInsert), len(toUpdate), skipped)

	// Insert new episodes in batches
	if len(toInsert) > 0 {
		log.Printf("Inserting %d new episodes...", len(toInsert))
		for i := 0; i < len(toInsert); i += batchSize {
			end := i + batchSize
			if end > len(toInsert) {
				end = len(toInsert)
			}
			batch := toInsert[i:end]

			values := make([]string, len(batch))
			args := make([]any, len(batch)*3)
			for j, ep := range batch {
				base := j * 3
				values[j] = fmt.Sprintf("($%d, $%d, $%d)", base+1, base+2, base+3)
				args[base] = ep.SeasonID
				args[base+1] = ep.Episode
				args[base+2] = ep.DisplayName
			}

			_, err := db.Exec(fmt.Sprintf(`
				INSERT INTO show_episodes (season_id, episode, display_name)
				VALUES %s
			`, strings.Join(values, ",")), args...)
			if err != nil {
				return fmt.Errorf("episode insert: %w", err)
			}

			if (i/batchSize+1)%20 == 0 || end >= len(toInsert) {
				log.Printf("  inserted %d/%d episodes...", end, len(toInsert))
			}
		}
	}

	// Update episodes with changed display names
	if len(toUpdate) > 0 {
		log.Printf("Updating %d episodes with new display names...", len(toUpdate))
		for i := 0; i < len(toUpdate); i += batchSize {
			end := i + batchSize
			if end > len(toUpdate) {
				end = len(toUpdate)
			}
			batch := toUpdate[i:end]

			// Build UPDATE with CASE
			args := make([]any, 0, len(batch)*2)
			cases := make([]string, len(batch))
			idPlaceholders := make([]string, len(batch))

			for j, ep := range batch {
				base := j * 2
				idPlaceholders[j] = fmt.Sprintf("$%d", base+1)
				cases[j] = fmt.Sprintf("WHEN id = $%d THEN $%d", base+1, base+2)
				args = append(args, ep.ID, ep.DisplayName)
			}

			_, err := db.Exec(fmt.Sprintf(`
				UPDATE show_episodes SET
					display_name = CASE %s END
				WHERE id IN (%s)
			`, strings.Join(cases, " "), strings.Join(idPlaceholders, ",")), args...)
			if err != nil {
				return fmt.Errorf("episode update: %w", err)
			}

			if (i/batchSize+1)%20 == 0 || end >= len(toUpdate) {
				log.Printf("  updated %d/%d episodes...", end, len(toUpdate))
			}
		}
	}

	log.Printf("Episodes done: %d inserted, %d updated, %d unchanged, %d skipped (missing season/episode)",
		len(toInsert), len(toUpdate), unchanged, skipped)

	return scanner.Err()
}

type ExistingRating struct {
	NumVotes      int
	AverageRating float64
}

func syncRatings(filepath string) error {
	// Load existing ratings into memory for diffing
	log.Println("Loading existing ratings from database...")
	existingRatings := make(map[string]ExistingRating)
	rows, err := db.Query(`SELECT imdb_id, COALESCE(num_votes, 0), COALESCE(average_rating, 0) FROM titles WHERE imdb_id IS NOT NULL`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var imdbID string
		var r ExistingRating
		rows.Scan(&imdbID, &r.NumVotes, &r.AverageRating)
		existingRatings[imdbID] = r
	}
	rows.Close()
	log.Printf("Loaded %d existing ratings", len(existingRatings))

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

	var batch []RatingRecord
	var scanned, updated, unchanged int64

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

		// Skip if unchanged
		if existing, ok := existingRatings[imdbID]; ok {
			if existing.NumVotes == numVotes && existing.AverageRating == float64(float32(averageRating)) {
				unchanged++
				continue
			}
		}

		batch = append(batch, RatingRecord{imdbID, numVotes, averageRating})

		if len(batch) >= batchSize {
			n, err := updateRatingsBatch(batch)
			if err != nil {
				return err
			}
			updated += n
			batch = batch[:0]
		}

		if scanned%500000 == 0 {
			log.Printf("Scanned %d ratings: %d unchanged, %d to update...", scanned, unchanged, updated+int64(len(batch)))
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		n, err := updateRatingsBatch(batch)
		if err != nil {
			return err
		}
		updated += n
	}

	log.Printf("Ratings complete: scanned %d, updated %d, unchanged %d", scanned, updated, unchanged)
	return scanner.Err()
}

func updateRatingsBatch(records []RatingRecord) (int64, error) {
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

	result, err := db.Exec(fmt.Sprintf(`
		UPDATE titles SET
			num_votes = CASE %s END,
			average_rating = CASE %s END
		WHERE imdb_id IN (%s)
	`, strings.Join(votesCases, " "), strings.Join(ratingCases, " "), strings.Join(idPlaceholders, ",")), args...)
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
					OriginCountry     []string `json:"origin_country"`
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
			detailURL := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?api_key=%s", tmdbID, tmdbAPIKey)
			if r.Type == "show" {
				detailURL = fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?api_key=%s", tmdbID, tmdbAPIKey)
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
				Runtime float64 `json:"runtime"`
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
				needs_backfill_tmdb = false
				WHERE id = $8`,
				tmdbID, imageURL, detail.OriginalLanguage, releaseDate,
				detail.Popularity, originCountry, int(detail.Runtime), r.ID)

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
