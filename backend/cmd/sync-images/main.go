package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

type TMDBFindResponse struct {
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

type TMDBEpisodeResponse struct {
	StillPath string `json:"still_path"`
	AirDate   string `json:"air_date"`
	Runtime   int    `json:"runtime"`
}

var apiKey string
var requestCount int
var startTime time.Time

func main() {
	flag.StringVar(&apiKey, "key", "", "TMDB API key (required)")
	dsn := flag.String("db", "postgres://localhost/mediacanon?sslmode=disable", "Database URL")
	limit := flag.Int("limit", 0, "Limit number of shows to process (0 = all)")
	skipSynced := flag.Bool("skip-synced", true, "Skip shows that already have image_url")
	flag.Parse()

	if apiKey == "" {
		log.Fatal("TMDB API key required: -key YOUR_KEY")
	}

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	startTime = time.Now()

	// Get all shows with IMDb IDs
	query := `
		SELECT s.id, t.imdb_id, t.display_name
		FROM shows s
		JOIN titles t ON s.title_id = t.id
		WHERE t.imdb_id IS NOT NULL AND t.imdb_id != ''
	`
	if *skipSynced {
		query += ` AND t.image_url IS NULL`
	}
	query += ` ORDER BY s.id`
	if *limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", *limit)
	}

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	type show struct {
		id      int
		imdbID  string
		name    string
	}
	var shows []show
	for rows.Next() {
		var s show
		rows.Scan(&s.id, &s.imdbID, &s.name)
		shows = append(shows, s)
	}
	rows.Close()

	log.Printf("Found %d shows to sync", len(shows))

	synced := 0
	skipped := 0
	errors := 0

	for i, s := range shows {
		// Progress every 100 shows
		if (i+1)%100 == 0 || i == 0 {
			elapsed := time.Since(startTime)
			rate := float64(requestCount) / elapsed.Seconds()
			log.Printf("Progress: %d/%d shows (%.1f req/s, %d synced, %d skipped, %d errors)",
				i+1, len(shows), rate, synced, skipped, errors)
		}

		err := syncShow(db, s.id, s.imdbID, s.name)
		if err != nil {
			if err.Error() == "not found on TMDB" {
				skipped++
			} else {
				log.Printf("Error syncing %s: %v", s.name, err)
				errors++
			}
		} else {
			synced++
		}

		// Small delay between shows
		time.Sleep(50 * time.Millisecond)
	}

	elapsed := time.Since(startTime)
	log.Printf("Done in %v. Synced: %d, Skipped: %d, Errors: %d, Total requests: %d",
		elapsed.Round(time.Second), synced, skipped, errors, requestCount)
}

func syncShow(db *sql.DB, showID int, imdbID, name string) error {
	// Get TMDB ID and poster
	tmdbID, posterURL, origLang, releaseDate, originCountry, popularity, err := fetchTMDBShow(imdbID)
	if err != nil {
		return err
	}

	if tmdbID == 0 {
		return fmt.Errorf("not found on TMDB")
	}

	// Update show poster, original_language, release_date, tmdb_popularity, origin_country
	if posterURL != "" {
		_, err = db.Exec(`
			UPDATE titles SET image_url = $1,
				original_language = COALESCE(NULLIF($3, ''), original_language),
				release_date = CASE WHEN $4 = '' THEN release_date ELSE $4::date END,
				tmdb_popularity = $5,
				origin_country = COALESCE(NULLIF($6, ''), origin_country)
			WHERE imdb_id = $2`,
			posterURL, imdbID, origLang, releaseDate, popularity, originCountry)
		if err != nil {
			return fmt.Errorf("updating poster: %w", err)
		}
	} else if origLang != "" || releaseDate != "" || originCountry != "" {
		_, err = db.Exec(`
			UPDATE titles SET
				original_language = COALESCE(NULLIF($2, ''), original_language),
				release_date = CASE WHEN $3 = '' THEN release_date ELSE $3::date END,
				tmdb_popularity = $4,
				origin_country = COALESCE(NULLIF($5, ''), origin_country)
			WHERE imdb_id = $1`,
			imdbID, origLang, releaseDate, popularity, originCountry)
		if err != nil {
			return fmt.Errorf("updating metadata: %w", err)
		}
	}

	// Get all episodes for this show
	rows, err := db.Query(`
		SELECT e.id, ss.season, e.episode
		FROM show_episodes e
		JOIN show_seasons ss ON e.season_id = ss.id
		WHERE ss.show_id = $1
		ORDER BY ss.season, e.episode
	`, showID)
	if err != nil {
		return err
	}

	type ep struct {
		id      int
		season  int
		episode int
	}
	var episodes []ep
	for rows.Next() {
		var e ep
		rows.Scan(&e.id, &e.season, &e.episode)
		episodes = append(episodes, e)
	}
	rows.Close()

	// Fetch episode data
	for _, e := range episodes {
		epData, err := fetchEpisodeData(tmdbID, e.season, e.episode)
		if err != nil {
			continue // Skip individual episode errors
		}

		if epData != nil {
			db.Exec(`
				UPDATE show_episodes
				SET image_url = $1, air_date = $2, runtime_minutes = $3
				WHERE id = $4
			`, epData.ImageURL, epData.AirDate, epData.Runtime, e.id)
		}

		// Rate limit: ~40 req/sec, so sleep 25ms between requests
		time.Sleep(25 * time.Millisecond)
	}

	// Mark episodes as checked so on-demand fetch doesn't redo this work
	db.Exec(`UPDATE titles SET episodes_checked_at = NOW() WHERE id = (SELECT title_id FROM shows WHERE id = $1)`, showID)

	return nil
}

func fetchTMDBShow(imdbID string) (tmdbID int, posterURL, originalLanguage, releaseDate, originCountry string, popularity float64, err error) {
	url := fmt.Sprintf(
		"https://api.themoviedb.org/3/find/%s?api_key=%s&external_source=imdb_id",
		imdbID, apiKey,
	)

	requestCount++
	resp, err := http.Get(url)
	if err != nil {
		return 0, "", "", "", "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		// Rate limited - wait and retry
		log.Println("Rate limited, waiting 10 seconds...")
		time.Sleep(10 * time.Second)
		return fetchTMDBShow(imdbID)
	}

	if resp.StatusCode != 200 {
		return 0, "", "", "", "", 0, fmt.Errorf("TMDB returned status %d", resp.StatusCode)
	}

	var result TMDBFindResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, "", "", "", "", 0, err
	}

	if len(result.TVResults) > 0 {
		tv := result.TVResults[0]
		posterURL := ""
		if tv.PosterPath != "" {
			posterURL = "https://image.tmdb.org/t/p/w500" + tv.PosterPath
		}
		oc := ""
		if len(tv.OriginCountry) > 0 {
			oc = tv.OriginCountry[0]
		}
		return tv.ID, posterURL, tv.OriginalLanguage, tv.FirstAirDate, oc, tv.Popularity, nil
	}

	if len(result.MovieResults) > 0 {
		mv := result.MovieResults[0]
		posterURL := ""
		if mv.PosterPath != "" {
			posterURL = "https://image.tmdb.org/t/p/w500" + mv.PosterPath
		}
		oc := ""
		if len(mv.OriginCountry) > 0 {
			oc = mv.OriginCountry[0]
		}
		return mv.ID, posterURL, mv.OriginalLanguage, mv.ReleaseDate, oc, mv.Popularity, nil
	}

	return 0, "", "", "", "", 0, nil
}

type EpisodeData struct {
	ImageURL *string
	AirDate  *string
	Runtime  *int
}

func fetchEpisodeData(tmdbID, season, episode int) (*EpisodeData, error) {
	url := fmt.Sprintf(
		"https://api.themoviedb.org/3/tv/%d/season/%d/episode/%d?api_key=%s",
		tmdbID, season, episode, apiKey,
	)

	requestCount++
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		// Rate limited - wait and retry
		log.Println("Rate limited, waiting 10 seconds...")
		time.Sleep(10 * time.Second)
		return fetchEpisodeData(tmdbID, season, episode)
	}

	if resp.StatusCode == 404 {
		return nil, nil // Episode not found on TMDB
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("TMDB returned status %d", resp.StatusCode)
	}

	var result TMDBEpisodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	data := &EpisodeData{}

	if result.StillPath != "" {
		url := "https://image.tmdb.org/t/p/w500" + result.StillPath
		data.ImageURL = &url
	}

	if result.AirDate != "" {
		data.AirDate = &result.AirDate
	}

	if result.Runtime > 0 {
		data.Runtime = &result.Runtime
	}

	return data, nil
}
