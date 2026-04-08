package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"

	"fyne.io/systray"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

//go:embed static/icon.png
var iconPNG []byte

//go:embed collections/*.yaml
var collectionsFS embed.FS

var (
	db         *sql.DB
	tmpls      map[string]*template.Template
	tmdbAPIKey string
	logFile    *os.File
	logPath    string

	// Carousel cache: "type:genre" -> top titles + total count
	carouselCache   map[string]carouselBucket
	carouselCacheMu sync.RWMutex
)

// Models

type Title struct {
	TitleID            int        `json:"title_id"`
	Type               string     `json:"type"`
	DisplayName        string     `json:"display_name"`
	StartYear          *int       `json:"start_year,omitempty"`
	EndYear            *int       `json:"end_year,omitempty"`
	IMDbID             *string    `json:"imdb_id,omitempty"`
	ImageURL           *string    `json:"image_url,omitempty"`
	TMDBID             *int       `json:"tmdb_id,omitempty"`
	NumVotes           *int       `json:"num_votes,omitempty"`
	AverageRating      *float64   `json:"average_rating,omitempty"`
	OriginalTitle      *string    `json:"original_title,omitempty"`
	OriginalLanguage   *string    `json:"original_language,omitempty"`
	ReleaseDate        *string    `json:"release_date,omitempty"`
	TMDBPopularity     *float64   `json:"tmdb_popularity,omitempty"`
	RuntimeMinutes     *int       `json:"runtime_minutes,omitempty"`
	OriginCountry      *string    `json:"origin_country,omitempty"`
	IsSeriesFinished   *bool      `json:"is_series_finished,omitempty"`
	WatchProviders          *json.RawMessage `json:"watch_providers,omitempty"`
	WatchProvidersCheckedAt *time.Time       `json:"-"`
	Networks                *json.RawMessage `json:"networks,omitempty"`
	ProductionCompanies     *json.RawMessage `json:"production_companies,omitempty"`
	NeedsBackfillTMDB       bool            `json:"-"`
	EpisodesCheckedAt       *time.Time      `json:"-"`
	Genres             []string   `json:"genres,omitempty"`
	Seasons            []Season   `json:"seasons,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

// DiscoverTitle is a lightweight struct for poster grid display
type DiscoverTitle struct {
	TitleID          int      `json:"title_id"`
	Type             string   `json:"type"`
	DisplayName      string   `json:"display_name"`
	StartYear        *int     `json:"start_year,omitempty"`
	EndYear          *int     `json:"end_year,omitempty"`
	ImageURL         *string  `json:"image_url,omitempty"`
	AverageRating    *float64 `json:"average_rating,omitempty"`
	NumVotes         *int     `json:"num_votes,omitempty"`
	TMDBPopularity   *float64 `json:"tmdb_popularity,omitempty"`
	Genres           []string `json:"genres,omitempty"`
	EngagementCount  int      `json:"engagement_count"`
}

type Collection struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	Slug        string   `json:"slug"`
	Description string   `json:"description,omitempty"`
	Strategy    string   `json:"strategy"`
	Pinned      bool     `json:"pinned"`
	Active      bool     `json:"active"`
	EngagementCount float64 `json:"engagement_count"`
	Languages   []string `json:"languages,omitempty"`
	Regions     []string `json:"regions,omitempty"`
}

// CollectionDef is the YAML structure for collection definition files
type CollectionDef struct {
	Slug        string   `yaml:"slug"`
	Name        string   `yaml:"name"`
	Description string   `yaml:"description"`
	Strategy    string   `yaml:"strategy"`
	Pinned      bool     `yaml:"pinned"`
	Languages   []string `yaml:"languages"`
	Regions     []string `yaml:"regions"`
	Filter      struct {
		Type     string `yaml:"type" json:"type"`
		Lang     string `yaml:"lang" json:"lang"`
		Genre    string `yaml:"genre" json:"genre"`
		Sort     string `yaml:"sort" json:"sort"`
		MinVotes int    `yaml:"min_votes" json:"min_votes"`
		Limit    int    `yaml:"limit" json:"limit"`
	} `yaml:"filter"`
	Titles []string `yaml:"titles"` // imdb_ids for static strategy
}

// TitleSearchResult for search/list endpoints
type TitleSearchResult struct {
	TitleID          int       `json:"title_id"`
	Type             string    `json:"type"`
	DisplayName      string    `json:"display_name"`
	StartYear        *int      `json:"start_year,omitempty"`
	EndYear          *int      `json:"end_year,omitempty"`
	IMDbID           *string   `json:"imdb_id,omitempty"`
	ImageURL         *string   `json:"image_url,omitempty"`
	TMDBID           *int      `json:"tmdb_id,omitempty"`
	NumVotes         *int      `json:"num_votes,omitempty"`
	AverageRating    *float64  `json:"average_rating,omitempty"`
	OriginalTitle    *string   `json:"original_title,omitempty"`
	OriginalLanguage *string   `json:"original_language,omitempty"`
	ReleaseDate      *string   `json:"release_date,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type Season struct {
	SeasonID         int       `json:"season_id"`
	TitleID          int       `json:"title_id"`
	SeasonNumber     int       `json:"season_number"`
	Episodes         []Episode `json:"episodes,omitempty"`
	IsSeasonFinished *bool     `json:"is_season_finished"`
}

type Episode struct {
	EpisodeID      int     `json:"episode_id"`
	SeasonID       int     `json:"season_id"`
	EpisodeNumber  int     `json:"episode_number"`
	DisplayName    *string `json:"display_name,omitempty"`
	ImageURL       *string `json:"image_url,omitempty"`
	AirDate        *string `json:"air_date,omitempty"`
	RuntimeMinutes *int    `json:"runtime_minutes,omitempty"`
	Synopsis       *string `json:"synopsis,omitempty"`
}

// V2 API Models

type MCTitleLayer struct {
	TitleID        int          `json:"title_id"`
	Kind           string       `json:"kind"`
	IMDbID         *string      `json:"imdb_id"`
	DisplayName    string       `json:"display_name"`
	PosterURL      *string      `json:"poster_url"`
	StartYear      *int         `json:"start_year"`
	EndYear        *int         `json:"end_year"`
	Genres         []string     `json:"genres"`
	Rating         *float64     `json:"rating"`
	VoteCount      *int         `json:"vote_count"`
	RuntimeMinutes  *int             `json:"runtime_minutes"`
	Show            *MCShowInfo      `json:"show"`
	WatchProviders  *json.RawMessage `json:"watch_providers,omitempty"`
	Networks        *json.RawMessage `json:"networks,omitempty"`
	ProductionCompanies *json.RawMessage `json:"production_companies,omitempty"`
}

type MCShowInfo struct {
	SeasonCount  int            `json:"season_count"`
	EpisodeCount int            `json:"episode_count"`
	IsFinished   bool           `json:"is_finished"`
	Seasons      []MCSeasonInfo `json:"seasons"`
}

type MCSeasonInfo struct {
	SeasonNumber int `json:"season_number"`
	EpisodeCount int `json:"episode_count"`
}

type MCEpisodeLayer struct {
	EpisodeNumber  int     `json:"episode_number"`
	DisplayName    *string `json:"display_name"`
	ImageURL       *string `json:"image_url"`
	Synopsis       *string `json:"synopsis"`
	RuntimeMinutes *int    `json:"runtime_minutes"`
	AirDate        *string `json:"air_date"`
}

// TMDB types for on-demand image fetching

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
	Name      string `json:"name"`
	Overview  string `json:"overview"`
	StillPath string `json:"still_path"`
	AirDate   string `json:"air_date"`
	Runtime   int    `json:"runtime"`
}

func main() {
	if os.Getenv("HEADLESS") == "1" {
		onReadyHeadless()
		return
	}
	systray.Run(onReady, onExit)
}

func onReady() {
	setupLogging()

	tmdbAPIKey = os.Getenv("TMDB_API_KEY")
	if tmdbAPIKey != "" {
		log.Println("TMDB API key configured — on-demand image fetching enabled")
	}

	// Parse templates
	funcMap := template.FuncMap{
		"langDisplay":    langDisplay,
		"countryDisplay": countryDisplay,
		"fmtRating":      fmtRating,
		"add":         func(a, b int) int { return a + b },
		"subtract":    func(a, b int) int { return a - b },
		"join": strings.Join,
		"derefStr": func(p *string) string {
			if p == nil {
				return ""
			}
			return *p
		},
		"derefInt": func(p *int) int {
			if p == nil {
				return 0
			}
			return *p
		},
		"derefFloat": func(p *float64) float64 {
			if p == nil {
				return 0
			}
			return *p
		},
		"fmtVotes": func(p *int) string {
			if p == nil {
				return ""
			}
			v := *p
			if v >= 1000000 {
				return fmt.Sprintf("%.1fM", float64(v)/1000000)
			}
			if v >= 1000 {
				return fmt.Sprintf("%.1fK", float64(v)/1000)
			}
			return strconv.Itoa(v)
		},
		"chipURL": func(data map[string]any, key, value string) string {
			// Build /discover?... URL toggling the given key/value while preserving others
			keyMap := map[string]string{"genre": "Genre", "country": "Country", "type": "Type", "sort": "Sort"}
			params := make(map[string]string)
			for k, tmplKey := range keyMap {
				if v, ok := data[tmplKey].(string); ok {
					params[k] = v
				}
			}
			if current := params[key]; current == value {
				params[key] = "" // toggle off
			} else {
				params[key] = value
			}
			var parts []string
			for _, k := range []string{"genre", "country", "type", "sort"} {
				if params[k] != "" {
					parts = append(parts, k+"="+params[k])
				}
			}
			if len(parts) == 0 {
				return "/discover"
			}
			return "/discover?" + strings.Join(parts, "&")
		},
	}
	tmpls = make(map[string]*template.Template)
	pages := []string{"home", "titles", "title", "add", "search", "api", "discover"}
	for _, page := range pages {
		t, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/base.html", "templates/"+page+".html")
		if err != nil {
			log.Fatalf("Error parsing %s template: %v", page, err)
		}
		tmpls[page] = t
	}

	// Database connection
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost/mediacanon?sslmode=disable"
	}
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Printf("Warning: database not connected: %v", err)
	}

	// Load collections from embedded YAML files into database
	loadCollections()

	// Build carousel cache (one query for all discover page data)
	buildCarouselCache()

	// Daily cleanup of old view/click tracking data
	go func() {
		cleanupOldViews()
		ticker := time.NewTicker(24 * time.Hour)
		for range ticker.C {
			cleanupOldViews()
		}
	}()

	mux := http.NewServeMux()

	// Static files — no caching so deploys take effect immediately
	mux.HandleFunc("/static/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		http.FileServer(http.FS(staticFS)).ServeHTTP(w, r)
	})

	// Disable caching on all non-static responses
	noCache := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			next(w, r)
		}
	}

	// Pages
	mux.HandleFunc("/", noCache(handleHome))
	mux.HandleFunc("/discover", noCache(handleDiscoverPage))
	mux.HandleFunc("/titles", noCache(handleTitlesList))
	mux.HandleFunc("/titles/", noCache(handleTitlePage))
	mux.HandleFunc("/add", noCache(handleAddPage))
	mux.HandleFunc("/api", noCache(handleAPIPage))
	mux.HandleFunc("/api/", noCache(handleAPISlash))

	// Legacy redirects

	// API - Titles
	mux.HandleFunc("/api/titles", noCache(handleAPITitles))
	mux.HandleFunc("/api/titles/", noCache(handleAPITitle))

	// API - Seasons
	mux.HandleFunc("/api/seasons/", noCache(handleAPISeason))

	// API - Episodes
	mux.HandleFunc("/api/episodes/", noCache(handleAPIEpisode))

	// API - Discover & Collections
	mux.HandleFunc("/api/discover/carousels", noCache(handleAPIDiscoverCarousels))
	mux.HandleFunc("/api/discover", noCache(handleAPIDiscover))
	mux.HandleFunc("/api/collections", noCache(handleAPICollections))
	mux.HandleFunc("/api/collections/", noCache(handleAPICollection))



	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Use net.Listen to get the actual bound address
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	addr := listener.Addr().(*net.TCPAddr)
	localIP := getLocalIP()
	displayAddr := fmt.Sprintf("%s:%d", localIP, addr.Port)

	log.Printf("MediaCanon running on http://%s", displayAddr)

	// Set up systray
	systray.SetTemplateIcon(iconPNG, iconPNG)
	systray.SetTitle("")
	systray.SetTooltip("MediaCanon")

	mAddr := systray.AddMenuItem(displayAddr, "Server address")
	mAddr.Disable()
	systray.AddSeparator()
	mLogs := systray.AddMenuItem("View Logs", "Open log file in Terminal")
	systray.AddSeparator()
	mQuit := systray.AddMenuItem("Quit", "Shut down MediaCanon")

	server := &http.Server{Handler: mux}

	// Start HTTP server
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Handle menu clicks
	go func() {
		for {
			select {
			case <-mLogs.ClickedCh:
				openLogs()
			case <-mQuit.ClickedCh:
				log.Println("Shutting down...")
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				server.Shutdown(ctx)
				cancel()
				db.Close()
				systray.Quit()
				return
			}
		}
	}()

	// Ignore SIGHUP so sleep/wake doesn't kill the process
	signal.Ignore(syscall.SIGHUP)

	// Handle OS signals
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Received signal, shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		server.Shutdown(ctx)
		cancel()
		db.Close()
		systray.Quit()
	}()
}

func onReadyHeadless() {
	setupLogging()

	tmdbAPIKey = os.Getenv("TMDB_API_KEY")
	if tmdbAPIKey != "" {
		log.Println("TMDB API key configured — on-demand image fetching enabled")
	}

	// Parse templates
	funcMap := template.FuncMap{
		"langDisplay":    langDisplay,
		"countryDisplay": countryDisplay,
		"fmtRating":      fmtRating,
		"add":         func(a, b int) int { return a + b },
		"subtract":    func(a, b int) int { return a - b },
		"join": strings.Join,
		"derefStr": func(p *string) string {
			if p == nil {
				return ""
			}
			return *p
		},
		"derefInt": func(p *int) int {
			if p == nil {
				return 0
			}
			return *p
		},
		"derefFloat": func(p *float64) float64 {
			if p == nil {
				return 0
			}
			return *p
		},
		"fmtVotes": func(p *int) string {
			if p == nil {
				return ""
			}
			v := *p
			if v >= 1000000 {
				return fmt.Sprintf("%.1fM", float64(v)/1000000)
			}
			if v >= 1000 {
				return fmt.Sprintf("%.1fK", float64(v)/1000)
			}
			return strconv.Itoa(v)
		},
		"chipURL": func(data map[string]any, key, value string) string {
			keyMap := map[string]string{"genre": "Genre", "country": "Country", "type": "Type", "sort": "Sort"}
			params := make(map[string]string)
			for k, tmplKey := range keyMap {
				if v, ok := data[tmplKey].(string); ok {
					params[k] = v
				}
			}
			if current := params[key]; current == value {
				params[key] = ""
			} else {
				params[key] = value
			}
			var parts []string
			for _, k := range []string{"genre", "country", "type", "sort"} {
				if params[k] != "" {
					parts = append(parts, k+"="+params[k])
				}
			}
			if len(parts) == 0 {
				return "/discover"
			}
			return "/discover?" + strings.Join(parts, "&")
		},
	}
	tmpls = make(map[string]*template.Template)
	pages := []string{"home", "titles", "title", "add", "search", "api", "discover"}
	for _, page := range pages {
		t, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/base.html", "templates/"+page+".html")
		if err != nil {
			log.Fatalf("Error parsing %s template: %v", page, err)
		}
		tmpls[page] = t
	}

	// Database connection
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost/mediacanon?sslmode=disable"
	}
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Printf("Warning: database not connected: %v", err)
	}

	loadCollections()
	buildCarouselCache()

	go func() {
		cleanupOldViews()
		ticker := time.NewTicker(24 * time.Hour)
		for range ticker.C {
			cleanupOldViews()
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/static/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		http.FileServer(http.FS(staticFS)).ServeHTTP(w, r)
	})

	noCache := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			next(w, r)
		}
	}

	mux.HandleFunc("/", noCache(handleHome))
	mux.HandleFunc("/discover", noCache(handleDiscoverPage))
	mux.HandleFunc("/titles", noCache(handleTitlesList))
	mux.HandleFunc("/titles/", noCache(handleTitlePage))
	mux.HandleFunc("/add", noCache(handleAddPage))
	mux.HandleFunc("/api", noCache(handleAPIPage))
	mux.HandleFunc("/api/", noCache(handleAPISlash))

	// Legacy redirects

	mux.HandleFunc("/api/titles", noCache(handleAPITitles))
	mux.HandleFunc("/api/titles/", noCache(handleAPITitle))
	mux.HandleFunc("/api/seasons/", noCache(handleAPISeason))
	mux.HandleFunc("/api/episodes/", noCache(handleAPIEpisode))
	mux.HandleFunc("/api/discover/carousels", noCache(handleAPIDiscoverCarousels))
	mux.HandleFunc("/api/discover", noCache(handleAPIDiscover))
	mux.HandleFunc("/api/collections", noCache(handleAPICollections))
	mux.HandleFunc("/api/collections/", noCache(handleAPICollection))



	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	addr := listener.Addr().(*net.TCPAddr)
	log.Printf("MediaCanon running on http://0.0.0.0:%d (headless)", addr.Port)

	server := &http.Server{Handler: mux}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Received signal, shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	server.Shutdown(ctx)
	cancel()
	db.Close()
}

func onExit() {
	if logFile != nil {
		logFile.Close()
	}
}

// setupLogging configures log output to both stdout and a log file
func setupLogging() {
	exePath, err := os.Executable()
	if err != nil {
		exePath = "."
	}
	logPath = filepath.Join(filepath.Dir(exePath), "mediacanon.log")

	logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Warning: could not open log file %s: %v", logPath, err)
		return
	}

	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.Printf("Logging to %s", logPath)
}

// openLogs opens Terminal with tail -f on the log file
func openLogs() {
	cmd := exec.Command("osascript", "-e",
		fmt.Sprintf(`tell application "Terminal"
			activate
			do script "tail -f '%s'"
		end tell`, logPath))
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to open logs: %v", err)
	}
}

// getLocalIP returns the first non-loopback IPv4 address
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "localhost"
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "localhost"
}

// TMDB on-demand image fetching

var tmdbClient = &http.Client{Timeout: 10 * time.Second}

func fetchAndStoreTMDBImage(imdbID, titleType string) (string, int) {
	if tmdbAPIKey == "" || imdbID == "" {
		return "", 0
	}

	url := fmt.Sprintf(
		"https://api.themoviedb.org/3/find/%s?api_key=%s&external_source=imdb_id",
		imdbID, tmdbAPIKey,
	)

	resp, err := tmdbClient.Get(url)
	if err != nil {
		log.Printf("TMDB fetch error for %s: %v", imdbID, err)
		return "", 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("TMDB returned status %d for %s", resp.StatusCode, imdbID)
		return "", 0
	}

	var result TMDBFindResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("TMDB decode error for %s: %v", imdbID, err)
		return "", 0
	}

	var posterPath string
	var tmdbID int
	var origLang, releaseDate, originCountry string
	var popularity float64
	if titleType == "show" && len(result.TVResults) > 0 {
		posterPath = result.TVResults[0].PosterPath
		tmdbID = result.TVResults[0].ID
		origLang = result.TVResults[0].OriginalLanguage
		releaseDate = result.TVResults[0].FirstAirDate
		popularity = result.TVResults[0].Popularity
		if len(result.TVResults[0].OriginCountry) > 0 {
			originCountry = result.TVResults[0].OriginCountry[0]
		}
	} else if titleType == "movie" && len(result.MovieResults) > 0 {
		posterPath = result.MovieResults[0].PosterPath
		tmdbID = result.MovieResults[0].ID
		origLang = result.MovieResults[0].OriginalLanguage
		releaseDate = result.MovieResults[0].ReleaseDate
		popularity = result.MovieResults[0].Popularity
		if len(result.MovieResults[0].OriginCountry) > 0 {
			originCountry = result.MovieResults[0].OriginCountry[0]
		}
	}
	// Fallback: check both result types
	if posterPath == "" && len(result.MovieResults) > 0 {
		posterPath = result.MovieResults[0].PosterPath
		if tmdbID == 0 {
			tmdbID = result.MovieResults[0].ID
			origLang = result.MovieResults[0].OriginalLanguage
			releaseDate = result.MovieResults[0].ReleaseDate
			popularity = result.MovieResults[0].Popularity
			if len(result.MovieResults[0].OriginCountry) > 0 {
				originCountry = result.MovieResults[0].OriginCountry[0]
			}
		}
	}
	if posterPath == "" && len(result.TVResults) > 0 {
		posterPath = result.TVResults[0].PosterPath
		if tmdbID == 0 {
			tmdbID = result.TVResults[0].ID
			origLang = result.TVResults[0].OriginalLanguage
			releaseDate = result.TVResults[0].FirstAirDate
			popularity = result.TVResults[0].Popularity
			if len(result.TVResults[0].OriginCountry) > 0 {
				originCountry = result.TVResults[0].OriginCountry[0]
			}
		}
	}

	// TMDB Find API often omits origin_country for movies — fetch from details API
	if originCountry == "" && tmdbID != 0 && tmdbAPIKey != "" {
		detailURL := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?api_key=%s", tmdbID, tmdbAPIKey)
		if titleType == "show" {
			detailURL = fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?api_key=%s", tmdbID, tmdbAPIKey)
		}
		if dresp, derr := http.Get(detailURL); derr == nil {
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

	if posterPath == "" {
		// Cache the miss so we don't re-fetch
		db.Exec(`UPDATE titles SET image_url = 'none', tmdb_id = $1,
			original_language = COALESCE(NULLIF($3, ''), original_language),
			release_date = CASE WHEN $4 = '' THEN release_date ELSE $4::date END,
			tmdb_popularity = $5,
			origin_country = COALESCE(NULLIF($6, ''), origin_country),
			needs_backfill_tmdb = false
			WHERE imdb_id = $2`, tmdbID, imdbID, origLang, releaseDate, popularity, originCountry)
		return "", tmdbID
	}

	imageURL := "https://image.tmdb.org/t/p/w500" + posterPath
	_, err = db.Exec(`UPDATE titles SET image_url = $1, tmdb_id = $2,
		original_language = COALESCE(NULLIF($4, ''), original_language),
		release_date = CASE WHEN $5 = '' THEN release_date ELSE $5::date END,
		tmdb_popularity = $6,
		origin_country = COALESCE(NULLIF($7, ''), origin_country),
		needs_backfill_tmdb = false
		WHERE imdb_id = $3`, imageURL, tmdbID, imdbID, origLang, releaseDate, popularity, originCountry)
	if err != nil {
		log.Printf("Failed to store TMDB image for %s: %v", imdbID, err)
	} else {
		log.Printf("Fetched TMDB image for %s (tmdb_id=%d)", imdbID, tmdbID)
	}
	return imageURL, tmdbID
}

// maybeFetchImage checks if a title needs an image and fetches from TMDB if so.
// Updates the title's ImageURL in place.
// hasImage returns true if the title has a real image (not the "TMDB_NOT_FOUND_DO_NOT_RETRY" sentinel)
func hasImage(imageURL *string) bool {
	return imageURL != nil && *imageURL != "" && *imageURL != "TMDB_NOT_FOUND_DO_NOT_RETRY"
}

// needsFetch returns true if the title has no image and hasn't been checked yet
func needsFetch(imageURL *string, imdbID *string) bool {
	if imdbID == nil || *imdbID == "" {
		return false
	}
	return imageURL == nil || *imageURL == ""
}

// maybeFetchImage checks if a title needs an image and fetches from TMDB if so.
// Updates the title's ImageURL in place.
func maybeFetchImage(title *Title) {
	if !needsFetch(title.ImageURL, title.IMDbID) {
		return
	}
	url, tmdbID := fetchAndStoreTMDBImage(*title.IMDbID, title.Type)
	if url != "" {
		title.ImageURL = &url
	} else {
		title.ImageURL = nil
	}
	if tmdbID != 0 {
		title.TMDBID = &tmdbID
	}
}

// maybeTMDBBackfill re-fetches TMDB metadata when needs_backfill_tmdb is true.
// Updates origin_country, image, popularity, language, release_date and clears the flag.
func maybeTMDBBackfill(title *Title) {
	if !title.NeedsBackfillTMDB || tmdbAPIKey == "" {
		return
	}
	if title.IMDbID == nil || *title.IMDbID == "" {
		// No IMDb ID — nothing to look up, just clear the flag
		db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, title.TitleID)
		title.NeedsBackfillTMDB = false
		return
	}

	imdbID := *title.IMDbID
	tmdbID := 0
	if title.TMDBID != nil {
		tmdbID = *title.TMDBID
	}

	// If we don't have a TMDB ID yet, look it up via Find API
	if tmdbID == 0 {
		findURL := fmt.Sprintf("https://api.themoviedb.org/3/find/%s?api_key=%s&external_source=imdb_id", imdbID, tmdbAPIKey)
		resp, err := http.Get(findURL)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return
		}
		var result TMDBFindResponse
		if json.NewDecoder(resp.Body).Decode(&result) != nil {
			return
		}
		if title.Type == "show" && len(result.TVResults) > 0 {
			tmdbID = result.TVResults[0].ID
		} else if title.Type == "movie" && len(result.MovieResults) > 0 {
			tmdbID = result.MovieResults[0].ID
		} else if len(result.MovieResults) > 0 {
			tmdbID = result.MovieResults[0].ID
		} else if len(result.TVResults) > 0 {
			tmdbID = result.TVResults[0].ID
		}
	}

	if tmdbID == 0 {
		db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, title.TitleID)
		title.NeedsBackfillTMDB = false
		return
	}

	// Call TMDB details API for full metadata
	detailURL := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?api_key=%s&append_to_response=watch/providers", tmdbID, tmdbAPIKey)
	if title.Type == "show" {
		detailURL = fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?api_key=%s&append_to_response=watch/providers", tmdbID, tmdbAPIKey)
	}
	dresp, err := http.Get(detailURL)
	if err != nil {
		return
	}
	defer dresp.Body.Close()
	if dresp.StatusCode != 200 {
		// Clear flag even on 404 so we don't retry forever
		db.Exec(`UPDATE titles SET needs_backfill_tmdb = false WHERE id = $1`, title.TitleID)
		title.NeedsBackfillTMDB = false
		return
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
	if json.NewDecoder(dresp.Body).Decode(&detail) != nil {
		return
	}

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
		detail.Popularity, originCountry, int(detail.Runtime), title.TitleID,
		detail.Networks, detail.ProductionCompanies, detail.WatchProviders.Results)

	if err != nil {
		log.Printf("TMDB backfill update failed for title %d: %v", title.TitleID, err)
		return
	}

	log.Printf("TMDB backfill complete for title %d (%s)", title.TitleID, imdbID)
	title.NeedsBackfillTMDB = false
	if tmdbID != 0 {
		title.TMDBID = &tmdbID
	}
	if originCountry != "" {
		title.OriginCountry = &originCountry
	}
	if imageURL != "" {
		title.ImageURL = &imageURL
	}
	if detail.Popularity > 0 {
		title.TMDBPopularity = &detail.Popularity
	}
	if detail.Networks != nil {
		title.Networks = &detail.Networks
	}
	if detail.ProductionCompanies != nil {
		title.ProductionCompanies = &detail.ProductionCompanies
	}
	if detail.WatchProviders.Results != nil {
		title.WatchProviders = &detail.WatchProviders.Results
	}
	now := time.Now()
	title.WatchProvidersCheckedAt = &now
}

// maybeFetchWatchProviders fetches watch providers from TMDB if stale (>7 days) or missing.
// This is a standalone lazy fetch — separate from maybeTMDBBackfill which runs once on backfill.
func maybeFetchWatchProviders(title *Title) {
	if tmdbAPIKey == "" || title.TMDBID == nil || *title.TMDBID == 0 {
		return
	}
	watchProvidersStale := title.WatchProvidersCheckedAt == nil || time.Since(*title.WatchProvidersCheckedAt) >= 7*24*time.Hour
	needsNetworks := title.Networks == nil
	needsProductionCompanies := title.ProductionCompanies == nil
	if !watchProvidersStale && !needsNetworks && !needsProductionCompanies {
		return
	}

	mediaType := "movie"
	if title.Type == "show" {
		mediaType = "tv"
	}
	url := fmt.Sprintf("https://api.themoviedb.org/3/%s/%d?api_key=%s&append_to_response=watch/providers", mediaType, *title.TMDBID, tmdbAPIKey)
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}

	var detail struct {
		Networks            json.RawMessage `json:"networks"`
		ProductionCompanies json.RawMessage `json:"production_companies"`
		WatchProviders      struct {
			Results json.RawMessage `json:"results"`
		} `json:"watch/providers"`
	}
	if json.NewDecoder(resp.Body).Decode(&detail) != nil {
		return
	}

	_, err = db.Exec(`UPDATE titles SET watch_providers = $1, watch_providers_checked_at = NOW(),
		networks = $2, production_companies = $3 WHERE id = $4`,
		detail.WatchProviders.Results, detail.Networks, detail.ProductionCompanies, title.TitleID)
	if err != nil {
		log.Printf("Watch providers update failed for title %d: %v", title.TitleID, err)
		return
	}

	if detail.WatchProviders.Results != nil {
		title.WatchProviders = &detail.WatchProviders.Results
	}
	if detail.Networks != nil {
		title.Networks = &detail.Networks
	}
	if detail.ProductionCompanies != nil {
		title.ProductionCompanies = &detail.ProductionCompanies
	}
	now := time.Now()
	title.WatchProvidersCheckedAt = &now
}

// fetchAndStoreEpisodeData fetches episode data from TMDB and stores it in the DB.
// Returns the fetched data so callers can update in-memory structs.
func fetchAndStoreEpisodeData(tmdbID, seasonNum, episodeNum, episodeID int) (imageURL, airDate, displayName, synopsis string, runtime int, ok, notFound bool) {
	if tmdbAPIKey == "" {
		return
	}

	url := fmt.Sprintf(
		"https://api.themoviedb.org/3/tv/%d/season/%d/episode/%d?api_key=%s",
		tmdbID, seasonNum, episodeNum, tmdbAPIKey,
	)

	var resp *http.Response
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		resp, err = tmdbClient.Get(url)
		if err != nil {
			log.Printf("TMDB episode fetch error for S%dE%d: %v", seasonNum, episodeNum, err)
			return
		}
		if resp.StatusCode == 429 {
			resp.Body.Close()
			wait := time.Duration(2<<attempt) * time.Second // 2s, 4s, 8s
			log.Printf("TMDB rate limited for S%dE%d, retrying in %v (attempt %d/3)", seasonNum, episodeNum, wait, attempt+1)
			time.Sleep(wait)
			continue
		}
		break
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		log.Printf("TMDB S%dE%d: 404 not found", seasonNum, episodeNum)
		notFound = true
		return
	}
	if resp.StatusCode != 200 {
		log.Printf("TMDB episode returned status %d for S%dE%d", resp.StatusCode, seasonNum, episodeNum)
		return
	}

	var ep TMDBEpisodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		log.Printf("TMDB episode decode error for S%dE%d: %v", seasonNum, episodeNum, err)
		return
	}

	if ep.StillPath != "" {
		imageURL = "https://image.tmdb.org/t/p/w400" + ep.StillPath
	}
	airDate = ep.AirDate
	displayName = ep.Name
	synopsis = ep.Overview
	runtime = ep.Runtime

	// Store a sentinel if no image, so we don't re-fetch
	dbImageURL := imageURL
	if dbImageURL == "" {
		dbImageURL = "TMDB_NOT_FOUND_DO_NOT_RETRY"
	}

	_, err = db.Exec(`
		UPDATE show_episodes
		SET image_url = $1, air_date = CASE WHEN $2 = '' THEN air_date ELSE $2::date END,
		    runtime_minutes = CASE WHEN $3 = 0 THEN runtime_minutes ELSE $3 END,
		    display_name = COALESCE(NULLIF($4, ''), display_name),
		    synopsis = $5
		WHERE id = $6
	`, dbImageURL, airDate, runtime, displayName, synopsis, episodeID)
	if err != nil {
		log.Printf("Failed to store TMDB episode data for S%dE%d: %v", seasonNum, episodeNum, err)
		return
	}

	ok = true
	return
}

// stripEpisodeSentinels removes TMDB_NOT_FOUND_DO_NOT_RETRY sentinels from episode image URLs
// so they render as missing images in the template.
func stripEpisodeSentinels(title *Title) {
	for si := range title.Seasons {
		for ei := range title.Seasons[si].Episodes {
			ep := &title.Seasons[si].Episodes[ei]
			if ep.ImageURL != nil && *ep.ImageURL == "TMDB_NOT_FOUND_DO_NOT_RETRY" {
				ep.ImageURL = nil
			}
		}
	}
}

// maybeFetchEpisodes fetches episode data from TMDB for episodes missing data.
// Uses a 24-hour cooldown (episodes_checked_at) to avoid hammering TMDB on every page visit,
// while still retrying episodes that previously had no image available.
func maybeFetchEpisodes(title *Title) {
	if tmdbAPIKey == "" {
		return
	}
	if title.IMDbID == nil || *title.IMDbID == "" {
		return
	}

	// Get TMDB ID — fetch it if we don't have it yet
	tmdbID := 0
	if title.TMDBID != nil {
		tmdbID = *title.TMDBID
	}
	if tmdbID == 0 {
		// Call fetchAndStoreTMDBImage to resolve the TMDB ID
		_, id := fetchAndStoreTMDBImage(*title.IMDbID, "show")
		tmdbID = id
		if tmdbID != 0 {
			title.TMDBID = &tmdbID
		}
	}
	if tmdbID == 0 {
		return
	}

	// Daily cooldown: if we checked recently, just strip sentinels and return
	if title.EpisodesCheckedAt != nil &&
		time.Since(*title.EpisodesCheckedAt) < 24*time.Hour {
		stripEpisodeSentinels(title)
		return
	}

	// Collect episodes needing fetch (including sentinel values, which are now eligible for retry)
	type epRef struct {
		seasonIdx  int
		episodeIdx int
		seasonNum  int
		episodeNum int
		episodeID  int
	}
	var toFetch []epRef
	for si, season := range title.Seasons {
		for ei, ep := range season.Episodes {
			if ep.ImageURL == nil || *ep.ImageURL == "" || *ep.ImageURL == "TMDB_NOT_FOUND_DO_NOT_RETRY" {
				toFetch = append(toFetch, epRef{si, ei, season.SeasonNumber, ep.EpisodeNumber, ep.EpisodeID})
			}
		}
	}

	if len(toFetch) == 0 {
		stripEpisodeSentinels(title)
		return
	}

	log.Printf("Fetching TMDB data for %d episodes of %s (tmdb_id=%d)", len(toFetch), title.DisplayName, tmdbID)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 5) // limit to 5 concurrent TMDB requests
	type epResult struct {
		ref                                            epRef
		imageURL, airDate, displayName, synopsis string
		runtime                                        int
		ok, notFound                                   bool
	}
	results := make([]epResult, len(toFetch))

	for i, ref := range toFetch {
		wg.Add(1)
		go func(i int, ref epRef) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release
			imgURL, airDate, name, synopsis, rt, ok, nf := fetchAndStoreEpisodeData(tmdbID, ref.seasonNum, ref.episodeNum, ref.episodeID)
			results[i] = epResult{ref, imgURL, airDate, name, synopsis, rt, ok, nf}
		}(i, ref)
	}
	wg.Wait()

	// Omniseason fallback: if all episodes for a season returned 404, TMDB likely uses
	// a flat season structure (e.g. one big "Season 1"). Retry by computing the absolute
	// episode number from our known previous season episode counts.
	// Build a map of season number → count of 404s and total episodes to fetch
	type seasonStats struct{ notFound, total int }
	seasonNotFound := map[int]*seasonStats{}
	for _, res := range results {
		sn := res.ref.seasonNum
		if seasonNotFound[sn] == nil {
			seasonNotFound[sn] = &seasonStats{}
		}
		seasonNotFound[sn].total++
		if res.notFound {
			seasonNotFound[sn].notFound++
		}
	}

	// Build cumulative episode offset per season from our data
	// e.g. season 1 has 24 eps, season 2 has 24 eps → offset for season 3 = 48
	episodeOffset := map[int]int{}
	cumulative := 0
	for _, s := range title.Seasons {
		episodeOffset[s.SeasonNumber] = cumulative
		cumulative += len(s.Episodes)
	}

	// Retry 404'd seasons using omniseason mapping (TMDB season 1 + absolute episode num)
	var omniRetry []int // indices into results
	for i, res := range results {
		if !res.notFound {
			continue
		}
		stats := seasonNotFound[res.ref.seasonNum]
		if stats == nil || stats.notFound != stats.total {
			continue // only retry if the entire season 404'd
		}
		if res.ref.seasonNum == 1 {
			continue // season 1 already tried, no point in omniseason
		}
		omniRetry = append(omniRetry, i)
	}

	if len(omniRetry) > 0 {
		log.Printf("Trying omniseason fallback for %d episodes of %s (mapping to TMDB S1)", len(omniRetry), title.DisplayName)
		var wg2 sync.WaitGroup
		for _, idx := range omniRetry {
			wg2.Add(1)
			go func(idx int) {
				defer wg2.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				ref := results[idx].ref
				absEp := episodeOffset[ref.seasonNum] + ref.episodeNum
				imgURL, airDate, name, synopsis, rt, ok, _ := fetchAndStoreEpisodeData(tmdbID, 1, absEp, ref.episodeID)
				if ok {
					results[idx] = epResult{ref, imgURL, airDate, name, synopsis, rt, true, false}
				}
			}(idx)
		}
		wg2.Wait()
	}

	// Update in-memory structs for immediate rendering
	fetched, failed := 0, 0
	for _, res := range results {
		if !res.ok {
			// Store sentinel for episodes that failed even after omniseason retry
			if res.notFound {
				sentinel := "TMDB_NOT_FOUND_DO_NOT_RETRY"
				db.Exec(`UPDATE show_episodes SET image_url = $1 WHERE id = $2`, sentinel, res.ref.episodeID)
			}
			failed++
			continue
		}
		fetched++
		ep := &title.Seasons[res.ref.seasonIdx].Episodes[res.ref.episodeIdx]
		if res.imageURL != "" {
			ep.ImageURL = &res.imageURL
		}
		if res.airDate != "" {
			ep.AirDate = &res.airDate
		}
		if res.displayName != "" && ep.DisplayName == nil {
			ep.DisplayName = &res.displayName
		}
		if res.synopsis != "" {
			ep.Synopsis = &res.synopsis
		}
		if res.runtime != 0 {
			ep.RuntimeMinutes = &res.runtime
		}
	}
	log.Printf("TMDB episode fetch done for %s: %d succeeded, %d failed out of %d", title.DisplayName, fetched, failed, len(toFetch))

	// Update the timestamp so we don't re-fetch within 24 hours
	db.Exec(`UPDATE titles SET episodes_checked_at = NOW() WHERE id = $1`, title.TitleID)

	stripEpisodeSentinels(title)
}

// Page Handlers

func handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	var movieCount, showCount int
	db.QueryRow("SELECT COUNT(*) FROM titles WHERE type = 'movie'").Scan(&movieCount)
	db.QueryRow("SELECT COUNT(*) FROM titles WHERE type = 'show'").Scan(&showCount)

	tmpls["home"].ExecuteTemplate(w, "base", map[string]any{
		"MovieCount": movieCount,
		"ShowCount":  showCount,
	})
}

type TitleListItem struct {
	TitleID          int
	Type             string
	DisplayName      string
	StartYear        *int
	EndYear          *int
	IMDbID           *string
	ImageURL         *string
	OriginalLanguage *string
	NumVotes         *int
	AverageRating    *float64
}

func handleTitlesList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	typeFilter := r.URL.Query().Get("type")
	langFilter := r.URL.Query().Get("lang")

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	perPage := 100
	offset := (page - 1) * perPage

	var items []TitleListItem

	// Build WHERE clause (shared between count and data queries)
	where := ` WHERE 1=1`
	var args []any
	argNum := 1

	if q != "" {
		where += ` AND t.display_name ILIKE '%' || $` + strconv.Itoa(argNum) + ` || '%'`
		args = append(args, q)
		argNum++
	}
	if typeFilter != "" {
		where += ` AND t.type = $` + strconv.Itoa(argNum)
		args = append(args, typeFilter)
		argNum++
	}
	if langFilter != "" {
		where += ` AND t.original_language = $` + strconv.Itoa(argNum)
		args = append(args, langFilter)
		argNum++
	}

	// Total count
	var total int
	db.QueryRow(`SELECT COUNT(*) FROM titles t`+where, args...).Scan(&total)

	query := `
		SELECT
			t.id as title_id, t.type, t.display_name, t.start_year, t.end_year,
			t.imdb_id, t.image_url, t.original_language,
			t.num_votes, t.average_rating
		FROM titles t` + where
	query += ` ORDER BY t.num_votes DESC NULLS LAST, t.display_name LIMIT ` + strconv.Itoa(perPage) + ` OFFSET ` + strconv.Itoa(offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		http.Error(w, "Database error", 500)
		log.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item TitleListItem
		rows.Scan(&item.TitleID, &item.Type, &item.DisplayName, &item.StartYear, &item.EndYear, &item.IMDbID, &item.ImageURL, &item.OriginalLanguage, &item.NumVotes, &item.AverageRating)
		items = append(items, item)
	}

	// Count languages for filter chips (only when searching)
	type LangCount struct {
		Code    string
		Display string
		Count   int
	}
	var langCounts []LangCount
	if q != "" {
		langQuery := `
			SELECT COALESCE(t.original_language, ''), COUNT(*)
			FROM titles t
			WHERE t.display_name ILIKE '%' || $1 || '%'`
		langArgs := []any{q}
		langArgNum := 2
		if typeFilter != "" {
			langQuery += ` AND t.type = $` + strconv.Itoa(langArgNum)
			langArgs = append(langArgs, typeFilter)
		}
		langQuery += ` GROUP BY t.original_language ORDER BY COUNT(*) DESC`

		langRows, err := db.Query(langQuery, langArgs...)
		if err == nil {
			defer langRows.Close()
			for langRows.Next() {
				var code string
				var count int
				langRows.Scan(&code, &count)
				if code != "" {
					langCounts = append(langCounts, LangCount{code, langDisplay(code), count})
				}
			}
		}
	}

	// Fetch missing images from TMDB concurrently
	type fetchResult struct {
		idx int
		url string
	}
	ch := make(chan fetchResult, len(items))
	pending := 0
	for i, item := range items {
		if needsFetch(item.ImageURL, item.IMDbID) {
			pending++
			go func(idx int, imdbID, titleType string) {
				url, _ := fetchAndStoreTMDBImage(imdbID, titleType)
				ch <- fetchResult{idx, url}
			}(i, *item.IMDbID, item.Type)
		}
	}
	for range pending {
		res := <-ch
		if res.url != "" {
			items[res.idx].ImageURL = &res.url
		}
	}

	// Strip "TMDB_NOT_FOUND_DO_NOT_RETRY" sentinel so templates don't render it
	for i := range items {
		if items[i].ImageURL != nil && *items[i].ImageURL == "TMDB_NOT_FOUND_DO_NOT_RETRY" {
			items[i].ImageURL = nil
		}
	}

	totalPages := (total + perPage - 1) / perPage

	tmpls["titles"].ExecuteTemplate(w, "base", map[string]any{
		"Titles":     items,
		"Query":      q,
		"Type":       typeFilter,
		"Lang":       langFilter,
		"LangCounts": langCounts,
		"Page":       page,
		"TotalPages": totalPages,
		"Total":      total,
	})
}

func handleTitlePage(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/titles/")
	if idStr == "" {
		http.Redirect(w, r, "/titles", http.StatusFound)
		return
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	title, err := getTitleByID(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	maybeFetchImage(&title)
	maybeTMDBBackfill(&title)
	maybeFetchWatchProviders(&title)

	if title.Type == "show" {
		loadSeasonsForTitle(&title)
		maybeFetchEpisodes(&title)
	}

	go logEngagement(title.TitleID, r.URL.Query().Get("source"))

	tmpls["title"].ExecuteTemplate(w, "base", title)
}

func handleAddPage(w http.ResponseWriter, r *http.Request) {
	tmpls["add"].ExecuteTemplate(w, "base", nil)
}

func handleAPIPage(w http.ResponseWriter, r *http.Request) {
	tmpls["api"].ExecuteTemplate(w, "base", nil)
}

func handleAPISlash(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/" {
		tmpls["api"].ExecuteTemplate(w, "base", nil)
		return
	}
	http.NotFound(w, r)
}

// readOnly is a no-op — writes are allowed.
func readOnly(w http.ResponseWriter, r *http.Request) bool {
	return false
}

// API Handlers - Titles

func handleAPITitles(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	switch r.Method {
	case "GET":
		q := r.URL.Query().Get("q")
		typeFilter := r.URL.Query().Get("type")
		langFilter := r.URL.Query().Get("lang")

		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 1 {
			page = 1
		}
		perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		if perPage < 1 || perPage > 100 {
			perPage = 100
		}
		offset := (page - 1) * perPage

		where := ` WHERE 1=1`
		var args []any
		argNum := 1

		if q != "" {
			where += ` AND t.display_name ILIKE '%' || $` + strconv.Itoa(argNum) + ` || '%'`
			args = append(args, q)
			argNum++
		}
		if typeFilter != "" {
			where += ` AND t.type = $` + strconv.Itoa(argNum)
			args = append(args, typeFilter)
			argNum++
		}
		if langFilter != "" {
			where += ` AND t.original_language = $` + strconv.Itoa(argNum)
			args = append(args, langFilter)
			argNum++
		}

		var total int
		db.QueryRow(`SELECT COUNT(*) FROM titles t`+where, args...).Scan(&total)

		// Language distribution across the full query (without lang filter)
		langWhere := ` WHERE 1=1`
		var langArgs []any
		langArgNum := 1
		if q != "" {
			langWhere += ` AND t.display_name ILIKE '%' || $` + strconv.Itoa(langArgNum) + ` || '%'`
			langArgs = append(langArgs, q)
			langArgNum++
		}
		if typeFilter != "" {
			langWhere += ` AND t.type = $` + strconv.Itoa(langArgNum)
			langArgs = append(langArgs, typeFilter)
		}
		var languages []map[string]any
		langRows, err := db.Query(`SELECT COALESCE(t.original_language, ''), COUNT(*) FROM titles t`+langWhere+` GROUP BY t.original_language ORDER BY COUNT(*) DESC`, langArgs...)
		if err == nil {
			defer langRows.Close()
			for langRows.Next() {
				var code string
				var count int
				langRows.Scan(&code, &count)
				if code != "" {
					languages = append(languages, map[string]any{"code": code, "count": count})
				}
			}
		}

		query := `
			SELECT t.id, t.type, t.display_name, t.start_year, t.end_year, t.imdb_id, t.image_url, t.tmdb_id,
			       t.num_votes, t.average_rating, t.original_title, t.original_language,
			       TO_CHAR(t.release_date, 'YYYY-MM-DD'), t.created_at, t.updated_at
			FROM titles t` + where
		query += ` ORDER BY t.num_votes DESC NULLS LAST, t.display_name LIMIT ` + strconv.Itoa(perPage) + ` OFFSET ` + strconv.Itoa(offset)

		rows, err := db.Query(query, args...)
		if err != nil {
			jsonError(w, "Database error", 500)
			return
		}
		defer rows.Close()

		var titles []TitleSearchResult
		for rows.Next() {
			var t TitleSearchResult
			rows.Scan(&t.TitleID, &t.Type, &t.DisplayName, &t.StartYear, &t.EndYear, &t.IMDbID, &t.ImageURL, &t.TMDBID,
				&t.NumVotes, &t.AverageRating, &t.OriginalTitle, &t.OriginalLanguage,
				&t.ReleaseDate, &t.CreatedAt, &t.UpdatedAt)
			titles = append(titles, t)
		}
		totalPages := (total + perPage - 1) / perPage
		jsonResponse(w, map[string]any{
			"titles":      titles,
			"total":       total,
			"page":        page,
			"per_page":    perPage,
			"total_pages": totalPages,
			"languages":   languages,
		})

	case "POST":
		var t Title
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			jsonError(w, "Invalid JSON", 400)
			return
		}
		if t.Type != "movie" && t.Type != "show" {
			jsonError(w, "Type must be 'movie' or 'show'", 400)
			return
		}
		if t.DisplayName == "" {
			jsonError(w, "display_name required", 400)
			return
		}

		err := db.QueryRow(`
			INSERT INTO titles (type, display_name, start_year, end_year, imdb_id, image_url)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id, created_at, updated_at
		`, t.Type, t.DisplayName, t.StartYear, t.EndYear, t.IMDbID, t.ImageURL).Scan(&t.TitleID, &t.CreatedAt, &t.UpdatedAt)

		if err != nil {
			jsonError(w, "Failed to create title: "+err.Error(), 500)
			return
		}
		w.WriteHeader(201)
		jsonResponse(w, t)

	default:
		w.WriteHeader(405)
	}
}

func handleAPITitle(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/titles/")
	parts := strings.SplitN(path, "/", 2)

	id, err := strconv.Atoi(parts[0])
	if err != nil {
		jsonError(w, "Invalid ID", 400)
		return
	}

	// Handle /api/titles/:id/seasons
	if len(parts) >= 2 && parts[1] == "seasons" {
		handleTitleSeasons(w, r, id)
		return
	}

	// Handle /api/titles/:id/episodes?season=N
	if len(parts) >= 2 && parts[1] == "episodes" {
		handleAPITitleEpisodes(w, r, id)
		return
	}

	switch r.Method {
	case "GET":
		t, err := getTitleByID(id)
		if err != nil {
			jsonError(w, "Not found", 404)
			return
		}

		maybeFetchWatchProviders(&t)

		layer := MCTitleLayer{
			TitleID:     t.TitleID,
			Kind:        t.Type,
			IMDbID:      t.IMDbID,
			DisplayName: t.DisplayName,
			PosterURL:   t.ImageURL,
			StartYear:   t.StartYear,
			EndYear:     t.EndYear,
			Genres:      t.Genres,
			Rating:      t.AverageRating,
			VoteCount:   t.NumVotes,
		}
		layer.WatchProviders = t.WatchProviders
		layer.Networks = t.Networks
		layer.ProductionCompanies = t.ProductionCompanies
		if layer.Genres == nil {
			layer.Genres = []string{}
		}

		if t.Type == "show" {
			rows, err := db.Query(`
				SELECT ss.season, COUNT(se.id)
				FROM show_seasons ss
				LEFT JOIN show_episodes se ON se.season_id = ss.id
				WHERE ss.title_id = $1
				GROUP BY ss.season
				ORDER BY ss.season
			`, id)
			if err != nil {
				jsonError(w, "database error", 500)
				return
			}
			defer rows.Close()

			var seasons []MCSeasonInfo
			totalEpisodes := 0
			for rows.Next() {
				var si MCSeasonInfo
				rows.Scan(&si.SeasonNumber, &si.EpisodeCount)
				totalEpisodes += si.EpisodeCount
				seasons = append(seasons, si)
			}

			isFinished := t.EndYear != nil
			layer.Show = &MCShowInfo{
				SeasonCount:  len(seasons),
				EpisodeCount: totalEpisodes,
				IsFinished:   isFinished,
				Seasons:      seasons,
			}
			if layer.Show.Seasons == nil {
				layer.Show.Seasons = []MCSeasonInfo{}
			}
		} else {
			layer.RuntimeMinutes = t.RuntimeMinutes
		}

		go logEngagement(t.TitleID, r.URL.Query().Get("source"))
		jsonResponse(w, layer)

	case "PUT":
		var t Title
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			jsonError(w, "Invalid JSON", 400)
			return
		}

		_, err := db.Exec(`
			UPDATE titles SET display_name = $1, start_year = $2, end_year = $3, imdb_id = $4, image_url = $5, updated_at = NOW()
			WHERE id = $6
		`, t.DisplayName, t.StartYear, t.EndYear, t.IMDbID, t.ImageURL, id)
		if err != nil {
			jsonError(w, "Update failed", 500)
			return
		}

		t.TitleID = id
		jsonResponse(w, t)

	case "DELETE":
		_, err := db.Exec("DELETE FROM titles WHERE id = $1", id)
		if err != nil {
			jsonError(w, "Delete failed", 500)
			return
		}
		w.WriteHeader(204)

	default:
		w.WriteHeader(405)
	}
}

// handleTitleSeasons handles /api/titles/:id/seasons
func handleTitleSeasons(w http.ResponseWriter, r *http.Request, titleID int) {
	if readOnly(w, r) {
		return
	}
	switch r.Method {
	case "GET":
		rows, err := db.Query(`SELECT id, title_id, season FROM show_seasons WHERE title_id = $1 ORDER BY season`, titleID)
		if err != nil {
			jsonError(w, "Database error", 500)
			return
		}
		defer rows.Close()

		var seasons []Season
		for rows.Next() {
			var s Season
			rows.Scan(&s.SeasonID, &s.TitleID, &s.SeasonNumber)
			seasons = append(seasons, s)
		}
		jsonResponse(w, seasons)

	case "POST":
		var req struct {
			SeasonNumber int `json:"season_number"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonError(w, "Invalid JSON", 400)
			return
		}

		var seasonID int
		err := db.QueryRow(`
			INSERT INTO show_seasons (title_id, season) VALUES ($1, $2) RETURNING id
		`, titleID, req.SeasonNumber).Scan(&seasonID)
		if err != nil {
			jsonError(w, "Failed to create season: "+err.Error(), 500)
			return
		}

		w.WriteHeader(201)
		jsonResponse(w, Season{SeasonID: seasonID, TitleID: titleID, SeasonNumber: req.SeasonNumber})

	default:
		w.WriteHeader(405)
	}
}

// API Handlers - Seasons

func handleAPISeason(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/seasons/")
	parts := strings.Split(path, "/")

	id, err := strconv.Atoi(parts[0])
	if err != nil {
		jsonError(w, "Invalid ID", 400)
		return
	}

	// Handle /api/seasons/:id/episodes
	if len(parts) >= 2 && parts[1] == "episodes" {
		handleSeasonEpisodes(w, r, id)
		return
	}

	switch r.Method {
	case "GET":
		var s Season
		err := db.QueryRow(`SELECT id, title_id, season FROM show_seasons WHERE id = $1`, id).Scan(&s.SeasonID, &s.TitleID, &s.SeasonNumber)
		if err != nil {
			jsonError(w, "Not found", 404)
			return
		}

		// Get episodes
		rows, _ := db.Query(`SELECT id, season_id, episode, display_name, image_url, TO_CHAR(air_date, 'YYYY-MM-DD'), runtime_minutes, synopsis FROM show_episodes WHERE season_id = $1 ORDER BY episode`, id)
		defer rows.Close()
		for rows.Next() {
			var e Episode
			rows.Scan(&e.EpisodeID, &e.SeasonID, &e.EpisodeNumber, &e.DisplayName, &e.ImageURL, &e.AirDate, &e.RuntimeMinutes, &e.Synopsis)
			s.Episodes = append(s.Episodes, e)
		}
		jsonResponse(w, s)

	case "DELETE":
		_, err := db.Exec("DELETE FROM show_seasons WHERE id = $1", id)
		if err != nil {
			jsonError(w, "Delete failed", 500)
			return
		}
		w.WriteHeader(204)

	default:
		w.WriteHeader(405)
	}
}

func handleSeasonEpisodes(w http.ResponseWriter, r *http.Request, seasonID int) {
	if readOnly(w, r) {
		return
	}
	switch r.Method {
	case "GET":
		rows, err := db.Query(`SELECT id, season_id, episode, display_name, image_url, TO_CHAR(air_date, 'YYYY-MM-DD'), runtime_minutes, synopsis FROM show_episodes WHERE season_id = $1 ORDER BY episode`, seasonID)
		if err != nil {
			jsonError(w, "Database error", 500)
			return
		}
		defer rows.Close()

		var episodes []Episode
		for rows.Next() {
			var e Episode
			rows.Scan(&e.EpisodeID, &e.SeasonID, &e.EpisodeNumber, &e.DisplayName, &e.ImageURL, &e.AirDate, &e.RuntimeMinutes, &e.Synopsis)
			episodes = append(episodes, e)
		}
		jsonResponse(w, episodes)

	case "POST":
		var req struct {
			EpisodeNumber int     `json:"episode_number"`
			DisplayName   *string `json:"display_name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonError(w, "Invalid JSON", 400)
			return
		}

		var episodeID int
		err := db.QueryRow(`
			INSERT INTO show_episodes (season_id, episode, display_name) VALUES ($1, $2, $3) RETURNING id
		`, seasonID, req.EpisodeNumber, req.DisplayName).Scan(&episodeID)
		if err != nil {
			jsonError(w, "Failed to create episode: "+err.Error(), 500)
			return
		}

		w.WriteHeader(201)
		jsonResponse(w, Episode{EpisodeID: episodeID, SeasonID: seasonID, EpisodeNumber: req.EpisodeNumber, DisplayName: req.DisplayName})

	default:
		w.WriteHeader(405)
	}
}

// API Handlers - Episodes

func handleAPIEpisode(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	idStr := strings.TrimPrefix(r.URL.Path, "/api/episodes/")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		jsonError(w, "Invalid ID", 400)
		return
	}

	switch r.Method {
	case "GET":
		var e Episode
		err := db.QueryRow(`SELECT id, season_id, episode, display_name, image_url, TO_CHAR(air_date, 'YYYY-MM-DD'), runtime_minutes, synopsis FROM show_episodes WHERE id = $1`, id).Scan(&e.EpisodeID, &e.SeasonID, &e.EpisodeNumber, &e.DisplayName, &e.ImageURL, &e.AirDate, &e.RuntimeMinutes, &e.Synopsis)
		if err != nil {
			jsonError(w, "Not found", 404)
			return
		}
		jsonResponse(w, e)

	case "PUT":
		var req struct {
			EpisodeNumber int     `json:"episode_number"`
			DisplayName   *string `json:"display_name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonError(w, "Invalid JSON", 400)
			return
		}

		_, err := db.Exec(`UPDATE show_episodes SET episode = $1, display_name = $2 WHERE id = $3`, req.EpisodeNumber, req.DisplayName, id)
		if err != nil {
			jsonError(w, "Update failed", 500)
			return
		}

		jsonResponse(w, Episode{EpisodeID: id, EpisodeNumber: req.EpisodeNumber, DisplayName: req.DisplayName})

	case "DELETE":
		_, err := db.Exec("DELETE FROM show_episodes WHERE id = $1", id)
		if err != nil {
			jsonError(w, "Delete failed", 500)
			return
		}
		w.WriteHeader(204)

	default:
		w.WriteHeader(405)
	}
}

func handleAPITitleEpisodes(w http.ResponseWriter, r *http.Request, titleID int) {
	var titleType string
	err := db.QueryRow(`SELECT type FROM titles WHERE id = $1`, titleID).Scan(&titleType)
	if err != nil {
		jsonError(w, "title not found", 404)
		return
	}

	if titleType != "show" {
		jsonError(w, "title is not a show", 400)
		return
	}

	seasonStr := r.URL.Query().Get("season")
	if seasonStr == "" {
		jsonError(w, "season parameter is required", 400)
		return
	}
	seasonNum, err := strconv.Atoi(seasonStr)
	if err != nil {
		jsonError(w, "season not found", 404)
		return
	}

	var seasonID int
	err = db.QueryRow(`SELECT id FROM show_seasons WHERE title_id = $1 AND season = $2`, titleID, seasonNum).Scan(&seasonID)
	if err != nil {
		jsonError(w, "season not found", 404)
		return
	}

	rows, err := db.Query(`
		SELECT episode, display_name, image_url, synopsis, runtime_minutes, TO_CHAR(air_date, 'YYYY-MM-DD')
		FROM show_episodes
		WHERE season_id = $1
		ORDER BY episode
	`, seasonID)
	if err != nil {
		jsonError(w, "database error", 500)
		return
	}
	defer rows.Close()

	episodes := []MCEpisodeLayer{}
	for rows.Next() {
		var e MCEpisodeLayer
		rows.Scan(&e.EpisodeNumber, &e.DisplayName, &e.ImageURL, &e.Synopsis, &e.RuntimeMinutes, &e.AirDate)
		episodes = append(episodes, e)
	}

	jsonResponse(w, episodes)
}

// Helper functions

func getTitleByID(id int) (Title, error) {
	var t Title
	err := db.QueryRow(`
		SELECT id, type, display_name, start_year, end_year, imdb_id, image_url, tmdb_id,
		       num_votes, average_rating, original_title, original_language,
		       TO_CHAR(release_date, 'YYYY-MM-DD'), tmdb_popularity, runtime_minutes,
		       origin_country, COALESCE(needs_backfill_tmdb, true), episodes_checked_at,
		       is_series_finished, created_at, updated_at,
		       watch_providers, watch_providers_checked_at, networks, production_companies
		FROM titles WHERE id = $1
	`, id).Scan(&t.TitleID, &t.Type, &t.DisplayName, &t.StartYear, &t.EndYear, &t.IMDbID, &t.ImageURL, &t.TMDBID,
		&t.NumVotes, &t.AverageRating, &t.OriginalTitle, &t.OriginalLanguage,
		&t.ReleaseDate, &t.TMDBPopularity, &t.RuntimeMinutes,
		&t.OriginCountry, &t.NeedsBackfillTMDB, &t.EpisodesCheckedAt,
		&t.IsSeriesFinished, &t.CreatedAt, &t.UpdatedAt,
		&t.WatchProviders, &t.WatchProvidersCheckedAt, &t.Networks, &t.ProductionCompanies)
	if err == nil {
		t.Genres = loadGenresForTitle(id)
	}
	return t, err
}

// loadSeasonsForTitle loads seasons and episodes for a show title in place.
func loadSeasonsForTitle(t *Title) {
	// Collect seasons first, then close rows before querying episodes.
	rows, _ := db.Query(`SELECT id, title_id, season FROM show_seasons WHERE title_id = $1 ORDER BY season`, t.TitleID)
	for rows.Next() {
		var sn Season
		rows.Scan(&sn.SeasonID, &sn.TitleID, &sn.SeasonNumber)
		t.Seasons = append(t.Seasons, sn)
	}
	rows.Close()

	// Now load episodes for each season (one connection at a time)
	for i, sn := range t.Seasons {
		epRows, _ := db.Query(`SELECT id, season_id, episode, display_name, image_url, TO_CHAR(air_date, 'YYYY-MM-DD'), runtime_minutes, synopsis FROM show_episodes WHERE season_id = $1 ORDER BY episode`, sn.SeasonID)
		for epRows.Next() {
			var e Episode
			epRows.Scan(&e.EpisodeID, &e.SeasonID, &e.EpisodeNumber, &e.DisplayName, &e.ImageURL, &e.AirDate, &e.RuntimeMinutes, &e.Synopsis)
			t.Seasons[i].Episodes = append(t.Seasons[i].Episodes, e)
		}
		epRows.Close()
	}

	// Derive is_series_finished and is_season_finished flags
	finished := t.EndYear != nil
	t.IsSeriesFinished = &finished

	maxSeason := 0
	for _, sn := range t.Seasons {
		if sn.SeasonNumber > maxSeason {
			maxSeason = sn.SeasonNumber
		}
	}
	for i := range t.Seasons {
		var sf bool
		if t.EndYear != nil {
			sf = true
		} else {
			sf = t.Seasons[i].SeasonNumber < maxSeason
		}
		t.Seasons[i].IsSeasonFinished = &sf
	}
}

// Genre and view tracking helpers

func loadGenresForTitle(titleID int) []string {
	rows, err := db.Query(`SELECT g.name FROM genres g JOIN title_genres tg ON tg.genre_id = g.id WHERE tg.title_id = $1 ORDER BY g.name`, titleID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var genres []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		genres = append(genres, name)
	}
	return genres
}

func loadGenresForTitles(titleIDs []int) map[int][]string {
	if len(titleIDs) == 0 {
		return nil
	}
	result := make(map[int][]string)
	placeholders := make([]string, len(titleIDs))
	args := make([]any, len(titleIDs))
	for i, id := range titleIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT tg.title_id, g.name FROM genres g JOIN title_genres tg ON tg.genre_id = g.id WHERE tg.title_id IN (%s) ORDER BY tg.title_id, g.name`, strings.Join(placeholders, ",")), args...)
	if err != nil {
		return result
	}
	defer rows.Close()
	for rows.Next() {
		var titleID int
		var name string
		rows.Scan(&titleID, &name)
		result[titleID] = append(result[titleID], name)
	}
	return result
}

func logTitleView(titleID int, source string) {
	db.Exec(`INSERT INTO title_views (title_id, source) VALUES ($1, $2)`, titleID, source)
}

func logEngagement(titleID int, source string) {
	logTitleView(titleID, source)
	if slug, ok := strings.CutPrefix(source, "collection-"); ok && slug != "" {
		var collectionID int
		err := db.QueryRow(`SELECT id FROM collections WHERE slug = $1`, slug).Scan(&collectionID)
		if err == nil {
			logCollectionClick(collectionID)
		}
	}
}

func logCollectionClick(collectionID int) {
	db.Exec(`INSERT INTO collection_clicks (collection_id) VALUES ($1)`, collectionID)
	db.Exec(`UPDATE collections SET engagement_count = (SELECT COUNT(*) FROM collection_clicks WHERE collection_id = $1)::real WHERE id = $1`, collectionID)
}

func cleanupOldViews() {
	log.Println("Cleaning up old view/click tracking data...")
	res, _ := db.Exec(`DELETE FROM title_views WHERE viewed_at < NOW() - INTERVAL '7 days'`)
	if n, err := res.RowsAffected(); err == nil {
		log.Printf("Cleaned up %d old title views", n)
	}
	res, _ = db.Exec(`DELETE FROM collection_clicks WHERE clicked_at < NOW() - INTERVAL '7 days'`)
	if n, err := res.RowsAffected(); err == nil {
		log.Printf("Cleaned up %d old collection clicks", n)
	}
	// Update collection engagement_count from rolling window
	db.Exec(`UPDATE collections SET engagement_count = COALESCE((SELECT COUNT(*) FROM collection_clicks WHERE collection_id = collections.id), 0)::real`)
}

// Collection loading from embedded YAML files

func loadCollections() {
	entries, err := collectionsFS.ReadDir("collections")
	if err != nil {
		log.Printf("Warning: could not read collections directory: %v", err)
		return
	}

	loaded := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		data, err := collectionsFS.ReadFile("collections/" + entry.Name())
		if err != nil {
			log.Printf("Warning: could not read %s: %v", entry.Name(), err)
			continue
		}

		var def CollectionDef
		if err := yaml.Unmarshal(data, &def); err != nil {
			log.Printf("Warning: could not parse %s: %v", entry.Name(), err)
			continue
		}

		if def.Slug == "" || def.Name == "" {
			log.Printf("Warning: %s missing slug or name, skipping", entry.Name())
			continue
		}
		if def.Strategy == "" {
			def.Strategy = "filter"
		}

		// Marshal filter params to JSON for storage
		var filterJSON []byte
		if def.Strategy == "filter" {
			filterJSON, _ = json.Marshal(def.Filter)
		}

		// Convert string slices to PostgreSQL arrays
		langArr := "{}"
		if len(def.Languages) > 0 {
			langArr = "{" + strings.Join(def.Languages, ",") + "}"
		}
		regionArr := "{}"
		if len(def.Regions) > 0 {
			regionArr = "{" + strings.Join(def.Regions, ",") + "}"
		}

		// Upsert collection (preserve engagement_count and active from DB)
		var collID int
		err = db.QueryRow(`
			INSERT INTO collections (slug, name, description, strategy, filter_params, languages, regions, pinned)
			VALUES ($1, $2, $3, $4, $5, $6::text[], $7::text[], $8)
			ON CONFLICT (slug) DO UPDATE SET
				name = EXCLUDED.name,
				description = EXCLUDED.description,
				strategy = EXCLUDED.strategy,
				filter_params = EXCLUDED.filter_params,
				languages = EXCLUDED.languages,
				regions = EXCLUDED.regions,
				pinned = EXCLUDED.pinned,
				updated_at = NOW()
			RETURNING id
		`, def.Slug, def.Name, def.Description, def.Strategy, filterJSON, langArr, regionArr, def.Pinned).Scan(&collID)
		if err != nil {
			log.Printf("Warning: could not upsert collection %s: %v", def.Slug, err)
			continue
		}

		// For static collections, resolve imdb_ids to title_ids
		if def.Strategy == "static" && len(def.Titles) > 0 {
			// Clear existing memberships
			db.Exec(`DELETE FROM collection_titles WHERE collection_id = $1`, collID)

			for rank, imdbID := range def.Titles {
				var titleID int
				err := db.QueryRow(`SELECT id FROM titles WHERE imdb_id = $1`, imdbID).Scan(&titleID)
				if err != nil {
					continue // Title not in our DB
				}
				db.Exec(`INSERT INTO collection_titles (collection_id, title_id, rank) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, collID, titleID, rank+1)
			}
		}

		loaded++
	}

	log.Printf("Loaded %d collections from YAML files", loaded)
}

type carouselBucket struct {
	Titles     []DiscoverTitle
	TotalCount int
}

// buildCarouselCache runs one SQL query to get the top 30 titles per (type, genre)
// combination and total counts, caches the result in memory for fast discover page rendering.
func buildCarouselCache() {
	log.Println("Building carousel cache...")
	start := time.Now()

	// Get total counts per type+genre (fast aggregate)
	counts := make(map[string]int)
	countRows, err := db.Query(`
		SELECT t.type, g.name, COUNT(DISTINCT t.id)
		FROM titles t
		JOIN title_genres tg ON tg.title_id = t.id
		JOIN genres g ON tg.genre_id = g.id
		WHERE t.image_url IS NOT NULL
			AND t.image_url NOT IN ('none', 'TMDB_NOT_FOUND_DO_NOT_RETRY')
			AND t.num_votes >= 5000
			AND t.average_rating IS NOT NULL
		GROUP BY t.type, g.name
	`)
	if err == nil {
		defer countRows.Close()
		for countRows.Next() {
			var typ, genre string
			var cnt int
			countRows.Scan(&typ, &genre, &cnt)
			counts[typ+":"+genre] = cnt
		}
	}

	// Get top 30 per type+genre
	rows, err := db.Query(`
		WITH ranked AS (
			SELECT t.id, t.type, t.display_name, t.start_year, t.end_year, t.image_url,
				t.average_rating, t.num_votes, t.tmdb_popularity,
				g.name as genre,
				COALESCE((SELECT COUNT(*) FROM title_views tv WHERE tv.title_id = t.id), 0) as engagement_count,
				ROW_NUMBER() OVER (
					PARTITION BY t.type, g.name
					ORDER BY t.average_rating DESC NULLS LAST, t.num_votes DESC NULLS LAST
				) as rn
			FROM titles t
			JOIN title_genres tg ON tg.title_id = t.id
			JOIN genres g ON tg.genre_id = g.id
			WHERE t.image_url IS NOT NULL
				AND t.image_url NOT IN ('none', 'TMDB_NOT_FOUND_DO_NOT_RETRY')
				AND t.num_votes >= 5000
				AND t.average_rating IS NOT NULL
		)
		SELECT id, type, display_name, start_year, end_year, image_url,
			average_rating, num_votes, tmdb_popularity, genre, engagement_count
		FROM ranked
		WHERE rn <= 30
		ORDER BY type, genre, rn
	`)
	if err != nil {
		log.Printf("buildCarouselCache query error: %v", err)
		return
	}
	defer rows.Close()

	type entry struct {
		title DiscoverTitle
		key   string
	}
	var entries []entry
	uniqueIDs := make(map[int]bool)

	for rows.Next() {
		var d DiscoverTitle
		var genre string
		rows.Scan(&d.TitleID, &d.Type, &d.DisplayName, &d.StartYear, &d.EndYear, &d.ImageURL,
			&d.AverageRating, &d.NumVotes, &d.TMDBPopularity, &genre, &d.EngagementCount)
		entries = append(entries, entry{d, d.Type + ":" + genre})
		uniqueIDs[d.TitleID] = true
	}

	// Batch load genres for all titles in one query
	var allIDs []int
	for id := range uniqueIDs {
		allIDs = append(allIDs, id)
	}
	genreMap := loadGenresForTitles(allIDs)

	// Build cache keyed by "type:Genre"
	titlesByKey := make(map[string][]DiscoverTitle)
	for _, e := range entries {
		e.title.Genres = genreMap[e.title.TitleID]
		titlesByKey[e.key] = append(titlesByKey[e.key], e.title)
	}

	cache := make(map[string]carouselBucket)
	for key, titles := range titlesByKey {
		cache[key] = carouselBucket{Titles: titles, TotalCount: counts[key]}
	}

	carouselCacheMu.Lock()
	carouselCache = cache
	carouselCacheMu.Unlock()

	log.Printf("Carousel cache built in %v: %d type:genre buckets, %d unique titles",
		time.Since(start), len(cache), len(uniqueIDs))
}

// Discover page helpers

func fetchDiscoverTitles(sortBy, typeFilter, langFilter, genreFilter, countryFilter, yearMin, ratingMin, minVotes string, limit, offset int) ([]DiscoverTitle, int) {
	where := `WHERE t.image_url IS NOT NULL AND t.image_url NOT IN ('none', 'TMDB_NOT_FOUND_DO_NOT_RETRY')`
	var args []any
	argNum := 1

	if typeFilter != "" {
		where += fmt.Sprintf(` AND t.type = $%d`, argNum)
		args = append(args, typeFilter)
		argNum++
	}
	if langFilter != "" {
		where += fmt.Sprintf(` AND t.original_language = $%d`, argNum)
		args = append(args, langFilter)
		argNum++
	}
	if genreFilter != "" {
		where += fmt.Sprintf(` AND EXISTS(SELECT 1 FROM title_genres tg JOIN genres g ON tg.genre_id=g.id WHERE tg.title_id=t.id AND g.name=$%d)`, argNum)
		args = append(args, genreFilter)
		argNum++
	}
	if countryFilter != "" {
		where += fmt.Sprintf(` AND t.origin_country = $%d`, argNum)
		args = append(args, countryFilter)
		argNum++
	}
	if yearMin != "" {
		if y, err := strconv.Atoi(yearMin); err == nil {
			where += fmt.Sprintf(` AND t.start_year >= $%d`, argNum)
			args = append(args, y)
			argNum++
		}
	}
	if ratingMin != "" {
		if r, err := strconv.ParseFloat(ratingMin, 64); err == nil {
			where += fmt.Sprintf(` AND t.average_rating >= $%d`, argNum)
			args = append(args, r)
			argNum++
		}
	}
	if minVotes != "" {
		if v, err := strconv.Atoi(minVotes); err == nil {
			where += fmt.Sprintf(` AND t.num_votes >= $%d`, argNum)
			args = append(args, v)
			argNum++
		}
	}

	orderBy := "t.num_votes DESC NULLS LAST"
	switch sortBy {
	case "most_rated":
		orderBy = "t.num_votes DESC NULLS LAST"
	case "trending":
		orderBy = "t.tmdb_popularity DESC NULLS LAST"
	case "popular":
		orderBy = "(SELECT COUNT(*) FROM title_views tv WHERE tv.title_id = t.id) DESC"
	case "top_rated":
		orderBy = "t.average_rating DESC NULLS LAST"
		if minVotes == "" && ratingMin == "" {
			where += ` AND t.num_votes >= 1000`
		}
	case "newest":
		orderBy = "t.start_year DESC NULLS LAST, t.release_date DESC NULLS LAST"
	case "hidden_gems":
		orderBy = "t.average_rating DESC NULLS LAST"
		where += ` AND t.average_rating >= 7.5 AND t.num_votes < 10000 AND t.num_votes > 100`
	case "a-z":
		orderBy = "t.display_name ASC"
	}

	// Get total count
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM titles t %s`, where)
	var total int
	db.QueryRow(countQuery, args...).Scan(&total)

	query := fmt.Sprintf(`
		SELECT t.id, t.type, t.display_name, t.start_year, t.end_year, t.image_url,
		       t.average_rating, t.num_votes, t.tmdb_popularity,
		       COALESCE((SELECT COUNT(*) FROM title_views tv WHERE tv.title_id = t.id), 0)
		FROM titles t
		%s
		ORDER BY %s
		LIMIT %d OFFSET %d
	`, where, orderBy, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("fetchDiscoverTitles error: %v", err)
		return nil, total
	}
	defer rows.Close()

	var titles []DiscoverTitle
	var titleIDs []int
	for rows.Next() {
		var d DiscoverTitle
		rows.Scan(&d.TitleID, &d.Type, &d.DisplayName, &d.StartYear, &d.EndYear, &d.ImageURL,
			&d.AverageRating, &d.NumVotes, &d.TMDBPopularity, &d.EngagementCount)
		titles = append(titles, d)
		titleIDs = append(titleIDs, d.TitleID)
	}

	// Load genres for all titles
	genreMap := loadGenresForTitles(titleIDs)
	for i := range titles {
		titles[i].Genres = genreMap[titles[i].TitleID]
	}

	return titles, total
}

func getCollectionTitles(collID int, strategy string, filterParamsJSON []byte) []DiscoverTitle {
	switch strategy {
	case "filter":
		var fp struct {
			Type     string `json:"type"`
			Lang     string `json:"lang"`
			Genre    string `json:"genre"`
			Sort     string `json:"sort"`
			MinVotes int    `json:"min_votes"`
			Limit    int    `json:"limit"`
		}
		json.Unmarshal(filterParamsJSON, &fp)
		if fp.Limit == 0 {
			fp.Limit = 100
		}
		if fp.Sort == "" {
			fp.Sort = "top_rated"
		}
		minVotes := ""
		if fp.MinVotes > 0 {
			minVotes = strconv.Itoa(fp.MinVotes)
		}
		titles, _ := fetchDiscoverTitles(fp.Sort, fp.Type, fp.Lang, fp.Genre, "", "", "", minVotes, fp.Limit, 0)
		return titles
	case "static", "llm":
		return fetchStaticCollectionTitles(collID)
	default:
		return nil
	}
}

func fetchStaticCollectionTitles(collID int) []DiscoverTitle {
	rows, err := db.Query(`
		SELECT t.id, t.type, t.display_name, t.start_year, t.end_year, t.image_url,
		       t.average_rating, t.num_votes, t.tmdb_popularity,
		       COALESCE((SELECT COUNT(*) FROM title_views tv WHERE tv.title_id = t.id), 0)
		FROM collection_titles ct
		JOIN titles t ON ct.title_id = t.id
		WHERE ct.collection_id = $1
		ORDER BY ct.rank
	`, collID)
	if err != nil {
		log.Printf("fetchStaticCollectionTitles error: %v", err)
		return nil
	}
	defer rows.Close()

	var titles []DiscoverTitle
	var titleIDs []int
	for rows.Next() {
		var d DiscoverTitle
		rows.Scan(&d.TitleID, &d.Type, &d.DisplayName, &d.StartYear, &d.EndYear, &d.ImageURL,
			&d.AverageRating, &d.NumVotes, &d.TMDBPopularity, &d.EngagementCount)
		titles = append(titles, d)
		titleIDs = append(titleIDs, d.TitleID)
	}

	genreMap := loadGenresForTitles(titleIDs)
	for i := range titles {
		titles[i].Genres = genreMap[titles[i].TitleID]
	}

	return titles
}

// Discover page and API handlers

type DiscoverSection struct {
	Title        string
	Description  string
	Slug         string
	CollectionID int
	EngagementCount float64
	TotalCount   int
	Titles       []DiscoverTitle
}

func handleDiscoverPage(w http.ResponseWriter, r *http.Request) {
	genre := r.URL.Query().Get("genre")
	typeFilter := r.URL.Query().Get("type")
	langFilter := r.URL.Query().Get("lang")
	sortBy := r.URL.Query().Get("sort")
	yearMin := r.URL.Query().Get("year_min")
	ratingMin := r.URL.Query().Get("rating_min")
	countryFilter := r.URL.Query().Get("country")
	collectionSlug := r.URL.Query().Get("collection")

	hasFilters := genre != "" || typeFilter != "" || langFilter != "" || sortBy != "" || yearMin != "" || ratingMin != "" || countryFilter != ""
	var filteredTitles []DiscoverTitle
	var filteredTotal int
	var collectionTitles []DiscoverTitle
	var activeCollection *Collection
	var sections []DiscoverSection

	// Query top genres and countries for chips (always needed)
	type chipItem struct {
		Name  string
		Code  string
		Count int
	}
	var genreChips []chipItem
	var countryChips []chipItem

	gRows, _ := db.Query(`SELECT g.name, COUNT(*) as cnt FROM genres g JOIN title_genres tg ON tg.genre_id = g.id JOIN titles t ON tg.title_id = t.id WHERE t.image_url IS NOT NULL AND t.image_url NOT IN ('none','TMDB_NOT_FOUND_DO_NOT_RETRY') GROUP BY g.name ORDER BY cnt DESC LIMIT 15`)
	if gRows != nil {
		for gRows.Next() {
			var ci chipItem
			gRows.Scan(&ci.Name, &ci.Count)
			genreChips = append(genreChips, ci)
		}
		gRows.Close()
	}

	cRows, _ := db.Query(`SELECT origin_country, COUNT(*) as cnt FROM titles WHERE origin_country IS NOT NULL AND origin_country != '' AND image_url IS NOT NULL AND image_url NOT IN ('none','TMDB_NOT_FOUND_DO_NOT_RETRY') GROUP BY origin_country ORDER BY cnt DESC LIMIT 15`)
	if cRows != nil {
		for cRows.Next() {
			var ci chipItem
			cRows.Scan(&ci.Code, &ci.Count)
			countryChips = append(countryChips, ci)
		}
		cRows.Close()
	}

	if collectionSlug != "" {
		// Collection detail view
		var c Collection
		var strategy string
		var filterParamsJSON []byte
		err := db.QueryRow(`SELECT id, name, slug, COALESCE(description, ''), strategy, pinned, active, engagement_count, filter_params FROM collections WHERE slug = $1 AND active = true`, collectionSlug).Scan(&c.ID, &c.Name, &c.Slug, &c.Description, &strategy, &c.Pinned, &c.Active, &c.EngagementCount, &filterParamsJSON)
		c.Strategy = strategy
		if err == nil {
			activeCollection = &c
			collectionTitles = getCollectionTitles(c.ID, strategy, filterParamsJSON)
			go logCollectionClick(c.ID)
		}
	} else if hasFilters {
		filteredTitles, filteredTotal = fetchDiscoverTitles(sortBy, typeFilter, langFilter, genre, countryFilter, yearMin, ratingMin, "", 100, 0)
	} else {
		// Default: alternating carousels from cache
		carouselCacheMu.RLock()
		cc := carouselCache
		carouselCacheMu.RUnlock()

		// One lightweight query for collection metadata (name, description, engagement_count, filter type+genre)
		type collMeta struct {
			Collection
			FilterType  string
			FilterGenre string
		}
		var showColls, movieColls []collMeta
		collRows, _ := db.Query(`SELECT id, name, slug, COALESCE(description, ''), strategy, pinned, active, engagement_count, COALESCE(filter_params::text, '{}') FROM collections WHERE active = true AND strategy = 'filter' ORDER BY pinned DESC, engagement_count DESC`)
		if collRows != nil {
			for collRows.Next() {
				var cm collMeta
				var fpStr string
				collRows.Scan(&cm.ID, &cm.Name, &cm.Slug, &cm.Description, &cm.Strategy, &cm.Pinned, &cm.Active, &cm.EngagementCount, &fpStr)
				var fp struct {
					Type  string `json:"type"`
					Genre string `json:"genre"`
				}
				json.Unmarshal([]byte(fpStr), &fp)
				cm.FilterType = fp.Type
				cm.FilterGenre = fp.Genre
				if fp.Type == "show" {
					showColls = append(showColls, cm)
				} else if fp.Type == "movie" {
					movieColls = append(movieColls, cm)
				}
			}
			collRows.Close()
		}

		// Interleave: show, movie, show, movie...
		maxLen := len(showColls)
		if len(movieColls) > maxLen {
			maxLen = len(movieColls)
		}
		for i := 0; i < maxLen; i++ {
			if i < len(showColls) {
				cm := showColls[i]
				key := cm.FilterType + ":" + cm.FilterGenre
				if bucket, ok := cc[key]; ok && len(bucket.Titles) > 0 {
					sections = append(sections, DiscoverSection{
						Title: cm.Name, Description: cm.Description, Slug: cm.Slug,
						CollectionID: cm.ID, EngagementCount: cm.EngagementCount,
						TotalCount: bucket.TotalCount, Titles: bucket.Titles,
					})
				}
			}
			if i < len(movieColls) {
				cm := movieColls[i]
				key := cm.FilterType + ":" + cm.FilterGenre
				if bucket, ok := cc[key]; ok && len(bucket.Titles) > 0 {
					sections = append(sections, DiscoverSection{
						Title: cm.Name, Description: cm.Description, Slug: cm.Slug,
						CollectionID: cm.ID, EngagementCount: cm.EngagementCount,
						TotalCount: bucket.TotalCount, Titles: bucket.Titles,
					})
				}
			}
		}
	}

	tmpls["discover"].ExecuteTemplate(w, "base", map[string]any{
		"ActiveCollection": activeCollection,
		"CollectionTitles": collectionTitles,
		"FilteredTitles":   filteredTitles,
		"FilteredTotal":    filteredTotal,
		"Sections":         sections,
		"HasFilters":       hasFilters,
		"Genre":            genre,
		"Type":             typeFilter,
		"Lang":             langFilter,
		"Sort":             sortBy,
		"YearMin":          yearMin,
		"RatingMin":        ratingMin,
		"Country":          countryFilter,
		"CollectionSlug":   collectionSlug,
		"GenreChips":       genreChips,
		"CountryChips":     countryChips,
	})
}

func handleAPIDiscover(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	genre := r.URL.Query().Get("genre")
	typeFilter := r.URL.Query().Get("type")
	langFilter := r.URL.Query().Get("lang")
	sortBy := r.URL.Query().Get("sort")
	yearMin := r.URL.Query().Get("year_min")
	ratingMin := r.URL.Query().Get("rating_min")
	countryFilter := r.URL.Query().Get("country")

	limit := 100
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 && l <= 100 {
		limit = l
	}

	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 1 {
		page = p
	}
	offset := (page - 1) * limit

	minVotes := r.URL.Query().Get("min_votes")
	titles, total := fetchDiscoverTitles(sortBy, typeFilter, langFilter, genre, countryFilter, yearMin, ratingMin, minVotes, limit, offset)
	jsonResponse(w, map[string]any{"titles": titles, "total": total, "page": page, "per_page": limit})
}

func handleAPIDiscoverCarousels(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}

	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 1 {
		page = p
	}
	perPage := 10
	if pp, err := strconv.Atoi(r.URL.Query().Get("per_page")); err == nil && pp > 0 && pp <= 50 {
		perPage = pp
	}

	// Build interleaved carousel list (same logic as web discover page)
	carouselCacheMu.RLock()
	cc := carouselCache
	carouselCacheMu.RUnlock()

	type collMeta struct {
		Collection
		FilterType  string
		FilterGenre string
	}
	var showColls, movieColls []collMeta
	collRows, _ := db.Query(`SELECT id, name, slug, COALESCE(description, ''), strategy, pinned, active, engagement_count, COALESCE(filter_params::text, '{}') FROM collections WHERE active = true AND strategy = 'filter' ORDER BY pinned DESC, engagement_count DESC`)
	if collRows != nil {
		for collRows.Next() {
			var cm collMeta
			var fpStr string
			collRows.Scan(&cm.ID, &cm.Name, &cm.Slug, &cm.Description, &cm.Strategy, &cm.Pinned, &cm.Active, &cm.EngagementCount, &fpStr)
			var fp struct {
				Type  string `json:"type"`
				Genre string `json:"genre"`
			}
			json.Unmarshal([]byte(fpStr), &fp)
			cm.FilterType = fp.Type
			cm.FilterGenre = fp.Genre
			if fp.Type == "show" {
				showColls = append(showColls, cm)
			} else if fp.Type == "movie" {
				movieColls = append(movieColls, cm)
			}
		}
		collRows.Close()
	}

	// Interleave: show, movie, show, movie...
	type carouselResult struct {
		Name            string          `json:"name"`
		Slug            string          `json:"slug"`
		Description     string          `json:"description,omitempty"`
		CollectionID    int             `json:"collection_id"`
		EngagementCount float64         `json:"engagement_count"`
		TotalCount      int             `json:"total_count"`
		Titles          []DiscoverTitle `json:"titles"`
	}
	var all []carouselResult

	maxLen := len(showColls)
	if len(movieColls) > maxLen {
		maxLen = len(movieColls)
	}
	for i := 0; i < maxLen; i++ {
		if i < len(showColls) {
			cm := showColls[i]
			key := cm.FilterType + ":" + cm.FilterGenre
			if bucket, ok := cc[key]; ok && len(bucket.Titles) > 0 {
				all = append(all, carouselResult{
					Name: cm.Name, Slug: cm.Slug, Description: cm.Description,
					CollectionID: cm.ID, EngagementCount: cm.EngagementCount,
					TotalCount: bucket.TotalCount, Titles: bucket.Titles,
				})
			}
		}
		if i < len(movieColls) {
			cm := movieColls[i]
			key := cm.FilterType + ":" + cm.FilterGenre
			if bucket, ok := cc[key]; ok && len(bucket.Titles) > 0 {
				all = append(all, carouselResult{
					Name: cm.Name, Slug: cm.Slug, Description: cm.Description,
					CollectionID: cm.ID, EngagementCount: cm.EngagementCount,
					TotalCount: bucket.TotalCount, Titles: bucket.Titles,
				})
			}
		}
	}

	total := len(all)
	start := (page - 1) * perPage
	end := start + perPage
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	jsonResponse(w, map[string]any{
		"carousels": all[start:end],
		"total":     total,
		"page":      page,
		"per_page":  perPage,
	})
}

func handleAPICollections(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	rows, err := db.Query(`SELECT id, name, slug, COALESCE(description, ''), strategy, pinned, active, engagement_count FROM collections WHERE active = true ORDER BY pinned DESC, engagement_count DESC`)
	if err != nil {
		jsonError(w, "Database error", 500)
		return
	}
	defer rows.Close()

	var collections []Collection
	for rows.Next() {
		var c Collection
		rows.Scan(&c.ID, &c.Name, &c.Slug, &c.Description, &c.Strategy, &c.Pinned, &c.Active, &c.EngagementCount)
		collections = append(collections, c)
	}
	jsonResponse(w, collections)
}

func handleAPICollection(w http.ResponseWriter, r *http.Request) {
	if readOnly(w, r) {
		return
	}
	idStr := strings.TrimPrefix(r.URL.Path, "/api/collections/")
	// Try slug first, then ID
	var c Collection
	var filterParams []byte
	err := db.QueryRow(`SELECT id, name, slug, COALESCE(description, ''), strategy, pinned, active, engagement_count, COALESCE(filter_params::text, '{}')::bytea FROM collections WHERE slug = $1 AND active = true`, idStr).Scan(&c.ID, &c.Name, &c.Slug, &c.Description, &c.Strategy, &c.Pinned, &c.Active, &c.EngagementCount, &filterParams)
	if err != nil {
		if id, convErr := strconv.Atoi(idStr); convErr == nil {
			err = db.QueryRow(`SELECT id, name, slug, COALESCE(description, ''), strategy, pinned, active, engagement_count, COALESCE(filter_params::text, '{}')::bytea FROM collections WHERE id = $1 AND active = true`, id).Scan(&c.ID, &c.Name, &c.Slug, &c.Description, &c.Strategy, &c.Pinned, &c.Active, &c.EngagementCount, &filterParams)
		}
	}
	if err != nil {
		jsonError(w, "Not found", 404)
		return
	}

	titles := getCollectionTitles(c.ID, c.Strategy, filterParams)
	go logCollectionClick(c.ID)

	jsonResponse(w, map[string]any{
		"collection": c,
		"titles":     titles,
		"total":      len(titles),
	})
}

var langMap = map[string]string{
	"en": "\U0001F1EC\U0001F1E7 English",
	"ja": "\U0001F1EF\U0001F1F5 Japanese",
	"ko": "\U0001F1F0\U0001F1F7 Korean",
	"fr": "\U0001F1EB\U0001F1F7 French",
	"es": "\U0001F1EA\U0001F1F8 Spanish",
	"de": "\U0001F1E9\U0001F1EA German",
	"it": "\U0001F1EE\U0001F1F9 Italian",
	"pt": "\U0001F1E7\U0001F1F7 Portuguese",
	"ru": "\U0001F1F7\U0001F1FA Russian",
	"zh": "\U0001F1E8\U0001F1F3 Chinese",
	"hi": "\U0001F1EE\U0001F1F3 Hindi",
	"ar": "\U0001F1F8\U0001F1E6 Arabic",
	"th": "\U0001F1F9\U0001F1ED Thai",
	"tr": "\U0001F1F9\U0001F1F7 Turkish",
	"pl": "\U0001F1F5\U0001F1F1 Polish",
	"nl": "\U0001F1F3\U0001F1F1 Dutch",
	"sv": "\U0001F1F8\U0001F1EA Swedish",
	"da": "\U0001F1E9\U0001F1F0 Danish",
	"no": "\U0001F1F3\U0001F1F4 Norwegian",
	"fi": "\U0001F1EB\U0001F1EE Finnish",
	"cs": "\U0001F1E8\U0001F1FF Czech",
	"hu": "\U0001F1ED\U0001F1FA Hungarian",
	"ro": "\U0001F1F7\U0001F1F4 Romanian",
	"el": "\U0001F1EC\U0001F1F7 Greek",
	"he": "\U0001F1EE\U0001F1F1 Hebrew",
	"id": "\U0001F1EE\U0001F1E9 Indonesian",
	"ms": "\U0001F1F2\U0001F1FE Malay",
	"vi": "\U0001F1FB\U0001F1F3 Vietnamese",
	"tl": "\U0001F1F5\U0001F1ED Filipino",
	"uk": "\U0001F1FA\U0001F1E6 Ukrainian",
	"fa": "\U0001F1EE\U0001F1F7 Persian",
	"bn": "\U0001F1E7\U0001F1E9 Bengali",
	"ta": "\U0001F1EE\U0001F1F3 Tamil",
	"te": "\U0001F1EE\U0001F1F3 Telugu",
	"ml": "\U0001F1EE\U0001F1F3 Malayalam",
	"kn": "\U0001F1EE\U0001F1F3 Kannada",
	"cn": "\U0001F1E8\U0001F1F3 Chinese",
}

var countryMap = map[string]string{
	"US": "\U0001F1FA\U0001F1F8 United States",
	"GB": "\U0001F1EC\U0001F1E7 United Kingdom",
	"CA": "\U0001F1E8\U0001F1E6 Canada",
	"AU": "\U0001F1E6\U0001F1FA Australia",
	"DE": "\U0001F1E9\U0001F1EA Germany",
	"FR": "\U0001F1EB\U0001F1F7 France",
	"JP": "\U0001F1EF\U0001F1F5 Japan",
	"KR": "\U0001F1F0\U0001F1F7 South Korea",
	"IN": "\U0001F1EE\U0001F1F3 India",
	"CN": "\U0001F1E8\U0001F1F3 China",
	"IT": "\U0001F1EE\U0001F1F9 Italy",
	"ES": "\U0001F1EA\U0001F1F8 Spain",
	"BR": "\U0001F1E7\U0001F1F7 Brazil",
	"MX": "\U0001F1F2\U0001F1FD Mexico",
	"RU": "\U0001F1F7\U0001F1FA Russia",
	"SE": "\U0001F1F8\U0001F1EA Sweden",
	"DK": "\U0001F1E9\U0001F1F0 Denmark",
	"NO": "\U0001F1F3\U0001F1F4 Norway",
	"FI": "\U0001F1EB\U0001F1EE Finland",
	"NL": "\U0001F1F3\U0001F1F1 Netherlands",
	"BE": "\U0001F1E7\U0001F1EA Belgium",
	"PL": "\U0001F1F5\U0001F1F1 Poland",
	"TR": "\U0001F1F9\U0001F1F7 Turkey",
	"TH": "\U0001F1F9\U0001F1ED Thailand",
	"TW": "\U0001F1F9\U0001F1FC Taiwan",
	"HK": "\U0001F1ED\U0001F1F0 Hong Kong",
	"NZ": "\U0001F1F3\U0001F1FF New Zealand",
	"IE": "\U0001F1EE\U0001F1EA Ireland",
	"IL": "\U0001F1EE\U0001F1F1 Israel",
	"AR": "\U0001F1E6\U0001F1F7 Argentina",
	"CO": "\U0001F1E8\U0001F1F4 Colombia",
	"PH": "\U0001F1F5\U0001F1ED Philippines",
	"ID": "\U0001F1EE\U0001F1E9 Indonesia",
	"CZ": "\U0001F1E8\U0001F1FF Czech Republic",
	"AT": "\U0001F1E6\U0001F1F9 Austria",
	"CH": "\U0001F1E8\U0001F1ED Switzerland",
	"PT": "\U0001F1F5\U0001F1F9 Portugal",
	"ZA": "\U0001F1FF\U0001F1E6 South Africa",
	"EG": "\U0001F1EA\U0001F1EC Egypt",
	"NG": "\U0001F1F3\U0001F1EC Nigeria",
	"SA": "\U0001F1F8\U0001F1E6 Saudi Arabia",
}

func countryDisplay(code string) string {
	if s, ok := countryMap[code]; ok {
		return s
	}
	return strings.ToUpper(code)
}

func fmtRating(rating *float64, votes *int) string {
	if rating == nil {
		return ""
	}
	s := fmt.Sprintf("%.1f", *rating)
	if votes != nil {
		var v string
		switch {
		case *votes >= 1000000:
			v = fmt.Sprintf("%.1fM", float64(*votes)/1000000)
		case *votes >= 1000:
			v = fmt.Sprintf("%.0fK", float64(*votes)/1000)
		default:
			v = strconv.Itoa(*votes)
		}
		s += " - " + v + " votes"
	}
	return s
}

func langDisplay(code string) string {
	if s, ok := langMap[code]; ok {
		return s
	}
	return strings.ToUpper(code)
}

func jsonResponse(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
