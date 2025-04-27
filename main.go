package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
	"github.com/rs/cors"
)

type TimeSeriesResponse struct {
	MetaData struct {
		Information string `json:"1. Information"`
		Symbol      string `json:"2. Symbol"`
	} `json:"Meta Data"`
	TimeSeriesDaily map[string]struct {
		Open   string `json:"1. open"`
		High   string `json:"2. high"`
		Low    string `json:"3. low"`
		Close  string `json:"4. close"`
		Volume string `json:"5. volume"`
	} `json:"Time Series (Daily)"`
}

func main() {

	c := cors.New(cors.Options{
		AllowedOrigins: []string{"http://localhost:5173"}, // Allow only your frontend URL
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE"}, // Allowed HTTP methods
		AllowedHeaders: []string{"Content-Type", "Authorization"}, // Allowed headers
	})

	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: No .env file found (using system env vars)")
	}

	dsn := os.Getenv("COCKROACHDB_DSN")
	if dsn == "" {
		log.Fatal("Missing required environment variable COCKROACHDB_DSN")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	initDB(db)

	r := mux.NewRouter()
	r.HandleFunc("/api/stocks/{symbol}", getStockDataHandler(db)).Methods("GET")
	r.HandleFunc("/api/stocks/{symbol}/refresh", refreshStockDataHandler(db)).Methods("POST")

	handler := c.Handler(r)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}


func initDB(db *sql.DB) {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS stock_data (
		date DATE NOT NULL,
		symbol STRING NOT NULL,
		open FLOAT,
		high FLOAT,
		low FLOAT,
		close FLOAT,
		volume INT,
		last_updated TIMESTAMP DEFAULT now(),
		PRIMARY KEY (date, symbol)
	)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
}

func getStockDataHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Log incoming request
		log.Println("🔍 Incoming GET request:", r.URL.Path)

		vars := mux.Vars(r)
		symbol := vars["symbol"]
		timeRange := r.URL.Query().Get("range")

		// Log symbol and range
		log.Printf("📊 Symbol: %s, Time Range: %s", symbol, timeRange)

		if symbol == "" {
			http.Error(w, "Symbol is required", http.StatusBadRequest)
			log.Println("❌ Error: Symbol is required")
			return
		}

		if timeRange == "" {
			http.Error(w, "Range parameter is required", http.StatusBadRequest)
			log.Println("❌ Error: Range parameter is required")
			return
		}

		var query string
		var args []interface{}

		switch timeRange {
		case "week":
			query = `SELECT date, open, high, low, close, volume 
					FROM stock_data 
					WHERE symbol = $1 
					ORDER BY date DESC 
					LIMIT 7`
			args = []interface{}{symbol}
		case "month":
			query = `SELECT date, open, high, low, close, volume 
					FROM stock_data 
					WHERE symbol = $1 
					ORDER BY date DESC 
					LIMIT 30`
			args = []interface{}{symbol}
		case "6month":
			query = `SELECT date, open, high, low, close, volume 
					FROM stock_data 
					WHERE symbol = $1 AND date >= CURRENT_DATE - INTERVAL '6 months' 
					ORDER BY date DESC`
			args = []interface{}{symbol}
		case "year":
			query = `SELECT date, open, high, low, close, volume 
					FROM stock_data 
					WHERE symbol = $1 AND date >= CURRENT_DATE - INTERVAL '1 year' 
					ORDER BY date DESC`
			args = []interface{}{symbol}
		default:
			http.Error(w, "Invalid time range specified", http.StatusBadRequest)
			log.Println("❌ Error: Invalid time range:", timeRange)
			return
		}

		rows, err := db.Query(query, args...)
		if err != nil {
			http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
			log.Println("❌ Error: Database query failed", err)
			return
		}
		defer rows.Close()

		type StockData struct {
			Date   string  `json:"date"`
			Open   float64 `json:"open"`
			High   float64 `json:"high"`
			Low    float64 `json:"low"`
			Close  float64 `json:"close"`
			Volume int     `json:"volume"`
		}

		var data []StockData
		for rows.Next() {
			var sd StockData
			var date time.Time
			err := rows.Scan(&date, &sd.Open, &sd.High, &sd.Low, &sd.Close, &sd.Volume)
			if err != nil {
				http.Error(w, "Error scanning row: "+err.Error(), http.StatusInternalServerError)
				log.Println("❌ Error: Scanning row", err)
				return
			}
			sd.Date = date.Format("2006-01-02")
			data = append(data, sd)
		}

		// Log response data
		log.Printf("✅ Successfully fetched %d stock data entries for symbol %s", len(data), symbol)

		// Send the response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}


func refreshStockDataHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := os.Getenv("ALPHA_VANTAGE_API_KEY")
		if apiKey == "" {
			http.Error(w, "Missing API key configuration", http.StatusInternalServerError)
			return
		}

		vars := mux.Vars(r)
		symbol := vars["symbol"]
		baseURL := "https://www.alphavantage.co/query"

		client := resty.New()
		data, err := fetchStockData(client, baseURL, apiKey, symbol, "compact")
		if err != nil {
			http.Error(w, "Failed to fetch stock data: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Get the latest date we already have in the database
		var latestDate sql.NullString
		err = db.QueryRow("SELECT MAX(date) FROM stock_data WHERE symbol = $1", symbol).Scan(&latestDate)
		if err != nil {
			log.Printf("Warning: Couldn't determine latest date in database: %v", err)
		}

		// Prepare the insert statement
		stmt, err := db.Prepare(`
			INSERT INTO stock_data (date, symbol, open, high, low, close, volume)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (date, symbol) DO NOTHING
		`)
		if err != nil {
			http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer stmt.Close()

		newRecords := 0
		for dateStr, stockData := range data.TimeSeriesDaily {
			// Skip if we already have data for this date
			if latestDate.Valid && dateStr <= latestDate.String {
				continue
			}

			_, err = stmt.Exec(
				dateStr, symbol, stockData.Open, stockData.High, 
				stockData.Low, stockData.Close, stockData.Volume,
			)

			if err != nil {
				log.Printf("Failed to insert data for %s: %v", dateStr, err)
			} else {
				newRecords++
			}
		}

		response := map[string]interface{}{
			"message":    "Stock data refreshed",
			"newRecords": newRecords,
			"symbol":     symbol,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}


func fetchStockData(client *resty.Client, baseURL, apiKey, symbol, size string) (*TimeSeriesResponse, error) {
	// Implement retry logic for API calls
	var result *TimeSeriesResponse
	var err error
	
	for attempt := 1; attempt <= 3; attempt++ {
		resp, e := client.R().
			SetQueryParams(map[string]string{
				"function":    "TIME_SERIES_DAILY",
				"symbol":      symbol,
				"apikey":      apiKey,
				"outputsize":  size,
			}).
			SetResult(&TimeSeriesResponse{}).
			Get(baseURL)

		if e != nil {
			err = fmt.Errorf("attempt %d: failed to get stock data: %v", attempt, e)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		if resp.IsError() {
			err = fmt.Errorf("attempt %d: API error: %v", attempt, resp.Status())
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		result = resp.Result().(*TimeSeriesResponse)
		err = nil
		break
	}

	return result, err
}