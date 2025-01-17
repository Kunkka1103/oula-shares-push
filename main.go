package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	opsDSN     = flag.String("opsDsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	outputPath = flag.String("output-path", "/opt/node-exporter/prom/", "Directory to write .prom files")
	interval   = flag.Int("interval", 5, "Check interval in minutes")
)

func main() {
	flag.Parse()
	if *opsDSN == "" || *outputPath == "" {
		log.Panicln("Both MySQL DSN and output-path parameters are required.")
	}

	// Check if output path exists
	if err := ensureOutputPath(*outputPath); err != nil {
		log.Panicln("Failed to check output path:", err)
	}

	// Initialize MySQL connection
	db, err := initDB(*opsDSN)
	if err != nil {
		log.Panicln("Failed to connect to MySQL:", err)
	}
	defer db.Close()

	// Periodically check and write metrics
	for {
		// Get the latest share counts from the database
		shareCounts, err := getShareCounts(db)
		if err != nil {
			log.Println("Error fetching share counts:", err)
			log.Println("Retrying in", *interval, "minutes...")
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// Write each chain's share count to a .prom file
		for chain, epochCount := range shareCounts {
			err = writeMetricToFile(*outputPath, chain, epochCount)
			if err != nil {
				log.Printf("Error writing metric for %s: %v\n", chain, err)
			} else {
				log.Printf("Successfully wrote metric for %s: %d\n", chain, epochCount)
			}
		}

		// Wait before the next check
		log.Println("Waiting for the next check...")
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}

// initDB initializes the MySQL connection
func initDB(dsn string) (*sql.DB, error) {
	log.Println("Connecting to MySQL...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// Check if connection is successful
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	log.Println("Successfully connected to MySQL.")
	return db, nil
}

// ensureOutputPath ensures that the output path exists
func ensureOutputPath(path string) error {
	// Check if the directory exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Printf("Output path does not exist: %s\n", path)
		// Try to create the directory
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		log.Printf("Output path created: %s\n", path)
	} else {
		log.Printf("Output path already exists: %s\n", path)
	}
	return nil
}

// getShareCounts fetches the share counts for each chain from the database
func getShareCounts(db *sql.DB) (map[string]int, error) {
	log.Println("Fetching share counts from the database...")
	shareCounts := make(map[string]int)

	rows, err := db.Query("SELECT chain, epoch_count FROM shares_epoch_counts")
	if err != nil {
		return nil, fmt.Errorf("failed to query share counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chain string
		var epochCount int
		if err := rows.Scan(&chain, &epochCount); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		shareCounts[chain] = epochCount
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	log.Println("Share counts fetched successfully.")
	return shareCounts, nil
}

// writeMetricToFile writes the metrics to a .prom file
func writeMetricToFile(path, chain string, epochCount int) error {
	// Prepare the metric content
	metricContent := fmt.Sprintf("%s{chain=\"%s\"} %d\n", chain, chain, epochCount)

	// Define the file path
	filePath := filepath.Join(path, fmt.Sprintf("%s.prom", chain))

	// Open the file for writing
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create file %s: %w", filePath, err)
	}
	defer file.Close()

	// Write the metric to the file
	if _, err := file.WriteString(metricContent); err != nil {
		return fmt.Errorf("failed to write to file %s: %w", filePath, err)
	}

	log.Printf("Successfully wrote metric for %s to %s\n", chain, filePath)
	return nil
}
