package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	_ "github.com/go-sql-driver/mysql"
)

// 命令行参数
var (
	mysqlDSN     = flag.String("mysqlDSN", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	pushGateway  = flag.String("pushGateway", "http://localhost:9091", "Prometheus Pushgateway URL")
	intervalMins = flag.Int("interval", 5, "Check interval in minutes")
)

// Metrics定义
var (
	sharesEpochCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shares_epoch_count",
			Help: "Number of shares per epoch",
		},
		[]string{"chain"},
	)
	sharesLatestNonZero = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shares_latest_nonzero",
			Help: "Share count of the latest epoch if not zero",
		},
		[]string{"chain"},
	)
)

func init() {
	prometheus.MustRegister(sharesEpochCount)
	prometheus.MustRegister(sharesLatestNonZero)
}

func main() {
	flag.Parse()
	if *mysqlDSN == "" || *pushGateway == "" {
		log.Panicln("Both mysqlDSN and pushGateway parameters are required.")
	}

	// 初始化 MySQL 连接
	mysqlDB, err := sql.Open("mysql", *mysqlDSN)
	if err != nil {
		log.Fatalf("Failed to open MySQL connection: %v", err)
	}
	defer mysqlDB.Close()

	// 测试连接
	if err := mysqlDB.Ping(); err != nil {
		log.Fatalf("Failed to ping MySQL: %v", err)
	}

	// 启动时，从 shares_epoch_counts 表中获取每个链的最新高度
	lastPushed, err := loadLastPushedEpochs(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to load last pushed epochs: %v", err)
	}

	log.Printf("Startup - Initial last pushed epochs: %+v\n", lastPushed)

	// 定时器
	ticker := time.NewTicker(time.Duration(*intervalMins) * time.Minute)
	defer ticker.Stop()

	log.Printf("Starting push loop with interval = %d minute(s)\n", *intervalMins)

	// 使用互斥锁保护 lastPushed 映射
	var mu sync.Mutex

	for {
		select {
		case <-ticker.C:
			mu.Lock()
			err := pushUpdatedShareCounts(mysqlDB, *pushGateway, lastPushed)
			if err != nil {
				log.Printf("Error during pushUpdatedShareCounts: %v\n", err)
			}
			mu.Unlock()
		}
	}
}

// loadLastPushedEpochs 从 shares_epoch_counts 表中加载每个链的最大 epoch
func loadLastPushedEpochs(db *sql.DB) (map[string]int64, error) {
	query := `SELECT chain, MAX(epoch) as max_epoch FROM shares_epoch_counts GROUP BY chain`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query shares_epoch_counts: %w", err)
	}
	defer rows.Close()

	lastPushed := make(map[string]int64)
	for rows.Next() {
		var chain string
		var maxEpoch sql.NullInt64
		if err := rows.Scan(&chain, &maxEpoch); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if maxEpoch.Valid {
			lastPushed[chain] = maxEpoch.Int64
		} else {
			lastPushed[chain] = 0
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return lastPushed, nil
}

// pushUpdatedShareCounts 检查并推送新增的 share_count
func pushUpdatedShareCounts(db *sql.DB, pushGW string, lastPushed map[string]int64) error {
	// 获取每个链的最新 epoch
	query := `SELECT chain, MAX(epoch) as max_epoch FROM shares_epoch_counts GROUP BY chain`
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query latest epochs: %w", err)
	}
	defer rows.Close()

	type ChainEpoch struct {
		Chain    string
		MaxEpoch int64
	}

	var chains []ChainEpoch
	for rows.Next() {
		var chain string
		var maxEpoch sql.NullInt64
		if err := rows.Scan(&chain, &maxEpoch); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		if maxEpoch.Valid {
			chains = append(chains, ChainEpoch{Chain: chain, MaxEpoch: maxEpoch.Int64})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	// 遍历每个链
	for _, ce := range chains {
		lastEpoch, exists := lastPushed[ce.Chain]
		if !exists {
			lastEpoch = 0
		}
		currentMax := ce.MaxEpoch

		if currentMax <= lastEpoch {
			log.Printf("[%s] No new epoch. lastPushed=%d, currentMax=%d\n", ce.Chain, lastEpoch, currentMax)
			continue
		}

		// 定义要推送的范围: [lastEpoch+1, currentMax]
		start := lastEpoch + 1
		end := currentMax

		// 查询需要推送的 epochs 及其 share_count
		shareCounts, err := getShareCounts(db, ce.Chain, start, end)
		if err != nil {
			log.Printf("Failed to get share counts for chain=%s: %v\n", ce.Chain, err)
			continue
		}

		// 获取 share_count for latest epoch
		latestShareCount, exists := shareCounts[end]
		if !exists {
			latestShareCount = 0
		}

		// 如果最新 epoch 的 share_count !=0，推送
		if latestShareCount != 0 {
			// 设置 shares_latest_nonzero
			sharesLatestNonZero.WithLabelValues(ce.Chain).Set(float64(latestShareCount))

			// 定义唯一的 job 和 instance
			job := fmt.Sprintf("shares_monitor_%s_latest_nonzero", ce.Chain)
			instance := fmt.Sprintf("latest_epoch_%d", end)

			// 推送到 Pushgateway
			err = push.New(pushGW, job).
				Collector(sharesLatestNonZero).
				Grouping("instance", instance).
				Push()
			if err != nil {
				log.Printf("Failed to push shares_latest_nonzero for chain=%s, epoch=%d: %v", ce.Chain, end, err)
				continue
			}

			log.Printf("Pushed shares_latest_nonzero for chain=%s, epoch=%d: %d", ce.Chain, end, latestShareCount)
		}

		// 遍历范围内的每个 epoch 并推送 share_count
		for epoch := start; epoch <= end; epoch++ {
			shareCount, exists := shareCounts[epoch]
			if !exists {
				shareCount = 0
			}

			// 仅推送 share_count 不为0
			if shareCount == 0 {
				continue
			}

			// 设置 shares_epoch_count
			sharesEpochCount.WithLabelValues(ce.Chain).Set(float64(shareCount))

			// 定义唯一的 job 和 instance
			job := fmt.Sprintf("shares_monitor_%s_epoch_%d", ce.Chain, epoch)
			instance := fmt.Sprintf("epoch_%d", epoch)

			// 推送到 Pushgateway
			err = push.New(pushGW, job).
				Collector(sharesEpochCount).
				Grouping("instance", instance).
				Push()
			if err != nil {
				log.Printf("Failed to push shares_epoch_count for chain=%s, epoch=%d: %v", ce.Chain, epoch, err)
				continue
			}

			log.Printf("Pushed shares_epoch_count for chain=%s, epoch=%d: %d", ce.Chain, epoch, shareCount)

			// 更新 lastPushed
			lastPushed[ce.Chain] = epoch
		}
	}

	return nil
}

// getShareCounts 查询指定链在[start, end]范围内的所有 epoch 的 share_count
func getShareCounts(db *sql.DB, chain string, start, end int64) (map[int64]int64, error) {
	query := `
        SELECT epoch, share_count
        FROM shares_epoch_counts
        WHERE chain = ? AND epoch BETWEEN ? AND ?
    `
	rows, err := db.Query(query, chain, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query share counts: %w", err)
	}
	defer rows.Close()

	shareCounts := make(map[int64]int64)
	for rows.Next() {
		var epoch, count int64
		if err := rows.Scan(&epoch, &count); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		shareCounts[epoch] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return shareCounts, nil
}
