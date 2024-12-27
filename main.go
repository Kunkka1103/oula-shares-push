package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	// 命令行参数
	mysqlDSN     = flag.String("mysqlDSN", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	pushGateway  = flag.String("pushGateway", "http://localhost:9091", "Prometheus Pushgateway URL")
	intervalMins = flag.Int("interval", 5, "Check interval in minutes")
)

// Metrics定义
var (
	shareCountMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shares_epoch_count",
			Help: "Number of shares per epoch",
		},
		[]string{"chain"},
	)
)

func init() {
	prometheus.MustRegister(shareCountMetric)
}

func main() {
	flag.Parse()
	if *mysqlDSN == "" || *pushGateway == "" {
		log.Panicln("Both mysqlDSN and pushGateway parameters are required.")
	}

	// 初始化MySQL连接
	mysqlDB, err := sql.Open("mysql", *mysqlDSN)
	if err != nil {
		log.Fatalf("Failed to open MySQL connection: %v", err)
	}
	defer mysqlDB.Close()

	// 测试连接
	if err := mysqlDB.Ping(); err != nil {
		log.Fatalf("Failed to ping MySQL: %v", err)
	}

	// 程序启动时，从 shares_epoch_counts 中获取每个项目的最新进度
	lastPushed, err := loadLastPushedEpochs(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to load last pushed epochs: %v", err)
	}

	log.Printf("Startup - lastPushed epochs: %+v\n", lastPushed)

	// 定时器
	ticker := time.NewTicker(time.Duration(*intervalMins) * time.Minute)
	defer ticker.Stop()

	log.Printf("Starting push loop with interval = %d minute(s)\n", *intervalMins)
	for {
		select {
		case <-ticker.C:
			err := pushUpdatedShareCounts(mysqlDB, *pushGateway, lastPushed)
			if err != nil {
				log.Printf("Error during pushUpdatedShareCounts: %v\n", err)
			}
		}
	}
}

// loadLastPushedEpochs 从 shares_epoch_counts 表中加载每个链的最大 epoch
func loadLastPushedEpochs(db *sql.DB) (map[string]int64, error) {
	query := `SELECT chain, MAX(epoch) FROM shares_epoch_counts GROUP BY chain`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query shares_epoch_counts: %w", err)
	}
	defer rows.Close()

	lastPushed := make(map[string]int64)
	for rows.Next() {
		var chain string
		var epoch sql.NullInt64
		if err := rows.Scan(&chain, &epoch); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		if epoch.Valid {
			lastPushed[chain] = epoch.Int64
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
	// 1. 获取每个链的最新 epoch
	query := `SELECT chain, MAX(epoch) FROM shares_epoch_counts GROUP BY chain`
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query latest epochs: %w", err)
	}
	defer rows.Close()

	currentMax := make(map[string]int64)
	for rows.Next() {
		var chain string
		var epoch sql.NullInt64
		if err := rows.Scan(&chain, &epoch); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		if epoch.Valid {
			currentMax[chain] = epoch.Int64
		} else {
			currentMax[chain] = 0
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	// 2. 对每个链，比较并推送新增的 epochs
	for chain, newMaxEpoch := range currentMax {
		lastEpoch, exists := lastPushed[chain]
		if !exists {
			lastEpoch = 0
		}

		if newMaxEpoch <= lastEpoch {
			log.Printf("[%s] No new epoch. lastPushed=%d, currentMax=%d\n", chain, lastEpoch, newMaxEpoch)
			continue
		}

		// 3. 获取新增的 epochs
		// 为了简化，不使用 generate_series，直接从 MySQL 表中查询已有的 epochs
		queryNew := `SELECT epoch, share_count FROM shares_epoch_counts WHERE chain = ? AND epoch > ? ORDER BY epoch ASC`
		rowsNew, err := db.Query(queryNew, chain, lastEpoch)
		if err != nil {
			log.Printf("[%s] Failed to query new epochs: %v\n", chain, err)
			continue
		}

		var epochsToPush []EpochData
		for rowsNew.Next() {
			var epoch int64
			var shareCount int64
			if err := rowsNew.Scan(&epoch, &shareCount); err != nil {
				log.Printf("[%s] Failed to scan epoch data: %v\n", chain, err)
				continue
			}
			epochsToPush = append(epochsToPush, EpochData{
				Epoch:      epoch,
				ShareCount: shareCount,
			})
		}
		if err := rowsNew.Err(); err != nil {
			log.Printf("[%s] Rows iteration error: %v\n", chain, err)
			rowsNew.Close()
			continue
		}
		rowsNew.Close()

		// 4. 推送每个新增的 epoch
		for _, epochData := range epochsToPush {
			// 设置指标
			shareCountMetric.WithLabelValues(chain).Set(float64(epochData.ShareCount))

			// 定义 job 和 instance 标签
			job := fmt.Sprintf("shares_monitor_%s", chain)
			instance := fmt.Sprintf("epoch_%d", epochData.Epoch)

			// 推送到 Pushgateway
			err := push.New(pushGW, job).
				Grouping("instance", instance).
				Collector(shareCountMetric).
				Push()
			if err != nil {
				log.Printf("[%s] Failed to push epoch=%d: %v\n", chain, epochData.Epoch, err)
				continue
			}

			log.Printf("[%s] Pushed epoch=%d with share_count=%d\n", chain, epochData.Epoch, epochData.ShareCount)

			// 更新 lastPushed
			lastPushed[chain] = epochData.Epoch
		}
	}

	return nil
}

// EpochData 结构体表示一个 epoch 的数据
type EpochData struct {
	Epoch      int64
	ShareCount int64
}
