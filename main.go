package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	opsDSN   = flag.String("opsDsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	pushAddr = flag.String("push-url", "http://localhost:9091", "Prometheus Pushgateway URL")
	interval = flag.Int("interval", 5, "Check interval in minutes")
)

func main() {
	flag.Parse()
	if *opsDSN == "" || *pushAddr == "" {
		log.Panicln("Both mysqlDSN and pushGateway parameters are required.")
	}

	// 初始化 MySQL 连接
	db, err := initDB(*opsDSN)
	if err != nil {
		log.Panicln("Failed to open ops connection:", err)
	}
	defer db.Close()

	// 上次的最大高度字典，用于跟踪每个链的最新高度
	lastHeights := make(map[string]int64)

	// 定期检查并推送数据
	for {
		// 获取每个链的最大高度
		shareCounts, err := getShareCounts(db)
		if err != nil {
			log.Println("Error getting share counts:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 推送每个链的分享计数
		for chain, maxHeight := range shareCounts {
			// 如果链的高度有变化，推送新的数据
			if lastHeight, exists := lastHeights[chain]; exists && maxHeight > lastHeight {
				// 推送从 lastHeight + 1 到 maxHeight 之间的所有高度
				for epoch := lastHeight + 1; epoch <= maxHeight; epoch++ {
					shareCount, err := getShareCountForEpoch(db, chain, epoch)
					if err != nil {
						log.Println("Error fetching share count for chain", chain, "epoch", epoch, ":", err)
						continue
					}

					// 推送 share_count 数据
					err = pushShareCount(*pushAddr, chain, float64(shareCount))
					if err != nil {
						log.Println("Error pushing share count for chain", chain, "epoch", epoch, ":", err)
						continue
					}

					log.Printf("Pushed share count for chain %s, epoch %d: %d\n", chain, epoch, shareCount)

					// 每次推送之后等待 `interval` 时间
					time.Sleep(time.Minute * time.Duration(*interval))
				}
			}

			// 更新链的最新高度
			lastHeights[chain] = maxHeight
		}

		// 等待下一轮检查
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}

// initDB 初始化 MySQL 连接
func initDB(DSN string) (*sql.DB, error) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// getShareCounts 获取每个链的最大高度
func getShareCounts(db *sql.DB) (map[string]int64, error) {
	rows, err := db.Query("SELECT chain, MAX(epoch) FROM shares_epoch_counts GROUP BY chain")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	shareCounts := make(map[string]int64)
	for rows.Next() {
		var chain string
		var maxEpoch int64
		if err := rows.Scan(&chain, &maxEpoch); err != nil {
			return nil, err
		}
		shareCounts[chain] = maxEpoch
	}

	return shareCounts, nil
}

// getShareCountForEpoch 获取指定链和指定 epoch 的 share_count
func getShareCountForEpoch(db *sql.DB, chain string, epoch int64) (int64, error) {
	var shareCount int64
	err := db.QueryRow("SELECT share_count FROM shares_epoch_counts WHERE chain = ? AND epoch = ?", chain, epoch).Scan(&shareCount)
	if err != nil {
		return 0, err
	}
	return shareCount, nil
}

// pushShareCount 推送指标到 Pushgateway
func pushShareCount(pushAddr, chain string, shareCount float64) error {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_shares_count", chain),
		Help: fmt.Sprintf("Share count for chain %s", chain),
	})
	gauge.Set(shareCount)

	// 只使用 job 标签
	err := push.New(pushAddr, fmt.Sprintf("job=%s", chain)).
		Collector(gauge).
		Push()
	return err
}
