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

	// 定期检查并推送数据
	for {
		// 获取每个链的最新高度和对应的 share_count
		shareCounts, err := getShareCounts(db)
		if err != nil {
			log.Println("Error getting share counts:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 推送每个链的最新分享计数
		for chain, epochCount := range shareCounts {
			// 推送指标，直接使用链名作为 job 标签
			err = pushShareCount(*pushAddr, chain, epochCount)
			if err != nil {
				log.Printf("Error pushing share count for %s: %v", chain, err)
			}
		}

		// 等待下次检查
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}

// 初始化 MySQL 连接
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

// 获取每个链的最新分享计数
func getShareCounts(db *sql.DB) (map[string]int64, error) {
	rows, err := db.Query("SELECT chain, MAX(epoch) AS latest_epoch FROM shares_epoch_counts GROUP BY chain")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	shareCounts := make(map[string]int64)

	for rows.Next() {
		var chain string
		var latestEpoch int64
		if err := rows.Scan(&chain, &latestEpoch); err != nil {
			return nil, err
		}
		// 查询该链的最新高度的 share_count
		count, err := getShareCountAtEpoch(db, chain, latestEpoch)
		if err != nil {
			log.Printf("Error getting share count for chain %s at epoch %d: %v", chain, latestEpoch, err)
			continue
		}
		shareCounts[chain] = count
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return shareCounts, nil
}

// 获取指定链在指定 epoch 高度的 share_count
func getShareCountAtEpoch(db *sql.DB, chain string, epoch int64) (int64, error) {
	var shareCount int64
	err := db.QueryRow("SELECT share_count FROM shares_epoch_counts WHERE chain = ? AND epoch = ?", chain, epoch).Scan(&shareCount)
	if err != nil {
		return 0, err
	}
	return shareCount, nil
}

// 推送当前链的最新分享计数到 Prometheus Pushgateway
func pushShareCount(pushAddr, chain string, shareCount int64) error {
	// 创建指标
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_shares_count", chain), // 使用链名作为指标名
		Help: fmt.Sprintf("Share count for chain %s", chain),
	})

	// 设置指标值
	gauge.Set(float64(shareCount))

	// 推送指标
	err := push.New(pushAddr, chain).
		Grouping("instance", "localhost").
		Collector(gauge).Push()
	if err != nil {
		return err
	}

	log.Printf("Pushed %s_shares_count{job=\"%s\"} = %d", chain, chain, shareCount)
	return nil
}
