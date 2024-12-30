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
		// 获取每个链的当前最大高度
		heights, err := getMaxHeights(db)
		if err != nil {
			log.Println("Error getting max heights:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 对每个链进行处理
		for chain, maxHeight := range heights {
			// 检查是否需要推送
			if lastHeight, ok := lastHeights[chain]; !ok || maxHeight > lastHeight {
				// 如果链的高度更新，推送新的 share_count 数据
				if lastHeight > 0 {
					// 推送从上次高度+1 到当前高度的所有 share_count
					err = pushShareCounts(db, chain, lastHeight+1, maxHeight)
					if err != nil {
						log.Printf("Error pushing share counts for %s: %v\n", chain, err)
					}
				}
				// 更新最后的高度
				lastHeights[chain] = maxHeight
			}
		}

		// 等待指定的时间间隔
		log.Printf("Waiting for %d minutes before checking again...\n", *interval)
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}

// 初始化数据库连接
func initDB(DSN string) (*sql.DB, error) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	log.Println("Database connection established successfully.")
	return db, nil
}

// 获取每个链的最大高度
func getMaxHeights(db *sql.DB) (map[string]int64, error) {
	rows, err := db.Query("SELECT chain, MAX(epoch) FROM shares_epoch_counts GROUP BY chain")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	heights := make(map[string]int64)
	for rows.Next() {
		var chain string
		var maxHeight int64
		if err := rows.Scan(&chain, &maxHeight); err != nil {
			return nil, err
		}
		heights[chain] = maxHeight
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return heights, nil
}

// 获取并推送链的分享计数数据
func pushShareCounts(db *sql.DB, chain string, startEpoch, endEpoch int64) error {
	// 查询从 startEpoch 到 endEpoch 之间的所有 share_count 数据
	rows, err := db.Query("SELECT epoch, share_count FROM shares_epoch_counts WHERE chain = ? AND epoch BETWEEN ? AND ?", chain, startEpoch, endEpoch)
	if err != nil {
		return err
	}
	defer rows.Close()

	// 为该链创建一个指标
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: fmt.Sprintf("%s_shares_count", chain),
			Help: "Number of shares for each epoch",
		},
		[]string{"epoch"},
	)

	// 遍历每行数据，设置相应的 share_count
	for rows.Next() {
		var epoch int64
		var shareCount int64
		if err := rows.Scan(&epoch, &shareCount); err != nil {
			return err
		}
		// 设置指标值
		gauge.WithLabelValues(fmt.Sprintf("%d", epoch)).Set(float64(shareCount))
	}

	// 推送数据到 Pushgateway
	err = push.New(*pushAddr, chain).
		Collector(gauge).
		Push()
	if err != nil {
		return err
	}

	log.Printf("Successfully pushed share counts for chain: %s from epoch %d to %d\n", chain, startEpoch, endEpoch)
	return nil
}
