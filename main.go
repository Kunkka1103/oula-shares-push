package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	opsDSN    = flag.String("opsDsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	interval  = flag.Int("interval", 5, "Check interval in minutes")
	outputDir = flag.String("output-dir", "/opt/node-exporter/prom", "Directory to write Prometheus metric files")
)

func main() {
	// 解析命令行标志
	flag.Parse()

	// 校验 DSN
	if *opsDSN == "" {
		log.Panicln("mysqlDSN is required.")
	}

	// 初始化数据库连接
	db, err := initDB(*opsDSN)
	if err != nil {
		log.Panicln("无法连接到数据库:", err)
	}
	// main 函数退出前关闭数据库连接
	defer db.Close()

	// 定期检查并推送数据
	for {
		// 从数据库获取各个链的最新分享计数
		shareCounts, err := getShareCounts(db)
		if err != nil {
			log.Println("获取 share counts 时发生错误:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 推送每个链的最新分享计数
		for chain, epochCount := range shareCounts {
			// 构建文件路径
			filePath := fmt.Sprintf("%s/%s_shares_count.prom", *outputDir, chain)
			log.Printf("正在写入指标数据到 %s", filePath)

			// 使用封装好的函数写文件
			if err := writeToPromFile(filePath, chain, epochCount); err != nil {
				log.Printf("写入文件 %s 时出错: %v", filePath, err)
			} else {
				log.Printf("成功写入到 %s", filePath)
			}
		}

		// 等待下次轮询
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

// 封装好的函数，用于写入 Prometheus 格式的数据到文件
func writeToPromFile(filePath, chain string, epochCount int64) error {
	file, err := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("无法打开文件 %s: %v", filePath, err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(
		file,
		"%s_shares_count{instance=\"jumperserver\",job=\"%s\"} %d\n",
		chain, chain, epochCount,
	)
	if err != nil {
		return fmt.Errorf("写入文件 %s 时发生错误: %v", filePath, err)
	}

	return nil
}
