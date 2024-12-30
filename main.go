package main

import (
	"flag"
	"fmt"
	"log"
	"oula-shares-push/dal"
	"oula-shares-push/promth"
	"time"
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
	db, err := dal.InitDB(*opsDSN)
	if err != nil {
		log.Panicln("Failed to open ops connection:", err)
	}
	defer db.Close()

	// 获取每个链的最大不为零的高度
	maxHeights, err := dal.GetMaxShareHeight(db)
	if err != nil {
		log.Panicln("Failed to get max share heights:", err)
	}

	// 定期检查并推送数据
	for {
		// 获取链的分享计数
		shareCounts, err := dal.GetShareCounts(db)
		if err != nil {
			log.Println("Error getting share counts:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 推送每个链的分享计数
		for chain, epochs := range shareCounts {
			// 获取该链最大不为零的高度
			maxHeight, exists := maxHeights[chain]
			if !exists {
				log.Printf("No max height found for chain %s\n", chain)
				continue
			}

			// 推送 share_epoch_count
			for epoch, shareCount := range epochs {
				// 推送分享计数
				err = promth.Push(*pushAddr, fmt.Sprintf("%s_shares_epoch_count", chain), chain, float64(shareCount))
				if err != nil {
					log.Printf("Error pushing share count for chain %s, epoch %d: %v\n", chain, epoch, err)
				}
			}

			// 推送 shares_latest_nonzero
			if maxHeight > 0 {
				err = promth.Push(*pushAddr, fmt.Sprintf("%s_shares_latest_nonzero", chain), chain, float64(maxHeight))
				if err != nil {
					log.Printf("Error pushing latest non-zero share count for chain %s: %v\n", chain, err)
				}
			}
		}

		// 等待指定时间间隔后再次执行
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}
