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
			// 推送每个链的 epoch_count
			for epoch, shareCount := range epochs {
				err := promth.Push(*pushAddr, fmt.Sprintf("%s_shares_epoch_count", chain), chain, float64(shareCount))
				if err != nil {
					log.Println("Error pushing share count:", err)
					continue
				}
				log.Printf("Pushed share count for chain: %s, epoch: %d, shareCount: %d\n", chain, epoch, shareCount)
			}
		}

		// 睡眠一段时间后继续检查
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}
