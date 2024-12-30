package dal

import (
	"database/sql"
	"log"
)

// InitDB 初始化 MySQL 连接
func InitDB(DSN string) (DB *sql.DB, err error) {
	DB, err = sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}

	log.Println("dsn check success")

	err = DB.Ping()
	if err != nil {
		return nil, err
	}

	log.Println("database connect success")

	return DB, nil
}

// GetShareCounts 从数据库中获取每个链的分享计数
func GetShareCounts(db *sql.DB) (map[string]map[int64]int64, error) {
	rows, err := db.Query("SELECT chain, epoch, share_count FROM shares_epoch_counts WHERE share_count > 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 按链和 epoch 存储分享计数
	shareCounts := make(map[string]map[int64]int64)

	for rows.Next() {
		var chain string
		var epoch int64
		var shareCount int64
		if err := rows.Scan(&chain, &epoch, &shareCount); err != nil {
			return nil, err
		}

		// 初始化链的map
		if _, exists := shareCounts[chain]; !exists {
			shareCounts[chain] = make(map[int64]int64)
		}
		shareCounts[chain][epoch] = shareCount
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return shareCounts, nil
}

// GetMaxShareHeight 获取每个链最大的不为零的高度
func GetMaxShareHeight(db *sql.DB) (map[string]int64, error) {
	rows, err := db.Query("SELECT chain, MAX(epoch) FROM shares_epoch_counts WHERE share_count > 0 GROUP BY chain")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	maxHeights := make(map[string]int64)

	for rows.Next() {
		var chain string
		var maxEpoch int64
		if err := rows.Scan(&chain, &maxEpoch); err != nil {
			return nil, err
		}

		maxHeights[chain] = maxEpoch
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return maxHeights, nil
}
