package cache

import (
	"camus/archiver"
	"encoding/json"
)

type CacheHandler struct {
	redis *archiver.RedisAdapter
}

type CacheDataRecord struct {
	TaskID      string  `json:"task_id"`
	ConcSize    int     `json:"concsize"`
	FullSize    int     `json:"fullsize"`
	RelConcSize float64 `json:"relconcsize"`
	Arf         float64 `json:"arf"`
	Finished    bool    `json:"finished"`
	Q0Hash      string  `json:"q0hash"`
	CacheFile   string  `json:"cachefile"`
	Readable    bool    `json:"readable"`
	PID         int     `json:"pid"`
	Created     int64   `json:"created"`
	LastUpd     int64   `json:"last_upd"`
	Error       error   `json:"error,omitempty"`
}

func (ch *CacheHandler) LoadConcCacheRecordByID(concID string) (CacheDataRecord, error) {
	archData, err := ch.redis.GetConcCacheRecord(concID)
	if err != nil {
		return CacheDataRecord{}, err
	}
	cacheData := CacheDataRecord{}
	err = json.Unmarshal([]byte(archData.Data), &cacheData)
	if err != nil {
		return CacheDataRecord{}, err
	}
	return cacheData, nil
}

func NewCacheHandler(redis *archiver.RedisAdapter) *CacheHandler {
	return &CacheHandler{
		redis: redis,
	}
}
