package cache

import (
	"camus/archiver"
	"encoding/json"
	"fmt"
)

type CacheHandler struct {
	redis *archiver.RedisAdapter
}

// CacheDataRecord is an exact rewrite of KonText's
// class ConcCacheStatus stored in lib/plugin_types/conc_cache.py
type CacheDataRecord struct {
	TaskID      string  `json:"task_id"`
	ConcSize    int     `json:"concsize"`
	FullSize    int     `json:"fullsize"`
	RelConcSize float64 `json:"relconcsize"`
	Arf         float64 `json:"arf"`
	Finished    bool    `json:"finished"`

	// Q0Hash refers to the initial user query which is at the beginning
	// of a possible query operation chain.
	Q0Hash    string `json:"q0hash"`
	CacheFile string `json:"cachefile"`
	Readable  bool   `json:"readable"`
	PID       int    `json:"pid"`

	// Created is the creation UNIX time with seconds precision.
	Created int64 `json:"created"`

	// LastUpd is the latest update UNIX time with seconds precision.
	LastUpd int64 `json:"last_upd"`
	Error   error `json:"error,omitempty"`
}

// IsZero tests whether the record can be used for our purposes,
// which is to measure time needed to calculate concordances.
func (rec CacheDataRecord) IsProcessable() bool {
	return rec.Created > 0 && rec.LastUpd > 0
}

func (rec CacheDataRecord) ProcTime() float64 {
	if rec.IsProcessable() {
		return float64(rec.LastUpd) - float64(rec.Created)
	}
	return -1
}

func (ch *CacheHandler) LoadConcCacheRecordByID(concID string) (CacheDataRecord, error) {
	archData, err := ch.redis.GetConcCacheRawRecord(concID)
	if err != nil {
		return CacheDataRecord{}, fmt.Errorf(
			"failed to load conc cache record for conc %s (cache id %s): %w", concID, archData.ID, err)
	}
	cacheData := CacheDataRecord{}
	err = json.Unmarshal([]byte(archData.Data), &cacheData)
	if err != nil {
		return CacheDataRecord{}, fmt.Errorf(
			"failed to load conc cache record for conc %s (cache id %s): %w", concID, archData.ID, err)
	}
	return cacheData, nil
}

func NewCacheHandler(redis *archiver.RedisAdapter) *CacheHandler {
	return &CacheHandler{
		redis: redis,
	}
}
