// Copyright 2024 Martin Zimandl <martin.zimandl@gmail.com>
// Copyright 2024 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2024 Institute of the Czech National Corpus,
//                Faculty of Arts, Charles University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kcache

import (
	"camus/archiver"
	"encoding/json"
	"fmt"
)

// CacheEntry is an exact rewrite of KonText's
// class ConcCacheStatus stored in lib/plugin_types/conc_cache.py
type CacheEntry struct {
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

	// Created is the creation UNIX time with (system dependent) sub-second precision.
	Created float64 `json:"created"`

	// LastUpd is the latest update UNIX time with (system dependent) sub-second precision.
	LastUpd float64 `json:"last_upd"`
	Error   error   `json:"error,omitempty"`
}

// IsZero tests whether the record can be used for our purposes,
// which is to measure time needed to calculate concordances.
func (rec CacheEntry) IsProcessable() bool {
	return rec.Created > 0 && rec.LastUpd > 0
}

func (rec CacheEntry) ProcTime() float64 {
	if rec.IsProcessable() {
		return rec.LastUpd - rec.Created
	}
	return -1
}

// -----------------------

type CacheReader struct {
	redis *archiver.RedisAdapter
}

// GetConcCacheRecordByConcID looks for a concordance cache record
// (do not confuse with conc. archive/persistence record) given a concordance ID.
// (conc. persistence and conc cache use different IDs).
func (ch *CacheReader) GetConcCacheRecordByConcID(concID string) (CacheEntry, error) {
	archData, err := ch.redis.GetConcCacheRawRecord(concID)
	if err != nil {
		return CacheEntry{}, fmt.Errorf(
			"failed to load conc cache record for conc %s (cache id %s): %w", concID, archData.ID, err)
	}
	cacheData := CacheEntry{}
	err = json.Unmarshal([]byte(archData.Data), &cacheData)
	if err != nil {
		return CacheEntry{}, fmt.Errorf(
			"failed to load conc cache record for conc %s (cache id %s): %w", concID, archData.ID, err)
	}
	return cacheData, nil
}

func NewCacheReader(redis *archiver.RedisAdapter) *CacheReader {
	return &CacheReader{
		redis: redis,
	}
}
