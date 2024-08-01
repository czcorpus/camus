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

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

type ArchKeeper struct {
	redis              *RedisAdapter
	db                 *sql.DB
	checkInterval      time.Duration
	checkIntervalChunk int
	dedup              *Deduplicator
	tz                 *time.Location
	stats              BgJobStats
}

func (job *ArchKeeper) Start(ctx context.Context) {
	ticker := time.NewTicker(job.checkInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close ArchKeeper")
				return
			case <-ticker.C:
				job.performCheck()
			}
		}
	}()
}

func (job *ArchKeeper) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping ArchKeeper")
	if err := job.dedup.OnClose(); err != nil {
		return fmt.Errorf("failed to stop ArchKeeper properly: %w", err)
	}
	return nil
}

func (job *ArchKeeper) StoreToDisk() {

}

func (job *ArchKeeper) GetStats() BgJobStats {
	return job.stats
}

func (job *ArchKeeper) LoadRecordsByID(concID string) ([]ArchRecord, error) {
	return LoadRecordsByID(job.db, concID)
}

func (job *ArchKeeper) handleImplicitReq(rec ArchRecord, item queueRecord, currStats *BgJobStats) bool {

	match, err := job.dedup.TestAndSolve(rec)
	if err != nil {
		log.Error().
			Err(err).
			Str("recordId", item.Key).
			Msg("failed to insert record, skipping")
		if err := job.redis.AddError(item, &rec); err != nil {
			log.Error().Err(err).Msg("failed to insert error key")
		}
		currStats.NumErrors++
		return false
	}
	if match {
		log.Warn().
			Str("recordId", item.Key).
			Msg("record already archived, data merged")
		currStats.NumMerged++
		return true
	}
	if err := InsertRecord(job.db, rec); err != nil {
		log.Error().
			Err(err).
			Str("recordId", item.Key).
			Msg("failed to insert record, skipping")
		if err := job.redis.AddError(item, &rec); err != nil {
			log.Error().Err(err).Msg("failed to insert error key")
		}
	}
	job.dedup.Add(rec.ID)
	currStats.NumInserted++
	return false
}

func (job *ArchKeeper) handleExplicitReq(rec ArchRecord, item queueRecord, currStats *BgJobStats) {
	exists, err := ContainsRecord(job.db, rec.ID)
	if err != nil {
		currStats.NumErrors++
		log.Error().
			Err(err).
			Str("recordId", item.Key).
			Msg("failed to test record existence, skipping")
	}
	if !exists {
		err := InsertRecord(job.db, rec)
		if err != nil {
			currStats.NumErrors++
			log.Error().
				Err(err).
				Str("recordId", item.Key).
				Msg("failed to insert record, skipping")

		} else {
			currStats.NumInserted++
		}
		job.dedup.Add(rec.ID)
	}
}

func (job *ArchKeeper) performCheck() error {
	items, err := job.redis.NextNItems(int64(job.checkIntervalChunk))
	log.Debug().
		AnErr("error", err).
		Int("itemsToProcess", len(items)).
		Msg("doing regular check")
	if err != nil {
		return fmt.Errorf("failed to fetch next queued chunk: %w", err)
	}
	var currStats BgJobStats
	var numFetched int
	for _, item := range items {
		currStats.NumFetched++
		rec, err := job.redis.GetConcRecord(item.KeyCode())
		if err != nil {
			log.Error().
				Err(err).
				Str("recordId", item.Key).
				Msg("failed to get record from Redis, skipping")
			if err := job.redis.AddError(item, nil); err != nil {
				log.Error().Err(err).Msg("failed to insert error key")
			}
			currStats.NumErrors++
			continue
		}
		rec.Created = time.Now().In(job.tz)
		if item.Explicit {
			job.handleExplicitReq(rec, item, &currStats)

		} else {
			job.handleImplicitReq(rec, item, &currStats)
		}
	}
	log.Info().
		Int("numInserted", currStats.NumInserted).
		Int("numMerged", currStats.NumMerged).
		Int("numErrors", currStats.NumErrors).
		Int("numFetched", numFetched).
		Msg("regular archiving report")
	job.stats.UpdateBy(currStats)
	return nil
}

func (job *ArchKeeper) DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error) {
	return DeduplicateInArchive(job.db, curr, rec, job.tz)
}

func NewArchKeeper(
	redis *RedisAdapter,
	db *sql.DB,
	dedup *Deduplicator,
	tz *time.Location,
	checkInterval time.Duration,
	checkIntervalChunk int,
) *ArchKeeper {
	return &ArchKeeper{
		redis:              redis,
		db:                 db,
		dedup:              dedup,
		tz:                 tz,
		checkInterval:      checkInterval,
		checkIntervalChunk: checkIntervalChunk,
	}
}
