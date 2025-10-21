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
	"camus/cncdb"
	"camus/reporting"
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// ArchKeeper handles continuous operations related
// to the concordance archive (contrary to the name, it
// also contains word lists, paradigm. queries and keyword
// queries).
// The main responsibility of ArchKeeper is to read queued
// query IDs, read them ASAP from Redis and store them
// to kontext_conc_persistence SQL table.
// Due to the nature of the partitioning of the table, ArchKeeper
// must also involve some deduplication to prevent extensive
// growth of duplicate records. It is not expected that
// ArchKeeper will catch 100% of duplicates because there is
// also a cleanup job that removes old unused records and
// for each checked record, it also performs a deduplication. But
// the job affects only years old records so we still need
// to prevent (at least some) recent duplicates so that the database
// is reasonably large.
type ArchKeeper struct {
	redis       *RedisAdapter
	dbArch      cncdb.IConcArchOps
	reporting   reporting.IReporting
	conf        *Conf
	dedup       *Deduplicator
	tz          *time.Location
	stats       reporting.OpStats
	recsToIndex chan<- cncdb.HistoryRecord
	recsToStats chan<- cncdb.CorpBoundRawRecord

	// corpSizes is a cache for storing corpus sizes
	// (otherwise loaded from cnc database)
	corpSizes map[string]int64
}

// Start starts the ArchKeeper service
func (job *ArchKeeper) Start(ctx context.Context) {
	ticker := time.NewTicker(job.conf.CheckInterval())
	log.Info().Msg("starting archiver.ArchKeeper task")
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close ArchKeeper")
				return
			case <-ticker.C:
				if err := job.performCheck(); err != nil {
					log.Error().Err(err).Msg("Failed to archive query persistence items")
				}
			}
		}
	}()
}

// Stop stops the ArchKeeper service
func (job *ArchKeeper) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping ArchKeeper task")
	close(job.recsToIndex)
	close(job.recsToStats)
	if err := job.dedup.OnClose(); err != nil {
		return fmt.Errorf("failed to stop ArchKeeper properly: %w", err)
	}
	return nil
}

// StoreToDisk stores current operations data from RAM
// to a configured disk file.
func (job *ArchKeeper) StoreToDisk() error {
	return job.dedup.StoreToDisk()
}

// Reset clears current operations data stored in RAM
// and initializes itself according to the configuration.
func (job *ArchKeeper) Reset() error {
	return job.dedup.Reset()
}

// GetStats returns statistics related to ArchKeeper operations.
// We use it mainly for pushing stats to a TimescaleDB instance.
func (job *ArchKeeper) GetStats() reporting.OpStats {
	return job.stats
}

func (job *ArchKeeper) LoadRecordsByID(concID string) ([]cncdb.QueryArchRec, error) {
	return job.dbArch.LoadRecordsByID(concID)
}

func (job *ArchKeeper) getCorpusSize(corpusID string) (int64, error) {
	cs, ok := job.corpSizes[corpusID]
	if !ok {
		csAlt, err := job.dbArch.CorpusSize(corpusID)
		if err != nil {
			return -1, err
		}
		job.corpSizes[corpusID] = csAlt
		cs = csAlt
	}
	return cs, nil
}

// handleImplicitReq returns true if everything was ok, otherwise
// false. Possible problems are logged.
func (job *ArchKeeper) handleImplicitReq(
	rec cncdb.QueryArchRec, item queueRecord, currStats *reporting.OpStats) bool {

	match, err := job.dedup.TestAndSolve(rec)
	if err != nil {
		log.Error().
			Err(err).
			Str("recordId", item.Key).
			Msg("failed to insert record, skipping")
		if err := job.redis.AddError(job.conf.FailedQueueKey, item, &rec); err != nil {
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
	if err := job.dbArch.InsertRecord(rec); err != nil {
		log.Error().
			Err(err).
			Str("recordId", item.Key).
			Msg("failed to insert record, skipping")
		if err := job.redis.AddError(job.conf.FailedQueueKey, item, &rec); err != nil {
			log.Error().Err(err).Msg("failed to insert error key")
		}
	}
	job.dedup.Add(rec.ID)
	currStats.NumInserted++
	return false
}

func (job *ArchKeeper) handleExplicitReq(
	rec cncdb.QueryArchRec, item queueRecord, currStats *reporting.OpStats) {
	exists, err := job.dbArch.ContainsRecord(rec.ID)
	if err != nil {
		currStats.NumErrors++
		log.Error().
			Err(err).
			Str("recordId", item.Key).
			Msg("failed to test record existence, skipping")
	}
	if !exists {
		err := job.dbArch.InsertRecord(rec)
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
	items, err := job.redis.NextNArchItems(job.conf.QueueKey, int64(job.conf.CheckIntervalChunk))
	log.Debug().
		AnErr("error", err).
		Int("itemsToProcess", len(items)).
		Msg("doing regular check")
	if err != nil {
		return fmt.Errorf("failed to fetch next queued chunk: %w", err)
	}
	var currStats reporting.OpStats
	var numFetched int
	for _, item := range items {
		currStats.NumFetched++
		rec, err := job.redis.GetConcRecord(item.KeyCode())
		if err != nil {
			log.Error().
				Err(err).
				Str("recordId", item.Key).
				Msg("failed to get record from Redis, skipping")
			if err := job.redis.AddError(job.conf.FailedQueueKey, item, nil); err != nil {
				log.Error().Err(err).Msg("failed to insert error key")
			}
			currStats.NumErrors++
			continue
		}
		rec.Created = time.Now().In(job.tz)

		switch item.Type {
		case QRTypeArchive, "":
			if item.Explicit {
				job.handleExplicitReq(rec, item, &currStats)

			} else {
				job.handleImplicitReq(rec, item, &currStats)
			}

			fdata, err := rec.FetchData()
			var corp string

			if err != nil {
				log.Error().
					Str("recordId", item.Key).
					Err(err).
					Msg("failed to determine corpus, no query stats will be written")
				continue
			}
			if len(fdata.GetCorpora()) > 0 {
				corp = fdata.GetCorpora()[0]
			}
			corpSize, err := job.getCorpusSize(corp)
			if err != nil {
				log.Error().
					Str("recordId", item.Key).
					Err(err).
					Msg("failed to determine corpus size, no query stats will be written")
				continue
			}
			job.recsToStats <- cncdb.CorpBoundRawRecord{
				RawRecord:  rec,
				Corpname:   corp,
				CorpusSize: corpSize,
			}
		case QRTypeHistory:
			job.recsToIndex <- cncdb.HistoryRecord{
				QueryID: item.Key,
				UserID:  item.UserID,
				Created: item.Created,
				Name:    item.Name,
				Rec:     &rec,
			}
		}
	}
	if currStats.ShowsActivity() {
		log.Info().
			Int("numInserted", currStats.NumInserted).
			Int("numMerged", currStats.NumMerged).
			Int("numErrors", currStats.NumErrors).
			Int("numFetched", numFetched).
			Msg("regular archiving report")
	}
	job.reporting.WriteOperationsStatus(currStats)
	job.stats.UpdateBy(currStats)
	return nil
}

func (job *ArchKeeper) DeduplicateInArchive(
	curr []cncdb.QueryArchRec, rec cncdb.QueryArchRec) (cncdb.QueryArchRec, error) {
	return job.dbArch.DeduplicateInArchive(curr, rec)
}

func NewArchKeeper(
	redis *RedisAdapter,
	concArchDb cncdb.IConcArchOps,
	dedup *Deduplicator,
	recsToIndex chan<- cncdb.HistoryRecord,
	recsToStats chan<- cncdb.CorpBoundRawRecord,
	reporting reporting.IReporting,
	tz *time.Location,
	conf *Conf,
) *ArchKeeper {
	return &ArchKeeper{
		redis:       redis,
		dbArch:      concArchDb,
		dedup:       dedup,
		recsToIndex: recsToIndex,
		recsToStats: recsToStats,
		reporting:   reporting,
		tz:          tz,
		conf:        conf,
	}
}
