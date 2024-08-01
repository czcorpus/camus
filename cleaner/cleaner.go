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

package cleaner

import (
	"camus/archiver"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/rs/zerolog/log"
)

const (
	dtFormat = "2006-01-02T15:04:05"
)

type CleanupStats struct {
	NumFetched int `json:"numFetched"`
	NumMerged  int `json:"numMerged"`
	NumErrors  int `json:"numErrors"`
}

type Service struct {
	conf           Conf
	db             *sql.DB
	rdb            *archiver.RedisAdapter
	tz             *time.Location
	dryRun         bool
	cleanupRunning bool
}

func (job *Service) Start(ctx context.Context) {
	ticker := time.NewTicker(job.conf.CheckInterval())
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close Cleaner")
				return
			case <-ticker.C:
				if job.cleanupRunning {
					log.Warn().Msg("cannot run next cleanup - the previous not finished yet")

				} else {
					err := job.performCleanup()
					if err != nil {
						log.Error().Err(err).Msg("failed to perform cleanup")
					}
				}
			}
		}
	}()
}

func (job *Service) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping Cleaner")
	return nil
}

func (job *Service) performCleanup() error {
	job.cleanupRunning = true
	defer func() { job.cleanupRunning = false }()
	t0 := time.Now()
	var stats CleanupStats
	lastDateRaw, err := job.rdb.Get(job.conf.StatusKey)
	if err != nil {
		return fmt.Errorf("failed to fetch last check date from Redis (key %s): %w", job.conf.StatusKey, err)
	}
	var lastDate time.Time
	if lastDateRaw != "" {
		lastDate, err = time.Parse(dtFormat, lastDateRaw)
		if err != nil {
			return fmt.Errorf("failed to parse last check date in Redis (key %s): %w", job.conf.StatusKey, err)
		}
	}
	log.Info().
		Time("lastCheck", lastDate).
		Int("itemsToLoad", job.conf.NumProcessItemsPerTick).
		Msg("performing archive cleanup")
	items, err := archiver.LoadRecordsFromDate(job.db, lastDate, job.conf.NumProcessItemsPerTick)
	if err != nil {
		return fmt.Errorf("failed to load requested items for cleanup from database: %w", err)
	}
	visitedIDs := collections.NewSet[string]()
	for _, item := range items {
		visitedIDs.Add(item.ID)
		stats.NumFetched++
		variants, err := archiver.LoadRecordsByID(job.db, item.ID)
		if err != nil {
			log.Warn().
				Err(err).
				Str("recordId", variants[0].ID).
				Msg("failed to load variants for, setting err flag and skipping")
			if err := archiver.UpdateRecordStatus(job.db, variants[0].ID, -1); err != nil {
				log.Error().
					Err(err).
					Str("recordId", variants[0].ID).
					Msg("failed to set error status")
			}
			stats.NumErrors++
			continue
		}

		err = archiver.ValidateQueryInstances(variants)
		if err != nil {
			log.Warn().
				Err(err).
				Str("recordId", variants[0].ID).
				Msg("archive record variants failed to validate, setting err flag and skipping")
			if err := archiver.UpdateRecordStatus(job.db, variants[0].ID, -1); err != nil {
				log.Error().
					Err(err).
					Str("recordId", variants[0].ID).
					Msg("failed to set error status")
			}
			stats.NumErrors++
			continue
		}

		if len(variants) > 1 {
			_, err := archiver.DeduplicateInArchive(job.db, variants, variants[0], job.tz)
			if err != nil {
				log.Warn().
					Err(err).
					Str("recordId", variants[0].ID).
					Msg("failed to deduplicate items in database, setting err flag and skipping")
				if err := archiver.UpdateRecordStatus(job.db, variants[0].ID, -1); err != nil {
					log.Error().
						Err(err).
						Str("recordId", variants[0].ID).
						Msg("failed to set error status")
				}
				stats.NumErrors++
				continue
			}
			stats.NumMerged++
		}
	}
	job.rdb.Set(job.conf.StatusKey, items[len(items)-1].Created.Format(dtFormat))
	log.Info().
		Any("stats", stats).
		Dur("procTime", time.Since(t0)).
		Msg("cleanup done")
	return nil
}

func NewService(db *sql.DB, rdb *archiver.RedisAdapter, conf Conf, tz *time.Location) *Service {
	return &Service{
		conf: conf,
		db:   db,
		rdb:  rdb,
		tz:   tz,
	}
}
