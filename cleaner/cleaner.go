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
	"camus/reporting"
	"context"
	"fmt"
	"time"

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/rs/zerolog/log"
)

const (
	dtFormat = "2006-01-02T15:04:05"
)

type Service struct {
	conf           Conf
	db             archiver.IMySQLOps
	rdb            *archiver.RedisAdapter
	tz             *time.Location
	cleanupRunning bool
	reporting      reporting.IReporting
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

	birthLimit := time.Now().In(job.tz).Add(-job.conf.MinAgeUnvisited())
	var stats reporting.CleanupStats
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
	items, err := job.db.LoadRecordsFromDate(lastDate, job.conf.NumProcessItemsPerTick)
	if err != nil {
		return fmt.Errorf("failed to load requested items for cleanup from database: %w", err)
	}
	visitedIDs := collections.NewSet[string]()
	for _, item := range items {
		visitedIDs.Add(item.ID)
		stats.NumFetched++
		variants, err := job.db.LoadRecordsByID(item.ID)
		if err != nil {
			log.Warn().
				Err(err).
				Str("recordId", variants[0].ID).
				Msg("failed to load variants for, setting err flag and skipping")
			if err := job.db.UpdateRecordStatus(variants[0].ID, -1); err != nil {
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
			if err := job.db.UpdateRecordStatus(variants[0].ID, -1); err != nil {
				log.Error().
					Err(err).
					Str("recordId", variants[0].ID).
					Msg("failed to set error status")
			}
			stats.NumErrors++
			continue
		}

		if len(variants) > 1 {
			mergedItem, err := job.db.DeduplicateInArchive(variants, variants[0])
			if err != nil {
				log.Warn().
					Err(err).
					Str("recordId", variants[0].ID).
					Msg("failed to deduplicate items in database, setting err flag and skipping")
				if err := job.db.UpdateRecordStatus(variants[0].ID, -1); err != nil {
					log.Error().
						Err(err).
						Str("recordId", variants[0].ID).
						Msg("failed to set error status")
				}
				stats.NumErrors++
				continue
			}
			stats.NumMerged++
			if mergedItem.NumAccess == 0 && mergedItem.Created.Before(birthLimit) {
				log.Debug().
					Str("recordId", mergedItem.ID).
					Time("limitBirth", birthLimit).
					Msg("record will be removed due to no access and high age")
				if err := job.db.RemoveRecordsByID(variants[0].ID); err != nil {
					if err := job.db.UpdateRecordStatus(variants[0].ID, -1); err != nil {
						log.Error().
							Err(err).
							Str("recordId", variants[0].ID).
							Msg("failed to set error status")
					}
					stats.NumErrors++
					continue
				}
			}

		} else {
			if variants[0].NumAccess == 0 && variants[0].Created.Before(birthLimit) {
				log.Debug().
					Str("recordId", variants[0].ID).
					Time("limitBirth", birthLimit).
					Msg("record will be removed due to no access and high age")
				if err := job.db.RemoveRecordsByID(variants[0].ID); err != nil {
					if err := job.db.UpdateRecordStatus(variants[0].ID, -1); err != nil {
						log.Error().
							Err(err).
							Str("recordId", variants[0].ID).
							Msg("failed to set error status")
					}
					stats.NumErrors++
					continue
				}
			}
		}
	}
	job.rdb.Set(job.conf.StatusKey, items[len(items)-1].Created.Format(dtFormat))
	log.Info().
		Any("stats", stats).
		Float64("procTime", time.Since(t0).Seconds()).
		Msg("cleanup done")
	return nil
}

func NewService(
	db archiver.IMySQLOps,
	rdb *archiver.RedisAdapter,
	reporting reporting.IReporting,
	conf Conf,
	tz *time.Location,
) *Service {
	return &Service{
		conf:      conf,
		db:        db,
		rdb:       rdb,
		reporting: reporting,
		tz:        tz,
	}
}
