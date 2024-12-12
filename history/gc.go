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

package history

import (
	"camus/archiver"
	"camus/cncdb"
	"camus/cnf"
	"camus/indexer"
	"camus/reporting"
	"context"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	gcUsersProcSetKey      = "camus_users_qh_gc"
	timeWaitAfterDelErrors = 5 * time.Minute
)

type GarbageCollector struct {
	db            cncdb.IQHistArchOps
	rdb           *archiver.RedisAdapter
	checkInterval time.Duration
	markInterval  time.Duration
	numPreserve   int
	maxNumDelete  int
	indexer       *indexer.Indexer
	statusWriter  reporting.IReporting
}

func (gc *GarbageCollector) Start(ctx context.Context) {
	log.Info().
		Str("rmCheckInterval", gc.checkInterval.String()).
		Msg("starting history.GarbageCollector task")

	timer := time.NewTimer(gc.checkInterval)
	markerTimer := time.NewTicker(gc.markInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close fulltext Service")
				return
			case <-markerTimer.C:
				gc.createPendingRecords()
			case <-timer.C:
				var numErr int
				indexSize, err := gc.indexer.Count()
				if err != nil {
					numErr++
					log.Error().Err(err).Msg("failed to obtain fulltext index size")
				}

				tableSize, err := gc.db.TableSize()
				if err != nil {
					numErr++
					log.Error().Err(err).Msg("failed to obtain table kontext_query_history size")
				}

				delStats := gc.processDeletionPendingRecords()
				delStats.NumErrors += numErr
				if delStats.NumErrors == 0 {
					delStats.IndexSize = int64(indexSize)
					delStats.SQLTableSize = tableSize
				}
				gc.statusWriter.WriteQueryHistoryDeletionStatus(delStats)

				if delStats.NumErrors == 0 {
					timer = time.NewTimer(gc.checkInterval)

				} else {
					go func() {
						log.Error().
							Msgf(
								"errors in deleting of pending records - going to wait %01.1f minutes then continue",
								timeWaitAfterDelErrors.Seconds(),
							)
						time.Sleep(timeWaitAfterDelErrors)
						timer = time.NewTimer(gc.checkInterval)
					}()
				}
			}
		}
	}()
}

func (gc *GarbageCollector) createPendingRecords() {
	numRm, err := gc.db.MarkOldRecords(gc.numPreserve)
	if err != nil {
		log.Error().
			Err(err).
			Msg("failed to mark kontext_query_history records for deletion (will try again)")

	} else {
		log.Info().
			Int64("numMarked", numRm).
			Msg("marked next set of kontext_query_history records for deletion")
	}
}

// processDeletionPendingRecords returns status whether we are allowed
// to run a new timer to process the next batch of records.
func (gc *GarbageCollector) processDeletionPendingRecords() reporting.QueryHistoryDelStats {
	log.Debug().Msg("retrieving next query history data with pending deletion")
	tx, err := gc.db.NewTransaction()
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve next query history data with pending deletion")
		return reporting.QueryHistoryDelStats{NumErrors: 1}
	}
	recs, err := gc.db.GetPendingDeletionRecords(tx, gc.maxNumDelete)
	log.Debug().
		Int("maxLimit", gc.maxNumDelete).
		Int("numRecords", len(recs)).
		Msg("searched query history records for deletion")
	if err != nil {
		log.Error().Err(err).Msg("failed to retrieve next query history data with pending deletion")
		if err := tx.Rollback(); err != nil {
			log.Error().Err(err).Msg("failed to rollback transaction")
		}
		return reporting.QueryHistoryDelStats{NumErrors: 1}
	}
	for _, rec := range recs {
		if err := gc.db.RemoveRecord(tx, rec.Created, rec.UserID, rec.QueryID); err != nil {
			log.Error().
				Int64("created", rec.Created).
				Int("userId", rec.UserID).
				Str("queryId", rec.QueryID).
				Err(err).
				Msg("failed to remove query history item")
			if err := tx.Rollback(); err != nil {
				log.Error().Err(err).Msg("failed to rollback transaction")
			}
			return reporting.QueryHistoryDelStats{NumErrors: 1}
		}
		if err := gc.indexer.Delete(rec.CreateIndexID()); err != nil {
			log.Error().
				Int64("created", rec.Created).
				Int("userId", rec.UserID).
				Str("queryId", rec.QueryID).
				Err(err).
				Msg("failed to delete item from Bleve index")
			if err := tx.Rollback(); err != nil {
				log.Error().Err(err).Msg("failed to rollback transaction")
			}
			return reporting.QueryHistoryDelStats{NumErrors: 1}
		}
	}
	if err := tx.Commit(); err != nil {
		log.Error().
			Err(err).
			Msg("failed to commit transaction in processDeletionPendingRecords")
		return reporting.QueryHistoryDelStats{NumErrors: 1}
	}

	return reporting.QueryHistoryDelStats{NumDeleted: len(recs)}
}

func (gc *GarbageCollector) Stop(ctx context.Context) error {
	return nil
}

func (gc *GarbageCollector) RunAdHoc(
	ctx context.Context,
	concArchDb cncdb.IConcArchOps,
	conf *cnf.Conf,
	chunkSize int,
) {

	cacheExists, err := gc.rdb.Exists(gcUsersProcSetKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to garbage collect query history")
		os.Exit(1)
		return
	}
	if !cacheExists {
		log.Info().Msg("processed user IDs not found - will create a new set")
		users, err := gc.db.GetAllUsersWithSomeRecords()
		if err != nil {
			log.Error().Err(err).Msg("failed to garbage collect query history")
			os.Exit(2)
			return
		}
		for _, uid := range users {
			gc.rdb.UintZAdd(gcUsersProcSetKey, uid)
		}
		log.Info().Int("numberOfUsers", len(users)).Msg("added users to process")
	}
	recsToIndex := make(chan cncdb.HistoryRecord)
	defer func() { close(recsToIndex) }()

	ftIndexer, err := indexer.NewIndexerOrDie(conf.Indexer, concArchDb, gc.db, gc.rdb, recsToIndex)
	if err != nil {
		log.Error().Err(err).Msg("failed to init query history")
		os.Exit(3)
		return
	}
	log.Info().Int("chunkSize", chunkSize).Msg("processing next chunk of users")
	for i := 0; i < chunkSize; i++ {
		nextUserID, err := gc.rdb.UintZRemLowest(gcUsersProcSetKey)
		if err != nil {
			log.Error().Err(err).Msg("failed to garbage collect query history")
			os.Exit(4)
			return
		}
		if nextUserID < 0 {
			// will fill-in users again in the next call of Run()
			break
		}

		rmFromIndex, err := gc.db.GetUserGarbageRecords(nextUserID)
		if err != nil {
			log.Error().
				Err(err).
				Int("userId", nextUserID).
				Msg("failed to garbage-collect queries for a user")
			continue
		}
		for _, v := range rmFromIndex {
			if err := ftIndexer.Delete(v.CreateIndexID()); err != nil {
				log.Error().
					Err(err).
					Int("userId", nextUserID).
					Str("fulltextId", v.CreateIndexID()).
					Msg("failed to garbage-collect queries for a user")
				continue
			}
		}

		numRemoved, err := gc.db.GarbageCollectRecords(nextUserID)
		if err != nil {
			log.Error().
				Err(err).
				Int("userId", nextUserID).
				Msg("failed to garbage-collect queries for a user")
			continue

		} else {
			log.Info().
				Int("userId", nextUserID).
				Int64("numRemoved", numRemoved).
				Msg("gargage-collected queries for user")
		}
		select {
		case <-ctx.Done():
			log.Info().Msg("interrupted by user")
			return
		default:
		}

	}
	remainingUsers, err := gc.rdb.ZCard(gcUsersProcSetKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to determine remaining num. of users to process")
		os.Exit(6)
		return
	}
	log.Info().
		Int("remainingUsers", remainingUsers).
		Int("chunkSize", chunkSize).
		Msg("chunk processed")
}

func NewGarbageCollector(
	db cncdb.IQHistArchOps,
	rdb *archiver.RedisAdapter,
	fulltext *indexer.Indexer,
	statusWriter reporting.IReporting,
	conf *indexer.Conf,
) *GarbageCollector {
	return &GarbageCollector{
		db:            db,
		rdb:           rdb,
		indexer:       fulltext,
		statusWriter:  statusWriter,
		checkInterval: conf.QueryHistoryCleanupIntervalDur(),
		markInterval:  conf.QueryHistoryMarkPendingIntervalDur(),
		maxNumDelete:  conf.QueryHistoryMaxNumDeleteAtOnce,
	}
}
