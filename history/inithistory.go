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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	usersProcSetKey = "camus_users_qh_init"
)

type DataInitializer struct {
	concArchDb  cncdb.IConcArchOps
	queryHistDb cncdb.IQHistArchOps
	rdb         *archiver.RedisAdapter
}

func (di *DataInitializer) processQuery(hRec cncdb.HistoryRecord, ftIndexer *indexer.Indexer) error {
	rec, err := di.rdb.GetConcRecord(hRec.QueryID)
	if err == cncdb.ErrRecordNotFound {
		recs, err := di.concArchDb.LoadRecordsByID(hRec.QueryID)
		if err != nil {
			return fmt.Errorf("failed to load query %s from MySQL: %w", hRec.QueryID, err)
		}
		if len(recs) == 0 {
			return fmt.Errorf("record %s is gone (both Redis and MySQL) - skipping", hRec.QueryID)
		}
		rec = recs[0]

	} else if err != nil {
		return fmt.Errorf("failed to process query %s: %w", hRec.QueryID, err)
	}
	hRec.Rec = &rec
	ok, err := ftIndexer.IndexRecord(&hRec)
	if err != nil {
		return fmt.Errorf("failed to index query %s: %w", hRec.QueryID, err)
	}
	if !ok {
		return fmt.Errorf("record %s is not indexable - skipped", hRec.QueryID)
	}
	return nil
}

func (di *DataInitializer) Run(
	ctx context.Context,
	conf *cnf.Conf,
	chunkSize int,
) {
	// check for status of possible previous run first
	keyType, err := di.rdb.Type(usersProcSetKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to init query history")
		os.Exit(1)
		return
	}
	if keyType == "string" {
		log.Error().
			Str("key", usersProcSetKey).
			Msg("it appears that a previous import was performed - to override, you must remove the key from Redis")
		os.Exit(1)
		return
	}

	var finishedAllChunks bool

	cacheExists, err := di.rdb.Exists(usersProcSetKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to init query history")
		os.Exit(1)
		return
	}
	if !cacheExists {
		log.Info().Msg("processed user IDs not found - will create a new set")
		users, err := di.queryHistDb.GetAllUsersWithSomeRecords()
		if err != nil {
			log.Error().Err(err).Msg("failed to init query history")
			os.Exit(2)
			return
		}
		for _, uid := range users {
			di.rdb.UintZAdd(usersProcSetKey, uid)
		}
		log.Info().Int("numberOfUsers", len(users)).Msg("added users to process")
	}
	recsToIndex := make(chan cncdb.HistoryRecord)
	defer func() { close(recsToIndex) }()

	ftIndexer, err := indexer.NewIndexerOrDie(conf.Indexer, di.concArchDb, di.queryHistDb, di.rdb, recsToIndex)
	if err != nil {
		log.Error().Err(err).Msg("failed to init query history")
		os.Exit(3)
		return
	}
	log.Info().Int("chunkSize", chunkSize).Msg("processing next chunk of users")
	for i := 0; i < chunkSize; i++ {
		nextUserID, err := di.rdb.UintZRemLowest(usersProcSetKey)
		if err != nil {
			log.Error().Err(err).Msg("failed to init query history")
			os.Exit(4)
			return
		}
		if nextUserID < 0 {
			finishedAllChunks = true
			break
		}
		qIDs, err := di.queryHistDb.GetUserRecords(nextUserID, conf.Indexer.QueryHistoryNumPreserve)
		log.Info().
			Int("userId", nextUserID).
			Err(err).
			Int("numRecords", len(qIDs)).Msg("processing next user")
		if err != nil {
			log.Error().Err(err).Msg("failed to init query history")
			os.Exit(5)
			return
		}
		for _, hRec := range qIDs {
			if err := di.processQuery(hRec, ftIndexer); err != nil {
				log.Error().
					Err(err).
					Int("userId", nextUserID).
					Str("queryId", hRec.QueryID).
					Msg("failed to process record, skipping")
			}
			select {
			case <-ctx.Done():
				log.Info().Msg("interrupted by user")
				return
			default:
			}
		}
		select {
		case <-ctx.Done():
			log.Info().Msg("interrupted by user")
			return
		default:
		}
	}
	remainingUsers, err := di.rdb.ZCard(usersProcSetKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to determine remaining num. of users to process")
		os.Exit(6)
		return
	}
	log.Info().
		Int("remainingUsers", remainingUsers).
		Int("chunkSize", chunkSize).
		Msg("chunk processed")
	if finishedAllChunks {
		rec := fmt.Sprintf("finished-%s", time.Now().In(conf.TimezoneLocation()))
		log.Info().Msgf("no more items - writing '%s' to Redis and ending", rec)
		if err := di.rdb.Set(usersProcSetKey, rec); err != nil {
			log.Error().Err(err).Msg("failed to write 'finished' record to Redis")
			os.Exit(5)
		}
	}
}

func NewDataInitializer(
	concArchDb cncdb.IConcArchOps,
	queryHistDb cncdb.IQHistArchOps,
	rdb *archiver.RedisAdapter,
) *DataInitializer {
	return &DataInitializer{
		concArchDb:  concArchDb,
		queryHistDb: queryHistDb,
		rdb:         rdb,
	}
}
