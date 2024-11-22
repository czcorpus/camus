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
	"os"

	"github.com/rs/zerolog/log"
)

const (
	gcUsersProcSetKey = "camus_users_qh_gc"
)

type GarbageCollector struct {
	db  cncdb.IMySQLOps
	rdb *archiver.RedisAdapter
}

func (gc *GarbageCollector) Run(
	ctx context.Context,
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
		users, err := gc.db.GetAllUsersWithQueryHistory()
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

	ftIndexer, err := indexer.NewIndexerOrDie(conf.Indexer, gc.db, gc.rdb, recsToIndex)
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

		rmFromIndex, err := gc.db.GetUserGarbageHistory(nextUserID)
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

		numRemoved, err := gc.db.GarbageCollectUserQueryHistory(nextUserID)
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
	db cncdb.IMySQLOps,
	rdb *archiver.RedisAdapter,
) *GarbageCollector {
	return &GarbageCollector{
		db:  db,
		rdb: rdb,
	}
}
