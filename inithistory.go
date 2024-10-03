package main

import (
	"camus/archiver"
	"camus/cncdb"
	"camus/cnf"
	"camus/indexer"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
)

const (
	usersProcSetKey = "camus_users_qh_init"
)

type dataInitializer struct {
	db  cncdb.IMySQLOps
	rdb *archiver.RedisAdapter
}

func (di *dataInitializer) processQuery(queryID string, ftIndexer *indexer.Indexer) error {
	rec, err := di.rdb.GetConcRecord(queryID)
	if err == cncdb.ErrRecordNotFound {
		log.Info().Str("queryId", queryID).Msg("record not found in Redis, trying MySQL")
		recs, err := di.db.LoadRecordsByID(queryID)
		if err != nil {
			return fmt.Errorf("failed to load query %s from MySQL: %w", queryID, err)
		}
		if len(recs) == 0 {
			log.Warn().Str("queryId", queryID).Msg("record is gone - cannot process, ignoring")
			return nil
		}
		rec = recs[0]

	} else if err != nil {
		return fmt.Errorf("failed to process query %s: %w", queryID, err)
	}
	ok, err := ftIndexer.IndexRecord(rec)
	if err != nil {
		return fmt.Errorf("failed to index query %s: %w", queryID, err)
	}
	if !ok {
		log.Warn().Str("queryId", queryID).Msg("record not indexable - skipped")
	}
	return nil
}

func (di *dataInitializer) run(
	conf *cnf.Conf,
	chunkSize int,
) {
	cacheExists, err := di.rdb.Exists(usersProcSetKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to init query history")
		os.Exit(1)
		return
	}
	if !cacheExists {
		log.Info().Msg("processed user IDs not found - will create a new set")
		users, err := di.db.GetAllUsersWithQueryHistory()
		if err != nil {
			log.Error().Err(err).Msg("failed to init query history")
			os.Exit(2)
			return
		}
		for _, uid := range users {
			di.rdb.UintZAdd(usersProcSetKey, uid)
		}
	}
	recsToIndex := make(chan cncdb.ArchRecord)
	defer func() { close(recsToIndex) }()
	ftIndexer, err := indexer.NewIndexer(conf.Indexer, di.db, di.rdb, recsToIndex)
	if err != nil {
		log.Error().Err(err).Msg("failed to init query history")
		os.Exit(3)
		return
	}

	for i := 0; i < chunkSize; i++ {
		nextone, err := di.rdb.UintZRemLowest(usersProcSetKey)
		if err != nil {
			log.Error().Err(err).Msg("failed to init query history")
			os.Exit(4)
			return
		}
		if nextone < 0 {
			log.Info().Msg("no more items - ending")
			break
		}
		qIDs, err := di.db.GetUserQueryHistory(conf.Indexer.KonTextHistoryTTL())
		log.Info().
			Int("userId", nextone).
			Err(err).
			Int("numRecords", len(qIDs)).Msg("processing next user")
		if err != nil {
			log.Error().Err(err).Msg("failed to init query history")
			os.Exit(5)
			return
		}
		for _, qID := range qIDs {
			di.processQuery(qID, ftIndexer)
		}
	}
}
