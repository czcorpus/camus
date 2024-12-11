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

package cncdb

import (
	"database/sql"
	"time"

	"github.com/rs/zerolog/log"
)

// MySQLConcArchDryRun is a dry-run mode version of mysql adapter. It performs
// read operations just like normal adapter but any modifying operation
// just logs its information.
type MySQLConcArchDryRun struct {
	db *MySQLConcArch
}

func (db *MySQLConcArchDryRun) NewTransaction() (*sql.Tx, error) {
	return db.db.NewTransaction()
}

func (db *MySQLConcArchDryRun) LoadRecentNRecords(num int) ([]ArchRecord, error) {
	return db.db.LoadRecentNRecords(num)
}

func (db *MySQLConcArchDryRun) LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error) {
	return db.db.LoadRecordsFromDate(fromDate, maxItems)
}

func (db *MySQLConcArchDryRun) ContainsRecord(concID string) (bool, error) {
	return db.db.ContainsRecord(concID)
}

func (db *MySQLConcArchDryRun) LoadRecordsByID(concID string) ([]ArchRecord, error) {
	return db.db.LoadRecordsByID(concID)
}

func (db *MySQLConcArchDryRun) InsertRecord(rec ArchRecord) error {
	log.Info().Msgf("DRY-RUN>>> InsertRecord(ArchRecord{ID: %s})", rec.ID)
	return nil
}

func (db *MySQLConcArchDryRun) UpdateRecordStatus(id string, status int) error {
	log.Info().Msgf("DRY-RUN>>> UpdateRecordStatus(%s, %d)", id, status)
	return nil
}

func (db *MySQLConcArchDryRun) RemoveRecordsByID(concID string) error {
	log.Info().Msgf("DRY-RUN>>> RemoveRecordsByID(%s)", concID)
	return nil
}

func (db *MySQLConcArchDryRun) DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error) {
	log.Info().Msgf("DRY-RUN>>> DeduplicateInArchive(..., ArchRecord{ID: %s})", rec.ID)
	return ArchRecord{}, nil
}

func (ops *MySQLConcArchDryRun) GetArchSizesByYears(forceLoad bool) ([][2]int, error) {
	return ops.db.GetArchSizesByYears(forceLoad)
}

func (ops *MySQLConcArchDryRun) GetSubcorpusProps(subcID string) (SubcProps, error) {
	return ops.db.GetSubcorpusProps(subcID)
}

// --------------------------------------------------------------

// MySQLQueryHistDryRun is a dry-run mode version of mysql adapter. It performs
// read operations just like normal adapter but any modifying operation
// just logs its information.
type MySQLQueryHistDryRun struct {
	db *MySQLQueryHist
}

func (ops *MySQLQueryHistDryRun) NewTransaction() (*sql.Tx, error) {
	return ops.db.NewTransaction()
}

func (ops *MySQLQueryHistDryRun) GetAllUsersWithQueryHistory() ([]int, error) {
	return ops.db.GetAllUsersWithQueryHistory()
}

func (ops *MySQLQueryHistDryRun) GetUserQueryHistory(userID int, numItems int) ([]HistoryRecord, error) {
	return ops.db.GetUserQueryHistory(userID, numItems)
}

func (ops *MySQLQueryHistDryRun) MarkOldQueryHistory(numPreserve int) (int64, error) {
	log.Info().Msgf("DRY-RUN>>> MarkOldQueryHistory(%d)", numPreserve)
	return 0, nil
}

func (db *MySQLQueryHistDryRun) LoadRecentNHistory(num int) ([]HistoryRecord, error) {
	return db.db.LoadRecentNHistory(num)
}

func (db *MySQLQueryHistDryRun) GarbageCollectUserQueryHistory(userID int) (int64, error) {
	log.Info().Msgf("DRY-RUN>>> GarbageCollectUserQueryHistory(%d)", userID)
	return 0, nil
}

func (db *MySQLQueryHistDryRun) GetUserGarbageHistory(userID int) ([]HistoryRecord, error) {
	return db.db.GetUserGarbageHistory(userID)
}

func (db *MySQLQueryHistDryRun) RemoveQueryHistory(tx *sql.Tx, created int64, userID int, queryID string) error {
	log.Info().Msgf("DRY-RUN>>> RemoveQueryHistory(%d, %d, %s)", created, userID, queryID)
	return nil
}

func (db *MySQLQueryHistDryRun) GetPendingDeletionHistory(tx *sql.Tx, maxItems int) ([]HistoryRecord, error) {
	return db.db.GetPendingDeletionHistory(tx, maxItems)
}

func NewMySQLDryRun(opsArch *MySQLConcArch, opsHist *MySQLQueryHist) (*MySQLConcArchDryRun, *MySQLQueryHistDryRun) {
	return &MySQLConcArchDryRun{db: opsArch}, &MySQLQueryHistDryRun{db: opsHist}
}
