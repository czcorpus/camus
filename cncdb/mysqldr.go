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
	"time"

	"github.com/rs/zerolog/log"
)

// MySQLDryRun is a dry-run mode version of mysql adapter. It performs
// read operations just like normal adapter but any modifying operation
// just logs its information.
type MySQLDryRun struct {
	db *MySQLOps
}

func (db *MySQLDryRun) LoadRecentNRecords(num int) ([]ArchRecord, error) {
	return db.db.LoadRecentNRecords(num)
}

func (db *MySQLDryRun) LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error) {
	return db.db.LoadRecordsFromDate(fromDate, maxItems)
}

func (db *MySQLDryRun) ContainsRecord(concID string) (bool, error) {
	return db.db.ContainsRecord(concID)
}

func (db *MySQLDryRun) LoadRecordsByID(concID string) ([]ArchRecord, error) {
	return db.db.LoadRecordsByID(concID)
}

func (db *MySQLDryRun) InsertRecord(rec ArchRecord) error {
	log.Info().Msgf("DRY-RUN>>> InsertRecord(ArchRecord{ID: %s})", rec.ID)
	return nil
}

func (db *MySQLDryRun) UpdateRecordStatus(id string, status int) error {
	log.Info().Msgf("DRY-RUN>>> UpdateRecordStatus(%s, %d)", id, status)
	return nil
}

func (db *MySQLDryRun) RemoveRecordsByID(concID string) error {
	log.Info().Msgf("DRY-RUN>>> RemoveRecordsByID(%s)", concID)
	return nil
}

func (db *MySQLDryRun) DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error) {
	log.Info().Msgf("DRY-RUN>>> DeduplicateInArchive(..., ArchRecord{ID: %s})", rec.ID)
	return ArchRecord{}, nil
}

func (ops *MySQLDryRun) GetArchSizesByYears(forceLoad bool) ([][2]int, error) {
	return ops.db.GetArchSizesByYears(forceLoad)
}

func (ops *MySQLDryRun) GetSubcorpusProps(subcID string) (SubcProps, error) {
	return ops.db.GetSubcorpusProps(subcID)
}

func (ops *MySQLDryRun) GetAllUsersWithQueryHistory() ([]int, error) {
	return ops.db.GetAllUsersWithQueryHistory()
}

func (ops *MySQLDryRun) GetUserQueryHistory(userID int, numItems int) ([]HistoryRecord, error) {
	return ops.db.GetUserQueryHistory(userID, numItems)
}

func (db *MySQLDryRun) LoadRecentNHistory(num int) ([]HistoryRecord, error) {
	return db.db.LoadRecentNHistory(num)
}

func (db *MySQLDryRun) GarbageCollectUserQueryHistory(userID int) (int64, error) {
	log.Info().Msgf("DRY-RUN>>> GarbageCollectUserQueryHistory(%d)", userID)
	return 0, nil
}

func (db *MySQLDryRun) GetUserGarbageHistory(userID int) ([]HistoryRecord, error) {
	return db.db.GetUserGarbageHistory(userID)
}

func NewMySQLDryRun(ops *MySQLOps) *MySQLDryRun {
	return &MySQLDryRun{db: ops}
}
