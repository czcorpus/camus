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
)

// DummyConcArchSQL is a testing implementation of IMySQLOps
type DummyConcArchSQL struct {
}

func (dsql *DummyConcArchSQL) NewTransaction() (*sql.Tx, error) {
	return nil, nil
}

func (dsql *DummyConcArchSQL) LoadRecentNRecords(num int) ([]ArchRecord, error) {
	return []ArchRecord{}, nil
}

func (dsql *DummyConcArchSQL) LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error) {
	return []ArchRecord{}, nil
}

func (dsql *DummyConcArchSQL) ContainsRecord(concID string) (bool, error) {
	return false, nil
}

func (dsql *DummyConcArchSQL) LoadRecordsByID(concID string) ([]ArchRecord, error) {
	return []ArchRecord{}, nil
}

func (dsql *DummyConcArchSQL) InsertRecord(rec ArchRecord) error {
	return nil
}

func (dsql *DummyConcArchSQL) UpdateRecordStatus(id string, status int) error {
	return nil
}

func (dsql *DummyConcArchSQL) RemoveRecordsByID(concID string) error {
	return nil
}

func (dsql *DummyConcArchSQL) DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error) {
	return ArchRecord{}, nil
}

func (dsql *DummyConcArchSQL) GetArchSizesByYears(forceLoad bool) ([][2]int, error) {
	return [][2]int{}, nil
}

func (dsql *DummyConcArchSQL) GetSubcorpusProps(subcID string) (SubcProps, error) {
	return SubcProps{}, nil
}

// ----------------------------------------

// DummyQHistSQL is a testing implementation of IMySQLOps
type DummyQHistSQL struct {
}

func (dsql *DummyQHistSQL) GetAllUsersWithSomeRecords() ([]int, error) {
	return []int{}, nil
}

func (dsql *DummyQHistSQL) GetUserRecords(userID int, numItems int) ([]HistoryRecord, error) {
	return []HistoryRecord{}, nil
}

func (dsql *DummyQHistSQL) MarkOldRecords(numPreserve int) (int64, error) {
	return 0, nil
}

func (dsql *DummyQHistSQL) LoadRecentNHistory(num int) ([]HistoryRecord, error) {
	return []HistoryRecord{}, nil
}

func (dsql *DummyQHistSQL) GarbageCollectRecords(userID int) (int64, error) {
	return 0, nil
}

func (dsql *DummyQHistSQL) GetUserGarbageRecords(userID int) ([]HistoryRecord, error) {
	return []HistoryRecord{}, nil
}
func (dsql *DummyQHistSQL) RemoveRecord(tx *sql.Tx, created int64, userID int, queryID string) error {
	return nil
}

func (dsql *DummyQHistSQL) GetPendingDeletionRecords(tx *sql.Tx, maxItems int) ([]HistoryRecord, error) {
	return []HistoryRecord{}, nil
}
