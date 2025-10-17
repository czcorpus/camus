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

type SubcProps struct {
	Name      string
	TextTypes map[string][]string
}

// IConcArchOps is an abstract interface for high level
// database operations for concordance archive.
type IConcArchOps interface {
	NewTransaction() (*sql.Tx, error)
	LoadRecentNRecords(num int) ([]RawRecord, error)
	LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]RawRecord, error)
	ContainsRecord(concID string) (bool, error)
	LoadRecordsByID(concID string) ([]RawRecord, error)
	InsertRecord(rec RawRecord) error
	UpdateRecordStatus(id string, status int) error
	RemoveRecordsByID(concID string) error
	DeduplicateInArchive(curr []RawRecord, rec RawRecord) (RawRecord, error)

	// GetArchSizesByYears
	// Without forceReload, the function refuses to perform actual query outside
	// defined night time.
	// Returns list of pairs where FIRST item is always YEAR, the SECOND one is COUNT
	GetArchSizesByYears(forceLoad bool) ([][2]int, error)

	// GetSubcorpusProps takes a subcorpus "hash" ID and returns
	// a corresponding name defined by the author.
	// The method should accept empty value by responding
	// with empty value (and without error).
	GetSubcorpusProps(subcID string) (SubcProps, error)
}

// IQHistArchOps is an abstract interface for high level
// database operations for query history (which itself is kind
// of a "tag" to the concordance archive table)
type IQHistArchOps interface {
	NewTransaction() (*sql.Tx, error)
	GetAllUsersWithSomeRecords() ([]int, error)

	GetUserRecords(userID int, numItems int) ([]HistoryRecord, error)
	MarkOldRecords(numPreserve int) (int64, error)
	GarbageCollectRecords(userID int) (int64, error)
	GetUserGarbageRecords(userID int) ([]HistoryRecord, error)
	RemoveRecord(tx *sql.Tx, created int64, userID int, queryID string) error

	// GetPendingDeletionRecords should return records with oldest
	// pending deletion time.
	GetPendingDeletionRecords(tx *sql.Tx, maxItems int) ([]HistoryRecord, error)
	LoadRecentNHistory(num int) ([]HistoryRecord, error)
	TableSize() (int64, error)
}
