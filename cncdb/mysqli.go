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
	LoadRecentNRecords(num int) ([]ArchRecord, error)
	LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error)
	ContainsRecord(concID string) (bool, error)
	LoadRecordsByID(concID string) ([]ArchRecord, error)
	InsertRecord(rec ArchRecord) error
	UpdateRecordStatus(id string, status int) error
	RemoveRecordsByID(concID string) error
	DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error)

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

type IQHistArchOps interface {
	NewTransaction() (*sql.Tx, error)
	GetAllUsersWithQueryHistory() ([]int, error)

	GetUserQueryHistory(userID int, numItems int) ([]HistoryRecord, error)
	MarkOldQueryHistory(numPreserve int) (int64, error)
	GarbageCollectUserQueryHistory(userID int) (int64, error)
	GetUserGarbageHistory(userID int) ([]HistoryRecord, error)
	RemoveQueryHistory(tx *sql.Tx, created int64, userID int, queryID string) error

	// GetPendingDeletionHistory should return records with oldest
	// pending deletion time.
	GetPendingDeletionHistory(tx *sql.Tx, maxItems int) ([]HistoryRecord, error)
	LoadRecentNHistory(num int) ([]HistoryRecord, error)
}
