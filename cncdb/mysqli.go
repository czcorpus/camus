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

import "time"

// IMySQLOps is an abstract interface for high level
// database operations. We need it mainly to allow
// injecting "dummy" database adapter for "dry-run" mode.
type IMySQLOps interface {
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

	// GetSubcorpusName takes a subcorpus "hash" ID and returns
	// a corresponding name defined by the author.
	// The method should accept empty value by responding
	// with empty value (and without error).
	GetSubcorpusName(subcID string) (string, error)

	GetAllUsersWithQueryHistory() ([]int, error)

	GetUserQueryHistory(userID int, ttl time.Duration) ([]string, error)
}
