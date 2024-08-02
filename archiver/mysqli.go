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

package archiver

import "time"

type IMySQLOps interface {
	LoadRecentNRecords(num int) ([]ArchRecord, error)
	LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error)
	ContainsRecord(concID string) (bool, error)
	LoadRecordsByID(concID string) ([]ArchRecord, error)
	InsertRecord(rec ArchRecord) error
	UpdateRecordStatus(id string, status int) error
	RemoveRecordsByID(concID string) error
	DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error)
}
