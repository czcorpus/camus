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

// DummySQL is a testing implementation of IMySQLOps
type DummySQL struct {
}

func (dsql *DummySQL) LoadRecentNRecords(num int) ([]ArchRecord, error) {
	return []ArchRecord{}, nil
}

func (dsql *DummySQL) LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error) {
	return []ArchRecord{}, nil
}

func (dsql *DummySQL) ContainsRecord(concID string) (bool, error) {
	return false, nil
}

func (dsql *DummySQL) LoadRecordsByID(concID string) ([]ArchRecord, error) {
	return []ArchRecord{}, nil
}

func (dsql *DummySQL) InsertRecord(rec ArchRecord) error {
	return nil
}

func (dsql *DummySQL) UpdateRecordStatus(id string, status int) error {
	return nil
}

func (dsql *DummySQL) RemoveRecordsByID(concID string) error {
	return nil
}

func (dsql *DummySQL) DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error) {
	return ArchRecord{}, nil
}

func (dsql *DummySQL) GetArchSizesByYears(forceLoad bool) ([][2]int, error) {
	return [][2]int{}, nil
}

func (dsql *DummySQL) GetSubcorpusProps(subcID string) (SubcProps, error) {
	return SubcProps{}, nil
}

func (dsql *DummySQL) GetAllUsersWithQueryHistory() ([]int, error) {
	return []int{}, nil
}

func (dsql *DummySQL) GetUserQueryHistory(userID int, ttl time.Duration) ([]HistoryRecord, error) {
	return []HistoryRecord{}, nil
}

func (dsql *DummySQL) LoadRecentNHistory(num int) ([]HistoryRecord, error) {
	return []HistoryRecord{}, nil
}
