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

import (
	"encoding/json"
	"fmt"
	"time"
)

type BgJobStats struct {
	NumErrors   int `json:"numErrors"`
	NumMerged   int `json:"numMerged"`
	NumInserted int `json:"numInserted"`
	NumFetched  int `json:"numFetched"`
}

func (bgs *BgJobStats) UpdateBy(other BgJobStats) {
	bgs.NumErrors += other.NumErrors
	bgs.NumMerged += other.NumMerged
	bgs.NumInserted += other.NumInserted
	bgs.NumFetched += other.NumFetched
}

// -------------------------

type Deduplication struct {
	NumMerged   int        `json:"numMerged"`
	FinalRecord ArchRecord `json:"finalRecord"`
	error       error
}

func (dedup Deduplication) Err() error {
	return dedup.error
}

func (dedup Deduplication) MarshalJSON() ([]byte, error) {
	var errMsg string
	if dedup.error != nil {
		errMsg = dedup.error.Error()
	}
	return json.Marshal(
		struct {
			NumMerged   int        `json:"numMerged"`
			FinalRecord ArchRecord `json:"finalRecord"`
			Error       string     `json:"error,omitempty"`
		}{
			NumMerged:   dedup.NumMerged,
			FinalRecord: dedup.FinalRecord,
			Error:       errMsg,
		},
	)
}

// ----------------------------------

type GeneralDataRecord map[string]any

func (rec GeneralDataRecord) GetPrevID() string {
	v, ok := rec["prev_id"]
	if !ok {
		return ""
	}
	typedV, ok := v.(string)
	if !ok {
		return ""
	}
	return typedV
}

func (rec GeneralDataRecord) GetCorpora() []string {
	v, ok := rec["corpora"]
	if !ok {
		return []string{}
	}
	typedV, ok := v.([]string)
	if !ok {
		return []string{}
	}
	return typedV
}

func (rec GeneralDataRecord) GetQuery() []string {
	v, ok := rec["q"]
	if !ok {
		return []string{}
	}
	typedV, ok := v.([]string)
	if !ok {
		return []string{}
	}
	return typedV
}

// ------------------------

type ArchRecord struct {
	ID         string
	Data       string
	Created    time.Time
	NumAccess  int
	LastAccess time.Time
	Permanent  int
}

func (rec ArchRecord) FetchData() (GeneralDataRecord, error) {
	ans := make(GeneralDataRecord)
	err := json.Unmarshal([]byte(rec.Data), &ans)
	if err != nil {
		return GeneralDataRecord{}, fmt.Errorf("failed to fetch ArchRecord data: %w", err)
	}
	return ans, nil
}
