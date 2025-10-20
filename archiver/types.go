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
	"camus/cncdb"
	"encoding/json"
)

// -----------------------

type ConcCacheRec struct {
	ID   string
	Data string
}

// -------------------------

type Deduplication struct {
	NumMerged   int                `json:"numMerged"`
	FinalRecord cncdb.QueryArchRec `json:"finalRecord"`
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
			NumMerged   int                `json:"numMerged"`
			FinalRecord cncdb.QueryArchRec `json:"finalRecord"`
			Error       string             `json:"error,omitempty"`
		}{
			NumMerged:   dedup.NumMerged,
			FinalRecord: dedup.FinalRecord,
			Error:       errMsg,
		},
	)
}
