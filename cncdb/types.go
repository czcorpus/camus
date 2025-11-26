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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	ErrRecordNotFound = errors.New("record not found")
)

// GeneralDataRecord is a general representation of any
// archived KonText query (concordance, word list, paradigmatic q.).
// Internally, it is just a key-value map but it comes with
// several methods allowing for unified access to key properties like
// used (sub)corpora and search query.
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

func (rec GeneralDataRecord) IsFlaggedAsSlow() bool {
	formData, ok := rec["lastop_form"]
	if !ok {
		return false
	}
	typedForm, ok := formData.(map[string]any)
	if !ok {
		log.Warn().Msg("treat_as_slow_query type problem - lastop_form is of a wrong type")
		return false
	}
	flag, ok := typedForm["treat_as_slow_query"]
	if !ok {
		return false
	}
	typedFlag, ok := flag.(bool)
	if !ok {
		log.Warn().Msg("treat_as_slow_query type problem - the flag is of a wrong type")
		return false
	}
	return typedFlag
}

func (rec GeneralDataRecord) RemoveSlowFlag() {
	formData, ok := rec["lastop_form"]
	if !ok {
		return
	}
	typedForm, ok := formData.(map[string]any)
	if !ok {
		log.Warn().Msg("treat_as_slow_query type problem - lastop_form is of a wrong type")
		return
	}
	delete(typedForm, "treat_as_slow_query")
}

func (rec GeneralDataRecord) GetSubcorpus() string {
	v, ok := rec["usesubcorp"]
	if !ok {
		return ""
	}
	typedV, ok := v.(string)
	if ok {
		return typedV
	}
	return ""
}

func (rec GeneralDataRecord) GetCorpora() []string {
	v, ok := rec["corpora"]
	if !ok {
		return []string{}
	}
	typedV, ok := v.([]any)
	if !ok {
		return []string{}
	}
	result := make([]string, 0, len(typedV))
	for _, item := range typedV {
		strItem, ok := item.(string)
		if ok {
			result = append(result, strItem)
		} else {
			return []string{}
		}
	}
	return result
}

func (rec GeneralDataRecord) GetQuery() []string {
	v, ok := rec["q"]
	if !ok {
		return []string{}
	}
	typedV, ok := v.([]any)
	if !ok {
		return []string{}
	}
	result := make([]string, 0, len(typedV))
	for _, item := range typedV {
		strItem, ok := item.(string)
		if ok {
			result = append(result, strItem)
		} else {
			return []string{}
		}
	}
	return result
}

// ----------------------------------

// QueryArchRec is a representation of raw Redis (or MariaDB) conc-archive record.
// The type holds record's unparsed JSON data along with ID and access metadata.
type QueryArchRec struct {
	ID         string
	Data       string
	Created    time.Time
	NumAccess  int
	LastAccess time.Time
	Permanent  int
}

// FetchData parses raw JSON data and returns the most general
// representation - GeneralDataRecord - which is able to fetch common
// properties no matter if the original query is a concordance one, word list one
// or a paradimatic query one.
func (rec QueryArchRec) FetchData() (GeneralDataRecord, error) {
	ans := make(GeneralDataRecord)
	err := json.Unmarshal([]byte(rec.Data), &ans)
	if err != nil {
		return GeneralDataRecord{}, fmt.Errorf("failed to fetch ArchRecord data: %w", err)
	}
	return ans, nil
}

// -------------------------

type CorpBoundRawRecord struct {
	RawRecord     QueryArchRec
	Corpname      string
	CorpusSize    int64
	SubcorpusSize int64
	FlaggedAsSlow bool
}

func (cbrec CorpBoundRawRecord) FetchData() (GeneralDataRecord, error) {
	return cbrec.RawRecord.FetchData()
}

func (cbrec CorpBoundRawRecord) ID() string {
	return cbrec.RawRecord.ID
}

// ----------------------------------

type HistoryRecord struct {
	QueryID string `json:"query_id"`
	UserID  int    `json:"user_id"`
	Created int64  `json:"created"`
	Name    string `json:"name"`
	Rec     *QueryArchRec
}

func (qh *HistoryRecord) CreateIndexID() string {
	return fmt.Sprintf("%d/%d/%s", qh.UserID, qh.Created, qh.QueryID)
}

// ----------------------------------

type ConcCacheRecord struct {
	ID    string
	Key   string
	Value string
}
