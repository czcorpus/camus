// Copyright 2024 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2024 Martin Zimandl <martin.zimandl@gmail.com>
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

package search

import (
	"camus/cncdb"
	"encoding/json"
	"fmt"
	"time"
)

// Document is a KonText query representation intended for
// fulltext indexing and search
type Document struct {
	QuerySupertype cncdb.QuerySupertype `json:"querySupertype"`

	Created time.Time `json:"created"`

	UserID int `json:"userId"`

	// Corpora contains all the searched corpora. Lenght > 1 means
	// the search was performed on a parallel corpus
	Corpora []string `json:"corpora"`

	Subcorpus string `json:"subcorpus"`

	// RawQuery is the original query written by a user
	// (multiple queries = aligned corpora)
	RawQueries []cncdb.RawQuery `json:"rawQueries"`

	// Structures contains a list of all structures involved in the query
	Structures []string `json:"structures"`

	// StructAttrs contains all the structural attributes and their values
	// used in the query. It does not matter whether the chunks were attr=val
	// or attr!=val. We want just to know which values are associated to which
	// attributes.
	// A typical source is `... within <doc txtype="fiction" & pubyear="2020" />`
	StructAttrs map[string][]string `json:"structAttrs"`

	// PosAttrs contains all the positional attributes and their values
	// in the query.
	PosAttrs map[string][]string `json:"posAttrs"`
}

func (doc *Document) IsValidCQLQuery(idx int) bool {
	return len(doc.RawQueries) > idx && doc.RawQueries[idx].Type == "advanced"
}

func RecToDoc(arec *cncdb.ArchRecord) (*Document, error) {
	var rec cncdb.QueryRecord
	if err := json.Unmarshal([]byte(arec.Data), &rec); err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	qstype, err := rec.GetSupertype()
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	rawq, err := rec.GetRawQueries()
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	ans := &Document{
		Created:        arec.Created,
		UserID:         rec.UserID,
		QuerySupertype: qstype,
		RawQueries:     rawq,
	}
	return ans, nil
}
