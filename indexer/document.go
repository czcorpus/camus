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

package indexer

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

// IsValidCQLQuery tests for indexability of a query at position idx
// (when considering a possible query to aligned corpora; for single-corpus
// queries, idx==0 is the only option)
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

	fmt.Println("SelectedTextTypes: ", rec.GetTextTypes())

	ans := &Document{
		Created:        arec.Created,
		UserID:         rec.UserID,
		QuerySupertype: qstype,
		RawQueries:     rawq,
	}

	if err := extractCQLProps(ans); err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}

	for attr, items := range rec.GetTextTypes() {
		_, ok := ans.StructAttrs[attr]
		if !ok {
			ans.StructAttrs[attr] = make([]string, 0, len(items))
		}
		ans.StructAttrs[attr] = append(ans.StructAttrs[attr], items...)
	}

	return ans, nil
}
