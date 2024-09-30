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
	"camus/indexer/documents"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
)

var (
	ErrRecordNotIndexable = errors.New("record is not indexable")
)

type IndexableMidDoc interface {
	GetQuerySupertype() cncdb.QuerySupertype
	GetID() string
}

type IndexableDoc interface {
	mapping.Classifier
	GetID() string
}

// RecToDoc converts a conc/wlist/... archive record into an indexable
// document. In case the record is OK but of an unsupported type (e.g. "shuffle"),
// nil document is returned along with ErrRecordNotIndexable error.
func RecToDoc(arec *cncdb.ArchRecord, db cncdb.IMySQLOps) (IndexableMidDoc, error) {
	var rec cncdb.QueryRecord
	if err := json.Unmarshal([]byte(arec.Data), &rec); err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	qstype, err := rec.GetSupertype()
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	if !qstype.IsIndexable() {
		return nil, ErrRecordNotIndexable
	}
	rawq, err := rec.GetRawQueries()
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}

	fmt.Println("SelectedTextTypes: ", rec.GetTextTypes())

	subc, err := rec.GetSubcorpus(db)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec to doc: %w", err)
	}

	ans := &documents.MidConc{
		ID:             arec.ID,
		Created:        arec.Created,
		UserID:         rec.UserID,
		Corpora:        rec.Corpora,
		Subcorpus:      subc,
		QuerySupertype: qstype,
		RawQueries:     rawq,
	}

	if err := documents.ExtractCQLProps(ans); err != nil {
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
