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

package documents

import (
	"camus/cncdb"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
)

type Concordance struct {
	ID string `json:"id"`

	Created time.Time `json:"created"`

	UserID string `json:"user_id"`

	IsSimpleQuery bool `json:"is_simple_query"`

	Corpora string `json:"corpora"`

	Subcorpus string `json:"subcorpus"`

	RawQuery string `json:"raw_query"`

	Structures string `json:"structures"`

	StructAttrNames string `json:"struct_attr_names"`

	StructAttrValues string `json:"struct_attr_values"`

	PosAttrNames string `json:"pos_attr_names"`

	PosAttrValues string `json:"pos_attr_values"`
}

func (bdoc *Concordance) Type() string {
	return "conc"
}

func (bdoc *Concordance) GetID() string {
	return bdoc.ID
}

// intermediate concordance

// MidConc is a KonText conc. query representation intended for
// fulltext indexing and search
type MidConc struct {
	ID string `json:"id"`

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

func (doc *MidConc) GetID() string {
	return doc.ID
}

func (doc *MidConc) GetQuerySupertype() cncdb.QuerySupertype {
	return doc.QuerySupertype
}

func (doc *MidConc) GetRawQueriesAsString() string {
	var ans strings.Builder
	for _, v := range doc.RawQueries {
		ans.WriteString(" " + v.Value)
	}
	return ans.String()
}

func (doc *MidConc) SetSubcorpusName(name string) {
	doc.Subcorpus = name
}

// IsValidCQLQuery tests for indexability of a query at position idx
// (when considering a possible query to aligned corpora; for single-corpus
// queries, idx==0 is the only option)
func (doc *MidConc) IsValidCQLQuery(idx int) bool {
	return len(doc.RawQueries) > idx && doc.RawQueries[idx].Type == "advanced"
}

func (doc *MidConc) AsIndexableDoc() IndexableDoc {
	posAttrNames := make([]string, 0, 5)
	posAttrValues := make([]string, 0, 5)
	for name, values := range doc.PosAttrs {
		posAttrNames = append(posAttrNames, name)
		posAttrValues = append(posAttrValues, values...)
	}

	structAttrNames := make([]string, 0, 5)
	structAttrValues := make([]string, 0, 5)
	for name, values := range doc.StructAttrs {
		structAttrNames = append(structAttrNames, name)
		structAttrValues = append(structAttrValues, values...)
	}
	bDoc := &Concordance{
		ID:               doc.ID,
		Created:          doc.Created,
		UserID:           strconv.Itoa(doc.UserID),
		Corpora:          strings.Join(doc.Corpora, " "),
		Subcorpus:        doc.Subcorpus,
		RawQuery:         doc.GetRawQueriesAsString(),
		Structures:       strings.Join(doc.Structures, " "),
		StructAttrNames:  strings.Join(structAttrNames, " "),
		StructAttrValues: strings.Join(structAttrValues, " "),
		PosAttrNames:     strings.Join(posAttrNames, " "),
		PosAttrValues:    strings.Join(posAttrValues, " "),
	}
	spew.Dump(bDoc)
	return bDoc
}
