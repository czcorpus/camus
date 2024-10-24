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
	"fmt"
	"strconv"
	"strings"
	"time"
)

type PQuery struct {
	ID string `json:"id"`

	Name string `json:"name"`

	Created time.Time `json:"created"`

	QuerySupertype string `json:"query_supertype"`

	UserID string `json:"user_id"`

	Corpora string `json:"corpora"`

	Subcorpus string `json:"subcorpus"`

	RawQuery string `json:"raw_query"`

	Structures string `json:"structures"`

	StructAttrNames string `json:"struct_attr_names"`

	StructAttrValues string `json:"struct_attr_values"`

	PosAttrNames string `json:"pos_attr_names"`

	PosAttrValues string `json:"pos_attr_values"`
}

func (pq *PQuery) Type() string {
	return "pquery"
}

func (pq *PQuery) GetID() string {
	return fmt.Sprintf("%s/%d/%s", pq.UserID, pq.Created.Unix(), pq.ID)
}

// intermediate PQuery

type MidPQuery struct {
	ID string `json:"id"`

	Name string `json:"name"`

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

func (doc *MidPQuery) AddStructAttr(name, value string) {
	if doc.StructAttrs == nil {
		doc.StructAttrs = make(map[string][]string)
	}
	doc.StructAttrs[name] = append(doc.StructAttrs[name], value)
}

func (doc *MidPQuery) AddPosAttr(name, value string) {
	if doc.PosAttrs == nil {
		doc.PosAttrs = make(map[string][]string)
	}
	doc.PosAttrs[name] = append(doc.PosAttrs[name], value)
}

func (doc *MidPQuery) AddStructure(name string) {
	if doc.Structures == nil {
		doc.Structures = make([]string, 0, 5)
	}
	doc.Structures = append(doc.Structures, name)
}

func (doc *MidPQuery) GetRawQueries() []cncdb.RawQuery {
	return doc.RawQueries
}

// methods to comply with IndexableMidDoc

func (doc *MidPQuery) GetID() string {
	return doc.ID
}

func (doc *MidPQuery) GetQuerySupertype() cncdb.QuerySupertype {
	return doc.QuerySupertype
}

func (doc *MidPQuery) getRawQueriesAsString() string {
	var ans strings.Builder
	for _, v := range doc.RawQueries {
		ans.WriteString(" " + v.Value)
	}
	return ans.String()
}

func (doc *MidPQuery) AsIndexableDoc() IndexableDoc {
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
	return &PQuery{
		ID:               doc.ID,
		Name:             doc.Name,
		QuerySupertype:   string(doc.QuerySupertype),
		Created:          doc.Created,
		UserID:           strconv.Itoa(doc.UserID),
		RawQuery:         doc.getRawQueriesAsString(),
		Structures:       strings.Join(doc.Structures, " "),
		PosAttrNames:     strings.Join(posAttrNames, " "),
		PosAttrValues:    strings.Join(posAttrValues, " "),
		StructAttrNames:  strings.Join(structAttrNames, " "),
		StructAttrValues: strings.Join(structAttrValues, " "),
	}
}
