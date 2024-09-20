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

import "time"

// Document is a KonText query representation intended for
// fulltext indexing and search
type Document struct {
	Created time.Time

	UserID int

	IsSimpleQuery bool

	// Corpora contains all the searched corpora. Lenght > 1 means
	// the search was performed on a parallel corpus
	Corpora []string

	Subcorpus string

	// RawQuery is the original query written by a user
	RawQuery string

	// Structures contains a list of all structures involved in the query
	Structures []string

	// StructAttrs contains all the structural attributes and their values
	// used in the query. It does not matter whether the chunks were attr=val
	// or attr!=val. We want just to know which values are associated to which
	// attributes.
	// A typical source is `... within <doc txtype="fiction" & pubyear="2020" />`
	StructAttrs map[string][]string

	// PosAttrs contains all the positional attributes and their values
	// in the query.
	PosAttrs map[string][]string
}

func (doc *Document) IsValidCQLQuery() bool {
	return !doc.IsSimpleQuery && doc.RawQuery != ""
}
