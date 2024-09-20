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
	"fmt"

	"github.com/czcorpus/cnc-gokit/collections"
	"github.com/czcorpus/cqlizer/cql"
)

// ExtractCQLProps parses advanced query stored in `doc` and
// extracts used attributes, structures and respective values
// into doc's properties.
// Note that in case doc is not valid for such use (e.g. simple query type,
// empty query), the function panics. A doc can be validate using
// method `IsValidCQLQuery`
func ExtractCQLProps(doc *Document) error {
	if !doc.IsValidCQLQuery() {
		panic("not a valid CQL query")
	}

	q, err := cql.ParseCQL("query", doc.RawQuery)
	if err != nil {
		return fmt.Errorf("failed to extract CQL properties: %w", err)
	}
	doc.StructAttrs = make(map[string][]string)
	doc.PosAttrs = make(map[string][]string)
	structures := collections.NewSet[string]()
	for _, attval := range q.GetAllAttvals() {
		if attval.Structure != "" {
			structures.Add(attval.Structure)
			key := fmt.Sprintf("%s.%s", attval.Structure, attval.Name)
			_, ok := doc.StructAttrs[key]
			if !ok {
				doc.StructAttrs[key] = make([]string, 0, 10)
			}
			doc.StructAttrs[key] = append(doc.StructAttrs[key], attval.Value)

		} else {
			_, ok := doc.PosAttrs[attval.Name]
			if !ok {
				doc.PosAttrs[attval.Name] = make([]string, 0, 10)
			}
			doc.PosAttrs[attval.Name] = append(doc.PosAttrs[attval.Name], attval.Value)
		}
	}
	doc.Structures = structures.ToSlice()
	return nil
}
