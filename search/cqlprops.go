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

	"github.com/czcorpus/cqlizer/cql"
)

// extractCQLProps parses queries stored in `doc` and
// extracts used attributes, structures and respective values
// into doc's properties.
// Note that only "advanced" queries are extracted. In case there
// are no advanced queries in the document, nothing is changed.
func extractCQLProps(doc *Document) error {

	doc.StructAttrs = make(map[string][]string)
	doc.PosAttrs = make(map[string][]string)
	doc.Structures = make([]string, 0, 5)

	for i, rq := range doc.RawQueries {
		if rq.Type != "advanced" {
			continue
		}
		q, err := cql.ParseCQL(fmt.Sprintf("query-%d", i), rq.Value)
		if err != nil {
			return fmt.Errorf("failed to extract CQL properties: %w", err)
		}

		for _, cqlProp := range q.ExtractProps() {
			if cqlProp.IsStructAttr() {
				key := fmt.Sprintf("%s.%s", cqlProp.Structure, cqlProp.Name)
				_, ok := doc.StructAttrs[key]
				if !ok {
					doc.StructAttrs[key] = make([]string, 0, 10)
				}
				doc.StructAttrs[key] = append(doc.StructAttrs[key], cqlProp.Value)

			} else if cqlProp.IsStructure() {
				doc.Structures = append(doc.Structures, cqlProp.Structure)

			} else if cqlProp.IsPosattr() {
				_, ok := doc.PosAttrs[cqlProp.Name]
				if !ok {
					doc.PosAttrs[cqlProp.Name] = make([]string, 0, 10)
				}
				doc.PosAttrs[cqlProp.Name] = append(doc.PosAttrs[cqlProp.Name], cqlProp.Value)
			}
		}
	}
	return nil
}
