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

package documents

import (
	"camus/cncdb"
	"fmt"
	"reflect"

	"github.com/czcorpus/cqlizer/cql"
	"github.com/rs/zerolog/log"
)

type CQLMidDoc interface {
	AddStructAttr(name, value string)
	AddPosAttr(name, value string)
	AddStructure(name string)
	GetRawQueries() []cncdb.RawQuery
}

// extractSimpleQueryProps decodes the convoluted JSON format KonText uses
// to encode simple conc. queries.
func extractSimpleQueryProps(form *cncdb.ConcFormRecord, doc CQLMidDoc) error {
	if form.LastopForm == nil || form.LastopForm.CurrParsedQueries == nil {
		return nil
	}
	for _, queryRec := range form.LastopForm.CurrParsedQueries {
		for _, conjRec := range queryRec {
			tuple, ok := conjRec.([]any)
			if !ok {
				return fmt.Errorf("simple query proc error: failed to unpack conjuction record")

			}
			queryTokens, ok := tuple[0].([]any)
			if !ok {
				return fmt.Errorf(
					"simple query proc error: failed to unpack properties part of a conjuction record item")
			}
			//    [  [lemma sublemma word] poklad  ]

			for _, token := range queryTokens {
				tokenPropsTmp, ok := token.([]any)
				if !ok {
					return fmt.Errorf("simple query proc error: failed to parse token props")
				}
				attrsTmp := tokenPropsTmp[0]
				attrs, ok := attrsTmp.([]any)
				if !ok {
					return fmt.Errorf("simple query proc error: failed to determine attribute list")
				}
				valueTmp := tokenPropsTmp[1]
				value, ok := valueTmp.(string)
				if !ok {
					return fmt.Errorf("simple query proc error: failed to determine query value")
				}
				for _, v := range attrs {
					tv, ok := v.(string)
					if !ok {
						log.Warn().
							Any("attrName", v).
							Str("attrType", reflect.TypeOf(v).String()).
							Msg("simple query proc warn: type assertion for an attribute name failed")
					}
					doc.AddPosAttr(tv, value)
				}
			}
		}
	}
	return nil
}

// ExtractQueryProps parses queries stored in `doc` and
// extracts used attributes, structures and respective values
// into doc's properties.
// Note that only "advanced" queries are extracted. In case there
// are no advanced queries in the document, nothing is changed.
func ExtractQueryProps(form *cncdb.ConcFormRecord, doc CQLMidDoc) error {

	for i, rq := range doc.GetRawQueries() {
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
				doc.AddStructAttr(key, cqlProp.Value)

			} else if cqlProp.IsStructure() {
				doc.AddStructure(cqlProp.Structure)

			} else if cqlProp.IsPosattr() {
				if cqlProp.Name != "" {
					doc.AddPosAttr(cqlProp.Name, cqlProp.Value)

				} else {
					doc.AddPosAttr(form.GetDefaultAttr(), cqlProp.Value)
				}
			}
		}
	}
	if err := extractSimpleQueryProps(form, doc); err != nil {
		return err
	}
	return nil
}
