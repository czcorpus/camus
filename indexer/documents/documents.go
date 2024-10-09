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

package documents

import "github.com/blevesearch/bleve/v2/mapping"

// IndexableDoc is a generalization of a document
// which can be added to a Bleve index. Please note
// that Bleve uses reflection to get all the values
// and that we rely on some hidden assumptions
// in the sense that all the required
// attributes are interpretable as expected.
// We recommend providing mostly flat string
// attributes even for structured original data
// (e.g. []string => string_of_ws_separated_values,
// map[string]string => string_of_ws_separated_keys,
// string_of_ws_separated_values etc.)
type IndexableDoc interface {
	mapping.Classifier
	GetID() string
	SetName(name string)
}
