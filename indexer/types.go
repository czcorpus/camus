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
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/v2/mapping"
)

type BleveDoc struct {
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

func (bdoc BleveDoc) Type() string {
	return "query"
}

func CreateMapping() mapping.IndexMapping {

	exactStringMapping := bleve.NewKeywordFieldMapping()

	multiValMapping := bleve.NewTextFieldMapping()
	multiValMapping.Analyzer = simple.Name

	dtMapping := bleve.NewDateTimeFieldMapping()

	bdocMapping := bleve.NewDocumentMapping()
	bdocMapping.AddFieldMappingsAt("id", exactStringMapping)
	bdocMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	bdocMapping.AddFieldMappingsAt("created", dtMapping)
	bdocMapping.AddFieldMappingsAt("raw_query", multiValMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("query", bdocMapping)
	indexMapping.DefaultAnalyzer = simple.Name
	return indexMapping
}
