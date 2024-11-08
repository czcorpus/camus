// Copyright 2024 Martin Zimandl <martin.zimandl@gmail.com>
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

import (
	"camus/indexer/lotokenizer"
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	"github.com/blevesearch/bleve/v2/mapping"
)

func CreateMapping() (mapping.IndexMapping, error) {

	// whole index
	indexMapping := bleve.NewIndexMapping()

	err := indexMapping.AddCustomAnalyzer(
		"kontext_label_analyzer",
		map[string]interface{}{
			"type":      custom.Name,
			"tokenizer": lotokenizer.Name,
			"token_filters": []string{
				lowercase.Name,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fulltext mappings: %w", err)
	}

	err = indexMapping.AddCustomAnalyzer(
		"kontext_query_analyzer",
		map[string]interface{}{
			"type":      custom.Name,
			"tokenizer": whitespace.Name,
			"token_filters": []string{
				lowercase.Name,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fulltext mappings: %w", err)
	}

	indexMapping.DefaultAnalyzer = "kontext_query_analyzer"

	// field types
	exactStringMapping := bleve.NewKeywordFieldMapping()
	queryMultiValMapping := bleve.NewTextFieldMapping()
	queryMultiValMapping.Analyzer = "kontext_query_analyzer"
	labelMultiValMapping := bleve.NewTextFieldMapping()
	labelMultiValMapping.Analyzer = "kontext_label_analyzer"
	dtMapping := bleve.NewDateTimeFieldMapping()

	// conc type
	concMapping := bleve.NewDocumentMapping()
	concMapping.AddFieldMappingsAt("id", exactStringMapping)
	concMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	concMapping.AddFieldMappingsAt("created", dtMapping)
	concMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	concMapping.AddFieldMappingsAt("is_simple_query", exactStringMapping)
	concMapping.AddFieldMappingsAt("corpora", labelMultiValMapping)
	concMapping.AddFieldMappingsAt("subcorpus", labelMultiValMapping)
	concMapping.AddFieldMappingsAt("raw_query", queryMultiValMapping)
	concMapping.AddFieldMappingsAt("structures", labelMultiValMapping)
	concMapping.AddFieldMappingsAt("struct_attr_names", labelMultiValMapping)
	concMapping.AddFieldMappingsAt("struct_attr_values", labelMultiValMapping)
	concMapping.AddFieldMappingsAt("pos_attr_names", labelMultiValMapping)
	concMapping.AddFieldMappingsAt("pos_attr_values", queryMultiValMapping)

	indexMapping.AddDocumentMapping("conc", concMapping)

	// wlist type

	wlistMapping := bleve.NewDocumentMapping()
	wlistMapping.AddFieldMappingsAt("id", exactStringMapping)
	wlistMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	wlistMapping.AddFieldMappingsAt("created", dtMapping)
	wlistMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	wlistMapping.AddFieldMappingsAt("corpora", labelMultiValMapping)
	wlistMapping.AddFieldMappingsAt("subcorpus", labelMultiValMapping)
	wlistMapping.AddFieldMappingsAt("raw_query", queryMultiValMapping)
	wlistMapping.AddFieldMappingsAt("pos_attr_names", labelMultiValMapping)
	wlistMapping.AddFieldMappingsAt("pfilter_words", queryMultiValMapping)
	wlistMapping.AddFieldMappingsAt("nfilter_words", queryMultiValMapping)

	indexMapping.AddDocumentMapping("wlist", wlistMapping)

	// kwords type
	kwordsMapping := bleve.NewDocumentMapping()
	kwordsMapping.AddFieldMappingsAt("id", exactStringMapping)
	kwordsMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	kwordsMapping.AddFieldMappingsAt("created", dtMapping)
	kwordsMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	kwordsMapping.AddFieldMappingsAt("corpora", labelMultiValMapping)
	kwordsMapping.AddFieldMappingsAt("subcorpus", labelMultiValMapping)
	kwordsMapping.AddFieldMappingsAt("raw_query", queryMultiValMapping)
	kwordsMapping.AddFieldMappingsAt("pos_attr_names", labelMultiValMapping)

	indexMapping.AddDocumentMapping("kwords", kwordsMapping)

	// pquery type
	pqueryMapping := bleve.NewDocumentMapping()
	pqueryMapping.AddFieldMappingsAt("id", exactStringMapping)
	pqueryMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	pqueryMapping.AddFieldMappingsAt("created", dtMapping)
	pqueryMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	pqueryMapping.AddFieldMappingsAt("corpora", labelMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("subcorpus", labelMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("raw_query", queryMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("structures", labelMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("struct_attr_names", labelMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("struct_attr_values", queryMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("pos_attr_names", labelMultiValMapping)
	pqueryMapping.AddFieldMappingsAt("pos_attr_values", queryMultiValMapping)

	indexMapping.AddDocumentMapping("pquery", pqueryMapping)

	return indexMapping, nil
}
