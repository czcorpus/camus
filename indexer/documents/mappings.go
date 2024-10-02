package documents

import (
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/v2/mapping"
)

func CreateMapping() (mapping.IndexMapping, error) {

	// whole index
	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultAnalyzer = simple.Name

	err := indexMapping.AddCustomAnalyzer(
		"kontext_query_analyzer",
		map[string]interface{}{
			"type":      "custom",
			"tokenizer": "whitespace",
			"token_filters": []string{
				"lowercase",
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fulltext mappings: %w", err)
	}

	// field types
	exactStringMapping := bleve.NewKeywordFieldMapping()
	multiValMapping := bleve.NewTextFieldMapping()
	multiValMapping.Analyzer = "kontext_query_analyzer"
	dtMapping := bleve.NewDateTimeFieldMapping()

	// conc type
	concMapping := bleve.NewDocumentMapping()
	concMapping.AddFieldMappingsAt("id", exactStringMapping)
	concMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	concMapping.AddFieldMappingsAt("created", dtMapping)
	concMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	concMapping.AddFieldMappingsAt("is_simple_query", exactStringMapping)
	concMapping.AddFieldMappingsAt("corpora", multiValMapping)
	concMapping.AddFieldMappingsAt("subcorpus", multiValMapping)
	concMapping.AddFieldMappingsAt("raw_query", multiValMapping)
	concMapping.AddFieldMappingsAt("structures", multiValMapping)
	concMapping.AddFieldMappingsAt("struct_attr_names", multiValMapping)
	concMapping.AddFieldMappingsAt("struct_attr_values", multiValMapping)
	concMapping.AddFieldMappingsAt("pos_attr_names", multiValMapping)
	concMapping.AddFieldMappingsAt("pos_attr_values", multiValMapping)

	indexMapping.AddDocumentMapping("conc", concMapping)

	// wlist type

	wlistMapping := bleve.NewDocumentMapping()
	wlistMapping.AddFieldMappingsAt("id", exactStringMapping)
	wlistMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	wlistMapping.AddFieldMappingsAt("created", dtMapping)
	wlistMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	wlistMapping.AddFieldMappingsAt("corpora", multiValMapping)
	wlistMapping.AddFieldMappingsAt("subcorpus", multiValMapping)
	wlistMapping.AddFieldMappingsAt("raw_query", multiValMapping)
	wlistMapping.AddFieldMappingsAt("pos_attr_names", multiValMapping)
	wlistMapping.AddFieldMappingsAt("pfilter_words", multiValMapping)
	wlistMapping.AddFieldMappingsAt("nfilter_words", multiValMapping)

	indexMapping.AddDocumentMapping("wlist", wlistMapping)

	// kwords type
	kwordsMapping := bleve.NewDocumentMapping()
	kwordsMapping.AddFieldMappingsAt("id", exactStringMapping)
	kwordsMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	kwordsMapping.AddFieldMappingsAt("created", dtMapping)
	kwordsMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	kwordsMapping.AddFieldMappingsAt("corpora", multiValMapping)
	kwordsMapping.AddFieldMappingsAt("subcorpus", multiValMapping)
	kwordsMapping.AddFieldMappingsAt("raw_query", multiValMapping)
	kwordsMapping.AddFieldMappingsAt("pos_attr_names", multiValMapping)

	indexMapping.AddDocumentMapping("kwords", kwordsMapping)

	// pquery type
	pqueryMapping := bleve.NewDocumentMapping()
	pqueryMapping.AddFieldMappingsAt("id", exactStringMapping)
	pqueryMapping.AddFieldMappingsAt("query_supertype", exactStringMapping)
	pqueryMapping.AddFieldMappingsAt("created", dtMapping)
	pqueryMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	pqueryMapping.AddFieldMappingsAt("corpora", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("subcorpus", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("raw_query", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("structures", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("struct_attr_names", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("struct_attr_values", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("pos_attr_names", multiValMapping)
	pqueryMapping.AddFieldMappingsAt("pos_attr_values", multiValMapping)

	indexMapping.AddDocumentMapping("pquery", pqueryMapping)

	return indexMapping, nil
}
