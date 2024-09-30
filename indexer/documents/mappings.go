package documents

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/v2/mapping"
)

func CreateMapping() mapping.IndexMapping {

	// field types
	exactStringMapping := bleve.NewKeywordFieldMapping()
	multiValMapping := bleve.NewTextFieldMapping()
	multiValMapping.Analyzer = simple.Name
	dtMapping := bleve.NewDateTimeFieldMapping()

	// whole index
	indexMapping := bleve.NewIndexMapping()
	indexMapping.DefaultAnalyzer = simple.Name

	// conc type
	concMapping := bleve.NewDocumentMapping()
	concMapping.AddFieldMappingsAt("id", exactStringMapping)
	concMapping.AddFieldMappingsAt("created", dtMapping)
	concMapping.AddFieldMappingsAt("user_id", exactStringMapping)
	concMapping.AddFieldMappingsAt("is_simple_query", exactStringMapping)
	concMapping.AddFieldMappingsAt("corpora", multiValMapping)
	concMapping.AddFieldMappingsAt("subcorpus", exactStringMapping)
	concMapping.AddFieldMappingsAt("raw_query", multiValMapping)
	concMapping.AddFieldMappingsAt("structures", multiValMapping)
	concMapping.AddFieldMappingsAt("struct_attr_names", multiValMapping)
	concMapping.AddFieldMappingsAt("struct_attr_values", multiValMapping)
	concMapping.AddFieldMappingsAt("pos_attr_names", multiValMapping)
	concMapping.AddFieldMappingsAt("pos_attr_values", multiValMapping)

	indexMapping.AddDocumentMapping("query", concMapping)

	// wlist type

	return indexMapping
}
