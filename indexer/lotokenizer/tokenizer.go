package lotokenizer

// this package contains "letter only tokenizer" (which is not exactly true but close)
// we use to process values which are more like "labels" (e.g. text types) where we don't
// want to preserve special characters

import (
	"unicode"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/character"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "query_tokenizer"

func TokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	return character.NewCharacterTokenizer(notSpace), nil
}

func isStopChar(r rune) bool {
	stopValues := []rune{
		':', ';', ',', '#', '?', '!', '.', '%', '$', '@', '(', ')', '*', '[', ']', '"', '\'',
		'~', '/', '|', '+', '=', '-', '_', '^', '&', '>', '<'}
	for _, v := range stopValues {
		if v == r {
			return true
		}
	}
	return false
}

func notSpace(r rune) bool {
	return !unicode.IsSpace(r) && !isStopChar(r)
}

func init() {
	registry.RegisterTokenizer(Name, TokenizerConstructor)
}
