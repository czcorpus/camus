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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractCQLProps(t *testing.T) {
	doc := MidConc{
		RawQueries: []cncdb.RawQuery{
			{
				Value: `[word="hi|hello"] [lemma="people" & tag="N.*" & word="p.*"] within <text txtypegroup="FIC: beletrie">`,
				Type:  "advanced",
			},
		},
	}
	form := &cncdb.ConcFormRecord{Q: []string{"aword,[]"}}
	err := ExtractQueryProps(form, &doc)
	assert.NoError(t, err)
	assert.Equal(t, []string{"hi|hello", "p.*"}, doc.PosAttrs["word"])
	assert.Equal(t, []string{"people"}, doc.PosAttrs["lemma"])
	assert.Equal(t, []string{"N.*"}, doc.PosAttrs["tag"])
	assert.Equal(t, []string{"text"}, doc.Structures)
	assert.Equal(t, []string{"FIC: beletrie"}, doc.StructAttrs["text.txtypegroup"])
}

func TestExtractCQLPropsWithDefaultAttr(t *testing.T) {
	doc := MidConc{
		RawQueries: []cncdb.RawQuery{
			{
				Value: `"party"`,
				Type:  "advanced",
			},
		},
	}
	form := &cncdb.ConcFormRecord{Q: []string{"aword,[]"}}
	err := ExtractQueryProps(form, &doc)
	assert.NoError(t, err)
	assert.Equal(t, []string{"party"}, doc.PosAttrs["word"])
}
