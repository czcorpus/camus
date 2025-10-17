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

package indexer

import (
	"camus/cncdb"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func prepareIndexer() *Indexer {
	tempDir, err := os.MkdirTemp("", "test-index")
	if err != nil {
		panic(err)
	}
	conf := Conf{
		IndexDirPath:            tempDir,
		QueryHistoryNumPreserve: 100,
	}
	idxer, err := NewIndexer(&conf, &cncdb.DummyConcArchSQL{}, &cncdb.MySQLQueryHistDryRun{}, nil, nil)
	if err != nil {
		panic(err)
	}
	return idxer
}

func cleanData(tempDir string) {
	os.RemoveAll(tempDir)
}

func TestEscaping(t *testing.T) {
	idxer := prepareIndexer()
	created := time.Now()

	query := "[word=\"doc.*\"]"

	form := map[string]any{
		"form_type":           "query",
		"curr_query_types":    map[string]string{"corp1": "advanced"},
		"curr_queries":        map[string]string{"corp1": query},
		"selected_text_types": map[string][]string{},
	}
	rec := unspecifiedQueryRecord{
		ID:         "foo",
		LastopForm: form,
	}

	rawForm, err := json.Marshal(rec)
	if err != nil {
		panic(err)
	}

	ok, err := idxer.IndexRecord(&cncdb.HistoryRecord{
		QueryID: "foo",
		Created: created.Unix(),
		UserID:  1,
		Name:    "test 1",
		Rec: &cncdb.RawRecord{
			ID:         "foo",
			Data:       string(rawForm),
			Created:    created,
			NumAccess:  1,
			LastAccess: created,
			Permanent:  0,
		},
	})
	assert.NoError(t, err)
	assert.True(t, ok)
	v, err := idxer.DocCount()
	if err != nil {
		panic(err)
	}
	assert.Equal(t, uint64(1), v)

	// perform a query

	result, err := idxer.SearchWithQuery("\\/d.*\\/", 1, []string{"id"}, []string{"id"})
	assert.NoError(t, err)
	assert.Equal(t, 1, result.Hits.Len())

	cleanData(idxer.DataPath())
}
