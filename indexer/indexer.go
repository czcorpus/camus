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
	"camus/cncdb"
	"camus/indexer/documents"
	"fmt"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog/log"
)

type Indexer struct {
	conf     *Conf
	db       cncdb.IMySQLOps
	bleveIdx bleve.Index
}

func (idx *Indexer) procMidConc(doc *documents.MidConc) IndexableDoc {
	posAttrNames := make([]string, 0, 5)
	posAttrValues := make([]string, 0, 5)
	for name, values := range doc.PosAttrs {
		posAttrNames = append(posAttrNames, name)
		posAttrValues = append(posAttrValues, values...)
	}

	structAttrNames := make([]string, 0, 5)
	structAttrValues := make([]string, 0, 5)
	for name, values := range doc.StructAttrs {
		structAttrNames = append(structAttrNames, name)
		structAttrValues = append(structAttrValues, values...)
	}
	bDoc := &documents.Concordance{
		ID:               doc.ID,
		Created:          doc.Created,
		UserID:           strconv.Itoa(doc.UserID),
		Corpora:          strings.Join(doc.Corpora, " "),
		Subcorpus:        doc.Subcorpus,
		RawQuery:         doc.GetRawQueriesAsString(),
		Structures:       strings.Join(doc.Structures, " "),
		StructAttrNames:  strings.Join(structAttrNames, " "),
		StructAttrValues: strings.Join(structAttrValues, " "),
		PosAttrNames:     strings.Join(posAttrNames, " "),
		PosAttrValues:    strings.Join(posAttrValues, " "),
	}
	spew.Dump(bDoc)
	return bDoc
}

func (idx *Indexer) IndexRecords() error {
	results, err := idx.db.LoadRecentNRecords(1000)
	if err != nil {
		return err
	}
	for _, rec := range results {
		doc, err := RecToDoc(&rec, idx.db)
		if err == ErrRecordNotIndexable {
			continue

		} else if err != nil {
			log.Error().Err(err).Any("rec", rec).Msg("invalid record")
			continue
		}
		var docToIndex IndexableDoc
		switch tDoc := doc.(type) {
		case *documents.MidConc:
			docToIndex = idx.procMidConc(tDoc)
		}
		if docToIndex == nil {
			log.Error().
				Str("id", doc.GetID()).
				Msg("failed to transform intermediate document to indexable document, skipping")
			continue
		}
		err = idx.bleveIdx.Index(docToIndex.GetID(), docToIndex)
		if err != nil {
			return err
		}
	}
	return nil
}

func (idx *Indexer) Count() (uint64, error) {
	return idx.bleveIdx.DocCount()
}

func (idx *Indexer) Search(q string) (*bleve.SearchResult, error) {
	query := bleve.NewQueryStringQuery(q)
	search := bleve.NewSearchRequest(query)
	search.Fields = []string{"*"}
	search.Size = 20 // TODO !!!
	return idx.bleveIdx.Search(search)
}

func NewIndexer(conf *Conf, db cncdb.IMySQLOps) (*Indexer, error) {
	bleveIdx, err := bleve.Open(conf.IndexDirPath)
	if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
		mapping := documents.CreateMapping()
		bleveIdx, err = bleve.New(conf.IndexDirPath, mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to create new index: %w", err)
		}

	} else if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}
	return &Indexer{
		conf:     conf,
		db:       db,
		bleveIdx: bleveIdx,
	}, nil
}
