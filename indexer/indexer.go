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
	"camus/indexer/documents"
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/rs/zerolog/log"
)

type Indexer struct {
	conf        *Conf
	db          cncdb.IMySQLOps
	bleveIdx    bleve.Index
	recsToIndex <-chan cncdb.ArchRecord
}

func (idx *Indexer) IndexRecords() error {
	results, err := idx.db.LoadRecentNRecords(1000)
	if err != nil {
		return fmt.Errorf("failed to index records: %w", err)
	}
	for _, rec := range results {
		err := idx.IndexRecord(rec)
		if err == ErrRecordNotIndexable {
			continue

		} else if err != nil {
			log.Error().Err(err).Any("rec", rec).Msg("invalid record, skipping")
			continue
		}
	}
	return nil
}

func (idx *Indexer) IndexRecord(rec cncdb.ArchRecord) error {
	doc, err := RecToDoc(&rec, idx.db)
	if err != nil {
		return fmt.Errorf("failed to index record: %w", err)
	}
	docToIndex := doc.AsIndexableDoc()
	err = idx.bleveIdx.Index(docToIndex.GetID(), docToIndex)
	if err != nil {
		return fmt.Errorf("failed to index record: %w", err)
	}
	log.Debug().Str("id", rec.ID).Msg("indexed record")
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

// Start initializes and runs Indexer
func (idx *Indexer) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close ArchKeeper")
				return
			case rec := <-idx.recsToIndex:
				if err := idx.IndexRecord(rec); err != nil {
					log.Error().Err(err).Any("rec", rec).Msg("unable to index record")
				}
			}
		}
	}()
}

// Stop stops the ArchKeeper service
func (idx *Indexer) Stop(ctx context.Context) error {
	return nil
}

func NewIndexer(conf *Conf, db cncdb.IMySQLOps, recsToIndex <-chan cncdb.ArchRecord) (*Indexer, error) {
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
		conf:        conf,
		db:          db,
		bleveIdx:    bleveIdx,
		recsToIndex: recsToIndex,
	}, nil
}
