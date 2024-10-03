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
	"camus/archiver"
	"camus/cncdb"
	"camus/indexer/documents"
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Indexer struct {
	conf        *Conf
	db          cncdb.IMySQLOps
	rdb         *archiver.RedisAdapter
	bleveIdx    bleve.Index
	recsToIndex <-chan cncdb.ArchRecord
}

// IndexRecentRecords takes latest `numLatest` records and
// (re)indexes them. It returns number of actually indexed
// records and possible error. In case there are unindexable
// records among the ones fetched for processing (which is a normal
// - non error thing - e.g. sample, shuffle, filter,...),
// such records are ignored.
func (idx *Indexer) IndexRecentRecords(numLatest int) (int, error) {
	results, err := idx.db.LoadRecentNRecords(numLatest)
	if err != nil {
		return 0, fmt.Errorf("failed to index records: %w", err)
	}
	var numIndexed int
	for _, rec := range results {
		indexed, err := idx.IndexRecord(rec)
		if !indexed && err == nil {
			continue

		} else if err != nil {
			log.Error().Err(err).Any("rec", rec).Msg("invalid record, skipping")
			continue
		}
		numIndexed++
	}
	return numIndexed, nil
}

// IndexRecord indexes a provided archive record. The returned bool
// specifies whether the record was indexed. It is perfectly OK if
// a provided document is not indexed and without returned error
// as not all records we deal with are supported for indexing
// (e.g. additional stages of concordance queries - like shuffle,
// filter, ...)
func (idx *Indexer) IndexRecord(rec cncdb.ArchRecord) (bool, error) {
	doc, err := RecToDoc(&rec, idx.db, idx.rdb)
	if err == ErrRecordNotIndexable {
		return false, nil

	} else if err != nil {
		return false, fmt.Errorf("failed to index record: %w", err)
	}
	docToIndex := doc.AsIndexableDoc()
	if zerolog.GlobalLevel() <= zerolog.DebugLevel {
		spew.Dump(docToIndex)
	}
	err = idx.bleveIdx.Index(docToIndex.GetID(), docToIndex)
	if err != nil {
		return false, fmt.Errorf("failed to index record: %w", err)
	}
	log.Debug().Str("id", rec.ID).Msg("indexed record")
	return true, nil
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
				if _, err := idx.IndexRecord(rec); err != nil {
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

func NewIndexer(
	conf *Conf,
	db cncdb.IMySQLOps,
	rdb *archiver.RedisAdapter,
	recsToIndex <-chan cncdb.ArchRecord,
) (*Indexer, error) {
	bleveIdx, err := bleve.Open(conf.IndexDirPath)
	if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
		mapping, err := documents.CreateMapping()
		if err != nil {
			return nil, err
		}
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
		rdb:         rdb,
		bleveIdx:    bleveIdx,
		recsToIndex: recsToIndex,
	}, nil
}
