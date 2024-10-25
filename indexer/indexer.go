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
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type requirement string

type searchedTerm struct {
	Field       string      `json:"field"`
	Value       string      `json:"value"`
	Requirement requirement `json:"requirement"`
	IsWildcard  bool        `json:"isWildCard"`
}

type Indexer struct {
	conf        *Conf
	db          cncdb.IMySQLOps
	rdb         *archiver.RedisAdapter
	bleveIdx    bleve.Index
	dataPath    string
	recsToIndex <-chan cncdb.HistoryRecord
}

func (idx *Indexer) DocCount() (uint64, error) {
	return idx.bleveIdx.DocCount()
}

func (idx *Indexer) DataPath() string {
	return idx.dataPath
}

// IndexRecentRecords takes latest `numLatest` records and
// (re)indexes them. It returns number of actually indexed
// records and possible error. In case there are unindexable
// records among the ones fetched for processing (which is a normal
// - non error thing - e.g. sample, shuffle, filter,...),
// such records are ignored.
func (idx *Indexer) IndexRecentRecords(numLatest int) (int, error) {
	history, err := idx.db.LoadRecentNHistory(numLatest)
	if err != nil {
		return 0, fmt.Errorf("failed to index records: %w", err)
	}
	var numIndexed int
	for _, hRec := range history {
		hRec.Rec, err = idx.GetConcRecord(hRec.QueryID)
		if err != nil {
			log.Error().Err(err).Msgf("failed to get record %s", hRec.QueryID)
			continue
		} else if hRec.Rec != nil {
			log.Debug().Any("item", hRec).Msg("about to store item to Bleve index")
			indexed, err := idx.IndexRecord(&hRec)
			if !indexed && err == nil {
				continue

			} else if err != nil {
				log.Error().Err(err).Any("hRec", hRec).Msg("invalid record, skipping")
				continue
			}
			numIndexed++
		}
	}
	return numIndexed, nil
}

// RecToDoc converts a conc/wlist/... archive record into an indexable
// document. In case the record is OK but of an unsupported type (e.g. "shuffle"),
// nil document is returned along with ErrRecordNotIndexable error.
func (idx *Indexer) RecToDoc(hRec *cncdb.HistoryRecord) (IndexableMidDoc, error) {
	var rec cncdb.UntypedQueryRecord
	if err := json.Unmarshal([]byte(hRec.Rec.Data), &rec); err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	qstype, err := rec.GetSupertype()
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	if !qstype.IsIndexable() {
		return nil, ErrRecordNotIndexable
	}
	var ans IndexableMidDoc
	switch qstype {
	case cncdb.QuerySupertypeConc:
		ans, err = importConc(&rec, qstype, hRec, idx.db)
	case cncdb.QuerySupertypeWlist:
		ans, err = importWlist(&rec, qstype, hRec, idx.db)
	case cncdb.QuerySupertypeKwords:
		ans, err = importKwords(&rec, qstype, hRec, idx.db)
	case cncdb.QuerySupertypePquery:
		ans, err = importPquery(&rec, qstype, hRec, idx.db, idx.rdb)
	default:
		err = ErrRecordNotIndexable
	}
	return ans, err
}

// IndexRecord indexes a provided archive record. The returned bool
// specifies whether the record was indexed. It is perfectly OK if
// a provided document is not indexed and without returned error
// as not all records we deal with are supported for indexing
// (e.g. additional stages of concordance queries - like shuffle,
// filter, ...)
func (idx *Indexer) IndexRecord(hRec *cncdb.HistoryRecord) (bool, error) {
	doc, err := idx.RecToDoc(hRec)
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
	log.Debug().Str("id", hRec.QueryID).Msg("indexed record")
	return true, nil
}

func (idx *Indexer) Count() (uint64, error) {
	return idx.bleveIdx.DocCount()
}

// SearchWithQuery is intended for human interface as it exposes Bleve's
// query language (stuff like `author: "Doe" +type: fiction -subtype: romance`)
func (idx *Indexer) SearchWithQuery(q string, limit int, order []string, fields []string) (*bleve.SearchResult, error) {
	query := bleve.NewQueryStringQuery(q)
	search := bleve.NewSearchRequest(query)
	search.Size = limit
	if len(order) > 0 {
		search.SortBy(order)
	} else {
		search.SortBy([]string{"-_score", "-created"})
	}
	if len(fields) > 0 {
		search.Fields = fields
	} else {
		search.Fields = []string{"*"}
	}
	return idx.bleveIdx.Search(search)
}

// Search provides a search interface for other applications
func (idx *Indexer) Search(terms []searchedTerm, limit int, order []string, fields []string) (*bleve.SearchResult, error) {
	boolQuery := bleve.NewBooleanQuery()
	for _, term := range terms {
		var addQueryFn func(m ...query.Query)
		switch term.Requirement {
		case "must":
			addQueryFn = boolQuery.AddMust
		case "must-not":
			addQueryFn = boolQuery.AddMustNot
		case "should":
			addQueryFn = boolQuery.AddShould
		default:
			return nil, fmt.Errorf("unexpected query object requirement: \"%s\"", term.Requirement)
		}
		if term.IsWildcard {
			wc := bleve.NewWildcardQuery("*" + term.Value + "*")
			wc.SetField(term.Field)
			addQueryFn(wc)

		} else {
			wc := bleve.NewMatchQuery(term.Value)
			wc.SetField(term.Field)
			addQueryFn(wc)
		}
	}
	search := bleve.NewSearchRequest(boolQuery)
	search.Size = limit
	if len(order) > 0 {
		search.SortBy(order)
	} else {
		search.SortBy([]string{"-_score", "-created"})
	}
	if len(fields) > 0 {
		search.Fields = fields
	} else {
		search.Fields = []string{"*"}
	}
	return idx.bleveIdx.Search(search)
}

func (idx *Indexer) Update(hRec *cncdb.HistoryRecord) error {
	rec, err := idx.GetConcRecord(hRec.QueryID)
	if err != nil {
		return err
	} else if rec == nil {
		return fmt.Errorf("query not found: %s", hRec.QueryID)
	}
	hRec.Rec = rec
	log.Debug().Any("item", hRec).Msg("about to store item to Bleve index")
	_, err = idx.IndexRecord(hRec)
	return err
}

func (idx *Indexer) Delete(recID string) error {
	return idx.bleveIdx.Delete(recID)
}

func (idx *Indexer) GetConcRecord(queryID string) (*cncdb.ArchRecord, error) {
	rec, err := idx.rdb.GetConcRecord(queryID)
	if err == cncdb.ErrRecordNotFound {
		log.Info().Str("queryId", queryID).Msg("record not found in Redis, trying MySQL")
		recs, err := idx.db.LoadRecordsByID(queryID)
		if err != nil {
			return nil, fmt.Errorf("failed to load query %s from MySQL: %w", queryID, err)
		}
		if len(recs) == 0 {
			log.Warn().Str("queryId", queryID).Msg("record is gone - cannot process, ignoring")
			return nil, nil
		}
		rec = recs[0]

	} else if err != nil {
		return nil, fmt.Errorf("failed to process query %s: %w", queryID, err)
	}
	return &rec, nil
}

// Start initializes and runs Indexer
func (idx *Indexer) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close ArchKeeper")
				return
			case hRec := <-idx.recsToIndex:
				if _, err := idx.IndexRecord(&hRec); err != nil {
					log.Error().Err(err).Any("hRec", hRec).Msg("unable to index record")
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
	recsToIndex <-chan cncdb.HistoryRecord,
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
		dataPath:    conf.IndexDirPath,
	}, nil
}

type asyncIndexerRes struct {
	value *Indexer
	err   error
}

func NewIndexerOrDie(
	conf *Conf,
	db cncdb.IMySQLOps,
	rdb *archiver.RedisAdapter,
	recsToIndex <-chan cncdb.HistoryRecord,
) (*Indexer, error) {
	resultChan := make(chan asyncIndexerRes, 1)
	go func() {
		res, err := NewIndexer(conf, db, rdb, recsToIndex)
		resultChan <- asyncIndexerRes{res, err}
	}()

	select {
	case ans := <-resultChan:
		return ans.value, ans.err
	case <-time.After(time.Second * 10):
		fmt.Println("Failed to open index due to timeout. The index is likely in use.")
		os.Exit(10)
	}
	return nil, fmt.Errorf("failed to open index - unknown error")
}
