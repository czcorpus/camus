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

package archiver

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/bits-and-blooms/bloom"
	"github.com/czcorpus/cnc-gokit/fs"
	"github.com/rs/zerolog/log"
)

const (
	bloomFilterNumBits       = 1000000
	bloomFilterProbCollision = 0.01
)

type Deduplicator struct {
	items           *bloom.BloomFilter
	concDB          *sql.DB
	tz              *time.Location
	preloadLastN    int
	storageFilePath string
}

func (dd *Deduplicator) StoreToDisk() error {
	f, err := os.OpenFile(dd.storageFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to store deduplicator state to disk: %w", err)
	}
	defer f.Close()
	_, err = dd.items.WriteTo(f)
	if err != nil {
		return fmt.Errorf("failed to store deduplicator state to disk: %w", err)
	}
	return nil
}

func (dd *Deduplicator) OnClose() error {
	return dd.StoreToDisk()
}

func (dd *Deduplicator) LoadFromDisk() error {
	f, err := os.Open(dd.storageFilePath)
	if err != nil {
		return fmt.Errorf("failed to load deduplicator state from disk: %w", err)
	}
	defer f.Close()
	_, err = dd.items.ReadFrom(f)
	if err != nil {
		return fmt.Errorf("failed to load deduplicator state from disk: %w", err)
	}
	return nil
}

func (dd *Deduplicator) Add(concID string) {
	dd.items.AddString(concID)
}

func (dd *Deduplicator) Reset() error {
	log.Warn().Msg("performing deduplicator reset")
	dd.items.ClearAll()
	if dd.preloadLastN > 0 {
		return dd.PreloadLastNItems(dd.preloadLastN)
	}
	return nil
}

func (dd *Deduplicator) PreloadLastNItems(num int) error {
	dd.preloadLastN = num
	items, err := LoadRecentNRecords(dd.concDB, num)
	if err != nil {
		return fmt.Errorf("failed to preload last N items: %w", err)
	}
	for _, item := range items {
		dd.Add(item.ID)
	}
	log.Debug().Int("numItems", num).Msg("preloaded items for better deduplication")
	return nil
}

func (dd *Deduplicator) TestRecord(concID string) bool {
	return dd.items.TestString(concID)
}

// TestAndSolve looks for whether the record has been recently used and if so
// it loads and returns the item. It also tries to deduplicate the record
// in the archive database.
// The "recently used" means that we keep track of recently stored IDs and test
// for them only. I.e. we do not perform full search in query persistence db
// for each and every concID we want to store.
func (dd *Deduplicator) TestAndSolve(newRec ArchRecord) (bool, error) {
	if !dd.items.TestString(newRec.ID) {
		return false, nil
	}
	recs, err := LoadRecordsByID(dd.concDB, newRec.ID)
	if err != nil {
		return false, fmt.Errorf("failed to deduplicate id %s: %w", newRec.ID, err)
	}
	if len(recs) == 0 {
		log.Warn().
			Str("concId", newRec.ID).
			Msg("possible Bloom filter false positive")
		return false, nil
	}
	log.Debug().
		Str("concId", newRec.ID).
		Int("numVariants", len(recs)).
		Msg("found archived record")
	queryTest := make(map[string][]ArchRecord)
	for _, rec := range recs {
		_, ok := queryTest[rec.Data]
		if !ok {
			queryTest[rec.Data] = make([]ArchRecord, 0, 10)
		}
		queryTest[rec.Data] = append(queryTest[rec.Data], rec)
	}
	var bestRecKey string
	var largestEntry int
	for k, v := range queryTest {
		if len(v) > largestEntry {
			largestEntry = len(v)
			bestRecKey = k
		}
	}
	if len(queryTest) > 1 {
		for k, v := range queryTest {
			if k == bestRecKey {
				continue
			}
			log.Error().
				Str("concId", newRec.ID).
				Str("query", v[0].Data).
				Msg("Conc. persistence consistency error")
		}
	}
	_, err = DeduplicateInArchive(dd.concDB, queryTest[bestRecKey], newRec, dd.tz)
	return true, err
}

func NewDeduplicator(concDB *sql.DB, loc *time.Location, stateFilePath string) (*Deduplicator, error) {
	filter := bloom.NewWithEstimates(bloomFilterNumBits, bloomFilterProbCollision)
	d := &Deduplicator{
		tz:              loc,
		items:           filter,
		concDB:          concDB,
		storageFilePath: stateFilePath,
	}
	isf, err := fs.IsFile(stateFilePath)
	if err != nil {
		return d, fmt.Errorf("failed to init Deduplicator: %w", err)
	}
	if isf {
		if err := d.LoadFromDisk(); err != nil {
			return d, fmt.Errorf("failed to init Deduplicator: %w", err)
		}
		log.Info().Str("file", stateFilePath).Msg("loaded previously stored dedup. state")
	}
	return d, nil
}
