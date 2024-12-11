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
	"fmt"
	"time"

	"github.com/czcorpus/cnc-gokit/datetime"
	"github.com/czcorpus/cnc-gokit/fs"
)

// Conf contains indexer's configuration as obtained
// from a JSON file (or chunk). Please note that the
// instance should be treated as ready only after
// ValidateAndDefaults is called. Otherwise, it may
// provide incorrect or inconsistent data.
type Conf struct {
	IndexDirPath            string `json:"indexDirPath"`
	QueryHistoryNumPreserve int    `json:"queryHistoryNumPreserve"`

	// HistoryCleanupInterval is a string encoded (10s, 1m, 5m30s etc.)
	// interval specifying how often will Camus look for outdated/excessing
	// records for each user.
	HistoryCleanupInterval string `json:"historyCleanupInterval"`
}

func (conf *Conf) HistoryCleanupIntervalDur() time.Duration {
	dur, err := datetime.ParseDuration(conf.HistoryCleanupInterval)
	if err != nil {
		panic(err) // we expect users to call ValidateAndDefaults() which
		// checks for this too in a more graceful way so we can afford
		// to panic here
	}
	return dur
}

func (conf *Conf) ValidateAndDefaults() error {
	if conf == nil {
		return fmt.Errorf("missing `indexer` section")
	}
	if conf.IndexDirPath == "" {
		return fmt.Errorf("missing path to index dir (indexDirPath)")
	}
	isDir, err := fs.IsDir(conf.IndexDirPath)
	if err != nil {
		return err
	} else if !isDir {
		return fmt.Errorf("index dir does not exist (indexDirPath)")
	}
	if conf.QueryHistoryNumPreserve <= 0 {
		return fmt.Errorf("queryHistoryNumPreserve not specified (recommended > 100)")
	}
	if dur, err := datetime.ParseDuration(conf.HistoryCleanupInterval); err != nil || dur == 0 {
		if err != nil {
			return fmt.Errorf("failed to validate historyCleanupInterval: %w", err)
		}
		if dur == 0 {
			return fmt.Errorf("historyCleanupInterval must be > 0")
		}
	}
	return nil
}
