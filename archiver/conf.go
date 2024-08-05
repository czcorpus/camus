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
	"camus/util"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	dfltPreloadLastNItems = 500
)

type Conf struct {
	DDStateFilePath    string `json:"ddStateFilePath"`
	CheckIntervalSecs  int    `json:"checkIntervalSecs"`
	CheckIntervalChunk int    `json:"checkIntervalChunk"`
	PreloadLastNItems  int    `json:"preloadLastNItems"`
}

func (conf *Conf) CheckInterval() time.Duration {
	return time.Duration(conf.CheckIntervalSecs) * time.Second
}

func (conf *Conf) ValidateAndDefaults() error {
	if conf == nil {
		return fmt.Errorf("missing `archiver` section")
	}
	if conf.DDStateFilePath == "" {
		return fmt.Errorf("missing path to deduplicator state file (ddStateFilePath)")
	}

	tmp, err := util.NearestPrime(conf.CheckIntervalSecs)
	if err != nil {
		return fmt.Errorf("failed to tune ops timing: %w", err)
	}
	if tmp != conf.CheckIntervalSecs {
		log.Warn().
			Int("oldValue", conf.CheckIntervalSecs).
			Int("newValue", tmp).
			Msg("tuned value of checkIntervalSecs so it cannot be easily overlapped by other timers")
		conf.CheckIntervalSecs = tmp
	}

	if conf.PreloadLastNItems == 0 {
		conf.PreloadLastNItems = dfltPreloadLastNItems
		log.Warn().
			Int("value", conf.PreloadLastNItems).
			Msg("archiver value `preloadLastNItems` not set, using default")
	}

	return nil
}
