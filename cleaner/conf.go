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

package cleaner

import (
	"camus/util"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	dfltStatusKey            = "camus_cleanup_status"
	minAllowedCheckInterval  = 10
	minAgeDaysUnvisitedLimit = 30 //365
	dfltNightItemsIncrease   = 2
)

type Conf struct {
	CheckIntervalSecs           int    `json:"checkIntervalSecs"`
	NumProcessItemsPerTick      int    `json:"numProcessItemsPerTick"`
	NumProcessItemsPerTickNight int    `json:"numProcessItemsPerTickNight"`
	StatusKey                   string `json:"statusKey"`
	MinAgeDaysUnvisited         int    `json:"minAgeDaysUnvisited"`
}

func (conf Conf) CheckInterval() time.Duration {
	return time.Duration(conf.CheckIntervalSecs) * time.Second
}

func (conf Conf) MinAgeUnvisited() time.Duration {
	return time.Duration(conf.MinAgeDaysUnvisited) * time.Hour * 24
}

func (conf *Conf) ValidateAndDefaults(opsCheckIntervalSecs int) error {
	if conf == nil {
		return fmt.Errorf("missing `cleaner` section")
	}
	if conf.CheckIntervalSecs < minAllowedCheckInterval {
		return fmt.Errorf(
			"invalid value %d for checkIntervalSecs (must be >= %d)",
			conf.CheckIntervalSecs, minAllowedCheckInterval,
		)
	}
	tmp, err := util.NearestPrime(conf.CheckIntervalSecs)
	if err != nil {
		return fmt.Errorf("failed to tune cleaner timing: %w", err)
	}
	if tmp == opsCheckIntervalSecs {
		tmp, err = util.NearestPrime(tmp + 1)
		if err != nil {
			return fmt.Errorf("failed to tune cleaner timing: %w", err)
		}
	}
	if tmp != conf.CheckIntervalSecs {
		log.Warn().
			Int("oldValue", conf.CheckIntervalSecs).
			Int("newValue", tmp).
			Msg("tuned value of checkIntervalSecs so it does not overlap with ops interval")
		conf.CheckIntervalSecs = tmp
	}
	if conf.NumProcessItemsPerTick < 1 || conf.NumProcessItemsPerTick > 5000 {
		return fmt.Errorf("invalid value for numProcessItemsPerTick (must be between 1 and 5000)")
	}
	if conf.NumProcessItemsPerTickNight == 0 {
		conf.NumProcessItemsPerTickNight = conf.NumProcessItemsPerTick * dfltNightItemsIncrease
		log.Warn().
			Int("value", conf.NumProcessItemsPerTickNight).
			Msg("cleanup configuration `numProcessItemsPerTickNight` not defined - using calculated default")
	}
	if conf.StatusKey == "" {
		log.Warn().Str("value", dfltStatusKey).Msg("cleanup configuration `statusKey` missing, using default")
		conf.StatusKey = dfltStatusKey
	}
	if conf.MinAgeDaysUnvisited < minAgeDaysUnvisitedLimit {
		return fmt.Errorf("cleanup configuration `minAgeDaysUnvisited` invalid (must be >= %d)", minAgeDaysUnvisitedLimit)
	}
	return nil
}
