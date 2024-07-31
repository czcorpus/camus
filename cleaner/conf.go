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
	"fmt"
	"time"
)

type Conf struct {
	CheckIntervalSecs      int    `json:"checkIntervalSecs"`
	NumProcessItemsPerTick int    `json:"numProcessItemsPerTick"`
	StatusKey              string `json:"statusKey"`
}

func (conf Conf) CheckInterval() time.Duration {
	return time.Duration(conf.CheckIntervalSecs) * time.Second
}

func (conf *Conf) ValidateAndDefaults() error {
	if conf.CheckIntervalSecs < 60 {
		return fmt.Errorf("invalid value for checkIntervalSecs (must be >= 60)")
	}
	if conf.NumProcessItemsPerTick < 1 || conf.NumProcessItemsPerTick > 5000 {
		return fmt.Errorf("invalid value for numProcessItemsPerTick (must be between 1 and 5000)")
	}
	return nil
}
