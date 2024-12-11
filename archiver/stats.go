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
	"camus/cncdb"
	"encoding/json"
	"fmt"
	"time"
)

const (
	yearStatsCacheKey = "camus_years_stats"
)

type CountPerYear struct {
	Year  int `json:"year"`
	Count int `json:"count"`
}

type YearsStats struct {
	Years      []CountPerYear `json:"years"`
	LastUpdate time.Time      `json:"lastUpdate"`
}

func (job *ArchKeeper) YearsStats(forceReload bool) (YearsStats, error) {
	var cached string
	var err error
	var ans YearsStats
	if !forceReload {
		cached, err = job.redis.Get(yearStatsCacheKey)
		if err != nil {
			return ans, fmt.Errorf("failed to get cached years stats: %w", err)
		}
	}
	if cached == "" {
		data, err := job.dbArch.GetArchSizesByYears(forceReload)
		if err == cncdb.ErrTooDemandingQuery {
			return ans, nil

		} else if err != nil {
			return ans, fmt.Errorf("failed to load years stats from db: %w", err)
		}
		ans.LastUpdate = time.Now().In(job.tz)
		ans.Years = make([]CountPerYear, len(data))
		for i, item := range data {
			ans.Years[i] = CountPerYear{Year: item[0], Count: item[1]}
		}
		jsonData, err := json.Marshal(ans)
		if err != nil {
			return ans, fmt.Errorf("failed to marshal recent years stats data: %w", err)
		}
		if err := job.redis.Set(yearStatsCacheKey, jsonData); err != nil {
			return ans, fmt.Errorf("failed to store recent years stats to cache: %w", err)
		}

	} else {
		if err := json.Unmarshal([]byte(cached), &ans); err != nil {
			return ans, fmt.Errorf("failed to unmarshal years stats from cache: %w", err)
		}
	}
	return ans, nil
}
