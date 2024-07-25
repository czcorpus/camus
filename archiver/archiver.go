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
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

type BgJob struct {
	redis              *RedisAdapter
	checkInterval      time.Duration
	checkIntervalChunk int
	doneChan           <-chan os.Signal
}

func (job *BgJob) performCheck() {
	log.Warn().Msg("doing regular check")
	items, err := job.redis.NextNItems(int64(job.checkIntervalChunk))
	if err != nil {
		log.Error().Err(err).Msg("failed to process chunk")
	}
	fmt.Println("ITEMS: ", items)
}

func (job *BgJob) GoRun() {
	ticker := time.NewTicker(job.checkInterval)
	go func() {
		for {
			select {
			case <-job.doneChan:
				return
			case <-ticker.C:
				job.performCheck()
			}
		}
	}()
}

func NewBgJob(redis *RedisAdapter, checkInterval time.Duration, checkIntervalChunk int, doneChan <-chan os.Signal) *BgJob {
	return &BgJob{
		redis:              redis,
		checkInterval:      checkInterval,
		checkIntervalChunk: checkIntervalChunk,
		doneChan:           doneChan,
	}
}
