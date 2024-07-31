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
	"context"
	"database/sql"
	"time"

	"github.com/rs/zerolog/log"
)

type Service struct {
	conf Conf
	db   *sql.DB
}

func (job *Service) Start(ctx context.Context) {
	ticker := time.NewTicker(job.conf.CheckInterval())
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close Cleaner")
				return
			case <-ticker.C:
				job.performCleanup()
			}
		}
	}()
}

func (job *Service) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping Cleaner")
	return nil
}

func (job *Service) performCleanup() {
	log.Debug().Msg("dummy cleanup")
}

func NewService(db *sql.DB, conf Conf) *Service {
	return &Service{
		conf: conf,
		db:   db,
	}
}
