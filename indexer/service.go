// Copyright 2024 Tomas Machalek <tomas.machalek@gmail.com>
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
	"camus/archiver"
	"camus/cncdb"
	"context"

	"github.com/rs/zerolog/log"
)

type Service struct {
	indexer *Indexer
	redis   *archiver.RedisAdapter
}

func (service *Service) Indexer() *Indexer {
	return service.indexer
}

func (service *Service) Start(ctx context.Context) {
	log.Info().
		Str("redisHost", service.redis.String()).
		Msg("starting indexer.Service task")
	go func() {
		for range ctx.Done() {
			log.Info().Msg("about to close fulltext Service")
			return
		}
	}()
}

func (service *Service) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping indexer.Service task")
	return nil
}

func (service *Service) GetRecord(ident string) (cncdb.QueryArchRec, error) {
	return service.redis.GetConcRecord(ident)
}

func NewService(
	conf *Conf,
	indexer *Indexer,
	redis *archiver.RedisAdapter,
) *Service {
	return &Service{
		indexer: indexer,
		redis:   redis,
	}
}
