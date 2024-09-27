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

package search

import (
	"camus/archiver"
	"camus/cncdb"
	"camus/indexer"
	"context"
	"encoding/json"

	"github.com/blevesearch/bleve/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type Service struct {
	index      *bleve.Index
	redis      *archiver.RedisAdapter
	rmChanName string
	rmChan     <-chan *redis.Message
}

func (service *Service) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close fulltext Service")
				return
			case msg := <-service.rmChan:
				var item QueryHistoryIdent
				if err := json.Unmarshal([]byte(msg.Payload), &item); err != nil {
					log.Error().Err(err).Msg("failed to unmarshal next fulltext remove item")
					continue
				}
				log.Debug().Any("item", item).Msg("about to remove item from Bleve index")
				// TODO use Bleve here

			}
		}
	}()
}

func (service *Service) Stop(ctx context.Context) error {
	log.Warn().Msg("shutting down fulltext search service")
	return nil
}

func (service *Service) GetRecord(ident string) (cncdb.ArchRecord, error) {
	return service.redis.GetConcRecord(ident)
}

func (service *Service) TriggerNextRmItem() {
	service.redis.TriggerChan(service.rmChanName, "next")
}

func NewService(
	conf *indexer.Conf,
	redis *archiver.RedisAdapter,
) *Service {
	return &Service{
		redis:      redis,
		rmChan:     redis.ChannelSubscribe(conf.DocRemoveChannel),
		rmChanName: conf.DocRemoveChannel,
	}
}
