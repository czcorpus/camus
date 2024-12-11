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

package reporting

import (
	"context"

	"github.com/rs/zerolog/log"
)

type DummyWriter struct {
}

func (job *DummyWriter) Start(ctx context.Context) {
	go func() {
		for range ctx.Done() {
			log.Info().Msg("about to close DummyWriter")
			return
		}
	}()
}

func (job *DummyWriter) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping DummyWriter")
	return nil
}

func (job *DummyWriter) WriteOperationsStatus(item OpStats) {
	log.Info().Any("stats", item).Msg("writing dummy operations report")
}

func (job *DummyWriter) WriteCleanupStatus(item CleanupStats) {
	log.Info().Any("stats", item).Msg("writing dummy cleanup report")
}

func (job *DummyWriter) WriteQueryHistoryDeletionStatus(item QueryHistoryDelStats) {
	log.Info().Any("stats", item).Msg("writing dummy query history deletion report")
}
