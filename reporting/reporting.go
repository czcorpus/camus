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
	"fmt"
	"time"

	"github.com/czcorpus/hltscl"
	"github.com/rs/zerolog/log"
)

/*
Expected tables:

create table camus_operations_stats (
  "time" timestamp with time zone NOT NULL,
  num_fetched int,
  num_errors int,
  num_merged int,
  num_inserted int
);

select create_hypertable('camus_operations_stats', 'time');

create table camus_cleanup_stats (
  "time" timestamp with time zone NOT NULL,
  num_fetched int,
  num_merged int,
  num_errors int
);

select create_hypertable('camus_cleanup_stats', 'time');

*/

type StatusWriter struct {
	tableWriter *hltscl.TableWriter
	dataCh      chan<- hltscl.Entry
	errCh       <-chan hltscl.WriteError
	location    *time.Location
}

func (job *StatusWriter) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("about to close ArchKeeper")
				return
			case err := <-job.errCh:
				log.Error().
					Err(err.Err).
					Str("entry", err.Entry.String()).
					Msg("error writing data to TimescaleDB")
				fmt.Println("reporting timescale write err: ", err.Err)
			}
		}
	}()
}

func (job *StatusWriter) Stop(ctx context.Context) error {
	log.Warn().Msg("stopping StatusWriter")
	return nil
}

func (ds *StatusWriter) WriteOperationsStatus(item OpStats) {
	if ds.tableWriter != nil {
		ds.dataCh <- *ds.tableWriter.NewEntry(time.Now()).
			Int("numMerged", item.NumMerged).
			Int("numErrors", item.NumErrors).
			Int("numFetched", item.NumFetched).
			Int("numInserted", item.NumInserted)
	}
}

func (ds *StatusWriter) WriteCleanupStatus(item CleanupStats) {
	if ds.tableWriter != nil {
		ds.dataCh <- *ds.tableWriter.NewEntry(time.Now()).
			Int("numErrors", item.NumErrors).
			Int("numFetched", item.NumFetched).
			Int("numMerged", item.NumMerged)
	}
}

func NewStatusWriter(conf hltscl.PgConf, tz *time.Location, onError func(err error)) (*StatusWriter, error) {

	conn, err := hltscl.CreatePool(conf)
	if err != nil {
		return nil, err
	}
	twriter := hltscl.NewTableWriter(conn, "instance_status", "time", tz)
	dataCh, errCh := twriter.Activate()
	return &StatusWriter{
		tableWriter: twriter,
		dataCh:      dataCh,
		errCh:       errCh,
		location:    tz,
	}, nil
}
