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

package main

import (
	"camus/archiver"
	"camus/cleaner"
	"camus/cncdb"
	"camus/cnf"
	"camus/reporting"
	"camus/search"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/czcorpus/cnc-gokit/logging"
	"github.com/rs/zerolog/log"
)

var (
	version   string
	buildDate string
	gitCommit string
)

type VersionInfo struct {
	Version   string `json:"version"`
	BuildDate string `json:"buildDate"`
	GitCommit string `json:"gitCommit"`
}

type service interface {
	Start(ctx context.Context)
	Stop(ctx context.Context) error
}

func createArchiver(
	db cncdb.IMySQLOps,
	rdb *archiver.RedisAdapter,
	reporting reporting.IReporting,
	conf *cnf.Conf,
) *archiver.ArchKeeper {
	dedup, err := archiver.NewDeduplicator(db, conf.Archiver, conf.TimezoneLocation())
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize deduplicator")
		os.Exit(1)
		return nil
	}
	return archiver.NewArchKeeper(
		rdb,
		db,
		dedup,
		reporting,
		conf.TimezoneLocation(),
		conf.Archiver,
	)
}

func cleanVersionInfo(v string) string {
	return strings.TrimLeft(strings.Trim(v, "'"), "v")
}

func main() {
	version := VersionInfo{
		Version:   cleanVersionInfo(version),
		BuildDate: cleanVersionInfo(buildDate),
		GitCommit: cleanVersionInfo(gitCommit),
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Camus - Concordance Archive Manager by and for US\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n\t%s [options] start [config.json]\n\t", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "%s [options] version\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	dryRun := flag.Bool(
		"dry-run", false, "If set, then instead of writing to database, Camus will just report operations to the log")
	dryRunCleaner := flag.Bool(
		"dry-run-cleaner", false, "If set, the Cleaner service will just report operations to log without writing them to database")
	flag.Parse()
	action := flag.Arg(0)
	if action == "version" {
		fmt.Printf("mquery %s\nbuild date: %s\nlast commit: %s\n", version.Version, version.BuildDate, version.GitCommit)
		return
	}
	conf := cnf.LoadConfig(flag.Arg(1))
	logging.SetupLogging(conf.LogFile, conf.LogLevel)
	log.Info().Msg("Starting Camus")
	cnf.ValidateAndDefaults(conf)
	syscallChan := make(chan os.Signal, 1)
	signal.Notify(syscallChan, os.Interrupt)
	signal.Notify(syscallChan, syscall.SIGTERM)
	exitEvent := make(chan os.Signal)
	jobExitEvent := make(chan os.Signal)
	go func() {
		evt := <-syscallChan
		exitEvent <- evt
		jobExitEvent <- evt
		close(exitEvent)
		close(jobExitEvent)
	}()

	switch action {
	case "start":
		db, err := cncdb.DBOpen(conf.MySQL)
		if err != nil {
			log.Error().Err(err).Msg("Failed to open SQL database")
			os.Exit(1)
			return
		}
		rdb := archiver.NewRedisAdapter(conf.Redis)

		var reportingService reporting.IReporting
		if conf.Reporting.Host != "" {
			reportingService, err = reporting.NewStatusWriter(
				conf.Reporting,
				conf.TimezoneLocation(),
				func(err error) {},
			)
			if err != nil {
				log.Error().Err(err).Msg("Failed to initialize reporting")
				os.Exit(1)
				return
			}

		} else {
			reportingService = &reporting.DummyWriter{}
		}

		var dbOps cncdb.IMySQLOps
		dbOpsRaw := cncdb.NewMySQLOps(db, conf.TimezoneLocation())
		if *dryRun {
			dbOps = cncdb.NewMySQLDryRun(dbOpsRaw)

		} else {
			dbOps = dbOpsRaw
		}

		arch := createArchiver(dbOps, rdb, reportingService, conf)

		var cleanerDbOps cncdb.IMySQLOps
		if *dryRunCleaner {
			cleanerDbOps = cncdb.NewMySQLDryRun(dbOpsRaw)

		} else {
			cleanerDbOps = dbOps
		}

		cln := cleaner.NewService(cleanerDbOps, rdb, reportingService, conf.Cleaner, conf.TimezoneLocation())

		fulltext := search.NewService(rdb) // TODO attach to some filesystem location etc.

		as := &apiServer{
			arch:            arch,
			conf:            conf,
			fulltextService: fulltext,
		}

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		services := []service{arch, cln, fulltext, as, reportingService}
		for _, m := range services {
			m.Start(ctx)
		}
		<-ctx.Done()
		log.Warn().Msg("shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		for _, s := range services {
			wg.Add(1)
			go func(srv service) {
				defer wg.Done()
				if err := srv.Stop(shutdownCtx); err != nil {
					log.Error().Err(err).Type("service", srv).Msg("Error shutting down service")
				}
			}(s)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			log.Info().Msg("Graceful shutdown completed")
		case <-shutdownCtx.Done():
			log.Warn().Msg("Shutdown timed out")
		}

	default:
		log.Fatal().Msgf("Unknown action %s", action)
	}

}
