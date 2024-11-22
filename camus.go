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
	"camus/history"
	"camus/indexer"
	"camus/reporting"
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
	recsToIndex chan<- cncdb.HistoryRecord,
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
		recsToIndex,
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
		fmt.Fprintf(os.Stderr, "Usage:\n\t%s [options] start [config.json]\n", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\t%s [options] init-query-history [config.json]\n", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\t%s [options] gc-query-history [config.json]\n", filepath.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\t%s [options] version\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	startCmd := flag.NewFlagSet("start", flag.ExitOnError)
	startCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "Camus - start the service\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options] start [config.json]\n", filepath.Base(os.Args[0]))
		startCmd.PrintDefaults()
	}
	dryRun := startCmd.Bool(
		"dry-run", false, "If set, then instead of writing to database, Camus will just report operations to the log")
	dryRunCleaner := startCmd.Bool(
		"dry-run-cleaner", false, "If set, the Cleaner service will just report operations to log without writing them to database")

	initQHCmd := flag.NewFlagSet("init-query-history", flag.ExitOnError)
	initChunkSize := initQHCmd.Int("chunk-size", 100, "How many items to process per run (can be run mulitple times while preserving proc. state)")
	logToConsole := initQHCmd.Bool("console-log", false, "Log to console (even if a file is specified in config json)")

	gcQueryHistoryCmd := flag.NewFlagSet("gc-query-history", flag.ExitOnError)
	initChunkSize2 := gcQueryHistoryCmd.Int("chunk-size", 100, "How many items to process per run (can be run mulitple times while preserving proc. state)")
	logToConsole2 := gcQueryHistoryCmd.Bool("console-log", false, "Log to console (even if a file is specified in config json)")

	versionCmd := flag.NewFlagSet("version", flag.ExitOnError)
	versionCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "Camus - get version information\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n\t%s version\n", filepath.Base(os.Args[0]))
		versionCmd.PrintDefaults()
	}

	var conf *cnf.Conf
	var action string
	if len(os.Args) > 1 {
		action = os.Args[1]
	}
	switch action {
	case "version":
		versionCmd.Parse(os.Args[2:])
		fmt.Printf("Camus %s\nbuild date: %s\nlast commit: %s\n", version.Version, version.BuildDate, version.GitCommit)
		return
	case "start":
		startCmd.Parse(os.Args[2:])
		conf = cnf.LoadConfig(startCmd.Arg(0))
		logging.SetupLogging(conf.Logging)
		log.Info().Msg("Starting Camus")
		cnf.ValidateAndDefaults(conf)
	case "init-query-history":
		initQHCmd.Parse(os.Args[2:])
		conf = cnf.LoadConfig(initQHCmd.Arg(0))
		if *logToConsole {
			conf.Logging.Path = ""
		}
		logging.SetupLogging(conf.Logging)
		cnf.ValidateAndDefaults(conf)
	case "gc-query-history":
		gcQueryHistoryCmd.Parse(os.Args[2:])
		conf = cnf.LoadConfig(gcQueryHistoryCmd.Arg(0))
		if *logToConsole2 {
			conf.Logging.Path = ""
		}
		logging.SetupLogging(conf.Logging)
		cnf.ValidateAndDefaults(conf)
	default:
		flag.Usage()
		fmt.Fprintf(
			os.Stderr,
			"\nUse 'camus COMMAND -h' for more information about a specific action\n\n",
		)
		os.Exit(0)
	}

	switch action {
	case "start":
		db, err := cncdb.DBOpen(conf.MySQL)
		if err != nil {
			log.Error().Err(err).Msg("Failed to open SQL database")
			os.Exit(1)
			return
		}

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		rdb := archiver.NewRedisAdapter(ctx, conf.Redis)

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
		dbOpsRaw := cncdb.NewMySQLOps(ctx, db, conf.TimezoneLocation())
		if *dryRun {
			dbOps = cncdb.NewMySQLDryRun(dbOpsRaw)

		} else {
			dbOps = dbOpsRaw
		}

		var cleanerDbOps cncdb.IMySQLOps
		if *dryRunCleaner {
			cleanerDbOps = cncdb.NewMySQLDryRun(dbOpsRaw)

		} else {
			cleanerDbOps = dbOps
		}

		recsToIndex := make(chan cncdb.HistoryRecord)

		ftIndexer, err := indexer.NewIndexer(conf.Indexer, dbOps, rdb, recsToIndex)
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize index")
			os.Exit(1)
			return
		}

		arch := createArchiver(dbOps, rdb, recsToIndex, reportingService, conf)

		cln := cleaner.NewService(
			cleanerDbOps, rdb, reportingService, conf.Cleaner, conf.TimezoneLocation())

		fulltext := indexer.NewService(conf.Indexer, ftIndexer, rdb)

		as := &apiServer{
			arch:            arch,
			conf:            conf,
			fulltextService: fulltext,
			rdb:             rdb,
		}

		services := []service{ftIndexer, arch, cln, fulltext, as, reportingService}
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
	case "init-query-history":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		db, err := cncdb.DBOpen(conf.MySQL)
		if err != nil {
			log.Error().Err(err).Msg("Failed to open SQL database")
			os.Exit(1)
			return
		}
		log.Info().Msgf("using database %s@%s", conf.MySQL.Name, conf.MySQL.Host)
		exec := history.NewDataInitializer(
			cncdb.NewMySQLOps(ctx, db, conf.TimezoneLocation()),
			archiver.NewRedisAdapter(ctx, conf.Redis),
		)
		exec.Run(ctx, conf, *initChunkSize)
	case "gc-query-history": // aka garbage-collect-query-history
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		db, err := cncdb.DBOpen(conf.MySQL)
		if err != nil {
			log.Error().Err(err).Msg("Failed to open SQL database")
			os.Exit(1)
			return
		}
		log.Info().Msgf("using database %s@%s", conf.MySQL.Name, conf.MySQL.Host)
		exec := history.NewGarbageCollector(
			cncdb.NewMySQLOps(ctx, db, conf.TimezoneLocation()),
			archiver.NewRedisAdapter(ctx, conf.Redis),
		)
		exec.Run(ctx, conf, *initChunkSize2)

	default:
		log.Fatal().Msgf("Unknown action %s", action)
	}

}
