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
	"camus/cnf"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/czcorpus/cnc-gokit/logging"
	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
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

func runApiServerAndBlock(
	conf *cnf.Conf,
	syscallChan chan os.Signal,
	exitEvent chan os.Signal,
) {
	if !conf.LogLevel.IsDebugMode() {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(logging.GinMiddleware())
	engine.NoMethod(uniresp.NoMethodHandler)
	engine.NoRoute(uniresp.NotFoundHandler)

	handler := Actions{}

	engine.GET("/overview", handler.Overview)

	log.Info().Msgf("starting to listen at %s:%d", conf.ListenAddress, conf.ListenPort)
	srv := &http.Server{
		Handler:      engine,
		Addr:         fmt.Sprintf("%s:%d", conf.ListenAddress, conf.ListenPort),
		WriteTimeout: time.Duration(conf.ServerWriteTimeoutSecs) * time.Second,
		ReadTimeout:  time.Duration(conf.ServerReadTimeoutSecs) * time.Second,
	}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			log.Error().Err(err).Msg("")
		}
		syscallChan <- syscall.SIGTERM
	}()

	select {
	case <-exitEvent:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err != nil {
			log.Info().Err(err).Msg("Shutdown request error")
		}
	}
}

func runArchiver(conf *cnf.Conf, exitEvent chan os.Signal) {
	rds := archiver.NewRedisAdapter(conf.Redis)
	job := archiver.NewBgJob(
		rds,
		time.Duration(conf.CheckIntervalSecs)*time.Second,
		conf.CheckIntervalChunk,
		exitEvent)
	job.GoRun()
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
		runArchiver(conf, jobExitEvent)
		runApiServerAndBlock(conf, syscallChan, exitEvent)
	default:
		log.Fatal().Msgf("Unknown action %s", action)
	}

}
