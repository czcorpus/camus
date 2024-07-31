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
	"fmt"
	"net/http"
	"time"

	"github.com/czcorpus/cnc-gokit/logging"
	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type apiServer struct {
	server *http.Server
	conf   *cnf.Conf
	arch   *archiver.ArchKeeper
}

func (api *apiServer) Start(ctx context.Context) {
	if !api.conf.LogLevel.IsDebugMode() {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(logging.GinMiddleware())
	engine.NoMethod(uniresp.NoMethodHandler)
	engine.NoRoute(uniresp.NotFoundHandler)

	handler := Actions{BgJob: api.arch}

	engine.GET("/overview", handler.Overview)
	engine.GET("/record/:id", handler.GetRecord)
	engine.GET("/validate/:id", handler.Validate)
	engine.POST("/fix/:id", handler.Fix)

	api.server = &http.Server{
		Handler:      engine,
		Addr:         fmt.Sprintf("%s:%d", api.conf.ListenAddress, api.conf.ListenPort),
		WriteTimeout: time.Duration(api.conf.ServerWriteTimeoutSecs) * time.Second,
		ReadTimeout:  time.Duration(api.conf.ServerReadTimeoutSecs) * time.Second,
	}

	go func() {
		if err := api.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("server error")
		}
	}()
}

func (s *apiServer) Stop(ctx context.Context) error {
	log.Warn().Msg("shutting down http api server")
	return s.server.Shutdown(ctx)
}
