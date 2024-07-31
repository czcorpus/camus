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
	"fmt"
	"net/http"
	"strings"

	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
)

type visitedIds map[string]int

func (v visitedIds) containsCycle() bool {
	for _, cnt := range v {
		if cnt > 1 {
			return true
		}
	}
	return false
}

func (v visitedIds) IDList() []string {
	ans := make([]string, 0, len(v))
	for k := range v {
		ans = append(ans, k)
	}
	return ans
}

// ------

type Actions struct {
	BgJob *archiver.ArchKeeper
}

func (a *Actions) Overview(ctx *gin.Context) {
	ans := make(map[string]any)
	ans["archiver"] = a.BgJob.GetStats()
	uniresp.WriteJSONResponse(ctx.Writer, ans)
}

func (a *Actions) GetRecord(ctx *gin.Context) {
	rec, err := a.BgJob.LoadRecordsByID(ctx.Param("id"))
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError) // TODO
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, rec)
}

func (a *Actions) Validate(ctx *gin.Context) {
	currID := ctx.Param("id")
	visitedIDs := make(visitedIds)
	for currID != "" {
		visitedIDs[currID]++
		if visitedIDs.containsCycle() {
			uniresp.WriteJSONResponse(
				ctx.Writer,
				map[string]any{"message": fmt.Sprintf("Possible cycle in %s", currID)},
			)
			return
		}
		recs, err := a.BgJob.LoadRecordsByID(currID)
		if err != nil {
			uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError) // TODO
			return
		}
		queryVariants := make(map[string]int)
		var reprData archiver.GeneralDataRecord
		for _, rec := range recs {
			data, err := rec.FetchData()
			if err != nil {
				uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError) // TODO
				return
			}
			queryVariants[strings.Join(data.GetQuery(), " ")]++
			reprData = data
		}
		if len(queryVariants) > 1 {
			uniresp.WriteJSONResponse(
				ctx.Writer,
				map[string]any{"message": "Inconsistent query across instances"},
			)
			return
		}
		currID = reprData.GetPrevID()
	}
	uniresp.WriteJSONResponse(
		ctx.Writer,
		map[string]any{
			"message":    "OK",
			"visitedIds": visitedIDs.IDList(),
		},
	)
}
