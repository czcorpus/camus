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
	"camus/cncdb"
	"net/http"

	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
)

type Actions struct {
	service *Service
}

func (a *Actions) RecordToDoc(ctx *gin.Context) {
	rec, err := a.service.GetRecord(ctx.Query("id"))
	if err == cncdb.ErrRecordNotFound {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusNotFound)
		return
	}
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	doc, err := RecToDoc(&rec)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, doc)

}

type queryHistRec struct {
	QueryID string `json:"query_id"`
	UserID  int    `json:"user_id"`
	Created int    `json:"created"`
}

type queryHistRecList []queryHistRec

func (a *Actions) RemoveFromIndex(ctx *gin.Context) {
	var recList queryHistRecList
	if err := ctx.ShouldBindJSON(&recList); err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusBadRequest)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, map[string]any{"ok": true, "records": recList})
}

func NewActions(service *Service) *Actions {
	return &Actions{service: service}
}
