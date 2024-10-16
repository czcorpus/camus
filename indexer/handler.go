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
	"camus/cncdb"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
)

const (
	defaultNumRecentRecs = 100
)

type Actions struct {
	idxService *Service
}

func (a *Actions) IndexLatestRecords(ctx *gin.Context) {
	numRec := ctx.Query("numRec")
	if numRec == "" {
		newURL := *ctx.Request.URL
		newQuery := newURL.Query()
		newQuery.Set("numRec", strconv.Itoa(defaultNumRecentRecs))
		newURL.RawQuery = newQuery.Encode()
		ctx.Redirect(http.StatusSeeOther, newURL.String())
		return
	}

	iNumRec, err := strconv.Atoi(numRec)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusBadRequest)
		return
	}

	numProc, err := a.idxService.Indexer().IndexRecentRecords(iNumRec)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	count, err := a.idxService.Indexer().Count()
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	resp := map[string]any{
		"totalDocuments": count,
		"numProcessed":   numProc,
	}
	uniresp.WriteJSONResponse(ctx.Writer, resp)
}

func (a *Actions) RecordToDoc(ctx *gin.Context) {
	hRec := cncdb.HistoryRecord{
		QueryID: ctx.Query("id"),
	}
	rec, err := a.idxService.GetRecord(hRec.QueryID)
	if err == cncdb.ErrRecordNotFound {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusNotFound)
		return
	}
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	hRec.Rec = &rec
	doc, err := a.idxService.Indexer().RecToDoc(&hRec)
	if err == ErrRecordNotIndexable {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusUnprocessableEntity)
		return

	} else if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, doc)

}

func (a *Actions) Search(ctx *gin.Context) {
	limit, err := strconv.Atoi(ctx.DefaultQuery("limit", "10"))
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusBadRequest)
		return
	}
	order := make([]string, 0, 3)
	if orderParam := ctx.Query("order"); orderParam != "" {
		order = append(order, strings.Split(orderParam, ",")...)
	}
	fields := make([]string, 0, 3)
	if fieldsParam := ctx.Query("fields"); fieldsParam != "" {
		fields = append(order, strings.Split(fieldsParam, ",")...)
	}
	srchQuery := fmt.Sprintf("+user_id:%s %s", ctx.Param("userId"), ctx.Query("q"))
	rec, err := a.idxService.indexer.Search(srchQuery, limit, order, fields)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, rec)
}

func (a *Actions) Update(ctx *gin.Context) {
	hRec := a.getHistoryRecord(ctx)
	if hRec == nil {
		return
	}
	hRec.Name = ctx.Query("name")
	if err := a.idxService.Indexer().Update(hRec); err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, hRec)
}

func (a *Actions) Delete(ctx *gin.Context) {
	hRec := a.getHistoryRecord(ctx)
	if hRec == nil {
		return
	}
	if err := a.idxService.Indexer().Delete(hRec.CreateIndexID()); err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, hRec)
}

func (a *Actions) getHistoryRecord(ctx *gin.Context) *cncdb.HistoryRecord {
	queryID := ctx.Param("queryId")
	userIDStr := ctx.Param("userId")
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, fmt.Errorf("invalid user ID"), http.StatusBadRequest)
		return nil
	}
	createdStr := ctx.Param("created")
	created, err := strconv.Atoi(createdStr)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, fmt.Errorf("invalid `created` unix timestamp"), http.StatusBadRequest)
		return nil
	}

	return &cncdb.HistoryRecord{
		QueryID: queryID,
		UserID:  userID,
		Created: int64(created),
	}
}

func NewActions(idxService *Service) *Actions {
	return &Actions{
		idxService: idxService,
	}
}
