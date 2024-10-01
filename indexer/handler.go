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
	"net/http"
	"strconv"

	"github.com/czcorpus/cnc-gokit/uniresp"
	"github.com/gin-gonic/gin"
)

const (
	defaultNumRecentRecs = 100
)

type Actions struct {
	indexer *Indexer
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

	numProc, err := a.indexer.IndexRecentRecords(iNumRec)
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	count, err := a.indexer.Count()
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

func (a *Actions) Search(ctx *gin.Context) {
	rec, err := a.indexer.Search(ctx.Query("q"))
	if err != nil {
		uniresp.RespondWithErrorJSON(ctx, err, http.StatusInternalServerError)
		return
	}
	uniresp.WriteJSONResponse(ctx.Writer, rec)

}

func NewActions(indexer *Indexer) *Actions {
	return &Actions{indexer: indexer}
}
