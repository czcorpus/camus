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

package cncdb

import (
	"fmt"
	"strings"
	"time"
)

const (
	QuerySupertypeConc        QuerySupertype = "conc"
	QuerySupertypePquery      QuerySupertype = "pquery"
	QuerySupertypeWlist       QuerySupertype = "wlist"
	QuerySupertypeKwords      QuerySupertype = "kwords"
	QuerySupertypeUnsupported QuerySupertype = ""
)

type QuerySupertype string

func (qs QuerySupertype) IsIndexable() bool {
	return qs == QuerySupertypeConc || qs == QuerySupertypePquery || qs == QuerySupertypeWlist ||
		qs == QuerySupertypeKwords
}

func FormTypeToSupertype(ft string) QuerySupertype {
	switch ft {
	case "query":
		return QuerySupertypeConc
	case "wlist":
		return QuerySupertypeWlist
	case "pquery":
		return QuerySupertypePquery
	case "kwords":
		return QuerySupertypeKwords
	default:
		return QuerySupertypeUnsupported
	}
}

type RawQuery struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

type concForm struct {
	FormType          string              `json:"form_type"`
	CurrQueryTypes    map[string]string   `json:"curr_query_types"`
	CurrQueries       map[string]string   `json:"curr_queries"`
	SelectedTextTypes map[string][]string `json:"selected_text_types"`

	// CurrParsedQueries encodes KonText's TypeScript type:
	// {[k:string]:Array<[Array<[string|Array<string>, string]>, boolean]>};
	CurrParsedQueries map[string][]any `json:"curr_parsed_queries"`
}

type wlistForm struct {
	FormType     string   `json:"form_type"`
	WLAttr       string   `json:"wlattr"`
	WLPattern    string   `json:"wlpat"`
	PFilterWords []string `json:"pfilter_words"`
	NFilterWords []string `json:"nfilter_words"`
}

type kwordsForm struct {
	FormType      string `json:"form_type"`
	RefCorpname   string `json:"ref_corpname"`
	RefUsesubcorp string `json:"ref_usesubcorp"`
	WLAttr        string `json:"wlattr"`
	WLPattern     string `json:"wlpat"`
}

type pqueryForm struct {
	FormType string   `json:"form_type"`
	ConcIDs  []string `json:"conc_ids"`
}

type ConcFormRecord struct {
	Q          []string  `json:"q"`
	LastopForm *concForm `json:"lastop_form"`
}

func (cr *ConcFormRecord) GetDefaultAttr() string {
	if len(cr.Q) == 0 || len(cr.Q[0]) == 0 {
		return ""
	}
	tmp := cr.Q[0][1:] // [1:] is ok here (first char is always 1 byte)
	chunks := strings.Split(tmp, ",")
	if len(chunks) >= 2 {
		return chunks[0]
	}
	return ""
}

type WlistFormRecord struct {
	Form wlistForm `json:"form"`
}

type KwordsFormRecord struct {
	Form kwordsForm `json:"form"`
}

type PQueryFormRecord struct {
	Form pqueryForm `json:"form"`
}

// UntypedQueryRecord represents any query record as saved by
// KonText. It is a mix of all possible variants
// (conc, wlist, pquery, kwords) with many data access methods
// for extracting supertype-specific values.
// It is up to the user to determine which access methods to use
// based on GetSupertype().
type UntypedQueryRecord struct {
	ID          string         `json:"id"`
	Created     time.Time      `json:"-"`
	UserID      int            `json:"user_id"`
	Corpora     []string       `json:"corpora"`
	SubcorpusID string         `json:"usesubcorp"`
	LastopForm  map[string]any `json:"lastop_form"`
	Form        map[string]any `json:"form"`
}

func (qr *UntypedQueryRecord) GetSupertype() (QuerySupertype, error) {
	if qr.LastopForm == nil && qr.Form == nil {
		return "", fmt.Errorf("cannot determine query supertype - no known form entry found")
	}
	v, ok := qr.LastopForm["form_type"]
	if !ok {
		v, ok = qr.Form["form_type"]
	}
	if !ok {
		return "", fmt.Errorf("failed to get query supertype - no `form_type` entry found")
	}
	tv, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("type assertion failed on query supertype %s", v)
	}
	st := FormTypeToSupertype(tv)
	return st, nil
}

func (qr *UntypedQueryRecord) GetSubcorpus(db IConcArchOps) (SubcProps, error) {
	return db.GetSubcorpusProps(qr.SubcorpusID)
}
