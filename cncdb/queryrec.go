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
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
)

const (
	QuerySupertypeConc   QuerySupertype = "conc"
	QuerySupertypePquery QuerySupertype = "pquery"
	QuerySupertypeWlist  QuerySupertype = "wlist"
	QuerySupertypeKwords QuerySupertype = "kwords"
)

type QuerySupertype string

func (qs QuerySupertype) Validate() error {
	if qs == QuerySupertypeConc || qs == QuerySupertypePquery || qs == QuerySupertypeWlist ||
		qs == QuerySupertypeKwords {
		return nil
	}
	return fmt.Errorf("invalid QuerySupertype: %s", qs)
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
		return ""
	}
}

var (
	ErrUnexpectedRecordStructure = errors.New("unexpected record structure")
)

type RawQuery struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

type QueryRecord struct {
	UserID     int            `json:"user_id"`
	Q          []string       `json:"q"`
	LastopForm map[string]any `json:"lastop_form"`
	Corpora    []string       `json:"corpora"`
	Subcorpus  string         `json:"usesubcorp"`
}

func (qr *QueryRecord) GetSupertype() (QuerySupertype, error) {
	v, ok := qr.LastopForm["form_type"]
	if !ok {
		return "", ErrUnexpectedRecordStructure
	}
	tv, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("type assertion failed on query supertype %s", v)
	}
	st := FormTypeToSupertype(tv)
	if err := st.Validate(); err != nil {
		return "", fmt.Errorf("failed to get supertype: %w", err)
	}
	return st, nil
}

func (qr *QueryRecord) GetTextTypes() map[string][]string {
	ans := make(map[string][]string)
	v, ok := qr.LastopForm["selected_text_types"]
	if !ok {
		return ans
	}
	vt, ok := v.(map[string]any)
	if !ok {
		// TODO at least log this
		log.Warn().Msg("unexpected structure of selected_text_types, not map[string]any")
		return ans
	}
	for k, values := range vt {
		tValues, ok := values.([]any)
		if !ok {
			log.Warn().Msg("unexpected structure of selected_text_types item, not []any")
			// TODO at least log this
			return ans
		}
		ans[k] = make([]string, len(tValues))
		for i, v := range tValues {
			vt, ok := v.(string)
			if !ok {
				log.Warn().Msg("unexpected value in selected_text_types item, not a string")
			}
			ans[k][i] = vt
		}
	}
	return ans
}

func (qr *QueryRecord) getQueryTypes() (map[string]string, error) {
	ans := make(map[string]string)
	v, ok := qr.LastopForm["curr_query_types"]
	if !ok {
		return ans, ErrUnexpectedRecordStructure
	}
	vt, ok := v.(map[string]any)
	if !ok {
		return ans, ErrUnexpectedRecordStructure
	}
	for k, v := range vt {
		vt, ok := v.(string)
		if !ok {
			return ans, ErrUnexpectedRecordStructure
		}
		ans[k] = vt
	}
	return ans, nil
}

func (qr *QueryRecord) GetRawQueries() ([]RawQuery, error) {
	v, ok := qr.LastopForm["curr_queries"]
	if !ok {
		return []RawQuery{}, ErrUnexpectedRecordStructure
	}
	queries, ok := v.(map[string]any)
	if !ok {
		return []RawQuery{}, ErrUnexpectedRecordStructure
	}
	queryTypes, err := qr.getQueryTypes()
	if err != nil {
		return []RawQuery{}, ErrUnexpectedRecordStructure
	}

	ans := make([]RawQuery, 0, 10)
	for corp, v := range queries {
		vt, ok := v.(string)
		if !ok {
			return []RawQuery{}, ErrUnexpectedRecordStructure
		}
		ans = append(ans, RawQuery{Value: vt, Type: queryTypes[corp]})
	}
	return ans, nil
}

func (qr *QueryRecord) GetSubcorpus(db IMySQLOps) (string, error) {
	return db.GetSubcorpusName(qr.Subcorpus)
}
