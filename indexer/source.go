// Copyright 2024 Martin Zimandl <martin.zimandl@gmail.com>
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

package indexer

import (
	"camus/cncdb"
	"fmt"
	"time"
)

// unspecifiedQueryRecord represents any query record as saved by
// KonText without specifying its concrete structure (conc, wlist,...).
// We use the fact that all stored queries have a common set of attributes
// and the difference starts mainly with `lastop_form` and `form` JSON
// properties for which we have separate types - each for one query type.
type unspecifiedQueryRecord struct {
	ID          string         `json:"id"`
	Created     time.Time      `json:"-"`
	UserID      int            `json:"user_id"`
	Corpora     []string       `json:"corpora"`
	SubcorpusID string         `json:"usesubcorp"`
	LastopForm  map[string]any `json:"lastop_form"`
	Form        map[string]any `json:"form"`
}

// GetSupertype extracts query type (supertype in terms used by KonText sources) info.
// This is used to decide which type will be used to instantiate actual query form.
func (qr *unspecifiedQueryRecord) GetSupertype() (cncdb.QuerySupertype, error) {
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
	st := cncdb.FormTypeToSupertype(tv)
	return st, nil
}

func (qr *unspecifiedQueryRecord) GetSubcorpusProps(db cncdb.IMySQLOps) (cncdb.SubcProps, error) {
	return db.GetSubcorpusProps(qr.SubcorpusID)
}
