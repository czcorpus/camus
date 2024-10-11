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

package documents

import (
	"camus/cncdb"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Kwords struct {
	ID string `json:"id"`

	Name string `json:"name"`

	Created time.Time `json:"created"`

	QuerySupertype string `json:"query_supertype"`

	UserID string `json:"user_id"`

	Corpora string `json:"corpora"`

	Subcorpus string `json:"subcorpus"`

	RawQuery string `json:"raw_query"`

	PosAttrNames string `json:"pos_attr_names"`
}

func (kw *Kwords) Type() string {
	return "kwords"
}

func (kw *Kwords) GetID() string {
	return fmt.Sprintf("%s/%d/%s", kw.UserID, kw.Created.Unix(), kw.ID)
}

// intermediate keywords record

type MidKwords struct {
	ID string `json:"id"`

	Name string `json:"name"`

	Created time.Time `json:"created"`

	QuerySupertype cncdb.QuerySupertype `json:"querySupertype"`

	UserID int `json:"userId"`

	Corpora []string `json:"corpora"`

	Subcorpora []string `json:"subcorpora"`

	RawQuery string `json:"rawQuery"`

	PosAttrNames []string `json:"posAttrNames"`
}

func (mkw *MidKwords) GetID() string {
	return mkw.ID
}

func (mkw *MidKwords) GetQuerySupertype() cncdb.QuerySupertype {
	return mkw.QuerySupertype
}

func (mkw *MidKwords) AsIndexableDoc() IndexableDoc {
	return &Kwords{
		ID:             mkw.ID,
		Name:           mkw.Name,
		Created:        mkw.Created,
		QuerySupertype: string(mkw.QuerySupertype),
		UserID:         strconv.Itoa(mkw.UserID),
		Corpora:        strings.Join(mkw.Corpora, " "),
		Subcorpus:      strings.Join(mkw.Subcorpora, " "),
		RawQuery:       mkw.RawQuery,
		PosAttrNames:   strings.Join(mkw.PosAttrNames, " "),
	}
}
