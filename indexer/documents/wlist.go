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

type Wordlist struct {
	ID string `json:"id"`

	Created time.Time `json:"created"`

	QuerySupertype string `json:"query_supertype"`

	UserID string `json:"user_id"`

	Corpora string `json:"corpora"`

	Subcorpus string `json:"subcorpus"`

	RawQuery string `json:"raw_query"`

	PosAttrNames string `json:"pos_attr_names"`

	PFilterWords string `json:"pfilter_words"`

	NFilterWords string `json:"nfilter_words"`
}

func (wlist *Wordlist) Type() string {
	return "wlist"
}

func (wlist *Wordlist) GetID() string {
	return fmt.Sprintf("%s-%d-%s", wlist.UserID, wlist.Created.Unix(), wlist.ID)
}

// intermediate word list data

type MidWordlist struct {
	ID string `json:"id"`

	QuerySupertype cncdb.QuerySupertype `json:"querySupertype"`

	Created time.Time `json:"created"`

	UserID int `json:"userId"`

	// Corpora contains all the searched corpora.
	// In case of word list search, lenght > 1 cannot
	// happen.
	Corpora []string `json:"corpora"`

	Subcorpus string `json:"subcorpus"`

	RawQuery string `json:"rawQuery"`

	PosAttrNames []string `json:"posAttrNames"`

	PFilterWords []string `json:"pfilterWords"`

	NFilterWords []string `json:"nfilterWords"`
}

func (mwl *MidWordlist) GetID() string {
	return mwl.ID
}

func (mwl *MidWordlist) GetQuerySupertype() cncdb.QuerySupertype {
	return mwl.QuerySupertype
}

func (mwl *MidWordlist) AsIndexableDoc() IndexableDoc {
	return &Wordlist{
		ID:             mwl.ID,
		Created:        mwl.Created,
		QuerySupertype: string(mwl.QuerySupertype),
		UserID:         strconv.Itoa(mwl.UserID),
		Corpora:        strings.Join(mwl.Corpora, " "),
		Subcorpus:      mwl.Subcorpus,
		RawQuery:       mwl.RawQuery,
		PosAttrNames:   strings.Join(mwl.PosAttrNames, " "),
		PFilterWords:   strings.Join(mwl.PFilterWords, " "),
		NFilterWords:   strings.Join(mwl.NFilterWords, " "),
	}
}
