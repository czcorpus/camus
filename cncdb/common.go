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

package cncdb

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrTooDemandingQuery = errors.New("too demanding query")
)

func TimeIsAtNight(t time.Time) bool {
	return t.Hour() >= 22 || t.Hour() <= 5
}

func MergeRecords(recs []QueryArchRec, newRec QueryArchRec, tz *time.Location) QueryArchRec {
	if len(recs) == 0 {
		panic("cannot merge empty slice of ArchRecords")
	}
	ans := newRec
	ans.NumAccess++
	ans.LastAccess = time.Now().In(tz)
	for _, rec := range recs {
		ans.NumAccess += rec.NumAccess
		if rec.Created.Before(ans.Created) && !rec.Created.IsZero() {
			ans.Created = rec.Created
		}
		if rec.LastAccess.After(ans.LastAccess) {
			ans.LastAccess = rec.LastAccess
		}
		if rec.Permanent > ans.Permanent {
			ans.Permanent = rec.Permanent
		}
	}
	return ans
}

func ValidateQueryInstances(variants []QueryArchRec) error {
	if len(variants) < 2 {
		return nil
	}
	queryVariants := make(map[string]int)
	for _, vr := range variants {
		data, err := vr.FetchData()
		if err != nil {
			// failed to parse data => still a variant of data
			u := uuid.New()
			queryVariants[u.String()]++
		}
		queryVariants[strings.Join(data.GetQuery(), " ")]++
	}
	if len(queryVariants) > 1 {
		return fmt.Errorf(
			"inconsistent variants of query between instances (id %s) - found %d variants",
			variants[0].ID,
			len(queryVariants),
		)
	}
	return nil
}
