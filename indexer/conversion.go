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

package indexer

import (
	"camus/cncdb"
	"camus/indexer/documents"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrRecordNotIndexable = errors.New("record is not indexable")
)

// IndexableMidDoc is an intermediate format
// extracted from KonText query records with attributes
// arranged and typed in an "ideal way" - i.e. in a way
// we would like to have them with some abstract ideal
// fulltext engine.
type IndexableMidDoc interface {
	GetQuerySupertype() cncdb.QuerySupertype
	GetID() string

	// AsIndexableDoc converts the "ideal" intermediate
	// format into the format acceptable by Bleve fulltext
	// indexing backend.
	AsIndexableDoc() documents.IndexableDoc
}

type concDB interface {
	GetConcRecord(id string) (cncdb.ArchRecord, error)
}

func importConc(
	rec *cncdb.UntypedQueryRecord,
	stype cncdb.QuerySupertype,
	arec *cncdb.ArchRecord,
	db cncdb.IMySQLOps,
) (IndexableMidDoc, error) {

	var form cncdb.ConcFormRecord
	if err := json.Unmarshal([]byte(arec.Data), &form); err != nil {
		return nil, err
	}
	subcName, err := rec.GetSubcorpus(db)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	ans := &documents.MidConc{
		ID:             rec.ID,
		Created:        arec.Created,
		UserID:         rec.UserID,
		Corpora:        rec.Corpora,
		Subcorpus:      subcName,
		QuerySupertype: stype,
		RawQueries:     make([]cncdb.RawQuery, 0, len(form.LastopForm.CurrQueries)),
	}

	for corp, query := range form.LastopForm.CurrQueries {
		ans.RawQueries = append(ans.RawQueries, cncdb.RawQuery{
			Value: query,
			Type:  form.LastopForm.CurrQueryTypes[corp],
		})
	}

	if err := documents.ExtractCQLProps(ans, form.GetDefaultAttr()); err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	if ans.StructAttrs == nil {
		ans.StructAttrs = make(map[string][]string)
	}
	for attr, items := range form.LastopForm.SelectedTextTypes {
		_, ok := ans.StructAttrs[attr]
		if !ok {
			ans.StructAttrs[attr] = make([]string, 0, len(items))
		}
		ans.StructAttrs[attr] = append(ans.StructAttrs[attr], items...)
	}

	return ans, nil
}

func importWlist(
	rec *cncdb.UntypedQueryRecord,
	stype cncdb.QuerySupertype,
	arec *cncdb.ArchRecord,
	db cncdb.IMySQLOps,
) (IndexableMidDoc, error) {
	var form cncdb.WlistFormRecord
	if err := json.Unmarshal([]byte(arec.Data), &form); err != nil {
		return nil, err
	}

	subcName, err := rec.GetSubcorpus(db)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	ans := &documents.MidWordlist{
		ID:             rec.ID,
		QuerySupertype: stype,
		Created:        arec.Created,
		UserID:         rec.UserID,
		Corpora:        rec.Corpora,
		Subcorpus:      subcName,
		RawQuery:       form.Form.WLPattern,
		PosAttrNames:   []string{form.Form.WLAttr},
		PFilterWords:   form.Form.PFilterWords,
		NFilterWords:   form.Form.NFilterWords,
	}
	return ans, nil
}

func importKwords(
	rec *cncdb.UntypedQueryRecord,
	stype cncdb.QuerySupertype,
	arec *cncdb.ArchRecord,
	db cncdb.IMySQLOps,
) (IndexableMidDoc, error) {
	var form cncdb.KwordsFormRecord
	if err := json.Unmarshal([]byte(arec.Data), &form); err != nil {
		return nil, err
	}

	subcorpora := make([]string, 0, 2)
	subcName1, err := rec.GetSubcorpus(db)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	if subcName1 != "" {
		subcorpora = append(subcorpora, subcName1)
	}
	subcName2, err := db.GetSubcorpusName(form.Form.RefUsesubcorp)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	if subcName2 != "" {
		subcorpora = append(subcorpora, subcName2)
	}
	corpora := append(rec.Corpora, form.Form.RefCorpname)

	ans := &documents.MidKwords{
		ID:             rec.ID,
		QuerySupertype: stype,
		Created:        arec.Created,
		UserID:         rec.UserID,
		Corpora:        corpora,
		Subcorpora:     subcorpora,
		RawQuery:       form.Form.WLPattern,
		PosAttrNames:   []string{form.Form.WLAttr},
	}
	return ans, nil
}

func importPquery(
	rec *cncdb.UntypedQueryRecord,
	stype cncdb.QuerySupertype,
	arec *cncdb.ArchRecord,
	db cncdb.IMySQLOps,
	cdb concDB,
) (IndexableMidDoc, error) {
	var form cncdb.PQueryFormRecord
	if err := json.Unmarshal([]byte(arec.Data), &form); err != nil {
		return nil, err
	}
	subcName, err := rec.GetSubcorpus(db)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}

	mergedStructures := make([]string, 0, 10)
	mergedStructAttrs := make(map[string][]string)
	mergedPosAttrs := make(map[string][]string)
	mergedRawQueries := make([]cncdb.RawQuery, 0, len(form.Form.ConcIDs))

	for i, id := range form.Form.ConcIDs {
		data, err := cdb.GetConcRecord(id)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch pquery concordance #%d: %w", i, err)
		}
		var crec cncdb.UntypedQueryRecord
		if err := json.Unmarshal([]byte(data.Data), &crec); err != nil {
			return nil, fmt.Errorf("failed to process pquery conc #%d: %w", i, err)
		}
		cqstype, err := crec.GetSupertype()
		if err != nil {
			return nil, fmt.Errorf("failed to process pquery conc #%d: %w", i, err)
		}
		if cqstype != cncdb.QuerySupertypeConc {
			return nil, fmt.Errorf("failed to process pquery conc #%d: not a conc. record", i)
		}
		conc, err := importConc(&crec, cqstype, &data, db)

		if err != nil {
			return nil, fmt.Errorf("failed to process pquery conc #%d: %w", i, err)
		}
		tConc, ok := conc.(*documents.MidConc)
		if !ok {
			panic("type assertion error when importing pquery concordance")
		}
		mergedRawQueries = append(mergedRawQueries, tConc.RawQueries...)
		for paName, paValues := range tConc.PosAttrs {
			mergedPosAttrs[paName] = append(mergedPosAttrs[paName], paValues...)
		}
		for saName, saValues := range tConc.StructAttrs {
			mergedStructAttrs[saName] = append(mergedStructAttrs[saName], saValues...)
		}
		mergedStructures = append(mergedStructures, tConc.Structures...)

	}
	ans := &documents.MidPQuery{
		ID:             rec.ID,
		Created:        arec.Created,
		UserID:         rec.UserID,
		Corpora:        rec.Corpora,
		Subcorpus:      subcName,
		QuerySupertype: stype,
		RawQueries:     mergedRawQueries,
		PosAttrs:       mergedPosAttrs,
		StructAttrs:    mergedStructAttrs,
		Structures:     mergedStructures,
	}
	return ans, nil
}

// RecToDoc converts a conc/wlist/... archive record into an indexable
// document. In case the record is OK but of an unsupported type (e.g. "shuffle"),
// nil document is returned along with ErrRecordNotIndexable error.
func RecToDoc(arec *cncdb.ArchRecord, db cncdb.IMySQLOps, cdb concDB) (IndexableMidDoc, error) {
	var rec cncdb.UntypedQueryRecord
	if err := json.Unmarshal([]byte(arec.Data), &rec); err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	qstype, err := rec.GetSupertype()
	if err != nil {
		return nil, fmt.Errorf("failed to convert rec. to doc.: %w", err)
	}
	if !qstype.IsIndexable() {
		return nil, ErrRecordNotIndexable
	}
	var ans IndexableMidDoc
	switch qstype {
	case cncdb.QuerySupertypeConc:
		ans, err = importConc(&rec, qstype, arec, db)
	case cncdb.QuerySupertypeWlist:
		ans, err = importWlist(&rec, qstype, arec, db)
	case cncdb.QuerySupertypeKwords:
		ans, err = importKwords(&rec, qstype, arec, db)
	case cncdb.QuerySupertypePquery:
		ans, err = importPquery(&rec, qstype, arec, db, cdb)
	default:
		err = ErrRecordNotIndexable
	}
	return ans, err

}
