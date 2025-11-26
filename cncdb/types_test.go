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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var src = "{\"user_id\":2,\"q\":[\"q[(lemma=\\\"voda\\\" | sublemma=\\\"voda\\\" | word=\\\"voda\\\")]\"],\"corpora\":[\"syn2020\"],\"usesubcorp\":null,\"lines_groups\":{\"data\":[],\"sorted\":false},\"lastop_form\":{\"form_type\":\"query\",\"curr_query_types\":{\"syn2020\":\"simple\"},\"curr_queries\":{\"syn2020\":\"voda\"},\"curr_parsed_queries\":{\"syn2020\":[[[[[\"lemma\",\"sublemma\",\"word\"],\"voda\"]],false]]},\"curr_pcq_pos_neg_values\":{\"syn2020\":\"pos\"},\"curr_include_empty_values\":{\"syn2020\":false},\"curr_lpos_values\":{\"syn2020\":null},\"curr_qmcase_values\":{\"syn2020\":true},\"curr_default_attr_values\":{\"syn2020\":\"\"},\"curr_use_regexp_values\":{\"syn2020\":true},\"asnc\":true,\"no_query_history\":false,\"shuffle\":true,\"fc_lemword_type\":\"all\",\"fc_lemword_wsize\":[-5,5],\"fc_lemword\":\"\",\"fc_pos_type\":\"all\",\"fc_pos_wsize\":[-5,5],\"fc_pos\":[],\"selected_text_types\":{},\"bib_mapping\":{},\"cutoff\":0,\"treat_as_slow_query\":true,\"alt_corpus\":null},\"id\":\"hQiqOwq8AycE\",\"prev_id\": \"xxx\", \"persist_level\":1}"

func TestGeneralDataRecordAttrAccess(t *testing.T) {
	var rec GeneralDataRecord

	err := json.Unmarshal([]byte(src), &rec)
	assert.NoError(t, err)
	assert.Equal(t, []string{"q[(lemma=\"voda\" | sublemma=\"voda\" | word=\"voda\")]"}, rec.GetQuery())
	assert.Equal(t, true, rec.IsFlaggedAsSlow())
	assert.Equal(t, []string{"syn2020"}, rec.GetCorpora())
	assert.Equal(t, "xxx", rec.GetPrevID())
}

func TestGeneralDataRecordRemoveSlowFlag(t *testing.T) {
	var rec GeneralDataRecord

	err := json.Unmarshal([]byte(src), &rec)
	assert.NoError(t, err)
	rec.RemoveSlowFlag()
	assert.Equal(t, false, rec.IsFlaggedAsSlow())
	newJSON, _ := json.Marshal(rec)
	assert.False(t, strings.Contains(string(newJSON), "\"treat_as_slow_query\""))
}
