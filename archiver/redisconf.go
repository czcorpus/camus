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

package archiver

import (
	"fmt"
)

type RedisConf struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	DB       int    `json:"db"`
	Password string `json:"password"`
}

func (conf *RedisConf) ValidateAndDefaults() error {
	if conf.DB == 0 {
		return fmt.Errorf("missing Redis configuration: `db`")
	}
	return nil
}
