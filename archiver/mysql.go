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
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	maxRecentRecords = 1000
)

type DBConf struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Name     string `json:"name"`
	User     string `json:"user"`
	Password string `json:"password"`
	PoolSize int    `json:"poolSize"`
}

func DBOpen(conf *DBConf) (*sql.DB, error) {
	mconf := mysql.NewConfig()
	mconf.Net = "tcp"
	mconf.Addr = conf.Host
	mconf.User = conf.User
	mconf.Passwd = conf.Password
	mconf.DBName = conf.Name
	mconf.ParseTime = true
	mconf.Loc = time.Local
	mconf.Params = map[string]string{"autocommit": "true"}
	db, err := sql.Open("mysql", mconf.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open sql database: %w", err)
	}
	return db, nil
}

func LoadRecentNRecords(db *sql.DB, num int) ([]ArchRecord, error) {
	if num > maxRecentRecords {
		panic(fmt.Sprintf("cannot load more than %d records at a time", maxRecentRecords))
	}
	rows, err := db.Query("SELECT id, data, created, num_access, last_access, permanent "+
		"FROM kontext_conc_persistence ORDER BY created DESC LIMIT ?", num)
	if err != nil {
		return []ArchRecord{}, fmt.Errorf("failed to load recent records: %w", err)
	}
	ans := make([]ArchRecord, 0, num)
	for rows.Next() {
		var item ArchRecord
		err := rows.Scan(&item.ID, &item.Data, &item.Created, &item.NumAccess, &item.LastAccess, &item.Permanent)
		if err != nil {
			return []ArchRecord{}, fmt.Errorf("failed to load recent records: %w", err)
		}
		ans = append(ans, item)
	}
	return ans, nil
}

func ContainsRecord(db *sql.DB, concID string) (bool, error) {
	row := db.QueryRow("SELECT COUNT(*) FROM kontext_conc_persistence "+
		"WHERE id = ? LIMIT 1", concID)
	if row.Err() != nil {
		return false, fmt.Errorf("failed to test existence of record %s: %w", concID, row.Err())
	}
	var ans bool
	row.Scan(&ans)
	return ans, nil
}

func InsertRecord(db *sql.DB, rec ArchRecord) error {
	_, err := db.Exec(
		"INSERT INTO kontext_conc_persistence (id, data, created, num_access, last_access, permanent) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		rec.ID, rec.Data, rec.Created, rec.NumAccess, rec.LastAccess, rec.Permanent,
	)
	if err != nil {
		return fmt.Errorf("failed to insert archive record: %w", err)
	}
	return nil
}
