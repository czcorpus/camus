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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
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

func generateRows(sqlRows *sql.Rows, expectedSize int) ([]ArchRecord, error) {
	ans := make([]ArchRecord, 0, expectedSize)
	for sqlRows.Next() {
		var item ArchRecord
		err := sqlRows.Scan(&item.ID, &item.Data, &item.Created, &item.NumAccess, &item.LastAccess, &item.Permanent)
		if err != nil {
			return []ArchRecord{}, fmt.Errorf("failed to load recent records: %w", err)
		}
		ans = append(ans, item)
	}
	return ans, nil
}

type MySQLOps struct {
	db  *sql.DB
	tz  *time.Location
	ctx context.Context
}

func (ops *MySQLOps) LoadRecentNRecords(num int) ([]ArchRecord, error) {
	// we use helperLimit to help partitioned table with millions of items
	// to avoid going through all the partitions (or is the query planner
	// able to determine it from `order by created DESC limit X` ?)
	helperLimit := time.Now().In(ops.tz).Add(-180 * 24 * time.Hour)
	if num > maxRecentRecords {
		panic(fmt.Sprintf("cannot load more than %d records at a time", maxRecentRecords))
	}
	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT id, data, created, num_access, last_access, permanent "+
			"FROM kontext_conc_persistence "+
			"WHERE created >= ? "+
			"ORDER BY created DESC LIMIT ?", helperLimit, num)
	if err != nil {
		return []ArchRecord{}, fmt.Errorf("failed to load recent records: %w", err)
	}
	return generateRows(rows, num)
}

func (ops *MySQLOps) LoadRecordsFromDate(fromDate time.Time, maxItems int) ([]ArchRecord, error) {
	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT id, data, created, num_access, last_access, permanent "+
			"FROM kontext_conc_persistence "+
			"WHERE created >= ? "+
			"ORDER BY created LIMIT ?", fromDate, maxItems)
	if err != nil {
		return []ArchRecord{}, fmt.Errorf("failed to load records: %w", err)
	}
	return generateRows(rows, maxItems)
}

func (ops *MySQLOps) ContainsRecord(concID string) (bool, error) {
	row := ops.db.QueryRowContext(
		ops.ctx,
		"SELECT COUNT(*) FROM kontext_conc_persistence "+
			"WHERE id = ? LIMIT 1", concID)
	if row.Err() != nil {
		return false, fmt.Errorf("failed to test existence of record %s: %w", concID, row.Err())
	}
	var ans bool
	row.Scan(&ans)
	return ans, nil
}

func (ops *MySQLOps) LoadRecordsByID(concID string) ([]ArchRecord, error) {
	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT data, created, num_access, last_access, permanent "+
			"FROM kontext_conc_persistence WHERE id = ?", concID)
	if err != nil {
		return []ArchRecord{}, fmt.Errorf("failed to get records with id %s: %w", concID, err)
	}
	ans := make([]ArchRecord, 0, 10)
	for rows.Next() {
		item := ArchRecord{ID: concID}
		err := rows.Scan(
			&item.Data, &item.Created, &item.NumAccess, &item.LastAccess,
			&item.Permanent)
		if err != nil {
			return []ArchRecord{}, fmt.Errorf("failed to get records with id %s: %w", concID, err)
		}
		ans = append(ans, item)
	}
	return ans, nil
}

func (ops *MySQLOps) InsertRecord(rec ArchRecord) error {
	_, err := ops.db.ExecContext(
		ops.ctx,
		"INSERT INTO kontext_conc_persistence (id, data, created, num_access, last_access, permanent) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		rec.ID, rec.Data, rec.Created, rec.NumAccess, rec.LastAccess, rec.Permanent,
	)
	if err != nil {
		return fmt.Errorf("failed to insert archive record: %w", err)
	}
	return nil
}

func (ops *MySQLOps) UpdateRecordStatus(id string, status int) error {
	res, err := ops.db.ExecContext(
		ops.ctx,
		"UPDATE kontext_conc_persistence SET permanent = ? WHERE id = ?", status, id)
	if err != nil {
		return fmt.Errorf("failed to update status of %s: %w", id, err)
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to update status of %s: %w", id, err)
	}
	if aff == 0 {
		return fmt.Errorf("cannot update record status, id %s not in archive", id)
	}
	return nil
}

func (ops *MySQLOps) RemoveRecordsByID(concID string) error {
	_, err := ops.db.ExecContext(
		ops.ctx,
		"DELETE FROM kontext_conc_persistence WHERE id = ?", concID)
	if err != nil {
		return fmt.Errorf("failed to remove records with id %s: %w", concID, err)
	}
	return nil
}

func (ops *MySQLOps) DeduplicateInArchive(curr []ArchRecord, rec ArchRecord) (ArchRecord, error) {
	err := ops.RemoveRecordsByID(rec.ID)
	if err != nil {
		return ArchRecord{}, fmt.Errorf("failed to finish deduplication for %s: %w", rec.ID, err)
	}
	ans := MergeRecords(curr, rec, ops.tz)
	err = ops.InsertRecord(ans)
	if err != nil {
		log.Error().
			Err(err).
			Str("concId", rec.ID).
			Str("data", ans.Data).
			Msg("failed to insert merged record")
		return ans, fmt.Errorf("failed to store merged record %s: %w", rec.ID, err)
	}
	return ans, nil
}

func (ops *MySQLOps) GetArchSizesByYears(forceLoad bool) ([][2]int, error) {
	if !forceLoad && !TimeIsAtNight(time.Now().In(ops.tz)) {
		return [][2]int{}, ErrTooDemandingQuery
	}
	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT COUNT(*), YEAR(created) AS yc "+
			"FROM kontext_conc_persistence "+
			"GROUP BY YEAR(created) ORDER BY yc")
	if err != nil {
		return [][2]int{}, fmt.Errorf("failed to fetch arch. sizes: %w", err)
	}
	ans := make([][2]int, 0, 30)
	for rows.Next() {
		var v, year int
		if err := rows.Scan(&v, &year); err != nil {
			return [][2]int{}, fmt.Errorf("failed to get values from arch. sizes row: %w", err)
		}
		ans = append(ans, [2]int{year, v})
	}
	return ans, nil
}

func (ops *MySQLOps) GetSubcorpusProps(subcID string) (SubcProps, error) {
	if subcID == "" {
		return SubcProps{}, nil
	}
	row := ops.db.QueryRowContext(
		ops.ctx,
		"SELECT name, text_types, within_cond FROM kontext_subcorpus WHERE id = ?", subcID)
	var name string
	var textTypes sql.NullString
	var withinCond sql.NullString
	if err := row.Scan(&name, &textTypes, &withinCond); err != nil {
		if err == sql.ErrNoRows {
			return SubcProps{}, nil
		}
		return SubcProps{}, fmt.Errorf("failed to get subcorpus props: %w", err)
	}
	tt := make(map[string][]string)
	if textTypes.Valid {
		if err := json.Unmarshal([]byte(textTypes.String), &tt); err != nil {
			return SubcProps{}, fmt.Errorf("failed to get subcorpus props: %w", err)
		}
	}
	var within string
	if withinCond.Valid {
		within = withinCond.String
	}
	return SubcProps{Name: name, TextTypes: tt, Within: within}, nil
}

func (ops *MySQLOps) GetAllUsersWithQueryHistory() ([]int, error) {
	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT DISTINCT user_id FROM kontext_query_history ORDER BY user_id",
	)
	if err != nil {
		return []int{}, fmt.Errorf("failed to get users with history: %w", err)
	}
	ans := make([]int, 0, 4000)
	for rows.Next() {
		var userID int
		err := rows.Scan(&userID)
		if err != nil {
			return []int{}, fmt.Errorf("failed to get users with history: %w", err)
		}
		ans = append(ans, userID)
	}
	return ans, nil
}

func (ops *MySQLOps) GetUserQueryHistory(userID int, ttl time.Duration) ([]HistoryRecord, error) {
	oldestDate := time.Now().In(ops.tz).Add(-ttl)
	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT query_id, created, name FROM kontext_query_history "+
			"WHERE user_id = ? AND (name IS NOT NULL OR created >= ?)",
		userID, oldestDate,
	)
	if err != nil {
		return []HistoryRecord{}, fmt.Errorf("failed to get user query history: %w", err)
	}
	ans := make([]HistoryRecord, 0, int(ttl.Hours()/24*10)) // cap: just a rough estimation
	for rows.Next() {
		hRec := HistoryRecord{UserID: userID}
		var name sql.NullString
		err := rows.Scan(&hRec.QueryID, &hRec.Created, &name)
		if err != nil {
			return []HistoryRecord{}, fmt.Errorf("failed to get user query history: %w", err)
		}
		hRec.Name = name.String
		ans = append(ans, hRec)
	}
	return ans, nil
}

func (ops *MySQLOps) LoadRecentNHistory(num int) ([]HistoryRecord, error) {
	// we use helperLimit to help partitioned table with millions of items
	// to avoid going through all the partitions (or is the query planner
	// able to determine it from `order by created DESC limit X` ?)
	helperLimit := time.Now().In(ops.tz).Add(-180 * 24 * time.Hour)
	if num > maxRecentRecords {
		panic(fmt.Sprintf("cannot load more than %d records at a time", maxRecentRecords))
	}

	rows, err := ops.db.QueryContext(
		ops.ctx,
		"SELECT user_id, query_id, created, name FROM kontext_query_history "+
			"WHERE created >= ? "+
			"ORDER BY created DESC LIMIT ?",
		helperLimit.Unix(), num,
	)
	if err != nil {
		return []HistoryRecord{}, fmt.Errorf("failed to get user query history: %w", err)
	}
	ans := make([]HistoryRecord, 0, num)
	for rows.Next() {
		var hRec HistoryRecord
		var name sql.NullString
		err := rows.Scan(&hRec.UserID, &hRec.QueryID, &hRec.Created, &name)
		if err != nil {
			return []HistoryRecord{}, fmt.Errorf("failed to get user query history: %w", err)
		}
		hRec.Name = name.String
		ans = append(ans, hRec)
	}
	return ans, nil
}

func NewMySQLOps(ctx context.Context, db *sql.DB, tz *time.Location) *MySQLOps {
	return &MySQLOps{
		ctx: ctx,
		db:  db,
		tz:  tz,
	}
}
