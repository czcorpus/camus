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
	"camus/cncdb"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type QueueRecordType string

const (
	QRTypeArchive QueueRecordType = "archive"
	QRTypeHistory QueueRecordType = "history"
)

type queueRecord struct {
	Type QueueRecordType `json:"type"`
	Key  string          `json:"key"`

	// query persistence data
	Explicit bool `json:"explicit"`

	// query history data
	UserID  int    `json:"user_id"`
	Created int64  `json:"created"`
	Name    string `json:"name"`
}

func (qr queueRecord) IsArchive() bool {
	return qr.Type == "archive"
}

func (qr queueRecord) IsHistory() bool {
	return qr.Type == "history"
}

func (qr queueRecord) KeyCode() string {
	if strings.HasPrefix(qr.Key, "concordance:") {
		return strings.Split(qr.Key, "concordance:")[1]
	}
	return qr.Key
}

type RedisAdapter struct {
	conf  *RedisConf
	redis *redis.Client
	ctx   context.Context
}

func (rd *RedisAdapter) String() string {
	if rd.redis == nil {
		return fmt.Sprintf(
			"RedisAdapter (inactive), address %s:%d, db %d",
			rd.conf.Host, rd.conf.Port, rd.conf.DB,
		)
	}
	return fmt.Sprintf(
		"RedisAdapter (active) address %s:%d, db %d",
		rd.conf.Host, rd.conf.Port, rd.conf.DB,
	)
}

func (rd *RedisAdapter) Type(k string) (string, error) {
	cmd := rd.redis.Type(rd.ctx, k)
	if cmd.Err() != nil {
		return "", fmt.Errorf("failed to determine type of %s: %w", k, cmd.Err())
	}
	return cmd.Val(), nil
}

func (rd *RedisAdapter) Get(k string) (string, error) {
	cmd := rd.redis.Get(rd.ctx, k)
	if cmd.Err() == redis.Nil {
		return "", nil
	}
	if cmd.Err() != nil {
		return "", fmt.Errorf("failed to get Redis entry %s: %w", k, cmd.Err())
	}
	return cmd.Val(), nil
}

func (rd *RedisAdapter) Set(k string, v any) error {
	cmd := rd.redis.Set(rd.ctx, k, v, 0)
	if cmd.Err() != nil {
		return fmt.Errorf("failed to set Redis item %s: %w", k, cmd.Err())
	}
	return nil
}

func (rd *RedisAdapter) Exists(key string) (bool, error) {
	cmd := rd.redis.Exists(rd.ctx, key)
	if cmd.Err() != nil {
		return false, fmt.Errorf("failed to test key %s: %w", key, cmd.Err())
	}
	return cmd.Val() > 0, nil
}

func (rd *RedisAdapter) TriggerChan(chname, value string) error {
	return rd.redis.Publish(rd.ctx, chname, value).Err()
}

func (rd *RedisAdapter) UintZAdd(key string, v int) error {
	if v < 0 {
		panic("UintZAdd - cannot add numbers < 0")
	}
	cmd := rd.redis.ZAdd(rd.ctx, key, redis.Z{Score: float64(v), Member: v})
	return cmd.Err()
}

func (rd *RedisAdapter) ZCard(key string) (int, error) {
	cmd := rd.redis.ZCard(rd.ctx, key)
	return int(cmd.Val()), cmd.Err()
}

// IntZRemLowest removes and returns an element with lowest score from ZSET
// returns true if the record was found and removed, otherwise false
// (i.e. not finding the record is not an error)
func (rd *RedisAdapter) UintZRemLowest(key string) (int, error) {
	cmd := rd.redis.ZRange(rd.ctx, key, 0, 0)
	if cmd.Err() == redis.Nil {
		return -1, nil

	} else if cmd.Err() != nil {
		return -1, cmd.Err()
	}
	if len(cmd.Val()) == 0 {
		return -1, nil

	} else if len(cmd.Val()) > 1 {
		return -1, fmt.Errorf("IntZRemLowest failed - more than one matching item")
	}
	vToRem, err := strconv.Atoi(cmd.Val()[0])
	if err != nil {
		return 0, fmt.Errorf("IntZRemLowest failed - item is not an integer")
	}
	cmd2 := rd.redis.ZRem(rd.ctx, key, vToRem)
	if cmd2.Err() != nil {
		err = fmt.Errorf("IntZRemLowest failed: %w", cmd2.Err())
	}
	return vToRem, err
}

// ChannelSubscribe subscribe to a Redis channel with a specified name.
func (rd *RedisAdapter) ChannelSubscribe(name string) <-chan *redis.Message {
	sub := rd.redis.Subscribe(rd.ctx, name)
	return sub.Channel()
}

// NextQueueItem fetches an item from the beginning of a Redis list
// (i.e. LPOP is used in the background and RPUSH is expected to be
// used to add new items on the other side).
func (rd *RedisAdapter) NextQueueItem(queue string) (string, error) {
	lpopCmd := rd.redis.LPop(rd.ctx, queue)
	if lpopCmd.Err() != nil {
		return "", lpopCmd.Err()
	}
	return lpopCmd.Val(), nil
}

func (rd *RedisAdapter) NextNArchItems(queueKey string, n int64) ([]queueRecord, error) {
	ans := make([]queueRecord, 0, n)
	ppl := rd.redis.Pipeline()
	lrangeCmd := ppl.LRange(rd.ctx, queueKey, -n, -1)
	ppl.LTrim(rd.ctx, queueKey, 0, -n-1)
	_, err := ppl.Exec(rd.ctx)
	if err != nil {
		return []queueRecord{}, fmt.Errorf("failed to get items from queue: %w", err)
	}
	items, err := lrangeCmd.Result()
	if err != nil {
		return []queueRecord{}, fmt.Errorf("failed to get items from queue: %w", err)
	}
	for i := len(items) - 1; i >= 0; i-- {
		if strings.Contains(items[i], `"key"`) {
			var v queueRecord
			err := json.Unmarshal([]byte(items[i]), &v)
			if err != nil {
				return []queueRecord{}, fmt.Errorf("failed to decode queue item `%s`: %w", items[i], err)
			}
			ans = append(ans, v)

		} else {
			ans = append(ans, queueRecord{Key: items[i]})
		}
	}
	return ans, nil
}

func (rd *RedisAdapter) AddError(errQueue string, item queueRecord, rec *cncdb.QueryArchRec) error {
	itemJSON, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to add error record %s: %w", item.Key, err)
	}
	cmd := rd.redis.LPush(rd.ctx, errQueue, string(itemJSON))
	if cmd.Err() != nil {
		return fmt.Errorf("failed to insert error key %s: %w", item.Key, cmd.Err())
	}
	if rec != nil {
		cmd = rd.redis.HSet(rd.ctx, errQueue, item.Key, rec.Data)
		if cmd.Err() != nil {
			return fmt.Errorf("failed to insert error record %s: %w", item.Key, cmd.Err())
		}
	}
	return nil
}

func (rd *RedisAdapter) mkKey(id string) string {
	return fmt.Sprintf("concordance:%s", id)
}

// GetConcRecord returns a concordance/wlist/pquery/kwords records
// with a specified ID. In case no such record is found, ErrRecordNotFound
// is returned.
func (rd *RedisAdapter) GetConcRecord(id string) (cncdb.QueryArchRec, error) {
	ans := rd.redis.Get(rd.ctx, rd.mkKey(id))
	if ans.Err() == redis.Nil {
		return cncdb.QueryArchRec{}, cncdb.ErrRecordNotFound
	}
	if ans.Err() != nil {
		return cncdb.QueryArchRec{}, fmt.Errorf("failed to get concordance record: %w", ans.Err())
	}
	return cncdb.QueryArchRec{
		ID:   id,
		Data: ans.Val(),
	}, nil
}

func (rd *RedisAdapter) mkConcCacheKey(corpusId string) string {
	return fmt.Sprintf("conc_cache:%s", strings.ToLower(corpusId))
}

// mkConcCacheField is an exact rewrite of KonText's `_uniqname` function stored in
// lib/plugins/default_conc_cache/__init__.py. It is important to keep this in sync as
// otherwise, we won't be able to fetch KonText's cache records.
func (rd *RedisAdapter) mkConcCacheField(corpusID, subcorpusID string, q []string, cutoff int) string {
	corpusIDLw := strings.ToLower(corpusID)
	corpKey := corpusIDLw
	if subcorpusID != "" {
		corpKey = corpusIDLw + "/" + subcorpusID
	}
	hashInput := strings.Join(q, "#") + corpKey + strconv.Itoa(cutoff)
	hash := sha1.Sum([]byte(hashInput))
	return fmt.Sprintf("%x", hash)
}

// GetConcCacheRawRecord gets a raw representation of conc. cache record
// (i.e. without parsed data).
func (rd *RedisAdapter) GetConcCacheRawRecord(id string) (ConcCacheRec, error) {
	concRecord, err := rd.GetConcRecord(id)
	if err != nil {
		return ConcCacheRec{}, fmt.Errorf("failed to get concordance record: %w", err)
	}
	data, err := concRecord.FetchData()
	if err != nil {
		return ConcCacheRec{}, fmt.Errorf("failed to fetch concordance record data: %w", err)
	}
	corpusId := data.GetCorpora()[0]
	subcorpId := data.GetSubcorpus()
	field := rd.mkConcCacheField(corpusId, subcorpId, data.GetQuery(), 0)
	ans := rd.redis.HGet(rd.ctx, rd.mkConcCacheKey(corpusId), field)
	if ans.Err() == redis.Nil {
		return ConcCacheRec{ID: field}, cncdb.ErrRecordNotFound
	}
	if ans.Err() != nil {
		return ConcCacheRec{}, fmt.Errorf("failed to get conc_cache record: %w", ans.Err())
	}
	return ConcCacheRec{
		ID:   field,
		Data: ans.Val(),
	}, nil
}

func NewRedisAdapter(ctx context.Context, conf *RedisConf) *RedisAdapter {
	ans := &RedisAdapter{
		conf: conf,
		redis: redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
			Password: conf.Password,
			DB:       conf.DB,
		}),
		ctx: ctx,
	}
	return ans
}
