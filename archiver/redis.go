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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type queueRecord struct {
	Key      string `json:"key"`
	Explicit bool   `json:"explicit"`
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

func (rd *RedisAdapter) TriggerChan(chname, value string) error {
	return rd.redis.Publish(rd.ctx, chname, value).Err()
}

func (rd *RedisAdapter) Set(k string, v any) error {
	cmd := rd.redis.Set(rd.ctx, k, v, 0)
	if cmd.Err() != nil {
		return fmt.Errorf("failed to set Redis item %s: %w", k, cmd.Err())
	}
	return nil
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
	fmt.Println("ITEMS: ", items)
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

func (rd *RedisAdapter) AddError(errQueue string, item queueRecord, rec *cncdb.ArchRecord) error {
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

func (rd *RedisAdapter) GetConcRecord(id string) (cncdb.ArchRecord, error) {
	ans := rd.redis.Get(rd.ctx, rd.mkKey(id))
	if ans.Err() == redis.Nil {
		return cncdb.ArchRecord{}, cncdb.ErrRecordNotFound
	}
	if ans.Err() != nil {
		return cncdb.ArchRecord{}, fmt.Errorf("failed to get concordance record: %w", ans.Err())
	}
	return cncdb.ArchRecord{
		ID:   id,
		Data: ans.Val(),
	}, nil
}

func NewRedisAdapter(conf *RedisConf) *RedisAdapter {
	ans := &RedisAdapter{
		conf: conf,
		redis: redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
			Password: conf.Password,
			DB:       conf.DB,
		}),
		ctx: context.Background(),
	}
	return ans
}
