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
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisAdapter struct {
	conf  *RedisConf
	redis *redis.Client
	ctx   context.Context
}

func (rd *RedisAdapter) NextNItems(n int64) ([]string, error) {
	ans := make([]string, 0, n)
	ppl := rd.redis.Pipeline()
	lrangeCmd := ppl.LRange(rd.ctx, rd.conf.QueueKey, 0, n-1)
	ppl.LTrim(rd.ctx, rd.conf.QueueKey, n, -1)
	_, err := ppl.Exec(rd.ctx)
	if err != nil {
		return []string{}, fmt.Errorf("failed to get items from queue: %w", err)
	}
	items, err := lrangeCmd.Result()
	if err != nil {
		return []string{}, fmt.Errorf("failed to get items from queue: %w", err)
	}
	ans = append(ans, items...)
	return ans, nil
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
