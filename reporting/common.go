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

package reporting

import (
	"context"
)

type BgJobStats struct {
	NumErrors   int `json:"numErrors"`
	NumMerged   int `json:"numMerged"`
	NumInserted int `json:"numInserted"`
	NumFetched  int `json:"numFetched"`
}

func (bgs *BgJobStats) UpdateBy(other BgJobStats) {
	bgs.NumErrors += other.NumErrors
	bgs.NumMerged += other.NumMerged
	bgs.NumInserted += other.NumInserted
	bgs.NumFetched += other.NumFetched
}

// ------------

type CleanupStats struct {
	NumFetched int `json:"numFetched"`
	NumMerged  int `json:"numMerged"`
	NumErrors  int `json:"numErrors"`
}

// ------------

type IReporting interface {
	Start(ctx context.Context)
	Stop(ctx context.Context) error
	WriteOperationsStatus(item BgJobStats)
	WriteCleanupStatus(item CleanupStats)
}
