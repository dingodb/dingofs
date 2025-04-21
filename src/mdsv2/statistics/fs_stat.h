// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_MDSV2_FS_STAT_H_
#define DINGOFS_MDSV2_FS_STAT_H_

#include <memory>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class FsStats;
using FsStatsUPtr = std::unique_ptr<FsStats>;

class FsStats {
 public:
  FsStats(KVStorageSPtr kv_storage) : kv_storage_(kv_storage) {};
  ~FsStats() = default;

  static FsStatsUPtr New(KVStorageSPtr kv_storage) { return std::make_unique<FsStats>(kv_storage); }

  Status UploadFsStat(Context& ctx, uint32_t fs_id, const pb::mdsv2::FsStatsData& stats);
  Status GetFsStat(Context& ctx, uint32_t fs_id, pb::mdsv2::FsStatsData& stats);
  Status GetFsStatsPerSecond(Context& ctx, uint32_t fs_id,
                             std::map<uint64_t, pb::mdsv2::FsStatsData>& stats_per_second);

 private:
  // persistence store stats
  KVStorageSPtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_FS_STAT_H_