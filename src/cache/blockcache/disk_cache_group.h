/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_cache_watcher.h"
#include "cache/common/bvar.h"
#include "cache/common/context.h"
#include "cache/iutil/con_hash.h"

namespace dingofs {
namespace cache {

struct DiskCacheGroupVarsCollector {
  DiskCacheGroupVarsCollector() = default;

  inline static const std::string prefix = "dingofs_disk_cache_group";

  OpVar op_stage{absl::StrFormat("%s_%s", prefix, "stage")};
  OpVar op_cache{absl::StrFormat("%s_%s", prefix, "cache")};
  OpVar op_load{absl::StrFormat("%s_%s", prefix, "load")};
};

using DiskCacheGroupVarsCollectorSPtr =
    std::shared_ptr<DiskCacheGroupVarsCollector>;

struct DiskCacheGroupVarsRecordGuard {
  DiskCacheGroupVarsRecordGuard(const std::string& op_name, size_t bytes,
                                Status& status,
                                DiskCacheGroupVarsCollectorSPtr metric)
      : op_name(op_name), bytes(bytes), status(status), metric(metric) {
    CHECK(op_name == "Stage" || op_name == "Cache" || op_name == "Load")
        << "Invalid operation name: " << op_name;
    timer.start();
  }

  ~DiskCacheGroupVarsRecordGuard() {
    timer.stop();

    OpVar* op;
    if (op_name == "Stage") {
      op = &metric->op_stage;
    } else if (op_name == "Cache") {
      op = &metric->op_cache;
    } else if (op_name == "Load") {
      op = &metric->op_load;
    }

    if (status.ok()) {
      op->op_per_second.total_count << 1;
      op->bandwidth_per_second.total_count << bytes;
      op->latency << timer.u_elapsed();
      op->total_latency << timer.u_elapsed();
    } else {
      op->error_per_second.total_count << 1;
    }
  }

  std::string op_name;
  size_t bytes;
  Status& status;
  butil::Timer timer;
  DiskCacheGroupVarsCollectorSPtr metric;
};

class DiskCacheGroup final : public CacheStore {
 public:
  explicit DiskCacheGroup(std::vector<DiskCacheOption> options);
  ~DiskCacheGroup() override = default;

  Status Start(UploadFunc uploader) override;
  Status Shutdown() override;

  Status Stage(ContextSPtr ctx, const BlockKey& key, const Block& block,
               StageOption option = StageOption()) override;
  Status RemoveStage(ContextSPtr ctx, const BlockKey& key,
                     RemoveStageOption option = RemoveStageOption()) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
               CacheOption option = CacheOption()) override;
  Status Load(ContextSPtr ctx, const BlockKey& key, off_t offset, size_t length,
              IOBuffer* buffer, LoadOption option = LoadOption()) override;

  std::string Id() const override;
  bool IsRunning() const override;
  bool IsCached(const BlockKey& key) const override;
  bool IsFull(const BlockKey& key) const override;
  bool Dump(Json::Value& value) const override;

 private:
  static std::vector<uint64_t> CalcWeights(
      std::vector<DiskCacheOption> options);
  DiskCacheSPtr GetStore(const BlockKey& key) const;
  DiskCacheSPtr GetStore(const std::string& store_id) const;

  std::atomic<bool> running_;
  const std::vector<DiskCacheOption> options_;
  std::unique_ptr<iutil::ConHash> chash_;
  std::unordered_map<std::string, DiskCacheSPtr> stores_;
  DiskCacheWatcherUPtr watcher_;
  DiskCacheGroupVarsCollectorSPtr vars_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_
