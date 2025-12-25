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

#include "client/vfs/metasystem/mds/id_cache.h"

#include <gflags/gflags.h>

#include <memory>

#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

DEFINE_uint32(client_vfs_id_cache_batch_size, 100000, "id cache batch size");

bool IdCache::GenID(uint64_t& id) { return GenID(1, id); }

bool IdCache::GenID(uint32_t num, uint64_t& id) {
  if (num == 0) {
    LOG(ERROR) << fmt::format("[meta.idcache.{}] num cant not 0.", name_);
    return false;
  }

  {
    utils::WriteLockGuard lk(lock_);

    if (next_id_ + num > last_alloc_id_) {
      auto status = AllocateIds(std::max(num, batch_size_));
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[meta.idcache.{}] allocate id fail, {}.",
                                  name_, status.ToString());
        return false;
      }
    }

    // allocate id
    id = next_id_;
    next_id_ += num;
  }

  LOG(INFO) << fmt::format("[meta.idcache.{}] alloc id({}) num({}).", name_, id,
                           num);

  return true;
}

Status IdCache::AllocateIds(uint32_t size) {
  uint64_t id = 0;
  auto context = std::make_shared<Context>("", "", "");
  auto status = mds_client_->NewSliceId(context, size, &id);
  if (!status.ok()) {
    return status;
  }

  next_id_ = id;
  last_alloc_id_ = id + size;

  return Status::OK();
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs