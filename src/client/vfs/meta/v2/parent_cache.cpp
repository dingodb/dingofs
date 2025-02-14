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

#include "client/vfs/meta/v2/parent_cache.h"

#include "fmt/core.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace client {
namespace filesystem {

ParentCache::ParentCache() {
  // root ino is its own parent
  ino_to_parent_map_.insert({1, 1});
}

bool ParentCache::Get(int64_t ino, int64_t& parent_ino) {
  utils::ReadLockGuard lk(lock_);

  auto it = ino_to_parent_map_.find(ino);
  if (it == ino_to_parent_map_.end()) {
    return false;
  }

  parent_ino = it->second;
  return true;
}

void ParentCache::Upsert(int64_t ino, int64_t parent_ino) {
  DINGO_LOG(INFO) << fmt::format("save map ino({}) to parent({}).", ino,
                                 parent_ino);

  utils::WriteLockGuard lk(lock_);

  ino_to_parent_map_[ino] = parent_ino;
}

void ParentCache::Delete(int64_t ino) {
  utils::WriteLockGuard lk(lock_);

  ino_to_parent_map_.erase(ino);
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs