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
namespace vfs {
namespace v2 {

ParentCache::ParentCache() {
  // root ino is its own parent
  ino_map_.insert({1, Entry{1, 0}});
}

bool ParentCache::GetParent(int64_t ino, int64_t& parent) {
  utils::ReadLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it == ino_map_.end()) {
    return false;
  }

  if (it->second.parent == 0) {
    return false;
  }

  parent = it->second.parent;
  return true;
}

bool ParentCache::GetVersion(int64_t ino, uint64_t& version) {
  utils::ReadLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it == ino_map_.end()) {
    return false;
  }

  version = it->second.version;
  return true;
}

void ParentCache::Upsert(int64_t ino, int64_t parent) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.parent = parent;
  } else {
    ino_map_[ino] = Entry{.parent = parent, .version = 0};
  }
}

void ParentCache::Upsert(int64_t ino, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.version = version;
  } else {
    ino_map_[ino] = Entry{.parent = 0, .version = version};
  }
}

void ParentCache::Upsert(int64_t ino, int64_t parent, uint64_t version) {
  utils::WriteLockGuard lk(lock_);

  auto it = ino_map_.find(ino);
  if (it != ino_map_.end()) {
    it->second.parent = parent;
    if (version > it->second.version) {
      it->second.version = version;
    }
  } else {
    ino_map_[ino] = Entry{.parent = parent, .version = version};
  }
}

void ParentCache::Delete(int64_t ino) {
  utils::WriteLockGuard lk(lock_);

  ino_map_.erase(ino);
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs