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

#include "mdsv2/filesystem/parent_memo.h"

namespace dingofs {
namespace mdsv2 {

void ParentMemo::Remeber(Ino ino, Ino parent) {
  utils::WriteLockGuard lk(rwlock_);

  auto it = parent_map_.find(ino);
  if (it == parent_map_.end()) {
    parent_map_.emplace(ino, parent);
  } else {
    it->second = parent;
  }
}

void ParentMemo::Forget(Ino ino) {
  utils::WriteLockGuard lk(rwlock_);

  parent_map_.erase(ino);
}

bool ParentMemo::GetParent(Ino ino, Ino& parent) {
  utils::ReadLockGuard lk(rwlock_);

  auto it = parent_map_.find(ino);
  if (it == parent_map_.end()) {
    return false;
  }

  parent = it->second;
  return true;
}

}  // namespace mdsv2
}  // namespace dingofs