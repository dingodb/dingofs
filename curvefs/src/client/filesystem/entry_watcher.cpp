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
 * Created Date: 2024-08-29
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/entry_watcher.h"

#include "curvefs/src/base/filepath/filepath.h"
#include "curvefs/src/base/string/string.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curvefs::base::filepath::HasSuffix;
using ::curvefs::base::string::StrSplit;

EntryWatcher::EntryWatcher(const std::string& nocto_suffix) {
  nocto_ = std::make_unique<LRUType>(65536);
  suffixs_ = StrSplit(nocto_suffix, ":");
}

void EntryWatcher::Remeber(const InodeAttr& attr, const std::string& filename) {
  if (!IsS3File(attr)) {
    return;
  }

  for (const auto& suffix : suffixs_) {
    if (HasSuffix(filename, suffix)) {
      WriteLockGuard lk(rwlock_);
      nocto_->Put(attr.inodeid(), true);
      return;
    }
  }
}

void EntryWatcher::Forget(Ino ino) {
  WriteLockGuard lk(rwlock_);
  nocto_->Remove(ino);
}

bool EntryWatcher::ShouldWriteback(Ino ino) {
  ReadLockGuard lk(rwlock_);
  bool ignore;
  return nocto_->Get(ino, &ignore);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
