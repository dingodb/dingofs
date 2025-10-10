/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "client/vfs/components/file_suffix_watcher.h"

#include <absl/strings/str_split.h>
#include <glog/logging.h>

#include <mutex>
#include <shared_mutex>

#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

FileSuffixWatcher::FileSuffixWatcher(const std::string& writeback_suffix) {
  if (writeback_suffix.empty()) {
    return;
  }

  std::vector<std::string> suffixs = absl::StrSplit(writeback_suffix, ":");
  for (const auto& suffix : suffixs) {
    VLOG(9) << "writeback_suffix " << writeback_suffix << ", split suffix "
            << suffix;
    if (!suffix.empty()) {
      suffixs_.push_back(suffix);
    }
  }
}

void FileSuffixWatcher::Remeber(const Attr& attr, const std::string& filename) {
  VLOG(9) << "Remeber file: " << filename << ", ino: " << attr.ino
          << ", type: " << FileType2Str(attr.type);
  if (attr.type != kFile) {
    return;
  }

  bool writeback = false;
  for (const auto& suffix : suffixs_) {
    if (absl::EndsWith(filename, suffix)) {
      VLOG(3) << "ino: " << attr.ino << " file: " << filename
              << " should writeback, because suffix: " << suffix;
      writeback = true;
      break;
    }
  }

  if (writeback) {
    std::unique_lock<std::shared_mutex> lg(rw_lock_);
    writeback_ino_.insert(attr.ino);
  }
}

void FileSuffixWatcher::Forget(Ino ino) {
  VLOG(9) << "Forget file with ino: " << ino;
  std::unique_lock<std::shared_mutex> lg(rw_lock_);
  writeback_ino_.erase(ino);
}

bool FileSuffixWatcher::ShouldWriteback(Ino ino) {
  VLOG(9) << "ShouldWriteback ino: " << ino;
  std::shared_lock<std::shared_mutex> lk(rw_lock_);
  return writeback_ino_.count(ino) > 0;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
