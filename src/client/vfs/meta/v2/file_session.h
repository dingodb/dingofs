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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_

#include "json/value.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

// used by open file
class FileSessionMap {
 public:
  FileSessionMap() = default;
  ~FileSessionMap() = default;

  bool Put(uint64_t fh, std::string session_id) {
    utils::WriteLockGuard lk(lock_);

    auto it = file_session_map_.find(fh);
    if (it != file_session_map_.end()) {
      return false;
    }

    file_session_map_.insert(std::make_pair(fh, session_id));

    return true;
  }

  void Delete(uint64_t fh) {
    utils::WriteLockGuard lk(lock_);

    file_session_map_.erase(fh);
  }
  std::string Get(uint64_t fh) {
    utils::ReadLockGuard lk(lock_);

    auto it = file_session_map_.find(fh);
    return (it != file_session_map_.end()) ? it->second : "";
  }

  // output json format string
  void Dump(Json::Value& items) {
    utils::ReadLockGuard lk(lock_);

    for (const auto& [fh, session_id] : file_session_map_) {
      Json::Value item;
      item["fh"] = fh;
      item["session_id"] = session_id;
      items.append(item);
    }
  }

  bool Load(const Json::Value& value) {
    utils::WriteLockGuard lk(lock_);

    file_session_map_.clear();
    for (const auto& item : value) {
      uint64_t fh = item["fh"].asUInt64();
      file_session_map_[fh] = item["session_id"].asString();
    }

    return true;
  }

 private:
  utils::RWLock lock_;
  // fh -> session id
  std::map<uint64_t, std::string> file_session_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_FILE_SESSION_H_