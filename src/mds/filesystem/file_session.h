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

#ifndef DINGOFS_MDS_FILESYSTEM_FILE_SESSION_H_
#define DINGOFS_MDS_FILESYSTEM_FILE_SESSION_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

using FileSessionSPtr = std::shared_ptr<FileSessionEntry>;

// cache file session
class FileSessionCache {
 public:
  FileSessionCache(uint32_t fs_id);
  ~FileSessionCache() = default;

  struct Key {
    uint64_t ino{0};
    std::string session_id;

    bool operator<(const Key& other) const {
      if (ino != other.ino) {
        return ino < other.ino;
      }
      return session_id < other.session_id;
    }
  };

  bool Put(FileSessionSPtr file_session);
  void Upsert(FileSessionSPtr file_session);
  void Delete(uint64_t ino, const std::string& session_id);
  void Delete(uint64_t ino);

  FileSessionSPtr Get(uint64_t ino, const std::string& session_id);
  std::vector<FileSessionSPtr> Get(uint64_t ino);
  bool IsExist(uint64_t ino);
  bool IsExist(uint64_t ino, const std::string& session_id);

 private:
  utils::RWLock lock_;
  // ino/session_id -> file_session
  std::map<Key, FileSessionSPtr> file_session_map_;

  // statistics
  bvar::Adder<int64_t> count_metrics_;
};

class FileSessionManager;
using FileSessionManagerUPtr = std::unique_ptr<FileSessionManager>;

// manage filesystem all client open file session
// persist store file session and cache file session
class FileSessionManager {
 public:
  FileSessionManager(uint32_t fs_id, OperationProcessorSPtr operation_processor);
  ~FileSessionManager() = default;

  FileSessionManager(const FileSessionManager&) = delete;
  FileSessionManager& operator=(const FileSessionManager&) = delete;
  FileSessionManager(FileSessionManager&&) = delete;
  FileSessionManager& operator=(FileSessionManager&&) = delete;

  static FileSessionManagerUPtr New(uint32_t fs_id, OperationProcessorSPtr operation_processor) {
    return std::make_unique<FileSessionManager>(fs_id, operation_processor);
  }

  FileSessionSPtr Create(uint64_t ino, const std::string& client_id) const;
  void Put(FileSessionSPtr file_session);
  Status IsExist(uint64_t ino, bool just_cache, bool& is_exist);
  Status Delete(uint64_t ino, const std::string& session_id);
  Status Delete(uint64_t ino);

  FileSessionSPtr Get(uint64_t ino, const std::string& session_id, bool just_cache = false);
  std::vector<FileSessionSPtr> Get(uint64_t ino, bool just_cache = false);
  Status GetAll(std::vector<FileSessionEntry>& file_sessions);

  FileSessionCache& GetFileSessionCache() { return file_session_cache_; }

 private:
  Status GetFileSessionsFromStore(uint64_t ino, std::vector<FileSessionSPtr>& file_sessions);
  Status GetFileSessionFromStore(uint64_t ino, const std::string& session_id, FileSessionSPtr& file_session);
  Status IsExistFromStore(uint64_t ino, bool& is_exist);

  uint32_t fs_id_;

  // cache file session
  FileSessionCache file_session_cache_;

  OperationProcessorSPtr operation_processor_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_FILE_SESSION_H_