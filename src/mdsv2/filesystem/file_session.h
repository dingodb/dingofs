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

#ifndef DINGOFS_MDV2_FILESYSTEM_FILE_SESSION_H_
#define DINGOFS_MDV2_FILESYSTEM_FILE_SESSION_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mdsv2 {

class FileSession;
using FileSessionPtr = std::shared_ptr<FileSession>;

// represent file session for client open file
class FileSession {
 public:
  FileSession() = default;
  FileSession(uint32_t fs_id, const std::string& session_id, uint64_t ino, const std::string& client_id)
      : fs_id_(fs_id), session_id_(session_id), ino_(ino), client_id_(client_id) {}

  static FileSessionPtr New(uint32_t fs_id, uint64_t ino, const std::string& client_id) {
    return std::make_shared<FileSession>(fs_id, utils::UUIDGenerator::GenerateUUID(), ino, client_id);
  }

  static FileSessionPtr New(uint32_t fs_id, const pb::mdsv2::FileSession& file_session) {
    return std::make_shared<FileSession>(fs_id, file_session.session_id(), file_session.ino(),
                                         file_session.client_id());
  }

  const std::string& SessionId() const { return session_id_; }
  uint64_t Ino() const { return ino_; }
  const std::string& ClientId() const { return client_id_; }

  static std::string EncodeKey(uint32_t fs_id, uint64_t ino, const std::string& session_id);
  std::string EncodeKey() const;
  std::string EncodeValue() const;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  std::string session_id_;
  std::string client_id_;
};

// cache file session
class FileSessionCache {
 public:
  FileSessionCache() = default;
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

  bool Put(FileSessionPtr file_session);
  void Upsert(FileSessionPtr file_session);
  void Delete(uint64_t ino, const std::string& session_id);
  void Delete(uint64_t ino);
  FileSessionPtr Get(uint64_t ino, const std::string& session_id);
  std::vector<FileSessionPtr> Get(uint64_t ino);
  bool IsExist(uint64_t ino);
  bool IsExist(uint64_t ino, const std::string& session_id);

 private:
  utils::RWLock lock_;
  // ino/session_id -> file_session
  std::map<Key, FileSessionPtr> file_session_map_;
};

class FileSessionManager;
using FileSessionManagerUPtr = std::unique_ptr<FileSessionManager>;

// manage filesystem all client open file session
// persist store file session and cache file session
class FileSessionManager {
 public:
  FileSessionManager(uint32_t fs_id, KVStoragePtr kv_storage) : fs_id_(fs_id), kv_storage_(kv_storage) {}
  ~FileSessionManager() = default;

  static FileSessionManagerUPtr New(uint32_t fs_id, KVStoragePtr kv_storage) {
    return std::make_unique<FileSessionManager>(fs_id, kv_storage);
  }

  Status Create(uint64_t ino, const std::string& client_id, FileSessionPtr& file_session);
  Status IsExist(uint64_t ino, bool just_cache, bool& is_exist);
  Status Delete(uint64_t ino, const std::string& session_id);
  Status Delete(uint64_t ino);
  FileSessionPtr Get(uint64_t ino, const std::string& session_id, bool just_cache = false);
  std::vector<FileSessionPtr> Get(uint64_t ino, bool just_cache = false);

  FileSessionCache& GetFileSessionCache() { return file_session_cache_; }

 private:
  Status GetFileSessionsFromStore(uint64_t ino, std::vector<FileSessionPtr>& file_sessions);
  Status GetFileSessionFromStore(uint64_t ino, const std::string& session_id, FileSessionPtr& file_session);
  Status IsExistFromStore(uint64_t ino, bool& is_exist);

  uint32_t fs_id_;

  // store file session
  KVStoragePtr kv_storage_;

  // cache file session
  FileSessionCache file_session_cache_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_FILE_SESSION_H_