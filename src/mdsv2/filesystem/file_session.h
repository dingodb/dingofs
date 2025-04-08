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
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mdsv2 {

// represent file session for client open file
class FileSession {
 public:
  FileSession() = default;
  FileSession(uint32_t fs_id, const std::string& session_id, uint64_t ino, const std::string& client_id)
      : fs_id_(fs_id), session_id_(session_id), ino_(ino), client_id_(client_id) {}

  static FileSession Create(uint32_t fs_id, uint64_t ino, const std::string& client_id) {
    utils::UUIDGenerator uuid_generator;
    return FileSession(fs_id, uuid_generator.GenerateUUID(), ino, client_id);
  }

  static FileSession Create(uint32_t fs_id, const pb::mdsv2::FileSession& file_session) {
    return FileSession(fs_id, file_session.session_id(), file_session.ino(), file_session.client_id());
  }

  std::string SessionId() const { return session_id_; }
  uint64_t Ino() const { return ino_; }
  std::string ClientId() const { return client_id_; }

  static std::string EncodeKey(uint32_t fs_id, uint64_t ino, const std::string& session_id);
  std::string EncodeKey() const;
  std::string EncodeValue() const;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  std::string session_id_;
  std::string client_id_;
};

class FileSessionManager;
using FileSessionManagerPtr = std::shared_ptr<FileSessionManager>;

// manage filesystem all client open file session
// persist store file session and cache file session
class FileSessionManager {
 public:
  FileSessionManager(uint32_t fs_id, KVStoragePtr kv_storage) : fs_id_(fs_id), kv_storage_(kv_storage) {}
  ~FileSessionManager() = default;

  static FileSessionManagerPtr New(uint32_t fs_id, KVStoragePtr kv_storage) {
    return std::make_shared<FileSessionManager>(fs_id, kv_storage);
  }

  Status Create(uint64_t ino, const std::string& client_id, FileSession& file_session);
  Status IsExist(uint64_t ino, bool& is_exist);
  Status Delete(uint64_t ino, const std::string& session_id);
  Status Delete(uint64_t ino);

 private:
  std::vector<FileSession> GetFileSessionsFromCache(uint64_t ino);
  std::vector<FileSession> GetFileSessionsFromStore(uint64_t ino);
  bool GetFileSessionFromCache(uint64_t ino, const std::string& session_id, FileSession& file_session);
  Status GetFileSessionFromStore(uint64_t ino, const std::string& session_id, FileSession& file_session);

  uint32_t fs_id_;

  KVStoragePtr kv_storage_;

  utils::RWLock lock_;
  // session_id -> file_session
  std::map<std::string, FileSession> file_sessions_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_FILE_SESSION_H_