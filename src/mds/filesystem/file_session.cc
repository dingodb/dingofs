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

#include "mds/filesystem/file_session.h"

#include <cstdint>
#include <string>
#include <utility>

#include "brpc/reloadable_flags.h"
#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {

DECLARE_uint32(mds_scan_batch_size);
DECLARE_uint32(mds_txn_max_retry_times);

DEFINE_uint32(mds_filesession_live_time_s, 600, "gc file session live time");
DEFINE_validator(mds_filesession_live_time_s, brpc::PassValidate);

static const std::string kFileSessionCacheMetricsPrefix = "dingofs_{}_filesession_{}";

static FileSessionSPtr NewFileSession(uint32_t fs_id, Ino ino, const std::string& client_id,
                                      const std::string& session_id) {
  auto file_session = std::make_shared<FileSessionEntry>();

  file_session->set_fs_id(fs_id);
  file_session->set_ino(ino);
  file_session->set_client_id(client_id);
  file_session->set_session_id(session_id);
  file_session->set_create_time_s(utils::Timestamp());
  file_session->set_expire_time_s(utils::Timestamp() + FLAGS_mds_filesession_live_time_s);

  return file_session;
}

FileSessionCache::FileSessionCache(uint32_t fs_id)
    : fs_id_(fs_id), total_count_(fmt::format(kFileSessionCacheMetricsPrefix, fs_id, "total_count")) {}

bool FileSessionCache::Put(FileSessionSPtr file_session) {
  auto key = Key{.ino = file_session->ino(), .session_id = file_session->session_id()};

  shard_map_.withWLock(
      [this, &key, &file_session](Map& map) mutable {
        auto it = map.find(key);
        if (it == map.end()) {
          map[key] = file_session;
          total_count_ << 1;
        }
      },
      key.ino);

  return true;
}

void FileSessionCache::Upsert(FileSessionSPtr file_session) {
  auto key = Key{.ino = file_session->ino(), .session_id = file_session->session_id()};

  shard_map_.withWLock(
      [this, &key, &file_session](Map& map) mutable {
        auto it = map.find(key);
        if (it == map.end()) {
          map[key] = file_session;
          total_count_ << 1;

        } else {
          it->second = file_session;
        }
      },
      key.ino);
}

void FileSessionCache::Delete(uint64_t ino, const std::string& session_id) {
  auto key = Key{.ino = ino, .session_id = session_id};

  shard_map_.withWLock([&key](Map& map) mutable { map.erase(key); }, key.ino);
}

void FileSessionCache::Delete(uint64_t ino) {
  auto key = Key{.ino = ino, .session_id = ""};

  shard_map_.withWLock(
      [&key](Map& map) mutable {
        for (auto it = map.upper_bound(key); it != map.end();) {
          if (it->first.ino != key.ino) break;

          it = map.erase(it);
        }
      },
      key.ino);
}

FileSessionSPtr FileSessionCache::Get(uint64_t ino, const std::string& session_id) {
  auto key = Key{.ino = ino, .session_id = session_id};

  FileSessionSPtr file_session;
  shard_map_.withRLock(
      [&key, &file_session](Map& map) mutable {
        auto it = map.find(key);
        if (it != map.end()) {
          file_session = it->second;
        }
      },
      ino);

  return file_session;
}

std::vector<FileSessionSPtr> FileSessionCache::Get(uint64_t ino) {
  auto key = Key{.ino = ino, .session_id = ""};

  std::vector<FileSessionSPtr> file_sessions;
  shard_map_.withRLock(
      [&key, &file_sessions](Map& map) mutable {
        for (auto it = map.upper_bound(key); it != map.end(); ++it) {
          if (it->first.ino != key.ino) break;

          file_sessions.push_back(it->second);
        }
      },
      ino);

  return file_sessions;
}

bool FileSessionCache::IsExist(uint64_t ino) {
  auto key = Key{.ino = ino, .session_id = ""};

  bool is_exist = false;
  shard_map_.withRLock([&key, &is_exist](Map& map) mutable { is_exist = map.upper_bound(key) != map.end(); }, ino);

  return is_exist;
}

bool FileSessionCache::IsExist(uint64_t ino, const std::string& session_id) {
  auto key = Key{.ino = ino, .session_id = session_id};

  bool is_exist = false;
  shard_map_.withRLock([&key, &is_exist](Map& map) mutable { is_exist = map.find(key) != map.end(); }, ino);

  return is_exist;
}

size_t FileSessionCache::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t FileSessionCache::Bytes() { return Size() * (sizeof(FileSessionEntry) + sizeof(Key)); }

void FileSessionCache::Summary(Json::Value& value) {
  value["name"] = "filesession";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
}

FileSessionManager::FileSessionManager(uint32_t fs_id, OperationProcessorSPtr operation_processor)
    : fs_id_(fs_id), file_session_cache_(fs_id), operation_processor_(operation_processor) {}

FileSessionSPtr FileSessionManager::Create(uint64_t ino, const std::string& client_id,
                                           const std::string& session_id) const {
  return NewFileSession(fs_id_, ino, client_id, session_id);
}

bool FileSessionManager::Put(FileSessionSPtr file_session) {
  if (!file_session_cache_.Put(file_session)) {
    LOG(WARNING) << fmt::format("[filesession] put file session fail, {}/{}", file_session->ino(),
                                file_session->session_id());
    return false;
  }

  return true;
}

FileSessionSPtr FileSessionManager::Get(uint64_t ino, const std::string& session_id, bool just_cache) {
  auto file_session = file_session_cache_.Get(ino, session_id);
  if (file_session != nullptr) {
    return file_session;
  }

  if (just_cache) {
    return nullptr;
  }

  auto status = GetFileSessionFromStore(ino, session_id, file_session);
  if (!status.ok()) {
    return nullptr;
  }

  // add to cache
  CHECK(file_session_cache_.Put(file_session))
      << fmt::format("[filesession] put file session fail, {}/{}", ino, session_id);

  return file_session;
}

std::vector<FileSessionSPtr> FileSessionManager::Get(uint64_t ino, bool just_cache) {
  auto file_sessions = file_session_cache_.Get(ino);
  if (!file_sessions.empty()) {
    return file_sessions;
  }

  if (just_cache) {
    return file_sessions;
  }

  auto status = GetFileSessionsFromStore(ino, file_sessions);
  if (!status.ok()) {
    return file_sessions;
  }

  return file_sessions;
}

Status FileSessionManager::GetAll(std::vector<FileSessionEntry>& file_sessions) {
  Trace trace;
  ScanFileSessionOperation operation(trace, fs_id_, [&](const FileSessionEntry& file_session) -> bool {
    file_sessions.push_back(file_session);
    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[filesession] scan file session fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

Status FileSessionManager::IsExist(uint64_t ino, bool just_cache, bool& is_exist) {
  is_exist = file_session_cache_.IsExist(ino);
  if (is_exist) {
    return Status::OK();
  }

  if (just_cache) {
    return Status(pb::error::ENOT_FOUND, "file session not found");
  }

  return IsExistFromStore(ino, is_exist);
}

Status FileSessionManager::Delete(uint64_t ino, const std::string& session_id) {
  // delete cache
  file_session_cache_.Delete(ino, session_id);

  return Status::OK();
}

Status FileSessionManager::Delete(uint64_t ino) {
  // delete cache
  file_session_cache_.Delete(ino);

  return Status::OK();
}

Status FileSessionManager::GetFileSessionsFromStore(uint64_t ino, std::vector<FileSessionSPtr>& file_sessions) {
  Trace trace;
  ScanFileSessionOperation operation(trace, fs_id_, ino, [&](const FileSessionEntry& file_session) -> bool {
    file_sessions.push_back(std::make_shared<FileSessionEntry>(file_session));
    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[filesession] scan file session fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

Status FileSessionManager::GetFileSessionFromStore(uint64_t ino, const std::string& session_id,
                                                   FileSessionSPtr& file_session) {
  Trace trace;
  GetFileSessionOperation operation(trace, fs_id_, ino, session_id);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[filesession] get file session fail, status({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();

  file_session = std::make_shared<FileSessionEntry>(result.file_session);

  return Status::OK();
}

Status FileSessionManager::IsExistFromStore(uint64_t ino, bool& is_exist) {
  Trace trace;
  ScanFileSessionOperation operation(trace, fs_id_, ino, [&](const FileSessionEntry&) -> bool {
    is_exist = true;
    return false;  // stop scanning
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[filesession] scan file session fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs