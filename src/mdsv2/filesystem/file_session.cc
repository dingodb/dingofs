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

#include "mdsv2/filesystem/file_session.h"

#include <cstdint>
#include <string>
#include <utility>

#include "dingofs/error.pb.h"
#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/store_operation.h"
#include "utils/concurrent/concurrent.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(fs_scan_batch_size);
DECLARE_uint32(txn_max_retry_times);

static const std::string kFileSessionCacheCountMetricsName = "dingofs_file_session_cache_count";

static const std::string kFileSessionTatalCountMetricsName = "dingofs_file_session_total_count";
static const std::string kFileSessionCountMetricsName = "dingofs_file_session_count";

static FileSessionPtr NewFileSession(uint32_t fs_id, Ino ino, const std::string& client_id) {
  auto file_session = std::make_shared<FileSessionEntry>();

  file_session->set_fs_id(fs_id);
  file_session->set_ino(ino);
  file_session->set_client_id(client_id);
  file_session->set_session_id(utils::UUIDGenerator::GenerateUUID());
  file_session->set_create_time_s(Helper::Timestamp());

  return file_session;
}

FileSessionCache::FileSessionCache() : count_metrics_(kFileSessionCacheCountMetricsName) {}

bool FileSessionCache::Put(FileSessionPtr file_session) {
  utils::WriteLockGuard guard(lock_);
  auto key = Key{.ino = file_session->ino(), .session_id = file_session->session_id()};
  auto it = file_session_map_.find(key);
  if (it != file_session_map_.end()) {
    return false;
  }

  file_session_map_[key] = file_session;

  count_metrics_ << 1;

  return true;
}

void FileSessionCache::Upsert(FileSessionPtr file_session) {
  utils::WriteLockGuard guard(lock_);

  auto key = Key{.ino = file_session->ino(), .session_id = file_session->session_id()};

  auto it = file_session_map_.find(key);
  if (it == file_session_map_.end()) {
    file_session_map_.insert(std::make_pair(key, file_session));

    count_metrics_ << 1;

  } else {
    it->second = file_session;
  }
}

void FileSessionCache::Delete(uint64_t ino, const std::string& session_id) {
  utils::WriteLockGuard guard(lock_);

  auto key = Key{.ino = ino, .session_id = session_id};
  file_session_map_.erase(key);
  count_metrics_ << -1;
}

void FileSessionCache::Delete(uint64_t ino) {
  utils::WriteLockGuard guard(lock_);

  auto key = Key{.ino = ino, .session_id = ""};

  for (auto it = file_session_map_.upper_bound(key); it != file_session_map_.end();) {
    if (it->first.ino != ino) {
      break;
    }

    it = file_session_map_.erase(it);
    count_metrics_ << -1;
  }
}

FileSessionPtr FileSessionCache::Get(uint64_t ino, const std::string& session_id) {
  utils::ReadLockGuard guard(lock_);

  auto key = Key{.ino = ino, .session_id = session_id};

  auto it = file_session_map_.find(key);
  return it != file_session_map_.end() ? it->second : nullptr;
}

std::vector<FileSessionPtr> FileSessionCache::Get(uint64_t ino) {
  utils::ReadLockGuard guard(lock_);

  auto key = Key{.ino = ino, .session_id = ""};

  std::vector<FileSessionPtr> file_sessions;
  for (auto it = file_session_map_.upper_bound(key); it != file_session_map_.end(); ++it) {
    if (it->first.ino != ino) {
      break;
    }

    file_sessions.push_back(it->second);
  }

  return file_sessions;
}

bool FileSessionCache::IsExist(uint64_t ino) {
  utils::ReadLockGuard guard(lock_);

  auto key = Key{.ino = ino, .session_id = ""};
  return file_session_map_.upper_bound(key) != file_session_map_.end();
}

bool FileSessionCache::IsExist(uint64_t ino, const std::string& session_id) {
  utils::ReadLockGuard guard(lock_);

  auto key = Key{.ino = ino, .session_id = session_id};
  return file_session_map_.find(key) != file_session_map_.end();
}

FileSessionManager::FileSessionManager(uint32_t fs_id, OperationProcessorSPtr operation_processor)
    : fs_id_(fs_id),
      operation_processor_(operation_processor),
      total_count_metrics_(kFileSessionTatalCountMetricsName),
      count_metrics_(kFileSessionCountMetricsName) {}

Status FileSessionManager::Create(uint64_t ino, const std::string& client_id, FileSessionPtr& file_session) {
  file_session = NewFileSession(fs_id_, ino, client_id);

  // add to cache
  CHECK(file_session_cache_.Put(file_session))
      << fmt::format("[filesession] put file session fail, {}/{}", ino, client_id);

  total_count_metrics_ << 1;
  count_metrics_ << 1;

  return Status::OK();
}

FileSessionPtr FileSessionManager::Get(uint64_t ino, const std::string& session_id, bool just_cache) {
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

std::vector<FileSessionPtr> FileSessionManager::Get(uint64_t ino, bool just_cache) {
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
  ScanFileSessionOperation operation(trace, fs_id_, 0, [&](const FileSessionEntry& file_session) -> bool {
    file_sessions.push_back(file_session);
    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] scan file session fail, status({}).", status.error_str());
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

  count_metrics_ << -1;

  return Status::OK();
}

Status FileSessionManager::Delete(uint64_t ino) {
  // delete cache
  file_session_cache_.Delete(ino);

  // count_metrics_ << (0 - static_cast<int64_t>(file_sessions.size()));

  return Status::OK();
}

Status FileSessionManager::GetFileSessionsFromStore(uint64_t ino, std::vector<FileSessionPtr>& file_sessions) {
  Trace trace;
  ScanFileSessionOperation operation(trace, fs_id_, ino, [&](const FileSessionEntry& file_session) -> bool {
    file_sessions.push_back(std::make_shared<FileSessionEntry>(file_session));
    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] scan file session fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

Status FileSessionManager::GetFileSessionFromStore(uint64_t ino, const std::string& session_id,
                                                   FileSessionPtr& file_session) {
  Trace trace;
  GetFileSessionOperation operation(trace, fs_id_, ino, session_id);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] get file session fail, status({}).", status.error_str());
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
    DINGO_LOG(ERROR) << fmt::format("[filesession] scan file session fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs