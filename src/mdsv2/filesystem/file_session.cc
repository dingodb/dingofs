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

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>
#include <utility>

#include "dingofs/error.pb.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_int32(fs_scan_batch_size);
DECLARE_uint32(txn_max_retry_times);

static const std::string kFileSessionCacheCountMetricsName = "dingofs_file_session_cache_count";

static const std::string kFileSessionTatalCountMetricsName = "dingofs_file_session_total_count";
static const std::string kFileSessionCountMetricsName = "dingofs_file_session_count";

std::string FileSession::EncodeKey(uint32_t fs_id, uint64_t ino, const std::string& session_id) {
  return MetaDataCodec::EncodeFileSessionKey(fs_id, ino, session_id);
}

std::string FileSession::EncodeKey() const { return MetaDataCodec::EncodeFileSessionKey(fs_id_, ino_, session_id_); }

std::string FileSession::EncodeValue() const {
  pb::mdsv2::FileSession file_session;
  file_session.set_session_id(session_id_);
  file_session.set_ino(ino_);
  file_session.set_client_id(client_id_);

  return MetaDataCodec::EncodeFileSessionValue(file_session);
}

FileSessionCache::FileSessionCache() : count_metrics_(kFileSessionCacheCountMetricsName) {}

bool FileSessionCache::Put(FileSessionPtr file_session) {
  utils::WriteLockGuard guard(lock_);
  auto key = Key{.ino = file_session->Ino(), .session_id = file_session->SessionId()};
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

  auto key = Key{.ino = file_session->Ino(), .session_id = file_session->SessionId()};

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

FileSessionManager::FileSessionManager(uint32_t fs_id, KVStorageSPtr kv_storage)
    : fs_id_(fs_id),
      kv_storage_(kv_storage),
      total_count_metrics_(kFileSessionTatalCountMetricsName),
      count_metrics_(kFileSessionCountMetricsName) {}

Status FileSessionManager::Create(uint64_t ino, const std::string& client_id, FileSessionPtr& file_session) {
  file_session = FileSession::New(fs_id_, ino, client_id);

  KVStorage::WriteOption write_option;
  auto status = kv_storage_->Put(write_option, file_session->EncodeKey(), file_session->EncodeValue());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] create fail, {}/{} {}", ino, client_id, status.error_str());
    return status;
  }

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
  auto status = kv_storage_->Delete(FileSession::EncodeKey(fs_id_, ino, session_id));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] delete fail, {}/{} {}", ino, session_id, status.error_str());
    return status;
  }

  // delete cache
  file_session_cache_.Delete(ino, session_id);

  count_metrics_ << -1;

  return Status::OK();
}

Status FileSessionManager::Delete(uint64_t ino) {
  std::vector<FileSessionPtr> file_sessions;
  auto status = GetFileSessionsFromStore(ino, file_sessions);
  if (!status.ok()) {
    return status;
  }

  if (file_sessions.empty()) {
    return Status::OK();
  }

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    for (auto& file_session : file_sessions) {
      txn->Delete(file_session->EncodeKey());
    }

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  if (status.ok()) {
    // delete cache
    file_session_cache_.Delete(ino);

    count_metrics_ << (0 - static_cast<int64_t>(file_sessions.size()));
  }

  return status;
}

Status FileSessionManager::GetFileSessionsFromStore(uint64_t ino, std::vector<FileSessionPtr>& file_sessions) {
  Range range;
  MetaDataCodec::GetFileSessionRange(fs_id_, ino, range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();
  std::vector<KeyValue> kvs;
  do {
    kvs.clear();

    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[filesession] scan fail, {} {}", ino, status.error_str());
      break;
    }

    for (auto& kv : kvs) {
      file_sessions.push_back(FileSession::New(fs_id_, MetaDataCodec::DecodeFileSessionValue(kv.value)));
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return Status::OK();
}

Status FileSessionManager::GetFileSessionFromStore(uint64_t ino, const std::string& session_id,
                                                   FileSessionPtr& file_session) {
  std::string key = FileSession::EncodeKey(fs_id_, ino, session_id);

  std::string value;
  auto status = kv_storage_->Get(key, value);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] get fail, {}/{} {}", ino, session_id, status.error_str());
    return status;
  }

  file_session = FileSession::New(fs_id_, MetaDataCodec::DecodeFileSessionValue(value));

  return Status::OK();
}

Status FileSessionManager::IsExistFromStore(uint64_t ino, bool& is_exist) {
  Range range;
  MetaDataCodec::GetFileSessionRange(fs_id_, ino, range.start_key, range.end_key);

  std::vector<KeyValue> kvs;
  auto txn = kv_storage_->NewTxn();
  auto status = txn->Scan(range, 1, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] scan fail, {} {}", ino, status.error_str());
    return status;
  }

  is_exist = !kvs.empty();

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs