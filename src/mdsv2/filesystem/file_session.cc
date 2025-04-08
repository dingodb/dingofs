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

#include <string>
#include <utility>

#include "mdsv2/common/codec.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_int32(fs_scan_batch_size);
DECLARE_uint32(txn_max_retry_times);

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

std::vector<FileSession> FileSessionManager::GetFileSessionsFromCache(uint64_t ino) {
  utils::ReadLockGuard guard(lock_);

  std::vector<FileSession> file_sessions;
  for (auto& [session_id, file_session] : file_sessions_) {
    if (file_session.Ino() == ino) {
      file_sessions.push_back(file_session);
    }
  }

  return file_sessions;
}

std::vector<FileSession> FileSessionManager::GetFileSessionsFromStore(uint64_t ino) {
  Range range;
  MetaDataCodec::GetFileSessionRange(fs_id_, ino, range.start_key, range.end_key);

  std::vector<FileSession> file_sessions;
  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    auto txn = kv_storage_->NewTxn();

    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[filesession] scan fail, {} {}", ino, status.error_str());
      break;
    }

    for (auto& kv : kvs) {
      file_sessions.push_back(FileSession::Create(fs_id_, MetaDataCodec::DecodeFileSessionValue(kv.value)));
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return file_sessions;
}

bool FileSessionManager::GetFileSessionFromCache(uint64_t ino, const std::string& session_id,
                                                 FileSession& file_session) {
  utils::ReadLockGuard guard(lock_);

  auto it = file_sessions_.find(FileSession::EncodeKey(fs_id_, ino, session_id));
  if (it == file_sessions_.end()) {
    return false;
  }

  file_session = it->second;

  return true;
}

Status FileSessionManager::GetFileSessionFromStore(uint64_t ino, const std::string& session_id,
                                                   FileSession& file_session) {
  std::string key = FileSession::EncodeKey(fs_id_, ino, session_id);

  std::string value;
  auto status = kv_storage_->Get(key, value);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] get fail, {}/{} {}", ino, session_id, status.error_str());
    return status;
  }

  file_session = FileSession::Create(fs_id_, MetaDataCodec::DecodeFileSessionValue(value));

  return Status::OK();
}

Status FileSessionManager::Create(uint64_t ino, const std::string& client_id, FileSession& file_session) {
  file_session = FileSession::Create(fs_id_, ino, client_id);

  KVStorage::WriteOption write_option;
  auto status = kv_storage_->Put(write_option, file_session.EncodeKey(), file_session.EncodeValue());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] create fail, {}/{} {}", ino, client_id, status.error_str());
    return status;
  }

  // add to cache
  {
    utils::WriteLockGuard guard(lock_);
    file_sessions_.insert(std::make_pair(file_session.SessionId(), file_session));
  }

  return Status::OK();
}

Status FileSessionManager::IsExist(uint64_t ino, bool& is_exist) { return Status::OK(); }

Status FileSessionManager::Delete(uint64_t ino, const std::string& session_id) {
  auto status = kv_storage_->Delete(FileSession::EncodeKey(fs_id_, ino, session_id));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[filesession] delete fail, {}/{} {}", ino, session_id, status.error_str());
    return status;
  }

  // delete cache
  {
    utils::WriteLockGuard guard(lock_);
    file_sessions_.erase(session_id);
  }

  return Status::OK();
}

Status FileSessionManager::Delete(uint64_t ino) {
  auto file_sessions = GetFileSessionsFromStore(ino);

  Status status;
  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    for (auto& file_session : file_sessions) {
      txn->Delete(file_session.EncodeKey());
    }

    status = txn->Commit();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  // delete cache
  {
    utils::WriteLockGuard guard(lock_);

    for (auto& file_session : file_sessions) {
      file_sessions_.erase(file_session.EncodeKey());
    }
  }

  return status;
}

}  // namespace mdsv2
}  // namespace dingofs