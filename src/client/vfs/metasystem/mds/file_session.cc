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

#include "client/vfs/metasystem/mds/file_session.h"

#include <cstdint>
#include <string>

#include "fmt/format.h"
#include "glog/logging.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

std::string FileSession::GetSessionID(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = session_id_map_.find(fh);
  return (it != session_id_map_.end()) ? it->second.session_id : "";
}

uint32_t FileSession::GetFlags(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = session_id_map_.find(fh);
  return (it != session_id_map_.end()) ? it->second.flags : 0;
}

void FileSession::AddSession(uint64_t fh, const std::string& session_id,
                             uint32_t flags) {
  utils::WriteLockGuard lk(lock_);

  session_id_map_[fh] = {flags, session_id};

  IncRef();
  chunk_set_->RefreshLastActiveTime();
}

uint32_t FileSession::DeleteSession(uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  session_id_map_.erase(fh);

  return DecRef();
}

size_t FileSession::Size() {
  utils::ReadLockGuard lk(lock_);

  return session_id_map_.size();
}

size_t FileSession::Bytes() {
  utils::ReadLockGuard lk(lock_);

  size_t bytes = 0;
  for (const auto& [_, session_info] : session_id_map_) {
    bytes +=
        session_info.session_id.size() + sizeof(uint64_t) + sizeof(uint32_t);
  }

  return sizeof(FileSession) + bytes;
}

bool FileSession::Dump(Json::Value& value) {
  value["ino"] = ino_;
  value["ref_count"] = ref_count_.load();

  // dump session_id_map
  Json::Value session_id_map = Json::arrayValue;
  for (const auto& [fh, session_info] : session_id_map_) {
    Json::Value item;
    item["fh"] = fh;
    item["session_id"] = session_info.session_id;
    item["flags"] = session_info.flags;

    session_id_map.append(item);
  }
  value["session_id_map"] = session_id_map;

  // dump chunk_set_
  Json::Value chunk_set_value = Json::objectValue;
  chunk_set_->Dump(chunk_set_value);
  value["chunk_set"] = chunk_set_value;

  return true;
}

bool FileSession::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] file_session is not object.";
    return false;
  }

  ino_ = value["ino"].asUInt64();
  ref_count_.store(value["ref_count"].asUInt());

  // load session_id_map
  const auto& session_id_map_value = value["session_id_map"];
  if (!session_id_map_value.isNull()) {
    if (!session_id_map_value.isArray()) {
      LOG(ERROR)
          << "[meta.filesession] file_session.session_id_map is not array.";
      return false;
    }

    session_id_map_.clear();
    for (const auto& item : session_id_map_value) {
      uint64_t fh = item["fh"].asUInt64();
      std::string session_id = item["session_id"].asString();
      uint32_t flags = item["flags"].asUInt();
      session_id_map_[fh] = {flags, session_id};
    }
  }

  return true;
}

FileSessionSPtr FileSessionMap::Put(InodeSPtr& inode, uint64_t fh,
                                    const std::string& session_id,
                                    uint32_t flags) {
  CHECK(inode != nullptr) << "inode is nullptr.";
  CHECK(fh != 0) << "fh is zero.";
  CHECK(!session_id.empty()) << "session_id is empty.";

  const Ino ino = inode->Ino();

  LOG(INFO) << fmt::format(
      "[meta.filesession.{}.{}] add file session, session_id({}).", ino, fh,
      session_id);

  FileSessionSPtr file_session;
  shard_map_.withWLock(
      [this, ino, inode, fh, &session_id, &file_session, flags](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          file_session = it->second;
          file_session->AddSession(fh, session_id, flags);
        } else {
          file_session =
              FileSession::New(ino, inode, chunk_cache_.GetOrCreate(ino));
          file_session->AddSession(fh, session_id, flags);
          map[ino] = file_session;
          total_count_ << 1;
        }
      },
      ino);

  return file_session;
}

void FileSessionMap::Delete(Ino ino, uint64_t fh) {
  CHECK(ino != 0) << "ino is zero.";
  CHECK(fh != 0) << "fh is zero.";

  uint32_t ref_count = 0;
  shard_map_.withWLock(
      [this, ino, fh, &ref_count](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          auto& file_session = it->second;
          ref_count = file_session->DeleteSession(fh);
          if (ref_count == 0) {
            file_session->GetChunkSet()->ResetLastWriteLength();
            map.erase(it);
          }
        }
      },
      ino);

  LOG(INFO) << fmt::format(
      "[meta.filesession.{}.{}] delete file session, ref_count({}).", ino, fh,
      ref_count);
}

std::string FileSessionMap::GetSessionID(Ino ino, uint64_t fh) {
  CHECK(ino != 0) << "ino is zero.";
  CHECK(fh != 0) << "fh is zero.";

  std::string session_id;
  shard_map_.withRLock(
      [this, ino, fh, &session_id](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          session_id = it->second->GetSessionID(fh);
        }
      },
      ino);

  return session_id;
}

FileSessionSPtr FileSessionMap::GetSession(Ino ino) {
  CHECK(ino != 0) << "ino is zero.";

  FileSessionSPtr file_session;
  shard_map_.withRLock(
      [this, ino, &file_session](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          file_session = it->second;
        }
      },
      ino);

  return file_session;
}

std::vector<FileSessionSPtr> FileSessionMap::GetAllSession() {
  std::vector<FileSessionSPtr> file_sessions;

  shard_map_.iterate([&file_sessions](const Map& map) {
    for (const auto& [_, file_session] : map) {
      file_sessions.push_back(file_session);
    }
  });

  return file_sessions;
}

size_t FileSessionMap::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t FileSessionMap::Bytes() {
  size_t bytes = 0;
  shard_map_.iterate([&bytes](Map& map) {
    for (const auto& [_, file_session] : map) {
      bytes += file_session->Bytes();
    }
  });
  return bytes;
}

void FileSessionMap::Summary(Json::Value& value) {
  value["name"] = "filesession";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
}

bool FileSessionMap::Dump(Ino ino, Json::Value& value) {
  auto file_session = GetSession(ino);

  value["ino"] = file_session->GetIno();
  return file_session->Dump(value);
}

// output json format string
bool FileSessionMap::Dump(Json::Value& value) {
  std::vector<FileSessionSPtr> file_sessions;

  shard_map_.iterate([&file_sessions](const Map& map) {
    for (const auto& [_, file_session] : map) {
      file_sessions.push_back(file_session);
    }
  });

  Json::Value file_sessions_items = Json::arrayValue;
  for (const auto& file_session : file_sessions) {
    Json::Value file_session_item;
    file_session_item["ino"] = file_session->GetIno();
    CHECK(file_session->Dump(file_session_item)) << "file session dump fail.";

    file_sessions_items.append(file_session_item);
  }

  value["file_sessions"] = file_sessions_items;

  LOG(INFO) << fmt::format("[meta.filesession] dump file session count({}).",
                           file_sessions.size());

  return true;
}

bool FileSessionMap::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] file_session_map is not an object.";
    return false;
  }

  // load file_session_map
  const auto& file_sessions = value["file_sessions"];
  if (!file_sessions.isNull()) {
    if (!file_sessions.isArray()) {
      LOG(ERROR) << "[meta.filesession] file_session_map is not an array.";
      return false;
    }

    for (const auto& item : file_sessions) {
      Ino ino = item["ino"].asUInt64();

      auto file_session = FileSession::New(ino, inode_cache_.Get(ino),
                                           chunk_cache_.GetOrCreate(ino));
      CHECK(file_session->Load(item))
          << fmt::format("load file session fail, ino({}).", ino);

      Put(file_session);
    }

    LOG(INFO) << fmt::format("[meta.filesession] load file session count({}).",
                             file_sessions.size());
  }

  return true;
}

void FileSessionMap::Put(FileSessionSPtr file_session) {
  Ino ino = file_session->GetIno();
  CHECK(ino != 0) << "ino is zero.";

  shard_map_.withWLock(
      [this, ino, &file_session](Map& map) {
        auto it = map.find(ino);
        if (it == map.end()) {
          map[ino] = file_session;
        }
      },
      ino);
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs