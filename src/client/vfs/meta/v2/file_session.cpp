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

#include "client/vfs/meta/v2/file_session.h"

#include <string>

#include "absl/strings/escaping.h"
#include "client/vfs/meta/v2/helper.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

FileSession::ChunkSPtr FileSession::Chunk::From(
    const mdsv2::ChunkEntry& chunk) {
  ChunkSPtr out_chunk = std::make_shared<Chunk>();

  out_chunk->index = chunk.index();
  out_chunk->chunk_size = chunk.chunk_size();
  out_chunk->block_size = chunk.block_size();

  out_chunk->slices.reserve(chunk.slices_size());
  for (const auto& slice : chunk.slices()) {
    out_chunk->slices.push_back(Helper::ToSlice(slice));
  }

  out_chunk->version = chunk.version();
  out_chunk->last_compaction_time_ms = chunk.last_compaction_time_ms();

  return out_chunk;
}

mdsv2::ChunkEntry FileSession::Chunk::To(FileSession::ChunkSPtr chunk) {
  mdsv2::ChunkEntry out_chunk;
  out_chunk.set_index(chunk->index);
  out_chunk.set_chunk_size(chunk->chunk_size);
  out_chunk.set_block_size(chunk->block_size);

  for (const auto& slice : chunk->slices) {
    *out_chunk.add_slices() = Helper::ToSlice(slice);
  }

  out_chunk.set_version(chunk->version);
  out_chunk.set_last_compaction_time_ms(chunk->last_compaction_time_ms);

  return std::move(out_chunk);
}

void FileSession::AddChunk(ChunkSPtr chunk) {
  utils::WriteLockGuard lk(lock_);

  chunk_map_.emplace(chunk->index, chunk);
}

void FileSession::AppendSlice(int64_t index, const std::vector<Slice>& slices) {
  utils::WriteLockGuard lk(lock_);

  auto it = chunk_map_.find(index);
  if (it != chunk_map_.end()) {
    auto& chunk = it->second;
    chunk->slices.insert(chunk->slices.end(), slices.begin(), slices.end());
  }
}

FileSession::ChunkSPtr FileSession::GetChunk(int64_t index) {
  utils::ReadLockGuard lk(lock_);

  auto it = chunk_map_.find(index);
  return (it != chunk_map_.end()) ? it->second : nullptr;
}

std::vector<FileSession::ChunkSPtr> FileSession::GetAllChunk() {
  utils::ReadLockGuard lk(lock_);

  std::vector<ChunkSPtr> chunks;
  chunks.reserve(chunk_map_.size());
  for (const auto& [_, chunk] : chunk_map_) {
    chunks.push_back(chunk);
  }

  return std::move(chunks);
}

bool FileSessionMap::Put(uint64_t fh, FileSessionSPtr file_session) {
  utils::WriteLockGuard lk(lock_);

  auto it = file_session_map_.find(fh);
  if (it != file_session_map_.end()) {
    return false;
  }

  file_session_map_.insert(std::make_pair(fh, file_session));

  return true;
}

void FileSessionMap::Delete(uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  file_session_map_.erase(fh);
}

std::string FileSessionMap::GetSessionID(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = file_session_map_.find(fh);
  return (it != file_session_map_.end()) ? it->second->GetSessionID() : "";
}

FileSessionSPtr FileSessionMap::GetSession(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = file_session_map_.find(fh);
  return (it != file_session_map_.end()) ? it->second : nullptr;
}

// output json format string
bool FileSessionMap::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value items = Json::arrayValue;
  for (const auto& [fh, file_session] : file_session_map_) {
    Json::Value item;
    item["fh"] = fh;
    item["session_id"] = file_session->GetSessionID();

    Json::Value chunk_items = Json::arrayValue;
    auto chunks = file_session->GetAllChunk();
    for (const auto& chunk : chunks) {
      std::string base64_str;
      absl::Base64Escape(FileSession::Chunk::To(chunk).SerializeAsString(),
                         &base64_str);
      chunk_items.append(base64_str);
    }
    item["chunks"] = chunk_items;

    items.append(item);
  }

  value["file_sessions"] = items;

  return true;
}

bool FileSessionMap::Load(const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  file_session_map_.clear();
  const Json::Value& items = value["file_sessions"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.filesession] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    uint64_t fh = item["fh"].asUInt64();
    auto file_session = FileSession::New();

    file_session->SetSessionId(item["session_id"].asString());

    for (const auto& chunk_item : item["chunks"]) {
      std::string origin_str;
      absl::Base64Unescape(chunk_item.asString(), &origin_str);
      mdsv2::ChunkEntry chunk;
      if (!chunk.ParseFromString(origin_str)) {
        LOG(ERROR) << "[meta.filesession] parse chunk fail.";
        return false;
      }

      file_session->AddChunk(FileSession::Chunk::From(chunk));
    }

    file_session_map_[fh] = file_session;
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs