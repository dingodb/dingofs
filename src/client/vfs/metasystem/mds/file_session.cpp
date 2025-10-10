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

#include <algorithm>
#include <string>
#include <vector>

#include "absl/strings/escaping.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/value.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

ChunkEntry ChunkMutation::GetChunkEntry() const {
  ChunkEntry chunk;

  chunk.set_index(index_);
  chunk.set_chunk_size(chunk_size_);
  chunk.set_block_size(block_size_);
  for (const auto& slice : slices_) {
    *chunk.add_slices() = Helper::ToSlice(slice);
  }
  chunk.set_version(version_);
  chunk.set_last_compaction_time_ms(last_compaction_time_ms_);

  return std::move(chunk);
}

void ChunkMutation::UpdateChunkIf(const ChunkEntry& chunk) {
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}] update chunk.", ino_,
                           chunk.index());

  utils::WriteLockGuard lk(lock_);

  if (chunk.version() <= version_) return;

  index_ = chunk.index();
  chunk_size_ = chunk.chunk_size();
  block_size_ = chunk.block_size();

  slices_.reserve(chunk.slices_size());
  for (const auto& slice : chunk.slices()) {
    slices_.push_back(Helper::ToSlice(slice));
  }

  version_ = chunk.version();
  last_compaction_time_ms_ = chunk.last_compaction_time_ms();
}

std::vector<Slice> ChunkMutation::GetAllSlice() {
  utils::ReadLockGuard lk(lock_);

  std::vector<Slice> slices = slices_;

  slices.insert(slices.end(), delta_slices_.begin(), delta_slices_.end());
  // can't sort by slice id

  return std::move(slices);
}

std::vector<Slice> ChunkMutation::GetDeltaSlice() {
  utils::ReadLockGuard lk(lock_);

  return delta_slices_;
}

void ChunkMutation::AppendSlice(const std::vector<Slice>& slices) {
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}] append slice.", ino_,
                           index_);

  utils::WriteLockGuard lk(lock_);

  delta_slices_.insert(delta_slices_.end(), slices.begin(), slices.end());
}

void ChunkMutation::DeleteDeltaSlice(
    const std::vector<uint64_t>& delete_slice_ids) {
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}] delete delta slice.", ino_,
                           index_);

  utils::WriteLockGuard lk(lock_);

  std::vector<Slice> reserve_delta_slices;
  for (auto& delta_slice : delta_slices_) {
    bool is_exist = false;
    for (const auto& slice_id : delete_slice_ids) {
      if (delta_slice.id == slice_id) {
        is_exist = true;
        break;
      }
    }

    if (!is_exist) reserve_delta_slices.push_back(delta_slice);
  }

  delta_slices_ = reserve_delta_slices;
}

bool ChunkMutation::Dump(Json::Value& value) {
  value["ino"] = ino_;
  value["index"] = index_;
  value["chunk_size"] = chunk_size_;
  value["block_size"] = block_size_;
  value["version"] = version_;
  value["last_compaction_time_ms"] = last_compaction_time_ms_;

  // dump slices
  Json::Value slice_items = Json::arrayValue;
  for (const auto& slice : slices_) {
    Json::Value slice_item;
    slice_item["id"] = slice.id;
    slice_item["offset"] = slice.offset;
    slice_item["length"] = slice.length;
    slice_item["compaction"] = slice.compaction;
    slice_item["is_zero"] = slice.is_zero;
    slice_item["size"] = slice.size;

    slice_items.append(slice_item);
  }
  value["slices"] = slice_items;

  // dump delta_slices
  Json::Value delta_slice_items = Json::arrayValue;
  for (const auto& slice : slices_) {
    Json::Value slice_item;
    slice_item["id"] = slice.id;
    slice_item["offset"] = slice.offset;
    slice_item["length"] = slice.length;
    slice_item["compaction"] = slice.compaction;
    slice_item["is_zero"] = slice.is_zero;
    slice_item["size"] = slice.size;

    delta_slice_items.append(slice_item);
  }
  value["delta_slices"] = delta_slice_items;

  return true;
}

bool ChunkMutation::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] chunk_mutation is not object.";
    return false;
  }

  // load basic
  ino_ = value["ino"].asUInt64();
  index_ = value["index"].asUInt64();
  chunk_size_ = value["chunk_size"].asUInt64();
  block_size_ = value["block_size"].asUInt64();
  version_ = value["version"].asUInt64();
  last_compaction_time_ms_ = value["last_compaction_time_ms"].asUInt64();

  CHECK(ino_ != 0) << "ino is zero.";
  CHECK(chunk_size_ != 0) << "chunk_size_ is zero.";
  CHECK(block_size_ != 0) << "block_size_ is zero.";

  // load slices
  if (!value["slices"].isNull()) {
    if (!value["slices"].isArray()) {
      LOG(ERROR) << "[meta.filesession] chunk_mutation.slices is not array.";
      return false;
    }

    slices_.clear();
    for (const auto& slice_item : value["slices"]) {
      Slice slice;
      slice.id = slice_item["id"].asUInt64();
      slice.offset = slice_item["offset"].asUInt64();
      slice.length = slice_item["length"].asUInt64();
      slice.compaction = slice_item["compaction"].asUInt64();
      slice.is_zero = slice_item["is_zero"].asBool();
      slice.size = slice_item["size"].asUInt64();

      slices_.push_back(slice);
    }
  }

  // load delta slices
  if (!value["delta_slices"].isNull()) {
    if (!value["delta_slices"].isArray()) {
      LOG(ERROR)
          << "[meta.filesession] chunk_mutation.delta_slices is not array.";
      return false;
    }

    delta_slices_.clear();
    for (const auto& slice_item : value["delta_slices"]) {
      Slice slice;
      slice.id = slice_item["id"].asUInt64();
      slice.offset = slice_item["offset"].asUInt64();
      slice.length = slice_item["length"].asUInt64();
      slice.compaction = slice_item["compaction"].asUInt64();
      slice.is_zero = slice_item["is_zero"].asBool();
      slice.size = slice_item["size"].asUInt64();

      delta_slices_.push_back(slice);
    }
  }

  return true;
}

void WriteMemo::AddRange(uint64_t offset, uint64_t size) {
  ranges_.emplace_back(Range{.start = offset, .end = offset + size});
  last_time_ns_ = mds::Helper::TimestampNs();
}

uint64_t WriteMemo::GetLength() {
  uint64_t length = 0;
  for (const auto& range : ranges_) {
    length = std::max(length, range.end);
  }

  return length;
}

// output json format string
bool WriteMemo::Dump(Json::Value& value) {
  // dump ranges
  Json::Value range_items = Json::arrayValue;
  for (const auto& range : ranges_) {
    Json::Value range_item;
    range_item["start"] = range.start;
    range_item["end"] = range.end;
    range_items.append(range_item);
  }
  value["ranges"] = range_items;

  value["last_time_ns"] = last_time_ns_;

  return true;
}

bool WriteMemo::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] write_memo is not object.";
    return false;
  }
  if (!value["last_time_ns"].isUInt64()) {
    LOG(ERROR) << "[meta.filesession] write_memo.last_time_ns is not uint64.";
    return false;
  }

  if (!value["ranges"].isNull()) {
    if (!value["ranges"].isArray()) {
      LOG(ERROR) << "[meta.filesession] write_memo.ranges is not array.";
      return false;
    }

    for (const auto& range_item : value["ranges"]) {
      Range range;
      range.start = range_item["start"].asUInt64();
      range.end = range_item["end"].asUInt64();
      ranges_.push_back(range);
    }
  }

  last_time_ns_ = value["last_time_ns"].asUInt64();

  return true;
}

FileSession::FileSession(mds::FsInfoSPtr fs_info, Ino ino, uint64_t fh,
                         const std::string& session_id)
    : fs_info_(fs_info), ino_(ino) {
  AddSession(fh, session_id);
}

std::string FileSession::GetSessionID(uint64_t fh) {
  utils::ReadLockGuard lk(lock_);

  auto it = session_id_map_.find(fh);
  return (it != session_id_map_.end()) ? it->second : "";
}

void FileSession::AddSession(uint64_t fh, const std::string& session_id) {
  utils::WriteLockGuard lk(lock_);

  session_id_map_[fh] = session_id;

  IncRef();
}

void FileSession::DeleteSession(uint64_t fh) {
  utils::WriteLockGuard lk(lock_);

  session_id_map_.erase(fh);
}

void FileSession::AddWriteMemo(uint64_t offset, uint64_t size) {
  utils::WriteLockGuard lk(lock_);

  write_memo_.AddRange(offset, size);
}

uint64_t FileSession::GetLength() {
  utils::ReadLockGuard lk(lock_);

  return write_memo_.GetLength();
}

uint64_t FileSession::GetLastTimeNs() {
  utils::ReadLockGuard lk(lock_);

  return write_memo_.LastTimeNs();
}

void FileSession::UpsertChunk(uint64_t fh,
                              const std::vector<ChunkEntry>& chunks) {
  if (chunks.empty()) return;
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}] upsert chunk, chunks({}).",
                           ino_, fh, chunks.size());

  utils::WriteLockGuard lk(lock_);

  for (const auto& chunk : chunks) {
    auto it = chunk_mutation_map_.find(chunk.index());
    if (it == chunk_mutation_map_.end()) {
      chunk_mutation_map_.emplace(chunk.index(),
                                  ChunkMutation::New(ino_, chunk));

    } else {
      it->second->UpdateChunkIf(chunk);
    }
  }
}

void FileSession::DeleteChunkMutation(int64_t index) {
  LOG(INFO) << fmt::format("[meta.filesession.{}] delete chunk mutation.", ino_,
                           index);

  utils::WriteLockGuard lk(lock_);

  chunk_mutation_map_.erase(index);
}

ChunkMutationSPtr FileSession::GetChunkMutation(int64_t index) {
  utils::ReadLockGuard lk(lock_);

  auto it = chunk_mutation_map_.find(index);
  return (it != chunk_mutation_map_.end()) ? it->second : nullptr;
}

bool FileSession::Dump(Json::Value& value) {
  value["ino"] = ino_;
  value["ref_count"] = ref_count_.load();

  // dump session_id_map
  Json::Value session_id_map = Json::arrayValue;
  for (const auto& [fh, session_id] : session_id_map_) {
    Json::Value item;
    item["fh"] = fh;
    item["session_id"] = session_id;

    session_id_map.append(item);
  }
  value["session_id_map"] = session_id_map;

  // dump write_memo_
  CHECK(write_memo_.Dump(value["write_memo"])) << "dump write memo fail.";

  // dump chunk_mutation_map_
  Json::Value chunk_mutation_items = Json::arrayValue;
  for (const auto& [index, chunk_mutation] : chunk_mutation_map_) {
    Json::Value chunk_mutation_item;
    chunk_mutation_item["index"] = index;
    CHECK(chunk_mutation->Dump(chunk_mutation_item))
        << "dump chunk mutation fail.";

    chunk_mutation_items.append(chunk_mutation_item);
  }

  value["chunk_mutation_map"] = chunk_mutation_items;

  return true;
}

bool FileSession::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] file_session is not object.";
    return false;
  }
  if (!value["ino"].isUInt64()) {
    LOG(ERROR) << "[meta.filesession] file_session.ino is not uint64.";
    return false;
  }

  ino_ = value["ino"].asUInt64();
  ref_count_.store(value["ref_count"].asUInt());

  // load session_id_map
  if (!value["session_id_map"].isNull()) {
    if (!value["session_id_map"].isArray()) {
      LOG(ERROR)
          << "[meta.filesession] file_session.session_id_map is not array.";
      return false;
    }

    session_id_map_.clear();
    for (const auto& item : value["session_id_map"]) {
      uint64_t fh = item["fh"].asUInt64();
      std::string session_id = item["session_id"].asString();
      session_id_map_[fh] = session_id;
    }
  }

  // load chunk_mutation_map
  if (!value["chunk_mutation_map"].isNull()) {
    if (!value["chunk_mutation_map"].isArray()) {
      LOG(ERROR)
          << "[meta.filesession] file_session.chunk_mutation_map is not array.";
      return false;
    }

    chunk_mutation_map_.clear();
    for (const auto& item : value["chunk_mutation_map"]) {
      int64_t index = item["index"].asInt64();

      auto chunk_mutation = ChunkMutation::New();
      CHECK(chunk_mutation->Load(item))
          << fmt::format("load chunk mutation fail, index({}).", index);

      chunk_mutation_map_[index] = chunk_mutation;
    }
  }

  return true;
}

FileSessionSPtr FileSessionMap::Put(Ino ino, uint64_t fh,
                                    const std::string& session_id) {
  CHECK(ino != 0) << "ino is zero.";
  CHECK(fh != 0) << "fh is zero.";
  CHECK(!session_id.empty()) << "session_id is empty.";

  LOG(INFO) << fmt::format(
      "[meta.filesession.{}.{}] add file session, session_id({}).", ino, fh,
      session_id);

  utils::WriteLockGuard lk(lock_);

  auto it = file_session_map_.find(ino);
  if (it != file_session_map_.end()) {
    auto file_session = it->second;
    file_session->AddSession(fh, session_id);
    return file_session;
  }

  auto file_session = FileSession::New(fs_info_, ino, fh, session_id);
  file_session_map_.insert(std::make_pair(ino, file_session));

  return file_session;
}

void FileSessionMap::Delete(Ino ino, uint64_t fh) {
  CHECK(ino != 0) << "ino is zero.";
  CHECK(fh != 0) << "fh is zero.";

  uint32_t ref_count = 0;
  {
    utils::WriteLockGuard lk(lock_);

    auto it = file_session_map_.find(ino);
    if (it == file_session_map_.end()) return;

    auto& file_session = it->second;

    file_session->DeleteSession(fh);
    ref_count = file_session->DecRef();
    if (ref_count == 0) {
      file_session_map_.erase(it);
    }
  }

  LOG(INFO) << fmt::format(
      "[meta.filesession.{}.{}] delete file session, ref_count({}).", ino, fh,
      ref_count);
}

std::string FileSessionMap::GetSessionID(Ino ino, uint64_t fh) {
  CHECK(ino != 0) << "ino is zero.";
  CHECK(fh != 0) << "fh is zero.";

  utils::ReadLockGuard lk(lock_);

  auto it = file_session_map_.find(ino);
  return (it != file_session_map_.end()) ? it->second->GetSessionID(fh) : "";
}

FileSessionSPtr FileSessionMap::GetSession(Ino ino) {
  CHECK(ino != 0) << "ino is zero.";

  utils::ReadLockGuard lk(lock_);

  auto it = file_session_map_.find(ino);
  return (it != file_session_map_.end()) ? it->second : nullptr;
}

// output json format string
bool FileSessionMap::Dump(Json::Value& value) {
  utils::ReadLockGuard lk(lock_);

  Json::Value file_sessions_items = Json::arrayValue;
  for (const auto& [ino, file_session] : file_session_map_) {
    Json::Value file_session_item;
    file_session_item["ino"] = ino;
    CHECK(file_session->Dump(file_session_item)) << "file session dump fail.";

    file_sessions_items.append(file_session_item);
  }

  value["file_sessions"] = file_sessions_items;

  return true;
}

bool FileSessionMap::Load(const Json::Value& value) {
  if (value.isNull()) return true;
  if (!value.isObject()) {
    LOG(ERROR) << "[meta.filesession] file_session_map is not an object.";
    return false;
  }

  utils::WriteLockGuard lk(lock_);

  // load file_session_map
  if (!value["file_sessions"].isNull()) {
    if (!value["file_sessions"].isArray()) {
      LOG(ERROR) << "[meta.filesession] file_session_map.file_sessions is not "
                    "an array.";
      return false;
    }

    file_session_map_.clear();
    for (const auto& item : value["file_sessions"]) {
      Ino ino = item["ino"].asUInt64();

      auto file_session = FileSession::New(fs_info_);
      CHECK(file_session->Load(item))
          << fmt::format("load file session fail, ino({}).", ino);

      file_session_map_[ino] = file_session;
    }
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs