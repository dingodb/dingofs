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

#include <algorithm>
#include <string>
#include <vector>

#include "absl/strings/escaping.h"
#include "client/vfs/meta/v2/helper.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/helper.h"

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
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}.{}] update chunk.", ino_,
                           fh_, chunk.index());

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

  // sort by id
  std::sort(slices.begin(), slices.end(),  // NOLINT
            [](const Slice& a, const Slice& b) { return a.id < b.id; });

  return std::move(slices);
}

std::vector<Slice> ChunkMutation::GetDeltaSlice() {
  utils::ReadLockGuard lk(lock_);

  return delta_slices_;
}

void ChunkMutation::AppendSlice(const std::vector<Slice>& slices) {
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}.{}] append slice.", ino_,
                           fh_, index_);

  utils::WriteLockGuard lk(lock_);

  delta_slices_.insert(delta_slices_.end(), slices.begin(), slices.end());
}

void ChunkMutation::DeleteDeltaSlice(
    const std::vector<uint64_t>& delete_slice_ids) {
  LOG(INFO) << fmt::format("[meta.filesession.{}.{}.{}] delete delta slice.",
                           ino_, fh_, index_);

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
  value["fh_"] = fh_;
  value["index"] = index_;
  value["chunk_size"] = chunk_size_;
  value["block_size"] = block_size_;
  value["version"] = version_;
  value["last_compaction_time_ms"] = last_compaction_time_ms_;

  Json::Value slice_array = Json::arrayValue;
  for (const auto& slice : slices_) {
    Json::Value slice_value;
    slice_value["id"] = slice.id;
    slice_value["offset"] = slice.offset;
    slice_value["length"] = slice.length;
    slice_value["compaction"] = slice.compaction;
    slice_value["is_zero"] = slice.is_zero;
    slice_value["size"] = slice.size;

    slice_array.append(slice_value);
  }
  value["slices"] = slice_array;

  Json::Value delta_slice_array = Json::arrayValue;
  for (const auto& slice : slices_) {
    Json::Value slice_value;
    slice_value["id"] = slice.id;
    slice_value["offset"] = slice.offset;
    slice_value["length"] = slice.length;
    slice_value["compaction"] = slice.compaction;
    slice_value["is_zero"] = slice.is_zero;
    slice_value["size"] = slice.size;

    delta_slice_array.append(slice_value);
  }
  value["delta_slices"] = delta_slice_array;

  return true;
}

bool ChunkMutation::Load(const Json::Value& value) {
  ino_ = value["ino"].asUInt64();
  fh_ = value["fh_"].asUInt64();
  index_ = value["index"].asUInt64();
  chunk_size_ = value["chunk_size"].asUInt64();
  block_size_ = value["block_size"].asUInt64();
  version_ = value["version"].asUInt64();
  last_compaction_time_ms_ = value["last_compaction_time_ms"].asUInt64();

  if (!value["slices"].isArray()) return false;

  for (const auto& slice_value : value["slices"]) {
    Slice slice;
    slice.id = slice_value["id"].asUInt64();
    slice.offset = slice_value["offset"].asUInt64();
    slice.length = slice_value["length"].asUInt64();
    slice.compaction = slice_value["compaction"].asUInt64();
    slice.is_zero = slice_value["is_zero"].asBool();
    slice.size = slice_value["size"].asUInt64();

    slices_.push_back(slice);
  }

  if (!value["delta_slices"].isArray()) return false;
  for (const auto& slice_value : value["delta_slices"]) {
    Slice slice;
    slice.id = slice_value["id"].asUInt64();
    slice.offset = slice_value["offset"].asUInt64();
    slice.length = slice_value["length"].asUInt64();
    slice.compaction = slice_value["compaction"].asUInt64();
    slice.is_zero = slice_value["is_zero"].asBool();
    slice.size = slice_value["size"].asUInt64();

    delta_slices_.push_back(slice);
  }

  return true;
}

void WriteMemo::AddRange(uint64_t offset, uint64_t size) {
  ranges_.emplace_back(Range{.start = offset, .end = offset + size});
  last_time_ns_ = mdsv2::Helper::TimestampNs();
}

uint64_t WriteMemo::GetLength() {
  uint64_t length = 0;
  for (const auto& range : ranges_) {
    length = std::max(length, range.end);
  }

  return length;
}

FileSession::FileSession(mdsv2::FsInfoPtr fs_info, Ino ino, uint64_t fh,
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

// void FileSession::UpsertChunkMutation(const ChunkEntry& chunk) {
//   LOG(INFO) << fmt::format("[meta.filesession.{}.{}.{}] upsert chunk
//   mutation.",
//                            ino_, fh_, chunk.index());

//   // utils::WriteLockGuard lk(lock_);

//   // auto it = chunk_mutation_map_.find(chunk.index());
//   // if (it == chunk_mutation_map_.end()) {
//   //   auto chunk_mutation = ChunkMutation::New(ino_, fh_, chunk);
//   //   chunk_mutation_map_.emplace(chunk.index(), chunk_mutation);

//   // } else {
//   //   it->second->UpdateChunkIf(chunk);
//   // }
// }

// void FileSession::AppendSlice(int64_t index, const std::vector<Slice>&
// slices) {
//   LOG(INFO) << fmt::format("[meta.filesession.{}.{}.{}] append slice.", ino_,
//                            fh_, index);

//   utils::WriteLockGuard lk(lock_);

//   // auto it = chunk_mutation_map_.find(index);
//   // if (it != chunk_mutation_map_.end()) {
//   //   auto& chunk_mutation = it->second;
//   //   chunk_mutation->AppendSlice(slices);

//   // } else {
//   //   auto chunk_mutation = ChunkMutation::New(
//   //       ino_, fh_, index, fs_info_->GetChunkSize(),
//   //       fs_info_->GetBlockSize());
//   //   chunk_mutation->AppendSlice(slices);
//   //   chunk_mutation_map_.insert(std::make_pair(index, chunk_mutation));
//   // }
// }

// void FileSession::DeleteChunkMutation(int64_t index) {
//   LOG(INFO) << fmt::format("[meta.filesession.{}.{}] delete chunk mutation.",
//                            ino_, fh_, index);

//   utils::WriteLockGuard lk(lock_);

//   chunk_mutation_map_.erase(index);
// }

// ChunkMutationSPtr FileSession::GetChunkMutation(int64_t index) {
//   utils::ReadLockGuard lk(lock_);

//   auto it = chunk_mutation_map_.find(index);
//   return (it != chunk_mutation_map_.end()) ? it->second : nullptr;
// }

// std::vector<ChunkMutationSPtr> FileSession::GetAllChunkMutation() {
//   utils::ReadLockGuard lk(lock_);

//   std::vector<ChunkMutationSPtr> chunks;
//   chunks.reserve(chunk_mutation_map_.size());
//   for (const auto& [_, chunk] : chunk_mutation_map_) {
//     chunks.push_back(chunk);
//   }

//   return std::move(chunks);
// }

// void FileSession::AddChunkMutation(ChunkMutationSPtr chunk_mutation) {
//   LOG(INFO) << fmt::format("[meta.filesession.{}.{}.{}] add chunk mutation.",
//                            ino_, fh_, chunk_mutation->GetIndex());

//   utils::WriteLockGuard lk(lock_);

//   chunk_mutation_map_.emplace(chunk_mutation->GetIndex(), chunk_mutation);
// }

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

    auto file_session = it->second;

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

  // Json::Value file_session_items = Json::arrayValue;
  // for (const auto& [key, file_session] : file_session_map_) {
  //   Json::Value file_session_item;
  //   file_session_item["ino"] = key.ino;
  //   file_session_item["fh"] = key.fh;
  //   file_session_item["session_id"] = file_session->GetSessionID();

  //   Json::Value chunk_items = Json::arrayValue;
  //   auto chunk_mutations = file_session->GetAllChunkMutation();
  //   for (const auto& chunk_mutation : chunk_mutations) {
  //     Json::Value chunk_item;
  //     chunk_mutation->Dump(chunk_item);
  //     chunk_items.append(chunk_item);
  //   }
  //   file_session_item["chunks"] = chunk_items;

  //   file_session_items.append(file_session_item);
  // }

  // value["file_sessions"] = file_session_items;

  return true;
}

bool FileSessionMap::Load(const Json::Value& value) {
  utils::WriteLockGuard lk(lock_);

  // file_session_map_.clear();
  // const Json::Value& items = value["file_sessions"];
  // if (!items.isArray()) {
  //   LOG(ERROR) << "[meta.filesession] value is not an array.";
  //   return false;
  // }

  // for (const auto& filesession_item : items) {
  //   uint64_t fh = filesession_item["fh"].asUInt64();
  //   Ino ino = filesession_item["ino"].asUInt64();
  //   std::string session_id = filesession_item["session_id"].asString();
  //   CHECK(fh != 0) << "fh is zero.";
  //   CHECK(ino != 0) << "ino is zero.";
  //   CHECK(!session_id.empty()) << "session_id is empty.";

  //   auto file_session = FileSession::New(ino, fh, session_id, fs_info_, {});

  //   for (const auto& chunk_item : filesession_item["chunks"]) {
  //     auto chunk_mutation = ChunkMutation::New(ino, fh, {});
  //     if (chunk_mutation->Load(chunk_item)) {
  //       file_session->AddChunkMutation(chunk_mutation);
  //     }
  //   }

  //   file_session_map_[{ino, fh}] = file_session;
  // }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs