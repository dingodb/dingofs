/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "client/vfs/handle/handle_manager.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <cstdint>
#include <sstream>
#include <utility>
#include <vector>

#include "absl/hash/hash.h"
#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data/reader/reader_registry.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_fh.h"
#include "common/const.h"
#include "common/sync_point.h"
#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

std::string Handle::ToString() const {
  std::ostringstream oss;
  oss << "Handle{ino: " << ino << ", fh: " << fh << ", flags: " << std::oct
      << flags << ", has_reader: " << (resources.reader ? "true" : "false")
      << ", has_writer: " << (resources.writer ? "true" : "false") << "}";
  return oss.str();
}

HandleManagerShard& HandleManager::GetShard(uint64_t fh) {
  return shards_[absl::HashOf(fh) & (kShardCount - 1)];
}

HandleManager::ShardLocks HandleManager::LockShards() {
  ShardLocks locks;
  for (size_t i = 0; i < shards_.size(); ++i) {
    locks[i] = shards_[i].LockForSnapshot();
  }
  return locks;
}

size_t HandleManager::Size() const {
  size_t count = 0;
  for (const auto& shard : shards_) {
    count += shard.Size();
  }
  return count;
}

HandleManager::~HandleManager() {
  Status s = Stop();
  if (!s.ok()) {
    LOG(ERROR) << fmt::format("HandleManager destructor flush failed: {}",
                              s.ToString());
  }
  for (auto& shard : shards_) {
    for (auto& [fh, handle] : shard.ExtractAll()) {
      ReleaseRefHandle(handle, shard);
    }
  }
}

HandleGuard::~HandleGuard() { Reset(); }

HandleGuard::HandleGuard(HandleGuard&& other) noexcept
    : manager_(std::exchange(other.manager_, nullptr)),
      handle_(std::exchange(other.handle_, nullptr)) {}

HandleGuard& HandleGuard::operator=(HandleGuard&& other) noexcept {
  if (this != &other) {
    Reset();
    manager_ = std::exchange(other.manager_, nullptr);
    handle_ = std::exchange(other.handle_, nullptr);
  }
  return *this;
}

void HandleGuard::Reset() {
  if (manager_ != nullptr && handle_ != nullptr) {
    manager_->ReleaseGuard(handle_);
    manager_ = nullptr;
    handle_ = nullptr;
  }
}

void HandleManager::ReleaseRefHandle(Handle* h, HandleManagerShard& shard) {
  const uint64_t fh = h->fh;
  int64_t orgin = h->refs.fetch_sub(1);
  VLOG(12) << fmt::format("handle-{} ReleaseRef origin refs: {}", fh, orgin);
  CHECK_GT(orgin, 0);
  if (orgin == 1) {
    DestroyHandle(h, shard);
  }
  shard.NotifyRefReleased();
}

void HandleManager::DestroyHandle(Handle* h, HandleManagerShard& shard) {
  ReleaseHandleResources(shard.Detach(h));
  delete h;
}

void HandleManager::ReleaseGuard(Handle* h) {
  ReleaseRefHandle(h, GetShard(h->fh));
}

void HandleManager::ReleaseHandleResources(HandleResources resources) {
  if (resources.reader != nullptr) {
    vfs_hub_->GetReaderRegistry()->Unregister(resources.reader);
    resources.reader->Close();
    resources.reader->ReleaseRef();
  }

  if (resources.writer != nullptr) {
    vfs_hub_->GetWriterTable()->ReleaseWriter(resources.writer);
  }
}

Status HandleManager::Start() { return Status::OK(); }

Status HandleManager::Stop() {
  std::lock_guard<std::mutex> stop_lock(stop_mutex_);
  if (stopped_) {
    return Status::OK();
  }
  stopped_ = true;

  size_t count = 0;
  {
    auto locks = LockShards();
    for (auto& shard : shards_) {
      shard.CloseAdmissionLocked();
      count += shard.SizeLocked();
    }
  }

  std::vector<HandleResources> resources_to_release;
  std::vector<Handle*> stop_refs;
  resources_to_release.reserve(count);
  stop_refs.reserve(count);
  for (auto& shard : shards_) {
    shard.Drain(resources_to_release, stop_refs);
  }

  // Detached holders pin every writer until the final flush has completed.
  // Never release an earlier shard's resources before flushing all writers.
  Status flush_status = vfs_hub_->GetWriterTable()->FlushAll();
  for (auto& resources : resources_to_release) {
    ReleaseHandleResources(resources);
  }
  for (auto* handle : stop_refs) {
    ReleaseGuard(handle);
  }
  return flush_status;
}

Handle* HandleManager::NewHandle(uint64_t fh, Ino ino, int flags) {
  auto* handle = new Handle();
  handle->fh = fh;
  handle->ino = ino;
  handle->flags = flags;

  // Reader is always per-fh.
  handle->resources.reader = new FileReader(vfs_hub_, fh, ino);
  handle->resources.reader->AcquireRef();
  CHECK(handle->resources.reader->Open().ok())
      << "FileReader::Open is currently infallible";
  // Writer only for writable opens. Borrowed from WriterTable.
  if ((flags & O_ACCMODE) != O_RDONLY) {
    handle->resources.writer = vfs_hub_->GetWriterTable()->AcquireWriter(ino);
    if (handle->resources.writer == nullptr) {
      LOG(ERROR) << fmt::format(
          "NewHandle: AcquireWriter failed, fh={}, ino={}", fh, ino);
      handle->resources.reader->Close();
      handle->resources.reader->ReleaseRef();
      delete handle;
      return nullptr;
    }
  }

  // Publish the reader in the per-inode index only after all Handle resources
  // have been acquired successfully. AddHandle failure is cleaned up through
  // DestroyHandle, which unregisters it via ReleaseHandleResources.
  vfs_hub_->GetReaderRegistry()->Register(handle->resources.reader);

  if (!AddHandle(handle)) {
    DestroyHandle(handle, GetShard(fh));
    return nullptr;
  }
  return handle;
}

bool HandleManager::AddHandle(Handle* handle) {
  if (!GetShard(handle->fh).Add(handle)) {
    LOG(WARNING) << "AddHandle rejected because HandleManager is stopped, fh: "
                 << handle->fh;
    return false;
  }
  total_count_ << 1;
  return true;
}

void HandleManager::ReleaseHandler(uint64_t fh) {
  auto& shard = GetShard(fh);
  auto* handle = shard.Remove(fh);
  if (handle != nullptr) {
    ReleaseRefHandle(handle, shard);
  }
}

HandleGuard HandleManager::FindHandlerGuard(uint64_t fh) {
  auto* handle = GetShard(fh).Find(fh, /*for_release=*/false);
  return handle == nullptr ? HandleGuard{} : HandleGuard(this, handle);
}

HandleGuard HandleManager::FindHandlerForRelease(uint64_t fh) {
  auto* handle = GetShard(fh).Find(fh, /*for_release=*/true);
  return handle == nullptr ? HandleGuard{} : HandleGuard(this, handle);
}

Status HandleManager::FlushByIno(Ino ino) {
  // O(1) hash lookup via WriterTable.
  auto* writer = vfs_hub_->GetWriterTable()->PeekWriter(ino);
  if (writer == nullptr) {
    return Status::OK();  // no writer for this ino → nothing to flush
  }
  Status s = writer->Flush();
  vfs_hub_->GetWriterTable()->ReleaseWriter(writer);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("FlushByIno failed, ino: {}, status: {}", ino,
                                s.ToString());
  }
  return s;
}

void HandleManager::Summary(Json::Value& value) {
  value["name"] = "handler";
  value["count"] = Size();
  value["total_count"] = total_count_.get_value();
}

bool HandleManager::Dump(Json::Value& value) {
  std::vector<HandleManagerShard::Identity> snapshot;
  {
    auto locks = LockShards();
    size_t count = 0;
    for (const auto& shard : shards_) {
      count += shard.SizeLocked();
    }
    snapshot.reserve(count);
    for (const auto& shard : shards_) {
      shard.AppendIdentitiesLocked(snapshot);
    }
  }

  Json::Value handlers = Json::arrayValue;
  for (const auto& handle : snapshot) {
    Json::Value item;
    item["ino"] = handle.ino;
    item["fh"] = handle.fh;
    item["flags"] = handle.flags;
    handlers.append(item);
  }
  value["handlers"] = std::move(handlers);
  LOG(INFO) << "successfuly dump " << snapshot.size() << " handlers";
  return true;
}

bool HandleManager::Load(const Json::Value& value) {
  const Json::Value& handlers = value["handlers"];
  if (!handlers.isArray()) {
    LOG(ERROR) << "handlers is not an array.";
    return false;
  }
  if (handlers.empty()) {
    LOG(INFO) << "no handlers to load";
    return true;
  }

  uint64_t max_fh = 0;
  for (const auto& handler : handlers) {
    Ino ino = handler["ino"].asUInt64();
    uint64_t fh = handler["fh"].asUInt64();
    uint flags = handler["flags"].asUInt();

    auto* h = NewHandle(fh, ino, flags);
    if (h == nullptr) {
      LOG(ERROR) << fmt::format("Load: NewHandle failed for fh={}, ino={}", fh,
                                ino);
      continue;
    }
    max_fh = std::max(max_fh, fh);
  }

  FhGenerator::UpdateNextFh(max_fh + 1);

  LOG(INFO) << "successfuly load " << Size()
            << " handlers, next fh is:" << FhGenerator::GetNextFh();

  return true;
}

void HandleManagerShard::Ref(Handle* handle) {
  const int64_t old_refs = handle->refs.fetch_add(1);
  VLOG(12) << fmt::format("handle-{} AcquireRef origin refs: {}", handle->fh,
                          old_refs);
  CHECK_GE(old_refs, 0);
}

bool HandleManagerShard::Add(Handle* handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (drain_.closing.load(std::memory_order_seq_cst)) {
    return false;
  }
  CHECK(handles_.emplace(handle->fh, handle).second)
      << "Duplicate fh: " << handle->fh;
  Ref(handle);
  return true;
}

Handle* HandleManagerShard::Find(uint64_t fh, bool for_release) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!for_release && drain_.closing.load(std::memory_order_seq_cst)) {
    return nullptr;
  }
  auto it = handles_.find(fh);
  if (it == handles_.end()) return nullptr;
  Ref(it->second);
  return it->second;
}

Handle* HandleManagerShard::Remove(uint64_t fh) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = handles_.find(fh);
  if (it == handles_.end()) return nullptr;
  auto* handle = it->second;
  handles_.erase(it);
  return handle;
}

size_t HandleManagerShard::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return handles_.size();
}

HandleManagerShard::HandleMap HandleManagerShard::ExtractAll() {
  HandleMap handles;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    handles.swap(handles_);
  }
  return handles;
}

void HandleManagerShard::CloseAdmissionLocked() {
  drain_.closing.store(true, std::memory_order_seq_cst);
}

void HandleManagerShard::AppendIdentitiesLocked(
    std::vector<Identity>& out) const {
  for (const auto& [fh, handle] : handles_) {
    out.push_back({handle->ino, fh, handle->flags});
  }
}

HandleResources HandleManagerShard::DetachLocked(Handle* handle) {
  return std::exchange(handle->resources, {});
}

HandleResources HandleManagerShard::Detach(Handle* handle) {
  std::lock_guard<std::mutex> lock(mutex_);
  return DetachLocked(handle);
}

void HandleManagerShard::Drain(std::vector<HandleResources>& resources,
                               std::vector<Handle*>& stop_refs) {
  std::unique_lock<std::mutex> lock(mutex_);
  drain_.cv.wait(lock, [&] {
    for (auto& [fh, handle] : handles_) {
      if (handle->refs.load(std::memory_order_seq_cst) != 1) {
        TEST_SYNC_POINT_CALLBACK("HandleManagerShard::Drain:waiting_for_guard",
                                 handle);
        return false;
      }
    }
    return true;
  });
  for (auto& [fh, handle] : handles_) {
    if (handle->ino == kStatsIno) continue;
    Ref(handle);
    stop_refs.push_back(handle);
    auto detached = DetachLocked(handle);
    if (detached.reader != nullptr || detached.writer != nullptr) {
      resources.push_back(detached);
    }
  }
}

void HandleManagerShard::NotifyRefReleased() {
  // SC ordering pairs the preceding ref decrement with closing/predicate.
  // Taking mutex_ excludes notification between the predicate and wait.
  if (drain_.closing.load(std::memory_order_seq_cst)) {
    std::lock_guard<std::mutex> lock(mutex_);
    drain_.cv.notify_all();
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
