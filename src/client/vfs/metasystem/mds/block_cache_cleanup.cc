
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

#include "client/vfs/metasystem/mds/block_cache_cleanup.h"

#include <glog/logging.h>

#include "common/block/block_utils.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

void BlockCacheCleanupTask::Run() {
  if (block_cache_cleaner_.IsStopped()) return;

  Status status = Clean();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.cleanup.{}] clean block cache fail, length({}) status({}).",
        ino_, length_, status.ToString());
  }
}

Status BlockCacheCleanupTask::Clean() {
  const uint64_t chunk_size = block_cache_cleaner_.GetChunkSize();
  auto& mds_client = block_cache_cleaner_.GetMdsClient();
  auto* block_store = block_cache_cleaner_.GetBlockStore();

  if (block_store == nullptr || !block_store->EnableCache())
    return Status::OK();

  auto* block_cache = block_store->GetBlockCache();
  if (block_cache == nullptr) return Status::OK();

  const uint32_t chunk_count =
      (length_ / chunk_size) + (length_ % chunk_size != 0 ? 1 : 0);

  auto ctx = std::make_shared<Context>("");
  ctx->inner_req = true;

  std::vector<ChunkDescriptor> chunk_descriptors;
  for (uint32_t i = 0; i < chunk_count; ++i) {
    ChunkDescriptor chunk_descriptor;
    chunk_descriptor.set_index(i);
    chunk_descriptor.set_version(0);
    chunk_descriptors.push_back(chunk_descriptor);
  }

  if (chunk_descriptors.empty()) return Status::OK();

  std::vector<mds::ChunkEntry> chunks;
  Status status = mds_client.ReadSlice(ctx, ino_, chunk_descriptors, chunks);
  if (!status.ok()) return status;

  for (const auto& chunk : chunks) {
    for (const auto& slice : chunk.slices()) {
      if (slice.id() == 0) continue;  // skip empty slice

      LOG_DEBUG << fmt::format(
          "[meta.cleanup.{}] clean block cache, slice({}/{}).", ino_,
          chunk.index(), slice.id());

      std::vector<BlockKey> block_keys = dingofs::EnumerateBlockKeys(
          slice.id(), slice.size(), chunk.block_size());

      for (const auto& key : block_keys) {
        BlockHandle handle(fs_id_, key);
        Status status = block_cache->Delete(handle);
        if (!status.ok()) {
          LOG(ERROR) << fmt::format(
              "[meta.cleanup.{}] clean block cache fail, block({}) status({}).",
              ino_, key.Filename(), status.ToString());
        }
      }
    }
  }

  return Status::OK();
}

void BlockCacheCleaner::Stop() { is_stopped_.store(true); }

void BlockCacheCleaner::Execute(Ino ino, uint64_t length) {
  if (is_stopped_.load()) return;
  if (block_store_ == nullptr || !block_store_->EnableCache()) return;

  auto task =
      std::make_shared<BlockCacheCleanupTask>(fs_id_, ino, length, *this);
  if (!executor_.ExecuteByHash(ino, task, false)) {
    LOG(ERROR) << fmt::format(
        "[meta.cleanup.{}] submit block cache cleanup task fail, "
        "length({}).",
        ino, length);
  }
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs