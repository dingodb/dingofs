/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2026-06-22
 * Author: AI
 */

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_CLIENT_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_CLIENT_H_

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <thread>

#include "cache/api/block_cache.h"
#include "cache/common/mds_client.h"
#include "cache/tier/tier_block_cache.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/prefix_block_accesser.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace integration {

// ---- block / pattern helpers -------------------------------------------

inline BlockHandle MakeHandle(uint32_t fs_id, uint64_t id, uint32_t index,
                              uint32_t size) {
  return BlockHandle(fs_id, BlockKey(id, index, size));
}

// Deterministic content for a block, so a reader can recompute and byte-compare
// without carrying the written bytes around. Varies with the block id/index.
inline std::string PatternFor(uint64_t id, uint32_t index, size_t n) {
  std::string s(n, '\0');
  uint64_t seed = id * 1315423911u + index * 2654435761u + 7u;
  for (size_t i = 0; i < n; i++) {
    seed = seed * 6364136223846793005ull + 1442695040888963407ull;
    s[i] = static_cast<char>((seed >> 33) & 0xff);
  }
  return s;
}

// A single contiguous (one backing block) IOBuffer. The cache write/upload
// paths require this: StorageClient::Put / LocalFileSystem::WriteFile call
// IOBuffer::Fetch1(), which CHECKs backing_block_num()==1.
inline IOBuffer MakeBlock(const std::string& s) {
  IOBuffer b;
  char* p = new char[s.size()];
  std::memcpy(p, s.data(), s.size());
  b.AppendUserData(p, s.size(),
                   [](void* x) { delete[] static_cast<char*>(x); });
  return b;
}

// A single-backing-block, pre-allocated output buffer of `n` bytes, required by
// the Range path (CHECK_EQ(BackingBlockNum(), 1) + writes into Fetch1()).
inline IOBuffer MakeReadBuf(size_t n) {
  IOBuffer b;
  char* p = new char[n];
  std::memset(p, 0, n);
  b.AppendUserData(p, n, [](void* x) { delete[] static_cast<char*>(x); });
  return b;
}

inline std::string ReadAll(IOBuffer& buf) {
  std::string out(buf.Size(), '\0');
  if (!out.empty()) buf.CopyTo(out.data());
  return out;
}

template <typename Pred>
inline bool WaitUntil(Pred pred, int timeout_ms = 15000) {
  for (int waited = 0; waited < timeout_ms; waited += 50) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return pred();
}

// Runs an async block-cache op and blocks until its callback fires, returning
// the delivered Status. `op` receives the AsyncCallback to hand to the op.
inline Status AwaitAsync(const std::function<void(AsyncCallback)>& op,
                         int timeout_ms = 15000) {
  std::atomic<bool> done{false};
  Status result;
  op([&](Status s) {
    result = s;
    done.store(true);
  });
  if (!WaitUntil([&] { return done.load(); }, timeout_ms)) {
    return Status::Internal("async callback timed out");
  }
  return result;
}

// Writeback-Puts a block to the remote tier, retrying until the client's
// upstream has discovered the cache group (the first remote op may race the
// periodic member sync).
inline void PutRemote(BlockCache* cache, const BlockHandle& h,
                      const std::string& content) {
  ASSERT_TRUE(WaitUntil([&] {
    return cache
        ->Put(h, MakeBlock(content),
              {.writeback = true, .tier = CacheTier::kRemote})
        .ok();
  })) << "remote Put never succeeded";
}

// Polls MDS ListMembers until `member_id` reaches the kOnline state.
inline bool WaitMemberOnline(MDSClientImpl* mds, const std::string& group,
                             const std::string& member_id,
                             int timeout_ms = 15000) {
  return WaitUntil(
      [&]() {
        std::vector<CacheGroupMember> members;
        if (!mds->ListMembers(group, &members).ok()) return false;
        for (const auto& m : members) {
          if (m.id == member_id && m.state == CacheGroupMemberState::kOnline) {
            return true;
          }
        }
        return false;
      },
      timeout_ms);
}

// Returns true if `member_id` is absent or no longer Online (left / went down).
inline bool WaitMemberNotOnline(MDSClientImpl* mds, const std::string& group,
                                const std::string& member_id,
                                int timeout_ms = 30000) {
  return WaitUntil(
      [&]() {
        std::vector<CacheGroupMember> members;
        if (!mds->ListMembers(group, &members).ok()) return false;
        for (const auto& m : members) {
          if (m.id == member_id) {
            return m.state != CacheGroupMemberState::kOnline;
          }
        }
        return true;  // member gone entirely
      },
      timeout_ms);
}

// Owns a local-file BlockAccesser and a TierBlockCache built on top of it.
// The cache flavour (local-only vs. remote) is decided by the cache flags
// (cache_store / cache_group) the caller sets before Open(). The same
// `storage_path` is the LOCALFILE backend a spawned cache node also resolves
// from the MDS, so storage helpers can verify what a node persisted.
class CacheClient {
 public:
  // `fs_name`, when non-empty, prefixes every backend key with "<fs_name>/" --
  // matching how a real cache node resolves its backend
  // (NewPrefixBlockAccesser(fs_name)). Distributed tests must pass it so a
  // client-seeded block lands where the spawned node looks for it; local tests
  // leave it empty (the client both writes and reads, so any layout is fine).
  Status Open(const std::string& storage_path,
              const std::string& fs_name = "") {
    storage_path_ = storage_path;
    fs_name_ = fs_name;
    blockaccess::BlockAccessOptions opt;
    opt.type = blockaccess::AccesserType::kLocalFile;
    opt.file_options.path = storage_path;
    accesser_ = fs_name.empty()
                    ? blockaccess::NewBlockAccesser(opt)
                    : blockaccess::NewPrefixBlockAccesser(fs_name, opt);
    auto status = accesser_->Init();
    if (!status.ok()) return status;

    cache_ = std::make_unique<TierBlockCache>(accesser_.get());
    return cache_->Start();
  }

  // Shuts down only the cache (keeping the backend + accesser), so the caller
  // can reopen against the same cache_dir to exercise on-disk reload.
  void ShutdownCacheOnly() {
    if (cache_) {
      cache_->Shutdown();
      cache_.reset();
    }
  }

  Status ReopenCache() {
    cache_ = std::make_unique<TierBlockCache>(accesser_.get());
    return cache_->Start();
  }

  void Close() {
    ShutdownCacheOnly();
    if (accesser_) {
      accesser_->Destroy();
      accesser_.reset();
    }
  }

  BlockCache* cache() { return cache_.get(); }
  blockaccess::BlockAccesser* accesser() { return accesser_.get(); }

  // Absolute path where the local-FS backend lands an uploaded block (mirrors
  // the accesser's key prefixing so node- and client-written blocks coincide).
  std::string StoragePath(const BlockHandle& h) const {
    if (fs_name_.empty()) return storage_path_ + "/" + h.StoreKey();
    return storage_path_ + "/" + fs_name_ + "/" + h.StoreKey();
  }
  bool StorageHas(const BlockHandle& h) const {
    std::error_code ec;
    return std::filesystem::exists(StoragePath(h), ec);
  }
  std::string ReadStorageFile(const BlockHandle& h) const {
    std::ifstream in(StoragePath(h), std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
  }
  // Write a block straight into the backend, bypassing the cache, so tests can
  // exercise the storage-reflow / prefetch paths.
  Status SeedStorage(const BlockHandle& h, const std::string& data) {
    return accesser_->Put(h.StoreKey(), blockaccess::PutPayload::Build(
                                            {{data.data(), data.size()}}));
  }

 private:
  std::string storage_path_;
  std::string fs_name_;
  blockaccess::BlockAccesserUPtr accesser_;
  BlockCacheUPtr cache_;
};

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_CLIENT_H_
