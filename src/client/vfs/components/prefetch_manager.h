#ifndef __DINGOFS_CLIENT_VFS_PREFETCH_MANAGER_H__
#define __DINGOFS_CLIENT_VFS_PREFETCH_MANAGER_H__

#include <fmt/format.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "bthread/countdown_event.h"
#include "bthread/mutex.h"
#include "cache/blockcache/cache_store.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/executor/executor.h"
namespace dingofs {
namespace client {
namespace vfs {

using BthreadRWLock = dingofs::utils::BthreadRWLock;
using BthreadMutex = bthread::Mutex;
using BCountDown = bthread::CountdownEvent;

class VFSHub;

using AsyncPrefetchCb = std::function<void(Status status, uint64_t len)>;

struct AsyncPrefecthInternalArgs {
  Status status;
  uint64_t len;
  BCountDown count_down;
};

struct BlockPrefetch {
  BlockPrefetch(cache::BlockKey block_key, uint64_t block_len)
      : key(block_key), len(block_len) {}
  cache::BlockKey key;
  uint64_t len;
};

struct ChunkPrefetch {
  ChunkPrefetch(Ino ino, uint64_t chunk_idx, uint64_t offset, uint64_t length)
      : ino{ino}, chunk_idx(chunk_idx), offset(offset), len(length) {}

  Ino ino;
  uint64_t chunk_idx;

  uint64_t offset;
  uint64_t len;

  std::string ToString() {
    return fmt::format("chunk prefetch in:{} chunk:{} {}-{}", ino, chunk_idx,
                       offset, offset + len);
  }
};

class PrefecthManager {
 public:
  PrefecthManager(VFSHub* hub, uint64_t fs_id, uint64_t chunk_size,
                  uint64_t block_size)
      : hub_(hub),
        fs_id_(fs_id),
        chunk_size_(chunk_size),
        block_size_(block_size) {}

  Status Start();
  void Stop();
  void AsyncPrefetch(Ino ino, uint64_t file_len, uint64_t start, uint64_t len) {
    prefetch_executor_->Execute([this, ino, file_len, start, len]() {
      this->DoPrefetch(ino, file_len, start, len);
    });
  }
  void AsyncPrefetch(Ino ino, AsyncPrefetchCb cb) {
    prefetch_executor_->Execute(
        [this, ino, cb]() { this->DoPrefetch(ino, cb); });
  }

 private:
  void DoPrefetch(Ino ino, uint64_t file_len, uint64_t start, uint64_t len);
  void DoPrefetch(Ino ino, AsyncPrefetchCb cb);
  std::vector<BlockPrefetch> Chunk2Block(ContextSPtr ctx, ChunkPrefetch& req);
  std::vector<ChunkPrefetch> File2Chunk(Ino ino, uint64_t offset,
                                        uint64_t len) const;
  std::vector<BlockPrefetch> FileRange2BlockKey(ContextSPtr ctx, Ino ino,
                                                uint64_t offset, uint64_t len);
  void addKey(const cache::BlockKey& key);
  void removeKey(const cache::BlockKey& key);
  bool isBusy(const cache::BlockKey& key);
  bool filterOut(const cache::BlockKey& key);
  BthreadRWLock rw_lock_;
  std::unordered_set<std::string> inflight_keys_;
  std::unique_ptr<Executor> prefetch_executor_;
  VFSHub* hub_;
  uint64_t fs_id_;
  uint64_t chunk_size_;
  uint64_t block_size_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif
