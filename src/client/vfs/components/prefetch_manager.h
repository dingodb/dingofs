#ifndef __DINGOFS_CLIENT_VFS_PREFETCH_MANAGER_H__
#define  __DINGOFS_CLIENT_VFS_PREFETCH_MANAGER_H__

#include <fmt/format.h>
#include <functional>
#include <memory>
#include <vector>

#include "options/client/vfs/vfs_dynamic_option.h"
#include "utils/concurrent/rw_lock.h"
#include "utils/executor/executor.h"
#include "client/vfs/data/reader/reader_common.h"
#include "client/meta/vfs_meta.h"
#include "trace/context.h"
#include "cache/blockcache/cache_store.h"
namespace dingofs {
namespace client {
namespace vfs {

using BthreadRWLock = dingofs::utils::BthreadRWLock;

class VFSHub;

struct ChunkPrefetch {
  ChunkPrefetch(Ino ino, uint64_t chunk_idx, uint64_t offset, uint64_t length) :
        ino{ino}, chunk_idx(chunk_idx), offset(offset), len(length) {}

  Ino ino;
  uint64_t chunk_idx;

  uint64_t offset;
  uint64_t len;

  std::string ToString() {return fmt::format("chunk prefetch in:{} chunk:{} {}-{}", 
                  ino, chunk_idx, offset, offset + len);}
};

class PrefecthManager {
public:
  PrefecthManager(VFSHub *hub, uint64_t fs_id, uint64_t chunk_size, uint64_t block_size) : 
    hub_(hub),
    fs_id_(fs_id),
    chunk_size_(chunk_size),
    block_size_(block_size){}

  Status Start();
  void Stop();
  void AsyncPrefetch(Ino ino, uint64_t file_len, uint64_t offset) {prefetch_executor_->Execute([this, ino, file_len, offset](){
                                this->DoPrefetch(ino, offset, file_len);});}

private:
  void DoPrefetch(Ino ino, uint64_t offset, uint64_t file_len);
  void DoChunkPrefetch(ContextSPtr ctx, ChunkPrefetch &req);
  void addKey(const cache::BlockKey &key);
  void removeKey(const cache::BlockKey &key);
  bool isBusy(const cache::BlockKey &key);
  bool filterOut(const cache::BlockKey &key);
  BthreadRWLock rw_lock_;
  std::unordered_set<std::string> inflight_keys_;
  std::unique_ptr<Executor> prefetch_executor_;
  VFSHub *hub_;
  uint64_t fs_id_;
  uint64_t chunk_size_;
  uint64_t block_size_;
};

} // namespace vfs
} //namespace client
} //namespace dingofs











#endif
