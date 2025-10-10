#include "prefetch_manager.h"

#include <fmt/format.h>
#include <rocksdb/attribute_groups.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "cache/blockcache/cache_store.h"
#include "cache/utils/context.h"
#include "client/common/const.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "options/client/option.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

using WriteLockGuard = dingofs::utils::WriteLockGuard;
using ReadLockGuard = dingofs::utils::ReadLockGuard;

Status PrefecthManager::Start() {
  prefetch_executor_ = std::make_unique<ExecutorImpl>(
      FLAGS_client_vfs_file_prefetch_executor_num);
  auto ok = prefetch_executor_->Start();
  if (!ok) {
    LOG(ERROR) << "start prefetch manager executor failed.";
    return Status::Internal("prefetch manager executor start failed");
  }

  LOG(INFO) << fmt::format(
      "PrefetchManager started prefetch block:{} executor num:{} has cache "
      "store:{}",
      FLAGS_client_vfs_file_prefetch_block_cnt,
      FLAGS_client_vfs_file_prefetch_executor_num,
      hub_->GetBlockCache()->HasCacheStore());

  return Status::OK();
}

void PrefecthManager::Stop() { prefetch_executor_->Stop(); }

std::vector<BlockPrefetch> PrefecthManager::Chunk2Block(ContextSPtr ctx,
                                                        ChunkPrefetch& req) {
  std::vector<Slice> slices;
  std::vector<BlockReadReq> block_reqs;
  std::vector<BlockPrefetch> block_keys;

  Status status =
      hub_->GetMetaSystem()->ReadSlice(ctx, req.ino, req.chunk_idx, 0, &slices);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "prefetch read ino:{} chunk:{} slice info failed {}", req.ino,
        req.chunk_idx, status.ToString());
    return block_keys;
  }

  FileRange range = {req.chunk_idx * chunk_size_ + req.offset, req.len};
  std::vector<SliceReadReq> slice_reqs = ProcessReadRequest(slices, range);

  for (auto& slice_req : slice_reqs) {
    VLOG(6) << "Read slice_req: " << slice_req.ToString();

    if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, fs_id_, req.ino, chunk_size_, block_size_);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    }
  }

  for (auto& block_req : block_reqs) {
    cache::BlockKey key(fs_id_, req.ino, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);

    if (filterOut(key)) {
      VLOG(6) << fmt::format("prefetch skip key {}", key.Filename());
      continue;
    }
    block_keys.emplace_back(key, block_req.block.block_len);
  }

  return block_keys;
}

std::vector<BlockPrefetch> PrefecthManager::FileRange2BlockKey(ContextSPtr ctx,
                                                               Ino ino,
                                                               uint64_t offset,
                                                               uint64_t len) {
  std::vector<ChunkPrefetch> chunk_tasks;
  std::vector<BlockPrefetch> block_keys;

  chunk_tasks = File2Chunk(ino, offset, len);

  for (auto task : chunk_tasks) {
    std::vector<BlockPrefetch> block_keys_tmp;
    block_keys_tmp = Chunk2Block(ctx, task);
    block_keys.insert(block_keys.end(), block_keys_tmp.begin(),
                      block_keys_tmp.end());
  }

  return block_keys;
}

std::vector<ChunkPrefetch> PrefecthManager::File2Chunk(Ino ino, uint64_t offset,
                                                       uint64_t len) const {
  std::vector<ChunkPrefetch> chunk_tasks;
  VLOG(6) << fmt::format("prefetch request ino {} offset {}", ino, offset);

  uint64_t chunk_idx = offset / chunk_size_;
  uint64_t chunk_offset = offset % chunk_size_;
  uint64_t prefetch_size;
  prefetch_size = len;

  while (prefetch_size > 0) {
    uint64_t chunk_fetch_size =
        std::min(prefetch_size, chunk_size_ - chunk_offset);
    ChunkPrefetch task(ino, chunk_idx, chunk_offset, chunk_fetch_size);

    VLOG(6) << task.ToString();
    chunk_tasks.push_back(task);

    chunk_idx++;
    chunk_offset = 0;
    prefetch_size -= chunk_fetch_size;
  }

  return chunk_tasks;
}

void PrefecthManager::DoPrefetch(Ino ino, uint64_t file_len, uint64_t start,
                                 uint64_t len) {
  auto span = hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  std::vector<BlockPrefetch> block_keys;
  uint64_t block_size = hub_->GetFsInfo().block_size;
  uint64_t prefecth_block_cnt = FLAGS_client_vfs_file_prefetch_block_cnt;

  if (prefecth_block_cnt == 0) {
    return;
  }

  VLOG(6) << fmt::format("prefetch file {} start posted", ino);

  uint64_t prefetch_start = ((start + len) / block_size) * block_size;
  uint64_t prefetch_len =
      std::max(prefecth_block_cnt * block_size,
               ((start + len + block_size - 1) / block_size) * block_size -
                   prefetch_start);
  prefetch_len = std::min(prefetch_len, file_len - prefetch_start);

  VLOG(6) << fmt::format(
      "prefetch transform read s-l-fl:{}-{}-{} to prefetch s-l:{}-{} with "
      "block_cnt {}",
      start, len, file_len, prefetch_start, prefetch_len, prefecth_block_cnt);

  block_keys =
      FileRange2BlockKey(span->GetContext(), ino, prefetch_start, prefetch_len);

  for (auto& key_len : block_keys) {
    cache::BlockKey key = key_len.key;
    uint64_t len = key_len.len;

    if (filterOut(key)) {
      VLOG(6) << fmt::format("prefetch skip block_key: {}", key.Filename());
      continue;
    }

    addKey(key);

    VLOG(6) << fmt::format("prefetch block_key:{} len {} start", key.Filename(),
                           len);
    hub_->GetBlockCache()->AsyncPrefetch(
        cache::NewContext(), key, len, [this, key, len](Status status) {
          VLOG(6) << fmt::format("prefetch block_key:{} status {} len {}",
                                 key.Filename(), status.ToString(), len);
          this->removeKey(key);
          if (!status.ok() && !status.IsExist()) {
            LOG(WARNING) << fmt::format("prefetch key:{} failed {}",
                                        key.Filename(), status.ToString());
          }
        });
  }
}

void PrefecthManager::DoPrefetch(Ino ino, AsyncPrefetchCb cb) {
  auto span = hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);

  VLOG(6) << fmt::format("prefetch file {} start", ino);

  Attr attr;
  auto ret = hub_->GetMetaSystem()->GetAttr(span->GetContext(), ino, &attr);
  if (!ret.ok()) {
    LOG(ERROR) << fmt::format("prefetch ino:{} get attr failed", ino);
    cb(ret, 0);
    return;
  }

  VLOG(6) << fmt::format("prefetch get file {} len {} start", ino, attr.length);

  std::vector<BlockPrefetch> block_keys;
  AsyncPrefecthInternalArgs args;
  block_keys = FileRange2BlockKey(span->GetContext(), ino, 0, attr.length);

  args.status = Status::OK();
  args.count_down.reset(block_keys.size());

  for (auto& key_len : block_keys) {
    cache::BlockKey key = key_len.key;
    uint64_t len = key_len.len;

    if (filterOut(key)) {
      VLOG(6) << fmt::format("prefetch skip block_key: {}", key.Filename());
      args.count_down.signal();
      continue;
    }

    addKey(key);

    VLOG(6) << fmt::format("prefetch block_key: {} len {}", key.Filename(),
                           len);
    hub_->GetBlockCache()->AsyncPrefetch(
        cache::NewContext(), key, len, [this, key, len, &args](Status status) {
          VLOG(6) << fmt::format("prefetch block_key: {} len {} status {}",
                                 key.Filename(), len, status.ToString());
          this->removeKey(key);
          if (!status.ok() && !status.IsExist()) {
            LOG(WARNING) << fmt::format("prefetch key:{} failed {}",
                                        key.Filename(), status.ToString());
            args.status = status;
          } else {
            args.len += len;
          }
          args.count_down.signal();
        });
  }

  args.count_down.wait();
  VLOG(6) << fmt::format("prefetch file {} status {}, len {}", ino,
                         args.status.ToString(), args.len);

  cb(args.status, args.len);
}

void PrefecthManager::addKey(const cache::BlockKey& key) {
  WriteLockGuard lw(rw_lock_);

  inflight_keys_.insert(key.Filename());
}

void PrefecthManager::removeKey(const cache::BlockKey& key) {
  WriteLockGuard lw(rw_lock_);
  inflight_keys_.erase(key.Filename());
}

bool PrefecthManager::isBusy(const cache::BlockKey& key) {
  ReadLockGuard lr(rw_lock_);

  return inflight_keys_.count(key.Filename()) != 0;
}

bool PrefecthManager::filterOut(const cache::BlockKey& key) {
  return isBusy(key) || hub_->GetBlockCache()->IsCached(key);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs