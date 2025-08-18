
#include <cstddef>
#include <memory>
#include <vector>

#include "prefetch_manager.h"
#include <fmt/format.h>
#include "cache/utils/context.h"
#include "client/meta/vfs_meta.h"
#include "client/vfs/data/common/common.h"
#include "common/status.h"
#include "options/client/vfs/vfs_dynamic_option.h"
#include "utils/executor/thread/executor_impl.h"
#include "client/const.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/hub/vfs_hub.h"



namespace dingofs {
namespace client {
namespace vfs{

using WriteLockGuard = dingofs::utils::WriteLockGuard;
using ReadLockGuard = dingofs::utils::ReadLockGuard;


Status PrefecthManager::Start() {
    prefetch_executor_ = std::make_unique<ExecutorImpl>(FLAGS_vfs_file_prefetch_executor_num);
    auto ok = prefetch_executor_->Start();
    if (!ok) {
        LOG(ERROR) << "start prefetch manager executor failed.";
        return Status::Internal("prefetch manager executor start failed");
    } 

    LOG(INFO) << fmt::format("PrefetchManager started prefetch block:{} executor num:{} has cache store:{}",
               FLAGS_vfs_file_prefetch_block_cnt, FLAGS_vfs_file_prefetch_executor_num, 
            hub_->GetBlockCache()->HasCacheStore());

    return Status::OK();
}

void PrefecthManager::Stop() {
    prefetch_executor_->Stop();
}

void PrefecthManager::DoChunkPrefetch(ContextSPtr ctx, ChunkPrefetch &req) {
    std::vector<Slice> slices;


    Status status = hub_->GetMetaSystem()->ReadSlice(ctx, req.ino, req.chunk_idx, 0, &slices);
    if (!status.ok()) {
        LOG(ERROR) << fmt::format("prefetch read ino:{} chunk:{} slice info failed {}",
                    req.ino, req.chunk_idx, status.ToString());
        return;
    }

    FileRange range = {req.chunk_idx * chunk_size_ + req.offset, req.len};
    std::vector<SliceReadReq> slice_reqs = ProcessReadRequest(slices, range);
    std::vector<BlockReadReq> block_reqs;

    for (auto& slice_req : slice_reqs) {

      VLOG(6) << "{} Read slice_req: " << slice_req.ToString();

      if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
        std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
            slice_req, fs_id_, req.ino, chunk_size_,
            block_size_);

        block_reqs.insert(block_reqs.end(),
                          std::make_move_iterator(reqs.begin()),
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

      addKey(key);
    
      VLOG(6) << fmt::format("prefetch block_key: {}, block_req:{}", 
                             key.StoreKey(), block_req.ToString());
      hub_->GetBlockCache()->AsyncPrefetch(cache::NewContext(), 
                    key, block_req.block.block_len, 
                    [this, key](Status status) {
                                VLOG(6) << fmt::format("prefetch key:{} status {}",key.Filename(), status.ToString());
                                this->removeKey(key);
                                if (!status.ok()) {
                                    LOG(ERROR) << fmt::format("prefetch key:{} failed {}",
                                        key.Filename(), status.ToString());
                                }});
    }
}

void PrefecthManager::DoPrefetch(Ino ino, uint64_t offset, uint64_t file_len)
{
    auto span = hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
    Attr attr;

    VLOG(6) << fmt::format("prefetch request ino {} offset {}", ino, offset);

    if (offset >= file_len) {
        LOG(WARNING) << fmt::format("prefetch read ino {} offset {} exceed file length {}", ino, offset, attr.length);
        return;
    }

    std::vector<ChunkPrefetch> chunk_tasks;
    uint64_t chunk_idx = offset / chunk_size_;
    uint64_t chunk_offset = offset % chunk_size_;
    uint64_t prefetch_size = std::min(FLAGS_vfs_file_prefetch_block_cnt * block_size_, attr.length - offset);

    while (prefetch_size > 0) {
        uint64_t chunk_fetch_size = std::min(prefetch_size, chunk_size_ - chunk_offset);
        ChunkPrefetch task(ino, chunk_idx, chunk_offset, chunk_fetch_size);

        VLOG(6) << task.ToString();
        chunk_tasks.push_back(task);

        chunk_idx++;
        chunk_offset = 0;
        prefetch_size -= chunk_fetch_size;
    }

    for (auto task : chunk_tasks) {
        DoChunkPrefetch(span->GetContext(), task);
    }
}

void PrefecthManager::addKey(const cache::BlockKey &key) {
    WriteLockGuard lw(rw_lock_);

    inflight_keys_.insert(key.Filename());
}

void PrefecthManager::removeKey(const cache::BlockKey &key) {
    WriteLockGuard lw(rw_lock_);
    inflight_keys_.erase(key.Filename());
}


bool PrefecthManager::isBusy(const cache::BlockKey &key) {
    ReadLockGuard lr(rw_lock_);

    return inflight_keys_.count(key.Filename()) != 0;
}

bool PrefecthManager::filterOut(const cache::BlockKey &key) {
    return isBusy(key) || hub_->GetBlockCache()->IsCached(key);
}

}
}
}