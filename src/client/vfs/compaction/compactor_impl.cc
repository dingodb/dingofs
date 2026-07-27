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

#include "client/vfs/compaction/compactor_impl.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <list>

#include "client/vfs/common/basync_util.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/compaction/compact_utils.h"
#include "client/vfs/data/reader/chunk_req_reader.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/slice_writer.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/block/block_utils.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {
using namespace compaction;

DEFINE_uint32(vfs_compact_cleanup_batch_size, 100,
              "maximum block keys per uncommitted compaction cleanup batch");

// S3 DeleteObjects hard limit; the sync BatchDelete path has no guard of its
// own, so clamp here (same bound as mds gc kBatchDeleteObjectSize).
constexpr size_t kMaxCleanupBatchSize = 1000;

Status CompactorImpl::Start() {
  LOG(INFO) << "CompactorImpl started";
  return Status::OK();
}

Status CompactorImpl::Stop() {
  std::unique_lock lg(mutex_);
  if (stopped_) {
    return Status::OK();
  }

  // Close admission before draining. Otherwise queued compaction tasks can
  // keep incrementing inflight_ while Stop() is waiting and make shutdown
  // depend on the entire pending queue being exhausted.
  stopped_ = true;

  while (inflight_ > 0) {
    LOG(INFO) << "CompactorImpl::Stop wait inflight_=" << inflight_;
    cv_.wait(lg);
  }

  LOG(INFO) << "CompactorImpl stopped";
  return Status::OK();
}

Status CompactorImpl::IncInflight() {
  std::unique_lock lg(mutex_);
  if (stopped_) {
    LOG(INFO) << "CompactorImpl is stopped, cannot accept new compaction";
    return Status::Stop("CompactorImpl stopped");
  }
  inflight_++;
  return Status::OK();
}

void CompactorImpl::DecInflight() {
  std::unique_lock lg(mutex_);
  inflight_--;
  if (inflight_ == 0) {
    cv_.notify_all();
  }
}

Status CompactorImpl::DoCompact(ContextSPtr ctx, Ino ino, int64_t chunk_index,
                                const std::vector<Slice>& slices,
                                Slice& out_slice) {
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "CompactorImpl::DoCompact", ctx->GetTraceSpan());
  auto fs_info = vfs_hub_->GetFsInfo();

  int32_t chunk_size = fs_info.chunk_size;
  int64_t chunk_start = chunk_index * chunk_size;

  FileRange file_range = GetSlicesFileRange(chunk_start, slices);
  int32_t offset_in_chunk =
      static_cast<int32_t>(file_range.offset % chunk_size);

  ChunkReq req(ino, chunk_index, offset_in_chunk, file_range);

  VLOG(9) << "Start comaction for req: " << req.ToString();

  std::string to_write;
  {
    // read data into our own destination buffer (compaction is not the FUSE
    // read-reply / RDMA path, so a plain buffer is the fill target here).
    to_write.resize(static_cast<size_t>(file_range.len));

    ChunkReqReader reader(vfs_hub_, req);

    Status s;
    BSynchronizer sync;
    ReadBufView dst{reinterpret_cast<uint8_t*>(to_write.data()), 0,
                    to_write.size()};
    reader.ReadAsync(SpanScope::GetContext(span), slices, dst,
                     sync.AsStatusCallBack(s));
    sync.Wait();

    if (!s.ok()) {
      LOG(WARNING) << "Faile compaction because read failed: " << s.ToString()
                   << ", req: " << req.ToString();
      return s;
    }
    // to_write is now filled in place.
  }

  Slice compacted;

  {
    auto page_size = vfs_hub_->GetWriteMemPool()->GetPageSize();

    SliceDataContext ctx(fs_info.id, ino, chunk_index, chunk_size,
                         fs_info.block_size, page_size);

    auto writer = std::make_shared<SliceWriter>(ctx, vfs_hub_, offset_in_chunk);

    Status ret =
        writer->Write(SpanScope::GetContext(span), to_write.data(),
                      static_cast<int32_t>(to_write.size()), offset_in_chunk);
    if (!ret.ok()) {
      // Compaction is best-effort: a write failure (e.g. NoSpace when the write
      // page pool is under back-pressure) must abort this compaction and be
      // retried later, never crash the client. Propagate like the read/flush
      // failures above; the caller (CompactChunkTask::Run) just logs it.
      LOG(WARNING) << "Fail compaction because write failed: " << ret.ToString()
                   << ", ino: " << ino << ", chunk_index: " << chunk_index;
      return ret;
    }

    Status s;
    BSynchronizer sync;
    writer->FlushAsync(sync.AsStatusCallBack(s));
    sync.Wait();
    if (!s.ok()) {
      uint64_t slice_id = writer->SliceId();
      LOG(WARNING) << "Fail compaction because flush failed: " << s.ToString()
                   << ", ino: " << ino << ", chunk_index: " << chunk_index
                   << ", slice_id: " << slice_id;
      // slice_id 0 means allocation never published: no block was ever
      // uploaded, so there is nothing to reclaim.
      if (slice_id != 0) {
        int32_t len = writer->Len();
        Slice orphan{.id = slice_id,
                     .size = len,
                     .off = 0,
                     .len = len,
                     .pos = offset_in_chunk};
        Status cleanup_status =
            DoCleanupUncommittedSlices(SpanScope::GetContext(span), {orphan});
        if (!cleanup_status.ok()) {
          LOG(WARNING) << "Fail cleanup of uncommitted compaction slice "
                       << slice_id << ": " << cleanup_status.ToString()
                       << ", ino: " << ino << ", chunk_index: " << chunk_index;
        }
      }
      return s;
    }

    compacted = writer->GetCommitSlice();
    VLOG(9) << "Success compaction, compacted_slice: " << Slice2Str(compacted);
  }

  out_slice = compacted;
  return Status::OK();
}

// Read/write failures upload nothing and flush failures self-clean in
// DoCompact; the remaining orphan case is a failed metadata commit, which the
// caller reclaims via CleanupUncommittedSlices.
Status CompactorImpl::Compact(ContextSPtr ctx, Ino ino, int64_t chunk_index,
                              const std::vector<Slice>& slices,
                              std::vector<Slice>& out_slices) {
  VLOG(9) << "CompactorImpl::Compact ino: " << ino
          << ", chunk_index: " << chunk_index
          << ", slices count: " << slices.size();
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";

  DINGOFS_RETURN_NOT_OK(IncInflight());
  auto cleanup = MakeScopedCleanup([&]() { DecInflight(); });

  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "CompactorImpl::Compact", ctx->GetTraceSpan());

  std::vector<Slice> to_compact;
  int64_t chunk_start = chunk_index * vfs_hub_->GetFsInfo().chunk_size;
  int32_t skip = Skip(chunk_start, slices);
  VLOG(9) << "CompactorImpl::Compact skip count: " << skip;

  for (size_t i = skip; i < slices.size(); ++i) {
    to_compact.push_back(slices[i]);
  }

  if (to_compact.empty()) {
    LOG(INFO) << "No slices to compact after skipping";
    return Status::OK();
  }

  VLOG(9) << "Compact slices count: " << to_compact.size()
          << ", skip count: " << skip
          << ", origin slices count: " << slices.size();

  Slice compacted;
  DINGOFS_RETURN_NOT_OK(DoCompact(SpanScope::GetContext(span), ino, chunk_index,
                                  to_compact, compacted));

  std::vector<Slice> out;
  out.reserve(skip + 1);

  for (int i = 0; i < skip; i++) {
    out.push_back(slices[i]);
  }
  out.push_back(compacted);

  out_slices.swap(out);

  return Status::OK();
}

Status CompactorImpl::ForceCompact(ContextSPtr ctx, Ino ino,
                                   int64_t chunk_index,
                                   const std::vector<Slice>& slices,
                                   std::vector<Slice>& out_slices) {
  VLOG(9) << "CompactorImpl::ForceCompact ino: " << ino
          << ", chunk_index: " << chunk_index
          << ", slices count: " << slices.size();
  CHECK(!slices.empty()) << "invalid compact, no slices to compact";

  DINGOFS_RETURN_NOT_OK(IncInflight());
  auto cleanup = MakeScopedCleanup([&]() { DecInflight(); });

  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "CompactorImpl::Compact", ctx->GetTraceSpan());
  Slice compacted;
  DINGOFS_RETURN_NOT_OK(DoCompact(SpanScope::GetContext(span), ino, chunk_index,
                                  slices, compacted));
  out_slices.push_back(compacted);

  return Status::OK();
}

Status CompactorImpl::CleanupUncommittedSlices(
    ContextSPtr ctx, const std::vector<Slice>& slices) {
  DINGOFS_RETURN_NOT_OK(IncInflight());
  auto cleanup = MakeScopedCleanup([&]() { DecInflight(); });

  return DoCleanupUncommittedSlices(ctx, slices);
}

Status CompactorImpl::DoCleanupUncommittedSlices(
    ContextSPtr ctx, const std::vector<Slice>& slices) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "CompactorImpl::CleanupUncommittedSlices", ctx->GetTraceSpan());

  uint32_t block_size = vfs_hub_->GetFsInfo().block_size;

  std::list<std::string> keys;
  for (const auto& slice : slices) {
    if (slice.id == 0 || slice.size <= 0) continue;  // holes have no blocks
    size_t slice_blocks = 0;
    for (const auto& key :
         EnumerateBlockKeys(slice.id, slice.size, block_size)) {
      keys.push_back(key.StoreKey());
      slice_blocks++;
    }
    LOG(INFO) << "Cleanup uncommitted compaction slice, slice_id: " << slice.id
              << ", size: " << slice.size << ", block_size: " << block_size
              << ", blocks: " << slice_blocks;
  }

  if (keys.empty()) return Status::OK();

  VLOG(9) << "CompactorImpl::CleanupUncommittedSlices slices: " << slices.size()
          << ", blocks: " << keys.size();

  // Keep each cleanup request bounded: zero would stall this loop forever,
  // and the upper cap keeps a misconfigured flag from tripping the backend
  // request limit.
  const size_t batch_size = std::clamp<size_t>(
      FLAGS_vfs_compact_cleanup_batch_size, 1, kMaxCleanupBatchSize);

  auto* accesser = vfs_hub_->GetBlockAccesser();
  Status status = Status::OK();
  while (!keys.empty()) {
    auto it = keys.begin();
    std::advance(it, std::min(keys.size(), batch_size));
    std::list<std::string> batch;
    batch.splice(batch.begin(), keys, keys.begin(), it);

    Status s = accesser->BatchDelete(batch);
    if (!s.ok()) {
      LOG(WARNING)
          << "CompactorImpl::CleanupUncommittedSlices batch delete failed: "
          << s.ToString() << ", batch size: " << batch.size();
      status = s;
    }
  }

  return status;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
