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

/*
 * Project: DingoFS
 * Created Date: 2025-05-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/local_filesystem.h"

#include <bits/types/struct_iovec.h>
#include <butil/memory/aligned_memory.h>
#include <butil/memory/scope_guard.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/aio_queue.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_health_checker.h"
#include "cache/common/macro.h"
#include "cache/common/slab_pool.h"
#include "cache/iutil/buffer_pool.h"
#include "cache/iutil/file_util.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_bool(fix_buffer, true, "whether to use fixed buffer for aio");

namespace {

constexpr size_t kAlignedIOBlockSize = 4096;

// io_uring fixed buffers = recv-pool (write path) buffers followed by send-pool
// (read path) buffers. Each slab is at once the io_uring fixed buffer and the
// RDMA-registered buffer, so disk<->slab<->NIC is fully zero copy. Reads and
// writes use separate pools to avoid cross-contention.
std::vector<iovec> BuildIoUringFixedBuffers() {
  auto buffers = GetGlobalRecvSlabPool().Fetch();
  auto reads = GetGlobalSendSlabPool().Fetch();
  buffers.insert(buffers.end(), reads.begin(), reads.end());
  return buffers;
}

bool IsAligned(uint64_t n, uint64_t m) { return (n % m) == 0; }

off_t AlignOffset(off_t offset) {
  if (!IsAligned(offset, kAlignedIOBlockSize)) {
    offset = offset - (offset % kAlignedIOBlockSize);
  }
  return offset;
}

size_t AlignLength(size_t length) {
  if (!IsAligned(length, kAlignedIOBlockSize)) {
    length = (length + kAlignedIOBlockSize - 1) & ~(kAlignedIOBlockSize - 1);
  }
  return length;
}

// Allocate an O_DIRECT-aligned buffer of `aligned_length` into `out`. With
// --fix_buffer it comes from a global slab pool -- the recv pool for writes, the
// send pool for reads -- so it is at once the io_uring fixed buffer and the
// RDMA-registered buffer (its meta carries the RDMA lkey). *buf_index is the
// absolute io_uring fixed-buffer index (recv buffers are registered first, send
// buffers after). Without --fix_buffer it falls back to an aligned malloc and
// *buf_index is -1.
Status AllocFixedBuffer(IOBuffer* out, size_t aligned_length, bool for_read,
                        int* buf_index) {
  if (!FLAGS_fix_buffer) {
    char* data =
        (char*)butil::AlignedAlloc(aligned_length, kAlignedIOBlockSize);
    out->AppendUserData(data, aligned_length, butil::AlignedFree);
    *buf_index = -1;
    return Status::OK();
  }

  // reads come from the send pool, writes from the recv pool.
  auto& slab_pool =
      for_read ? GetGlobalSendSlabPool() : GetGlobalRecvSlabPool();
  auto* slab = slab_pool.Alloc(aligned_length);
  if (slab == nullptr) {
    return Status::CacheFull("slab pool exhausted");
  }
  // meta carries the RDMA lkey (set once at server start); the deleter returns
  // the slab to the right pool when the IOBuffer is finally destroyed.
  out->AppendUserDataWithMeta(
      slab->data, aligned_length,
      [for_read, slab](void*) {
        (for_read ? GetGlobalSendSlabPool() : GetGlobalRecvSlabPool())
            .Free(slab);
      },
      slab->meta);
  *buf_index =
      (for_read ? static_cast<int>(GetGlobalRecvSlabPool().BufferCount()) : 0) +
      static_cast<int>(slab->index);
  return Status::OK();
}

}  // namespace

LocalFileSystem::LocalFileSystem(DiskCacheLayoutSPtr layout)
    : running_(false),
      layout_(layout),
      inflight_(FLAGS_iodepth),
      aio_queue_(std::make_unique<AioQueue>(BuildIoUringFixedBuffers())),
      health_checker_(std::make_unique<DiskHealthChecker>(layout)) {}

Status LocalFileSystem::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "LocalFileSystem already started";
    return Status::OK();
  }

  LOG(INFO) << "LocalFileSystem is starting...";

  auto status = aio_queue_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start AioQueue";
    return status;
  }

  health_checker_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "LocalFileSystem started";
  return Status::OK();
}

Status LocalFileSystem::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "LocalFileSystem already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "LocalFileSystem is shutting down...";

  health_checker_->Shutdown();

  auto status = aio_queue_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown AioQueue";
    return status;
  }

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "LocalFilesystem is down";
  return Status::OK();
}

Status LocalFileSystem::WriteFile(const std::string& path,
                                  const IOBuffer* buffer) {
  DCHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  auto tmppath = TempFilepath(path);
  status = iutil::MkDirs(iutil::ParentDir(tmppath));
  if (!status.ok() && !status.IsExist()) {
    LOG(ERROR) << "Fail to mkdirs `" << iutil::ParentDir(tmppath) << "'";
    return status;
  }

  int fd;
  status = iutil::OpenFile(tmppath, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT,
                           0644, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to open file=`" << tmppath << "'";
    return status;
  }

  BRPC_SCOPE_EXIT {
    iutil::Close(fd);
    if (!status.ok()) {
      iutil::Unlink(tmppath);
    }
  };

  size_t aligned_length = AlignLength(buffer->Size());
  if (buffer->Size() != aligned_length) {
    status = iutil::Fallocate(fd, 0, 0, aligned_length);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to fallocate file=`" << path << "'";
      return status;
    }
  }

  // Fast path: the source is already a single slab-backed block of the exact
  // O_DIRECT size (e.g. the RDMA request attachment) — write it in place as an
  // io_uring fixed buffer, zero copy. Otherwise stage it into an aligned slab
  // buffer (this also zero-pads when the block is not 4K-aligned).
  // The write source (e.g. the RDMA request attachment) lives in the recv pool,
  // registered first in io_uring, so its slab index is the absolute fixed index.
  auto& slab_pool = GetGlobalRecvSlabPool();
  if (FLAGS_fix_buffer && buffer->Size() == aligned_length &&
      buffer->ConstIOBuf().backing_block_num() == 1 &&
      slab_pool.Contains(buffer->Fetch1())) {
    int buf_index = slab_pool.IndexOf(buffer->Fetch1());
    status = AioWrite(fd, buffer->Fetch1(), aligned_length, buf_index);
  } else {
    IOBuffer tbuffer;
    int buf_index;
    status = AllocFixedBuffer(&tbuffer, aligned_length, /*for_read=*/false,
                              &buf_index);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to allocate write buffer for `" << path << "'";
      return status;
    }
    buffer->CopyTo(tbuffer.Fetch1(), buffer->Size());
    status = AioWrite(fd, tbuffer.Fetch1(), aligned_length, buf_index);
  }
  if (!status.ok()) {
    LOG(ERROR) << "Fail to write file'`" << tmppath << "'";
    return status;
  }

  status = iutil::Rename(tmppath, path);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to rename `" << tmppath << "' to `" << path << "'";
    return status;
  }
  return status;
}

Status LocalFileSystem::ReadFile(const std::string& path, off_t offset,
                                 size_t length, IOBuffer* buffer) {
  CHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  int fd;
  status = iutil::OpenFile(path, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to open file";
    return status;
  }

  BRPC_SCOPE_EXIT { iutil::Close(fd); };

  off_t aligned_offset = AlignOffset(offset);
  size_t aligned_length = AlignLength(length + offset - aligned_offset);

  // O_DIRECT requires an aligned buffer and an aligned superset read, so the
  // read always lands in a freshly allocated aligned slab first.
  IOBuffer aligned;
  int buf_index;
  status = AllocFixedBuffer(&aligned, aligned_length, /*for_read=*/true,
                            &buf_index);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to allocate read buffer for `" << path << "'";
    return status;
  }

  status =
      AioRead(fd, aligned_offset, aligned_length, aligned.Fetch1(), buf_index);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to read file=`" << path << "'";
    return status;
  }

  if (aligned_offset != offset) {
    aligned.PopFront(offset - aligned_offset);
  }
  if (aligned_length != length) {
    aligned.PopBack(aligned_offset + aligned_length - (offset + length));
  }

  if (buffer->Size() == 0) {
    // No caller destination (cache server / uploader): hand back the slab -- it
    // is the io_uring fixed buffer and, bubbling up (meta=lkey), the RDMA-write
    // source too. Zero copy.
    *buffer = std::move(aligned);
  } else {
    // Caller-allocated destination (e.g. the FUSE read-mempool slot, unaligned):
    // copy the [offset, offset+length) slice in place (one copy).
    FillDest(buffer, aligned, length);
  }

  return status;
}

// The inflight for aio which use fixed buffer is controlled by buffer pool,
// others need to be tracked here.
struct InflightAioGuard {
  InflightAioGuard(int fd, iutil::InflightTracker* inflight)
      : fd(fd), inflight(inflight) {
    if (!FLAGS_fix_buffer) {
      CHECK(inflight->Add(std::to_string(fd)).ok());
    }
  }

  ~InflightAioGuard() {
    if (!FLAGS_fix_buffer) {
      inflight->Remove(std::to_string(fd));
    }
  }

  int fd;
  iutil::InflightTracker* inflight;
};

Status LocalFileSystem::AioWrite(int fd, char* buffer, size_t length,
                                 int buf_index) {
  InflightAioGuard guard(fd, &inflight_);

  auto aio = Aio(fd, 0, length, buffer, buf_index, false);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.Result().status;
}

Status LocalFileSystem::AioRead(int fd, off_t offset, size_t length,
                                char* buffer, int buf_index) {
  InflightAioGuard guard(fd, &inflight_);

  auto aio = Aio(fd, offset, length, buffer, buf_index, true);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.Result().status;
}

}  // namespace cache
}  // namespace dingofs
