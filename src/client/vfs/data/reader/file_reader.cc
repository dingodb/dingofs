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

#include "client/vfs/data/reader/file_reader.h"

#include <absl/synchronization/blocking_counter.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <vector>

#include "client/common/const.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/data/reader/chunk_reader.h"
#include "client/vfs/data/reader/reader_common.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("FileReader::" + std::string(__FUNCTION__))

static void ChunkReadCallback(const ChunkReadReq& req,
                              ReaderSharedState& shared, Status s) {
  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "FileReader fail read chunk, chunk_req: {}, status: {}", req.ToString(),
        s.ToString());
  } else {
    VLOG(3) << fmt::format(
        "FileReader success read chunk, chunk_req: {}, status: {}",
        req.ToString(), s.ToString());
  }

  {
    std::unique_lock<std::mutex> lock(shared.mtx);
    if (s.ok()) {
      shared.read_size += req.to_read_size;
    } else {
      if (shared.status.ok()) {
        shared.status = s;
      }
    }

    shared.num_done++;
    CHECK_GE(shared.total, shared.num_done);
    if (shared.num_done >= shared.total) {
      shared.cv.notify_all();
    }
  }
}

uint64_t FileReader::GetChunkSize() const {
  return vfs_hub_->GetFsInfo().chunk_size;
}

ChunkReader* FileReader::GetOrCreateChunkReader(uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto iter = chunk_readers_.find(chunk_index);
  if (iter != chunk_readers_.end()) {
    return iter->second.get();
  } else {
    auto chunk_reader =
        std::make_shared<ChunkReader>(vfs_hub_, fh_, ino_, chunk_index);
    chunk_readers_[chunk_index] = std::move(chunk_reader);
    return chunk_readers_[chunk_index].get();
  }
}

Status FileReader::Read(ContextSPtr ctx, char* buf, uint64_t size,
                        uint64_t offset, uint64_t* out_rsize) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  Attr attr;
  DINGOFS_RETURN_NOT_OK(GetAttr(span->GetContext(), &attr));

  if (attr.length <= offset) {
    *out_rsize = 0;
    return Status::OK();
  }

  uint64_t time_now = WarmupHelper::GetTimeSecs();
  if (FLAGS_client_vfs_intime_warmup_enable &&
      ((time_now - last_intime_warmup_trigger_) >
           FLAGS_client_vfs_warmup_trigger_restart_interval_secs ||
       (attr.mtime - last_intime_warmup_mtime_) >
           fLI64::FLAGS_client_vfs_warmup_mtime_restart_interval_secs)) {
    WarmupInfo info(ino_);
    last_intime_warmup_trigger_ = time_now;
    last_intime_warmup_mtime_ = attr.mtime;
    vfs_hub_->GetWarmupManager()->AsyncWarmupProcess(info);
  }

  uint64_t chunk_size = GetChunkSize();

  uint64_t chunk_index = offset / chunk_size;
  uint64_t chunk_offset = offset % chunk_size;

  uint64_t total_read_size = std::min(size, attr.length - offset);

  if (FLAGS_client_vfs_file_prefetch_block_cnt > 0 &&
      vfs_hub_->GetBlockCache()->HasCacheStore()) {
    vfs_hub_->GetPrefetchManager()->AsyncPrefetch(ino_, attr.length, offset,
                                                  total_read_size);
  }

  std::vector<ChunkReadReq> read_reqs;

  while (total_read_size > 0) {
    uint64_t read_size = std::min(total_read_size, chunk_size - chunk_offset);

    ChunkReadReq req{
        .ino = ino_,
        .index = chunk_index,
        .offset = chunk_offset,
        .to_read_size = read_size,
        .buf = buf,
    };

    read_reqs.push_back(req);

    buf += read_size;
    total_read_size -= read_size;

    offset += read_size;
    chunk_index = offset / chunk_size;
    chunk_offset = offset % chunk_size;
  }

  CHECK_GT(read_reqs.size(), 0);

  ReaderSharedState shared;
  shared.total = read_reqs.size();
  shared.num_done = 0;
  shared.status = Status::OK();

  for (auto& req : read_reqs) {
    ChunkReader* chunk_reader = GetOrCreateChunkReader(req.index);
    chunk_reader->ReadAsync(
        span->GetContext(), req, [this, &req, &shared](auto&& PH1) {
          ChunkReadCallback(req, shared, std::forward<decltype(PH1)>(PH1));
        });
  }

  Status ret;
  {
    std::unique_lock<std::mutex> lock(shared.mtx);
    while (shared.num_done < shared.total) {
      shared.cv.wait(lock);
    }

    ret = shared.status;
    if (ret.ok()) {
      *out_rsize = shared.read_size;
    }
  }

  return ret;
}

void FileReader::Invalidate() {
  VLOG(1) << fmt::format("FileReader::Invalidate, ino: {}", ino_);

  std::lock_guard<std::mutex> lock(mutex_);
  validated_ = false;
  for (auto& [index, reader] : chunk_readers_) {
    reader->Invalidate();
  }
}

Status FileReader::GetAttr(ContextSPtr ctx, Attr* attr) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  std::lock_guard<std::mutex> lock(mutex_);
  if (validated_) {
    *attr = attr_;
    return Status::OK();
  }

  Status s =
      vfs_hub_->GetMetaSystem()->GetAttr(span->GetContext(), ino_, &attr_);

  if (s.ok()) {
    validated_ = true;
    *attr = attr_;
  } else {
    LOG(WARNING) << fmt::format(
        "FileReader::GetAttr failed, ino: {}, status: {}", ino_, s.ToString());
  }

  return s;
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs
