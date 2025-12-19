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
#include <aws/s3/model/ObjectLockRetentionMode.h>
#include <butil/strings/string_split.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>

#include "client/common/const.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/chunk_reader.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "read_request.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace vfs {

// TODO: maybe we need rreq manager in future
static bvar::Adder<uint64_t> vfs_rreq_in_queue("vfs_rreq_in_queue");
static bvar::Adder<uint64_t> vfs_rreq_inflighting("vfs_rreq_inflighting");

static std::atomic<uint64_t> req_id_gen{1};

static const uint64_t kReqValidityTimeoutS = 30;
static const uint32_t kMaxReadRequests = 64;

#define METHOD_NAME() ("FileReader::" + std::string(__FUNCTION__))

FileReader::FileReader(VFSHub* hub, uint64_t fh, uint64_t ino)
    : vfs_hub_(hub),
      fh_(fh),
      ino_(ino),
      chunk_size_(hub->GetFsInfo().chunk_size),
      block_size_(hub->GetFsInfo().block_size),
      policy_(new ReadaheadPoclicy(fh)) {}

// when file reader destructor called,
// meas no reader and all background read requests should be done
FileReader::~FileReader() {
  std::unique_lock<std::mutex> lock(mutex_);
  CHECK(closing_) << "FileReader destructor called without Close";

  std::vector<ReadRequest*> to_delete;
  for (auto& [req_id, req_ptr] : requests_) {
    VLOG(12) << fmt::format("FileReader destructor delete req: {}",
                            req_ptr->ToString());
    CHECK_EQ(req_ptr->refs, 0);
    req_ptr->ToState(ReadRequestState::kInvalid, TransitionReason::kInvalidate);
    to_delete.push_back(req_ptr.get());
  }

  for (auto* req : to_delete) {
    DeleteReadRequest(req);
  }

  if (FLAGS_vfs_print_readahead_stats) {
    LOG(INFO) << fmt::format(
        "FileReader ino: {}, fh: {} done, readahead_stats: {}", ino_, fh_,
        policy_->readahead_stats.ToString());
  }
}

void FileReader::AcquireRef() {
  int64_t orgin = refs_.fetch_add(1);
  VLOG(12) << "FileReader::AcquireRef origin refs=" << orgin;
  CHECK_GE(orgin, 0);
}

void FileReader::ReleaseRef() {
  int64_t orgin = refs_.fetch_sub(1);
  VLOG(12) << "FileReader::ReleaseRef origin refs=" << orgin;
  CHECK_GT(orgin, 0);
  if (orgin == 1) {
    delete this;
  }
}

void FileReader::Close() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (closing_) {
    return;
  }

  VLOG(9) << "FileReader Close called";
  closing_ = true;
}

void FileReader::RunReadRequest(ReadRequest* req) {
  AcquireRef();
  vfs_rreq_in_queue << 1;

  vfs_hub_->GetReadExecutor()->Execute([this, req]() {
    vfs_rreq_in_queue << -1;

    vfs_rreq_inflighting << 1;
    DoReadRequst(req);
    vfs_rreq_inflighting << -1;
  });
}

void FileReader::Invalidate(int64_t offset, int64_t size) {
  std::unique_lock<std::mutex> lock(mutex_);

  std::vector<ReadRequest*> to_delete;

  for (auto& [req_id, req_ptr] : requests_) {
    auto* req = req_ptr.get();
    VLOG(9) << "Invalidate check req " << req->ToString() << " for range ["
            << offset << "," << (offset + size) << ")";

    FileRange frange{.offset = offset, .len = size};

    if (!req->frange.Overlaps(frange)) {
      continue;
    }

    if (req->state == ReadRequestState::kBusy) {
      req->ToState(ReadRequestState::kRefresh, TransitionReason::kInvalidate);
    } else if (req->state == ReadRequestState::kReady) {
      if (req->refs > 0) {
        req->ToState(ReadRequestState::kNew, TransitionReason::kInvalidate);
        RunReadRequest(req);
      } else {
        req->ToState(ReadRequestState::kInvalid, TransitionReason::kInvalidate);
        to_delete.push_back(req);
      }
    }

  }  // end for

  for (auto* req : to_delete) {
    DeleteReadRequest(req);
  }
}

// split request by block boundary
ReadRequest* FileReader::NewReadRequest(int64_t s, int64_t e) {
  int64_t chunk_indx = s / chunk_size_;
  int64_t chunk_offset = s % chunk_size_;

  int64_t block_end_in_chunk = (chunk_offset / block_size_ + 1) * block_size_;
  int64_t block_end = (chunk_indx * chunk_size_) + block_end_in_chunk;

  int64_t req_end = std::min(e, block_end);

  VLOG(12) << fmt::format(
      "NewReadRequest split: requested=[{}-{}), block_boundary={}, "
      "actual=[{}-{}), len={}",
      s, e, block_end, s, req_end, (req_end - s));

  std::unique_ptr<ReadRequest> req(
      new ReadRequest{.req_id = req_id_gen.fetch_add(1),
                      .ino = ino_,
                      .chunk_index = chunk_indx,
                      .chunk_offset = chunk_offset,
                      .frange = FileRange{.offset = s, .len = (req_end - s)}});

  req->state = ReadRequestState::kNew;
  req->status = Status::OK();
  req->access_sec = butil::monotonic_time_s();
  req->refs = 0;

  auto [it, inserted] = requests_.emplace(req->req_id, std::move(req));
  CHECK(inserted);
  ReadRequest* new_req = it->second.get();
  VLOG(9) << fmt::format("NewReadRequest req: {}", new_req->ToString());

  TakeMem(new_req->frange.len);

  RunReadRequest(new_req);

  return new_req;
}

void FileReader::DeleteReadRequest(ReadRequest* req) {
  VLOG(9) << fmt::format("DeleteReadRequest req: {}", req->ToString());
  CHECK(req->refs == 0);
  CHECK(req->state == ReadRequestState::kInvalid);

  ReleaseMem(req->frange.len);

  CHECK(requests_.erase(req->req_id) == 1);
}

static ChunkReadReq GenChunkReqdReq(ReadRequest* req) {
  return ChunkReadReq{
      .req_id = req->req_id,
      .ino = req->ino,
      .index = req->chunk_index,
      .offset = req->chunk_offset,
      .frange = req->frange,
  };
}

void FileReader::ReadRequstDone(ReadRequest* req) {
  if (closing_) {
    LOG(WARNING) << fmt::format(
        "ReadRequstDone mark req: {} invalid due to closing", req->ToString());
    req->ToState(ReadRequestState::kInvalid, TransitionReason::kReadDone);
  }

  switch (req->state) {
    case ReadRequestState::kNew:
      RunReadRequest(req);
      break;
    case ReadRequestState::kReady:
      req->cv.notify_all();
      break;
    case ReadRequestState::kInvalid:
      if (req->refs == 0) {
        DeleteReadRequest(req);
      } else {
        // it's user's responsibility to clean up
        req->cv.notify_all();
      }
      break;
    default:
      LOG(FATAL) << fmt::format("ReadRequstDone invalid state req: {}",
                                req->ToString());
  }
}

void FileReader::OnReadRequestComplete(ChunkReader* reader, ReadRequest* req,
                                       Status s) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!s.ok()) {
      LOG(WARNING) << fmt::format("Failed read read_req: {}, status: {}",
                                  req->ToString(), s.ToString());
    }

    CHECK(req->state == ReadRequestState::kBusy ||
          req->state == ReadRequestState::kRefresh);

    if (req->state == ReadRequestState::kRefresh) {
      // if state is kRefresh, continue read even previous read failed
      req->ToState(ReadRequestState::kNew, TransitionReason::kReadDone);
      req->status = Status::OK();
    } else {
      req->status = s;

      if (req->status.ok()) {
        req->buffer = reader->GetDataBuffer();
        req->access_sec = butil::monotonic_time_s();
        req->ToState(ReadRequestState::kReady, TransitionReason::kReadDone);
      } else {
        LOG(ERROR) << fmt::format(
            "Invalid read_req: {} due to read fail, status: {}",
            req->ToString(), req->status.ToString());
        req->ToState(ReadRequestState::kInvalid, TransitionReason::kReadDone);
      }
    }

    ReadRequstDone(req);
  }

  // must out of lock to avoid deadlock
  ReleaseRef();
}

void FileReader::DoReadRequst(ReadRequest* req) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    VLOG(9) << fmt::format("DoReadRequst start req: {}", req->ToString());
    CHECK(req->state == ReadRequestState::kNew);
    req->state = ReadRequestState::kBusy;
  }

  // NOTE: becareful here, because related param is const, so we can do this
  // out of lock
  ChunkReadReq chunk_req = GenChunkReqdReq(req);

  auto span =
      vfs_hub_->GetTracer()->StartSpan(kVFSWrapperMoudule, METHOD_NAME());
  ContextSPtr span_ctx = span->GetContext();

  auto* reader = new ChunkReader(vfs_hub_, fh_, chunk_req);
  reader->ReadAsync(span_ctx,
                    [this, reader, req, span_ptr = span.release()](Status s) {
                      std::unique_ptr<ITraceSpan> spoped_span(span_ptr);
                      this->OnReadRequestComplete(reader, req, s);
                      delete reader;
                    });
}

void FileReader::TakeMem(int64_t size) {
  vfs_hub_->GetReadBufferManager()->Take(size);
}

void FileReader::ReleaseMem(int64_t size) {
  vfs_hub_->GetReadBufferManager()->Release(size);
}

int64_t FileReader::TotalMem() const {
  return vfs_hub_->GetReadBufferManager()->GetTotalBytes();
}

int64_t FileReader::UsedMem() const {
  return vfs_hub_->GetReadBufferManager()->GetUsedBytes();
}

bool FileReader::IsProtectedReq(ReadRequest* req) const {
  int64_t readahead = policy_->ReadaheadSize();
  int64_t bt = std::max(readahead / 8, (int64_t)block_size_);

  int64_t s = policy_->last_offset >= bt ? policy_->last_offset - bt : 0;
  int64_t e = policy_->last_offset + readahead;

  VLOG(9) << "IsUsefulReq check req " << req->ToString()
          << " policy: " << policy_->ToString() << ", [" << s << "," << e
          << ")";

  if (s >= e) {
    return false;
  }

  FileRange frange{.offset = s, .len = e - s};
  if (req->frange.Overlaps(frange)) {
    return true;
  }

  return false;
};

void FileReader::MakeReadahead(ContextSPtr ctx, const FileRange& frange) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);
  VLOG(9) << "MakeReadahead: input frange=" << frange.ToString();
  CHECK_GT(frange.len, 0);

  if (UsedMem() > TotalMem()) {
    LOG(INFO) << "MakeReadahead skipped due to buffer full, used=" << UsedMem()
              << " max=" << TotalMem();
    return;
  }

  FileRange ahead = frange;

  for (auto it = requests_.rbegin(); it != requests_.rend(); ++it) {
    int64_t req_id = it->first;
    ReadRequest* req = it->second.get();

    VLOG(9) << "MakeReadahead check req " << req->ToString() << " for frange "
            << ahead.ToString();

    if (req->state == ReadRequestState::kInvalid) {
      continue;
    }

    if (req->frange.offset <= ahead.offset &&
        req->frange.End() > ahead.offset) {
      if (req->frange.End() < ahead.End()) {
        // ahead:              |--------|
        // or existing req:  |---|
        // NOTE: sequence is important here
        ahead.len = ahead.End() - req->frange.End();
        ahead.offset = req->frange.End();
      } else {
        // ahead:              |--------|
        // existing req:     |------------|
        ahead.len = 0;
        break;
      }
    } else {
      // has overlap
      // ahead:              |--------|
      // or existing req:       |---|

      // ahead:              |--------|
      // or existing req:            |----|
      // has overlap
      if (!(ahead.offset >= req->frange.End() ||
            ahead.End() <= req->frange.offset)) {
        ahead.len = 0;
        break;
      }

      // others cases, no overlap ,readahead frange
      // ahead:              |--------|
      // or existing req:|-|

      // ahead:              |--------|
      // or existing req:                 |-|
    }
  };

  VLOG(9) << fmt::format("MakeReadahead: final_ahead: {}, origin_ahead: {}",
                         ahead.ToString(), frange.ToString());

  if (ahead.len > 0) {
    int64_t s = ahead.offset;
    int64_t e = ahead.End();

    while (s < e) {
      VLOG(9) << fmt::format(
          "MakeReadahead create new req for range [{},{}), len: {}", s, e,
          (e - s));
      auto* req = NewReadRequest(s, e);

      s = req->frange.End();
    }
  }
}

void FileReader::CheckReadahead(ContextSPtr ctx, const FileRange& frange,
                                int64_t flen) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  VLOG(9) << "CheckReadahead frange: " << frange.ToString()
          << ", policy: " << policy_->ToString() << ", flen: " << flen;
  CHECK_GT(flen, frange.offset);

  int64_t read_buffer_used = UsedMem();
  int64_t max_read_buffer_size = TotalMem();
  policy_->UpdateOnRead(frange, read_buffer_used, max_read_buffer_size);

  if (policy_->level > 0) {
    int64_t s = frange.End();
    int64_t ahead_size = policy_->ReadaheadSize();
    FileRange ahead = {.offset = s, .len = ahead_size};

    if (ahead.End() > flen) {
      ahead.len = flen - ahead.offset;
    }

    VLOG(9) << "CheckReadahead try make readahead: " << ahead.ToString()
            << ", for frange: " << frange.ToString() << ", flen=" << flen
            << ", cal_ahead_size: " << ahead_size
            << ", policy: " << policy_->ToString();

    if (ahead.len > 0) {
      MakeReadahead(span->GetContext(), ahead);
    }
  }

  policy_->last_offset = std::max(frange.End(), policy_->last_offset);
}

std::vector<int64_t> FileReader::SplitRange(ContextSPtr ctx,
                                            const FileRange& frange) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  std::vector<int64_t> ranges;
  ranges.push_back(frange.offset);
  ranges.push_back(frange.End());

  auto contains = [&ranges](uint64_t point) -> bool {
    return boost::range::find(ranges, point) != ranges.end();
  };

  for (const auto& [uuid, req] : requests_) {
    VLOG(9) << "SplitRange check req " << req->ToString() << " for frange "
            << frange.ToString();

    if (req->state == ReadRequestState::kInvalid) {
      continue;
    }

    if (frange.Contains(req->frange.offset) && !contains(req->frange.offset)) {
      ranges.push_back(req->frange.offset);
    }

    uint64_t req_end = req->frange.End();
    if (frange.Contains(req_end) && !contains(req_end)) {
      ranges.push_back(req_end);
    }
  }

  boost::range::sort(ranges);
  return ranges;
};

std::vector<PartialReadRequest> FileReader::PrepareRequests(
    ContextSPtr ctx, const std::vector<int64_t>& ranges) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);
  std::vector<PartialReadRequest> read_reqs;

  int64_t edges = ranges.size();
  for (int64_t i = 0; i < edges - 1; ++i) {
    CHECK(ranges[i] < ranges[i + 1])
        << " invalid range edges: " << ranges[i] << " >= " << ranges[i + 1];
    bool added = false;
    int64_t s = ranges[i];
    int64_t e = ranges[i + 1];

    for (const auto& [uuid, req] : requests_) {
      VLOG(9) << "PrepareRequests check req " << req->ToString()
              << " for range [" << s << "," << e << ")";

      if (req->frange.offset <= s && req->frange.End() >= e) {
        read_reqs.emplace_back(PartialReadRequest{
            .req = req.get(), .offset = s - req->frange.offset, .len = e - s});
        req->access_sec = butil::monotonic_time_s();
        req->AcquireRef();
        added = true;
        break;
      }
    }

    if (!added) {
      while (s < e) {
        VLOG(9) << "PrepareRequests create new req for range [" << s << "," << e
                << "), len: " << (e - s);
        auto* req = NewReadRequest(s, e);

        read_reqs.emplace_back(PartialReadRequest{
            .req = req, .offset = 0, .len = req->frange.len});
        req->AcquireRef();

        s = req->frange.End();
      }
    }
  }

  return read_reqs;
};

void FileReader::CleanUpRequest(ContextSPtr ctx, const FileRange& frange) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  const uint64_t now = butil::monotonic_time_s();
  uint32_t req_num = requests_.size();

  auto can_remove = [&req_num, now, this](ReadRequest* req) -> bool {
    if (req->access_sec + kReqValidityTimeoutS < now) {
      VLOG(12) << "CleanUpRequest remove timeout req: " << req->ToString()
               << " req_num: " << req_num;
      return true;
    }

    if (req_num > kMaxReadRequests && !IsProtectedReq(req)) {
      VLOG(12) << "CleanUpRequest remove useless req: " << req->ToString()
               << " req_num: " << req_num;
      return true;
    }

    return false;
  };

  // TODO: if req.offset ia large than the file
  // length, can delete directly
  auto should_delete = [&](ReadRequest* req) -> bool {
    if (req->state == ReadRequestState::kInvalid) {
      // when invalid, if no one holds ref, can delete directly
      // if some one holds ref, it's holder's responsibility to clean up later
      return req->refs == 0;
    }

    if (frange.Overlaps(req->frange)) {
      return false;
    }

    if (!can_remove(req)) {
      return false;
    }

    if (req->state == ReadRequestState::kReady && req->refs == 0) {
      req->ToState(ReadRequestState::kInvalid, TransitionReason::kCleanUp);
      return true;
    }

    return false;
  };

  std::vector<ReadRequest*> to_delete;

  for (auto& [req_id, req_ptr] : requests_) {
    auto* req = req_ptr.get();
    VLOG(9) << "CleanUpRequest check req " << req->ToString();

    if (should_delete(req)) {
      to_delete.push_back(req);
      req_num--;
    }
  }

  for (auto* req : to_delete) {
    DeleteReadRequest(req);
  }
}

void FileReader::CheckPrefetch(ContextSPtr ctx, const Attr& attr,
                               const FileRange& frange) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  uint64_t time_now = butil::monotonic_time_s();
  if (FLAGS_vfs_intime_warmup_enable &&
      ((time_now - last_intime_warmup_trigger_) >
           FLAGS_vfs_warmup_trigger_restart_interval_secs ||
       (attr.mtime - last_intime_warmup_mtime_) >
           FLAGS_vfs_warmup_mtime_restart_interval_secs)) {
    LOG(INFO) << "Trigger intime warmup for ino: " << ino_ << ".";
    last_intime_warmup_trigger_ = time_now;
    last_intime_warmup_mtime_ = attr.mtime;

    vfs_hub_->GetWarmupManager()->SubmitTask(WarmupTaskContext{ino_});
  }

  // Prefetch blocks if enabled
  if (FLAGS_vfs_prefetch_blocks > 0 &&
      vfs_hub_->GetBlockStore()->EnableCache()) {
    vfs_hub_->GetPrefetchManager()->SubmitTask(PrefetchContext{
        ino_, frange.offset, attr.length, FLAGS_vfs_prefetch_blocks});
  }
}

Status FileReader::Read(ContextSPtr ctx, DataBuffer* data_buffer, int64_t size,
                        int64_t offset, uint64_t* out_rsize) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  Attr attr;
  DINGOFS_RETURN_NOT_OK(GetAttr(span->GetContext(), &attr));

  if (offset >= attr.length || size == 0) {
    *out_rsize = 0;
    return Status::OK();
  }

  FileRange frange{.offset = offset, .len = size};

  if (frange.End() > attr.length) {
    frange.len = attr.length - frange.offset;
  }

  CheckPrefetch(span->GetContext(), attr, frange);

  int64_t used_mem = UsedMem();
  int64_t total_mem = TotalMem();
  while (used_mem > 2 * total_mem) {
    uint64_t wait_time = (used_mem / total_mem) * 1;
    LOG(INFO) << fmt::format(
        "Read wait due to buffer full, ino: {}, fh: {}, used: {}, total: {}, "
        "wait_time_s: {}",
        ino_, fh_, used_mem, total_mem, wait_time);
    sleep(wait_time);
    used_mem = UsedMem();
  }

  std::unique_lock<std::mutex> lock(mutex_);

  if (closing_) {
    LOG(WARNING) << "Read failed due to closing";
    return Status::Abort("Read aborted due to closing");
  }

  CleanUpRequest(span->GetContext(), frange);

  uint64_t last_bs = 32 << 10;  // 32KB
  if (frange.End() + last_bs > attr.length) {
    FileRange last;
    last.offset = attr.length - last_bs;
    last.len = last_bs;
    if (attr.length < last_bs) {
      last.offset = 0;
      last.len = attr.length;
    }

    VLOG(9) << "Read MakeReadahead for last bs, last: " << last.ToString();
    MakeReadahead(span->GetContext(), last);
  }

  std::vector<int64_t> ranges = SplitRange(span->GetContext(), frange);

  std::vector<PartialReadRequest> reqs =
      PrepareRequests(span->GetContext(), ranges);

  CheckReadahead(span->GetContext(), frange, attr.length);

  SCOPED_CLEANUP({
    auto release_span = vfs_hub_->GetTracer()->StartSpanWithParent(
        kVFSDataMoudule, "FileReader::Read::ReleaseRequests", *span);
    for (auto& partial_req : reqs) {
      partial_req.req->ReleaseRef();
      if (partial_req.req->refs == 0 &&
          partial_req.req->state == ReadRequestState::kInvalid) {
        DeleteReadRequest(partial_req.req);
      }
    }
  });

  uint64_t read_size{0};
  Status ret;

  {
    auto wait_span = vfs_hub_->GetTracer()->StartSpanWithParent(
        kVFSDataMoudule, "FileReader::Read::WaitRequests", *span);

    // TODO: support wait with timeout
    for (PartialReadRequest& partial_req : reqs) {
      VLOG(9) << fmt::format("Read wait req: {}", partial_req.ToString());

      while (partial_req.req->state != ReadRequestState::kReady &&
             partial_req.req->state != ReadRequestState::kInvalid) {
        partial_req.req->cv.wait(lock);
      }

      if (closing_) {
        LOG(WARNING) << "Read aborted due to closing";
        ret = Status::Abort("Read aborted due to closing");
        break;
      }

      if (partial_req.req->state == ReadRequestState::kInvalid) {
        LOG(ERROR) << fmt::format("Read failed req: {}, status: {}",
                                  partial_req.req->ToString(),
                                  partial_req.req->status.ToString());
        CHECK(!partial_req.req->status.ok());
        ret = partial_req.req->status;
        break;
      }

      CHECK(partial_req.req->state == ReadRequestState::kReady);
      size_t ret = partial_req.req->buffer.AppendTo(
          data_buffer->RawIOBuffer(), partial_req.len, partial_req.offset);
      if (ret != partial_req.len) {
        LOG(FATAL) << fmt::format(
            "Read buffer append failed req: {}, expected len: {}, actual len: "
            "{}",
            partial_req.req->ToString(), partial_req.len, size);
      }

      read_size += ret;
    }
  }

  *out_rsize = read_size;
  return ret;
}

Status FileReader::GetAttr(ContextSPtr ctx, Attr* attr) {
  auto span = vfs_hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                          METHOD_NAME(), ctx);

  Status s = vfs_hub_->GetMetaSystem()->GetAttr(span->GetContext(), ino_, attr);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "FileReader::GetAttr failed, ino: {}, status: {}", ino_, s.ToString());
  }

  return s;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs