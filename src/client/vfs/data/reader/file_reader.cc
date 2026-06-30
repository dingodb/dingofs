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
#include <bvar/latency_recorder.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>

#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/chunk_reader.h"
#include "client/vfs/data_buffer.h"
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

// Read-mempool backpressure observability (for tuning the watermark; see
// read-path-readmempool.md §5). All auto-exposed on /vars.
static bvar::Adder<uint64_t> vfs_read_readahead_suppressed_num(
    "vfs_read_readahead_suppressed_num");  // readahead skipped (over watermark)
static bvar::Adder<uint64_t> vfs_read_backpressure_num(
    "vfs_read_backpressure_num");  // foreground reads that hit backpressure
static bvar::LatencyRecorder vfs_read_backpressure_wait(
    "vfs_read_backpressure_wait");  // foreground wait time (us)
static bvar::Adder<uint64_t> vfs_read_backpressure_timeout_num(
    "vfs_read_backpressure_timeout_num");  // wait window expired still over wm

static const uint64_t kReqValidityTimeoutS = 30;
static const uint32_t kMaxReadRequests = 64;

#define METHOD_NAME() ("FileReader::" + std::string(__FUNCTION__))

namespace {

bool CanDeleteRequest(const ReadRequestSptr& req) {
  std::unique_lock<std::mutex> req_lock(req->mutex);
  return req->readers == 0 && req->state == ReadRequestState::kInvalid;
}

};  // namespace

FileReader::FileReader(VFSHub* hub, uint64_t fh, uint64_t ino)
    : vfs_hub_(hub),
      fh_(fh),
      ino_(ino),
      uuid_(fmt::format("file_reader-{}-{}", ino, fh)),
      chunk_size_(hub->GetFsInfo().chunk_size),
      block_size_(hub->GetFsInfo().block_size),
      policy_(new ReadaheadPoclicy(fh)) {}

// when file reader destructor called,
// meas no reader and all background read requests should be done
FileReader::~FileReader() {
  CHECK(closing_.load(std::memory_order_acquire))
      << uuid_ << " FileReader destructor called without Close";

  {
    std::vector<ReadRequestSptr> to_delete;

    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& [req_id, req_ptr] : requests_) {
      {
        std::lock_guard<std::mutex> req_lock(req_ptr->mutex);
        VLOG(12) << fmt::format("{} FileReader destructor delete req: {}",
                                uuid_, req_ptr->ToStringUnlock());
        CHECK_EQ(req_ptr->readers, 0);
        req_ptr->ToStateUnLock(ReadRequestState::kInvalid,
                               TransitionReason::kInvalidate);
      }
      to_delete.push_back(req_ptr);
    }

    for (const auto& req : to_delete) {
      DeleteReadRequestUnlock(req);
    }
  }

  if (FLAGS_vfs_print_readahead_stats) {
    LOG(INFO) << fmt::format("{} FileReader done, readahead_stats: {}", uuid_,
                             policy_->readahead_stats.ToString());
  }
}

Status FileReader::Open() {
  VLOG(9) << fmt::format("{} FileReader opened", uuid_);
  SchedulePeriodicShrink();
  return Status::OK();
}

void FileReader::Close() {
  if (closing_.load(std::memory_order_acquire)) {
    return;
  }
  closing_.store(true, std::memory_order_release);
  VLOG(9) << fmt::format("{} FileReader closed", uuid_);
}

void FileReader::AcquireRef() {
  int64_t orgin = refs_.fetch_add(1);
  VLOG(12) << fmt::format("{} AcquireRef origin refs: {}", uuid_, orgin);
  CHECK_GE(orgin, 0);
}

void FileReader::ReleaseRef() {
  std::string uuid = uuid_;
  int64_t orgin = refs_.fetch_sub(1);
  VLOG(12) << fmt::format("{} ReleaseRef origin refs: {}", uuid, orgin);
  CHECK_GT(orgin, 0);
  if (orgin == 1) {
    delete this;
  }
}

void FileReader::ShrinkMem() {
  int64_t used = UsedMem();
  int64_t total = TotalMem();

  // Only reclaim idle readahead once the pool is under pressure.
  if (UsedRatio() < FLAGS_vfs_read_mempool_readahead_watermark) {
    VLOG(3) << fmt::format(
        "{} ShrinkMem skipped, used: {}, total: {}, ratio < {}", uuid_, used,
        total, FLAGS_vfs_read_mempool_readahead_watermark);
    return;
  }

  int64_t now = butil::monotonic_time_s();
  int64_t idle_sec = 60;
  if (total > 0) {
    idle_sec /= std::max<int64_t>(1, used / total);
  }
  VLOG(9) << fmt::format(
      "{} ShrinkMem start, used: {}, total: {}, idle_sec: {}, now: {}", uuid_,
      used, total, idle_sec, now);

  std::vector<ReadRequestSptr> to_delete;

  std::unique_lock<std::mutex> lock(mutex_);
  for (auto& [req_id, req] : requests_) {
    std::unique_lock<std::mutex> req_lock(req->mutex);
    VLOG(9) << fmt::format("{} ShrinkMem check req: {}", uuid_,
                           req->ToStringUnlock());

    // We can only delete if no one is reading it
    if (req->readers > 0) {
      continue;
    }

    // TODO: support delete new/busy requests
    bool should_drop = false;
    if (req->state == ReadRequestState::kInvalid) {
      should_drop = true;
    } else if (req->state == ReadRequestState::kReady) {
      if (req->access_sec + idle_sec < now) {
        should_drop = true;
      } else if (!IsProtectedReq(req)) {
        should_drop = true;
      }
    }

    if (should_drop) {
      if (req->state == ReadRequestState::kReady) {
        req->ToStateUnLock(ReadRequestState::kInvalid,
                           TransitionReason::kCleanUp);
      }
      to_delete.push_back(req);
    }
  }

  for (const auto& req : to_delete) {
    DeleteReadRequestUnlock(req);
  }
}

void FileReader::SchedulePeriodicShrink() {
  if (closing_.load(std::memory_order_acquire)) {
    LOG(INFO) << fmt::format("{} SchedulePeriodicShrink skipped because closed",
                             uuid_);
    return;
  }

  AcquireRef();
  vfs_hub_->GetReadCleanupExecutor()->Schedule(
      [this] {
        RunPeriodicShrink();
        ReleaseRef();
      },
      FLAGS_vfs_periodic_flush_interval_ms);
}

void FileReader::RunPeriodicShrink() {
  if (closing_.load(std::memory_order_acquire)) {
    LOG(INFO) << fmt::format("{} RunPeriodicShrink skipped because closed",
                             uuid_);
    return;
  }

  ShrinkMem();

  SchedulePeriodicShrink();
}

void FileReader::Invalidate(int64_t offset, int64_t size) {
  VLOG(9) << fmt::format("{} Invalidate for range [{}-{}))", uuid_, offset,
                         offset + size);
  std::vector<ReadRequestSptr> to_delete;

  std::unique_lock<std::mutex> lock(mutex_);

  for (auto& [req_id, req] : requests_) {
    VLOG(9) << fmt::format("{} Invalidate check req: {}", uuid_,
                           req->ToString());

    FileRange frange{.offset = offset, .len = size};

    if (!req->Overlaps(frange)) {
      continue;
    }

    {
      std::unique_lock<std::mutex> req_lock(req->mutex);
      if (req->state == ReadRequestState::kBusy) {
        req->ToStateUnLock(ReadRequestState::kRefresh,
                           TransitionReason::kInvalidate);
      } else if (req->state == ReadRequestState::kReady) {
        if (req->readers > 0) {
          req->ToStateUnLock(ReadRequestState::kNew,
                             TransitionReason::kInvalidate);
          RunReadRequest(req);
        } else {
          req->ToStateUnLock(ReadRequestState::kInvalid,
                             TransitionReason::kInvalidate);
          to_delete.push_back(req);
        }
      }
    }
  }  // end for

  for (const auto& req : to_delete) {
    DeleteReadRequestUnlock(req);
  }
}

void FileReader::RunReadRequest(ReadRequestSptr req) {
  AcquireRef();
  vfs_rreq_in_queue << 1;

  vfs_hub_->GetReadExecutor()->Execute([this, req]() {
    vfs_rreq_in_queue << -1;

    vfs_rreq_inflighting << 1;
    DoReadRequst(req);
    vfs_rreq_inflighting << -1;

    ReleaseRef();
  });
}

// split request by block boundary
ReadRequestSptr FileReader::NewReadRequest(int64_t s, int64_t e) {
  int64_t chunk_indx = s / chunk_size_;
  int64_t chunk_offset = s % chunk_size_;

  int64_t block_end_in_chunk = ((chunk_offset / block_size_) + 1) * block_size_;
  int64_t block_end = (chunk_indx * chunk_size_) + block_end_in_chunk;

  int64_t req_end = std::min(e, block_end);

  VLOG(12) << fmt::format(
      "{} NewReadRequest split: requested=[{}-{}), block_boundary={}, "
      "actual=[{}-{}), len={}",
      uuid_, s, e, block_end, s, req_end, (req_end - s));

  std::shared_ptr<ReadRequest> req(
      new ReadRequest(ino_, chunk_indx, chunk_offset,
                      FileRange{.offset = s, .len = (req_end - s)}));

  req->state = ReadRequestState::kNew;
  req->status = Status::OK();
  req->access_sec = butil::monotonic_time_s();
  req->readers = 0;

  auto [it, inserted] = requests_.emplace(req->ReqId(), std::move(req));
  CHECK(inserted);
  ReadRequestSptr new_req = it->second;
  VLOG(9) << fmt::format("{} NewReadRequest req: {}", uuid_,
                         new_req->ToString());

  // Allocate one contiguous pool slot for the whole request; lower layers fill
  // it in place (dst base + offset). buffer holds it via AppendUserData;
  // the slot returns to the pool when buffer's IOBuf block refcount hits zero.
  {
    size_t len = static_cast<size_t>(new_req->req.frange.len);
    ReadMemPool* pool = vfs_hub_->GetReadMemPool();
    ReadBuf rb = pool->Allocate(len);
    if (rb) {
      new_req->dst = ReadBufView{rb.data(), 0, len};
      new_req->buffer.AppendUserData(rb.Disown(), len, pool->Deleter());
    } else {
      // exhausted even after the pool's internal reclaim; leave dst empty
      // and let DoReadRequst fail the request with ENOMEM (Step 3 may upgrade
      // this to a bounded cv-wait). Pool-only: never malloc-fallback.
      LOG(ERROR) << fmt::format("{} read mempool exhausted, len={}", uuid_,
                                len);
    }
  }

  RunReadRequest(new_req);

  return new_req;
}

// NOTE: caller holds req lock. Schedule cleanup outside the completion callback
// path so request erasure runs in the reader cleanup lane.
void FileReader::ScheduleReadRequestCleanup(ReadRequestSptr req) {
  AcquireRef();
  vfs_hub_->GetReadCleanupExecutor()->Execute([&, req]() {
    DeleteReadRequest(req);
    ReleaseRef();
  });
}

void FileReader::DeleteReadRequest(ReadRequestSptr req) {
  std::lock_guard<std::mutex> lock(mutex_);
  DeleteReadRequestUnlock(req);
}

// Caller holds mutex_; req->mutex is not required.
//
// erase() is intentionally idempotent. A request may be reclaimed by several
// uncoordinated paths that all delete on the same (kInvalid && readers==0)
// condition: the scheduled cleanup from ScheduleReadRequestCleanup,
// OnReadRequestComplete, the per-read CleanUpRequest, the periodic ShrinkMem,
// and the foreground Read cleanup. The async path decides to delete while
// holding only req->mutex and erases later under mutex_, so a synchronous path
// can erase the same request in the gap -- erase() then legitimately finds it
// already gone. A CHECK(erase()==1) here SIGABRTs once pool exhaustion turns
// many readahead requests kInvalid at the same time.
void FileReader::DeleteReadRequestUnlock(ReadRequestSptr req) {
  VLOG(9) << fmt::format("{} DeleteReadRequest req: {}", uuid_,
                         req->ToString());
  CHECK(CanDeleteRequest(req));

  // The pool slot is released when req->buffer's IOBuf block refcount hits zero
  // (after the reply), which auto-updates the pool's OutstandingBytes. No
  // explicit release here.

  requests_.erase(req->ReqId());
}

void FileReader::OnReadRequestComplete(ReadRequestSptr req, Status s) {
  std::unique_lock<std::mutex> lock(req->mutex);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("{} Failed read read_req: {}, status: {}",
                                uuid_, req->ToStringUnlock(), s.ToString());
  }

  CHECK(req->state == ReadRequestState::kBusy ||
        req->state == ReadRequestState::kRefresh);

  if (req->state == ReadRequestState::kRefresh) {
    // if state is kRefresh, continue read even previous read failed
    req->ToStateUnLock(ReadRequestState::kNew, TransitionReason::kReadDone);
    req->status = Status::OK();
  } else {
    req->status = s;

    if (req->status.ok()) {
      // buffer is the pool slot allocated in NewReadRequest, already filled in
      // place by the lower layers; nothing to assign here.
      req->access_sec = butil::monotonic_time_s();
      req->ToStateUnLock(ReadRequestState::kReady, TransitionReason::kReadDone);
    } else {
      LOG(ERROR) << fmt::format(
          "{} Invalid read_req: {} due to read fail, status: {}", uuid_,
          req->ToStringUnlock(), req->status.ToString());
      req->ToStateUnLock(ReadRequestState::kInvalid,
                         TransitionReason::kReadDone);
    }
  }

  if (closing_.load(std::memory_order_acquire)) {
    LOG(WARNING) << fmt::format(
        "{} ReadRequstDone mark req: {} invalid due to closing", uuid_,
        req->ToStringUnlock());
    req->ToStateUnLock(ReadRequestState::kInvalid, TransitionReason::kReadDone);
  }

  switch (req->state) {
    case ReadRequestState::kNew:
      RunReadRequest(req);
      break;
    case ReadRequestState::kReady:
      req->cv.notify_all();
      break;
    case ReadRequestState::kInvalid:
      if (req->readers == 0) {
        ScheduleReadRequestCleanup(req);
      } else {
        // it's user's responsibility to clean up
        req->cv.notify_all();
      }
      break;
    default:
      LOG(FATAL) << fmt::format("{} ReadRequstDone invalid state req: {}",
                                uuid_, req->ToStringUnlock());
  }
}

void FileReader::DoReadRequst(ReadRequestSptr req) {
  {
    std::unique_lock<std::mutex> req_lock(req->mutex);
    VLOG(9) << fmt::format("{} DoReadRequst start req: {}", uuid_,
                           req->ToStringUnlock());
    CHECK(req->state == ReadRequestState::kNew);
    req->ToStateUnLock(ReadRequestState::kBusy, TransitionReason::kReading);
  }

  // Pool slot allocation failed in NewReadRequest -> fail this request without
  // reading. ENOMEM (not ENOSPC): the fs isn't full, we transiently lack a read
  // buffer. Pool-only, never malloc-fallback. (Step 3 may upgrade this to a
  // bounded wait + EAGAIN.)
  if (req->dst.base == nullptr) {
    OnReadRequestComplete(req, Status::OutOfMemory("read mempool exhausted"));
    return;
  }

  auto span =
      vfs_hub_->GetTraceManager()->StartSpan("FileReader::DoReadRequst");

  AcquireRef();

  auto* reader = new ChunkReader(vfs_hub_, fh_, req->req);
  reader->ReadAsync(SpanScope::GetContext(span), req->dst,
                    [this, reader, req, span](Status s) {
                      SpanScope::AddEvent(span, "Complete ReadAsync callback");
                      this->OnReadRequestComplete(req, s);
                      delete reader;
                      ReleaseRef();
                    });
}

int64_t FileReader::TotalMem() const {
  return static_cast<int64_t>(vfs_hub_->GetReadMemPool()->TotalSize());
}

// Bytes currently held by live read requests (the pool is the accountant now;
// ReadBufferManager is gone). Equals the sum of in-flight request slots.
int64_t FileReader::UsedMem() const {
  return vfs_hub_->GetReadMemPool()->OutstandingBytes();
}

double FileReader::UsedRatio() const {
  return vfs_hub_->GetReadMemPool()->UsageRatio();
}

bool FileReader::IsProtectedReq(const ReadRequestSptr& req) const {
  if (policy_->level == 0) {
    VLOG(12) << fmt::format(
        "{} IsProtectedReq check req {} policy: {}, not protected due to "
        "policy level 0",
        uuid_, req->ToStringUnlock(), policy_->ToString());
    return false;
  }

  int64_t readahead = policy_->ReadaheadSize();
  int64_t bt = std::max(readahead / 8, (int64_t)block_size_);

  int64_t s = policy_->last_offset >= bt ? policy_->last_offset - bt : 0;
  int64_t e = policy_->last_offset + readahead;

  VLOG(9) << fmt::format(
      "{} IsProtectedReq check req {} policy: {}, range: [{}-{})", uuid_,
      req->ToStringUnlock(), policy_->ToString(), s, e);

  if (s >= e) {
    return false;
  }

  FileRange frange{.offset = s, .len = e - s};
  if (req->Overlaps(frange)) {
    return true;
  }

  return false;
};

void FileReader::MakeReadahead(ContextSPtr ctx, const FileRange& frange) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "FileReader::MakeReadahead", ctx->GetTraceSpan());
  VLOG(9) << fmt::format("{} MakeReadahead, input frange: {}", uuid_,
                         frange.ToString());
  CHECK_GT(frange.len, 0);

  // Readahead is speculative: suppress it once pool usage is above the
  // readahead watermark so the foreground reads keep their headroom.
  if (UsedRatio() > FLAGS_vfs_read_mempool_readahead_watermark) {
    vfs_read_readahead_suppressed_num << 1;
    LOG(INFO) << fmt::format(
        "{} MakeReadahead skipped, pool usage ratio {} > {}", uuid_,
        UsedRatio(), FLAGS_vfs_read_mempool_readahead_watermark);
    return;
  }

  FileRange ahead = frange;

  for (auto it = requests_.rbegin(); it != requests_.rend(); ++it) {
    int64_t req_id = it->first;
    ReadRequest* req = it->second.get();

    VLOG(9) << fmt::format("{} MakeReadahead check req: {} for frange: {}",
                           uuid_, req->ToString(), ahead.ToString());

    if (req->state == ReadRequestState::kInvalid) {
      continue;
    }

    if (req->req.frange.offset <= ahead.offset &&
        req->req.frange.End() > ahead.offset) {
      if (req->req.frange.End() < ahead.End()) {
        // ahead:              |--------|
        // or existing req:  |---|
        // NOTE: sequence is important here
        ahead.len = ahead.End() - req->req.frange.End();
        ahead.offset = req->req.frange.End();
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
      if (!(ahead.offset >= req->req.frange.End() ||
            ahead.End() <= req->req.frange.offset)) {
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

  VLOG(9) << fmt::format("{} MakeReadahead: final_ahead: {}, origin_ahead: {}",
                         uuid_, ahead.ToString(), frange.ToString());

  if (ahead.len > 0) {
    int64_t s = ahead.offset;
    int64_t e = ahead.End();

    while (s < e) {
      VLOG(9) << fmt::format(
          "{} MakeReadahead create new req for range [{},{}), len: {}", uuid_,
          s, e, (e - s));
      auto req = NewReadRequest(s, e);

      s = req->req.frange.End();
    }
  }
}

void FileReader::CheckReadahead(ContextSPtr ctx, const FileRange& frange,
                                int64_t flen) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "FileReader::CheckReadahead", ctx->GetTraceSpan());

  VLOG(9) << fmt::format("{} CheckReadahead frange: {} policy: {}, flen: {}",
                         uuid_, frange.ToString(), policy_->ToString(), flen);
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

    VLOG(9) << fmt::format(
        "{} CheckReadahead try make readahead: {} for frange: {}, flen: {}, "
        "cal_ahead_size: {}, policy: {}",
        uuid_, ahead.ToString(), frange.ToString(), flen, ahead_size,
        policy_->ToString());

    if (ahead.len > 0) {
      MakeReadahead(SpanScope::GetContext(span), ahead);
    }
  }

  policy_->last_offset = frange.End();
}

std::vector<int64_t> FileReader::SplitRange(ContextSPtr ctx,
                                            const FileRange& frange) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "FileReader::SplitRange", ctx->GetTraceSpan());

  std::vector<int64_t> ranges;
  ranges.push_back(frange.offset);
  ranges.push_back(frange.End());

  auto contains = [&ranges](uint64_t point) -> bool {
    return boost::range::find(ranges, point) != ranges.end();
  };

  for (const auto& [uuid, req] : requests_) {
    VLOG(9) << fmt::format("{} SplitRange check req: {} for frange: {}", uuid_,
                           req->ToString(), frange.ToString());

    if (req->state == ReadRequestState::kInvalid) {
      continue;
    }

    if (frange.Contains(req->req.frange.offset) &&
        !contains(req->req.frange.offset)) {
      ranges.push_back(req->req.frange.offset);
    }

    uint64_t req_end = req->req.frange.End();
    if (frange.Contains(req_end) && !contains(req_end)) {
      ranges.push_back(req_end);
    }
  }

  boost::range::sort(ranges);
  return ranges;
};

std::vector<PartialReadRequest> FileReader::PrepareRequests(
    ContextSPtr ctx, const std::vector<int64_t>& ranges) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "FileReader::PrepareRequests", ctx->GetTraceSpan());
  std::vector<PartialReadRequest> read_reqs;

  int64_t edges = ranges.size();
  for (int64_t i = 0; i < edges - 1; ++i) {
    CHECK(ranges[i] < ranges[i + 1])
        << " invalid range edges: " << ranges[i] << " >= " << ranges[i + 1];
    bool added = false;
    int64_t s = ranges[i];
    int64_t e = ranges[i + 1];

    for (const auto& [uuid, req] : requests_) {
      std::unique_lock<std::mutex> req_lock(req->mutex);

      VLOG(9) << fmt::format(
          "{} PrepareRequests check req: {} for range [{}-{}))", uuid_,
          req->ToStringUnlock(), s, e);

      if (req->state == ReadRequestState::kInvalid) {
        continue;
      }

      if (req->req.frange.offset <= s && req->req.frange.End() >= e) {
        read_reqs.emplace_back(PartialReadRequest{
            .req = req, .offset = s - req->req.frange.offset, .len = e - s});
        req->access_sec = butil::monotonic_time_s();
        req->IncReaderUnlock();
        added = true;

        VLOG(9) << fmt::format(
            "{} PrepareRequests reuse existing req: {} for range [{}-{}), "
            "len: {}",
            uuid_, req->UUID(), s, e, (e - s));
        break;
      }
    }  // end for

    if (!added) {
      while (s < e) {
        VLOG(9) << fmt::format(
            "{} PrepareRequests create new req for range [{}-{}), len: {}",
            uuid_, s, e, (e - s));
        auto req = NewReadRequest(s, e);

        read_reqs.emplace_back(PartialReadRequest{
            .req = req, .offset = 0, .len = req->req.frange.len});
        req->IncReader();

        s = req->req.frange.End();
      }
    }
  }

  return read_reqs;
};

void FileReader::CleanUpRequest(ContextSPtr ctx, const FileRange& frange) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "FileReader::CleanUpRequest", ctx->GetTraceSpan());

  const uint64_t now = butil::monotonic_time_s();
  uint32_t req_num = requests_.size();

  auto can_remove = [&req_num, now, this](const ReadRequestSptr& req) -> bool {
    if (req->access_sec + kReqValidityTimeoutS < now) {
      VLOG(12) << fmt::format(
          "{} CleanUpRequest remove timeout req: {} req_num: {}", uuid_,
          req->ToStringUnlock(), req_num);
      return true;
    }

    if (req_num > kMaxReadRequests && !IsProtectedReq(req)) {
      VLOG(12) << fmt::format(
          "{} CleanUpRequest remove useless req: {} req_num: {}", uuid_,
          req->ToStringUnlock(), req_num);
      return true;
    }

    return false;
  };

  // TODO: if req.offset ia large than the file
  // length, can delete directly
  auto should_delete = [&](const ReadRequestSptr& req) -> bool {
    std::unique_lock<std::mutex> req_lock(req->mutex);

    if (req->state == ReadRequestState::kInvalid) {
      // when invalid, if no one holds ref, can delete directly
      // if some one holds ref, it's holder's responsibility to clean up later
      return req->readers == 0;
    }

    if (frange.Overlaps(req->req.frange)) {
      return false;
    }

    if (!can_remove(req)) {
      return false;
    }

    if (req->state == ReadRequestState::kReady && req->readers == 0) {
      req->ToStateUnLock(ReadRequestState::kInvalid,
                         TransitionReason::kCleanUp);
      return true;
    }

    return false;
  };

  std::vector<ReadRequestSptr> to_delete;

  for (auto& [req_id, req] : requests_) {
    VLOG(9) << fmt::format("{} CleanUpRequest check req: {}", uuid_,
                           req->ToString());
    if (should_delete(req)) {
      to_delete.push_back(req);
      req_num--;
    }
  }

  for (const auto& req : to_delete) {
    DeleteReadRequestUnlock(req);
  }
}

void FileReader::CheckPrefetch(ContextSPtr ctx, const Attr& attr,
                               const FileRange& frange) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan(
      "FileReader::CheckPrefetch", ctx->GetTraceSpan());

  uint64_t time_now = butil::monotonic_time_s();
  if (FLAGS_vfs_intime_warmup_enable &&
      ((time_now - last_intime_warmup_trigger_) >
           FLAGS_vfs_warmup_trigger_restart_interval_secs ||
       (attr.mtime - last_intime_warmup_mtime_) >
           FLAGS_vfs_warmup_mtime_restart_interval_secs)) {
    LOG(INFO) << fmt::format("{} Trigger intime warmup", uuid_);
    last_intime_warmup_trigger_ = time_now;
    last_intime_warmup_mtime_ = attr.mtime;

    vfs_hub_->GetWarmupManager()->SubmitTask(WarmupTaskContext{ino_});
  }

  // Prefetch blocks if enabled
  if (FLAGS_vfs_prefetch_blocks > 0 &&
      vfs_hub_->GetBlockStore()->EnableCache()) {
    CHECK(frange.offset >= 0);
    vfs_hub_->GetPrefetchManager()->SubmitTask(
        PrefetchContext{ino_, frange.offset, static_cast<int64_t>(attr.length),
                        FLAGS_vfs_prefetch_blocks});
  }
}

Status FileReader::Read(ContextSPtr ctx, DataBuffer* data_buffer, int64_t size,
                        int64_t offset, uint64_t* out_rsize) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("FileReader::Read",
                                                          ctx->GetTraceSpan());

  Attr attr;
  DINGOFS_RETURN_NOT_OK(GetAttr(SpanScope::GetContext(span), &attr));

  if (offset >= attr.length || size == 0) {
    *out_rsize = 0;
    return Status::OK();
  }

  FileRange frange{.offset = offset, .len = size};

  if (frange.End() > attr.length) {
    frange.len = attr.length - frange.offset;
  }

  VLOG(6) << fmt::format("{} FileReader::Read frange: {} ", uuid_,
                         frange.ToString());

  CheckPrefetch(SpanScope::GetContext(span), attr, frange);

  // Foreground backpressure: above the backpressure watermark, wait a bounded
  // window for in-flight reads to release their slots (and for the periodic
  // RunPeriodicShrink to reclaim idle readahead) before proceeding. Reclaim is
  // intentionally NOT driven from the read path -- it runs on its own timer.
  // The per-request pool Allocate still hard-fails -> ENOMEM if truly exhausted
  // (pool-only, no malloc). Step-3 follow-up may turn this into a bounded
  // cv-wait.
  if (UsedRatio() > FLAGS_vfs_read_mempool_backpressure_watermark) {
    vfs_read_backpressure_num << 1;
    int64_t wait_start_us = butil::gettimeofday_us();
    for (int waited_ms = 0;
         UsedRatio() > FLAGS_vfs_read_mempool_backpressure_watermark &&
         waited_ms < 2000 && !closing_.load(std::memory_order_acquire);
         waited_ms += 10) {
      usleep(10 * 1000);
    }
    vfs_read_backpressure_wait << (butil::gettimeofday_us() - wait_start_us);
    if (UsedRatio() > FLAGS_vfs_read_mempool_backpressure_watermark) {
      vfs_read_backpressure_timeout_num << 1;  // wait didn't relieve pressure
    }
  }

  if (closing_.load(std::memory_order_acquire)) {
    LOG(WARNING) << fmt::format("{} Read failed due to closing", uuid_);
    return Status::Abort("Read aborted due to closing");
  }

  std::vector<PartialReadRequest> reqs;
  {
    auto release_span = vfs_hub_->GetTraceManager()->StartChildSpan(
        "FileReader::Read::PreProcess", span);
    std::unique_lock<std::mutex> lock(mutex_);

    CleanUpRequest(SpanScope::GetContext(span), frange);

    uint64_t last_bs = 32 << 10;  // 32KB
    if (frange.End() + last_bs > attr.length) {
      FileRange last;
      last.offset = attr.length - last_bs;
      last.len = last_bs;
      if (attr.length < last_bs) {
        last.offset = 0;
        last.len = attr.length;
      }

      VLOG(9) << fmt::format(
          "{} Read MakeReadahead for last bs, last: {}, attr.length: {}", uuid_,
          last.ToString(), attr.length);
      MakeReadahead(SpanScope::GetContext(span), last);
    }

    std::vector<int64_t> ranges =
        SplitRange(SpanScope::GetContext(span), frange);

    reqs = PrepareRequests(SpanScope::GetContext(span), ranges);

    CheckReadahead(SpanScope::GetContext(span), frange, attr.length);
  }

  SCOPED_CLEANUP({
    auto release_span = vfs_hub_->GetTraceManager()->StartChildSpan(
        "FileReader::Read::ReleaseRequests", span);

    std::unique_lock<std::mutex> lock(mutex_);

    for (auto& partial_req : reqs) {
      partial_req.req->DecReader();

      if (CanDeleteRequest(partial_req.req)) {
        DeleteReadRequestUnlock(partial_req.req);
      }
    }
  });

  uint64_t read_size{0};
  Status ret;

  {
    auto wait_span = vfs_hub_->GetTraceManager()->StartChildSpan(
        "FileReader::Read::WaitRequests", span);

    // TODO: support wait with timeout
    for (PartialReadRequest& partial_req : reqs) {
      VLOG(9) << fmt::format("{} Read wait req: {}", uuid_,
                             partial_req.ToString());

      std::unique_lock<std::mutex> req_lock(partial_req.req->mutex);

      while (partial_req.req->state != ReadRequestState::kReady &&
             partial_req.req->state != ReadRequestState::kInvalid) {
        partial_req.req->cv.wait(req_lock);
      }

      if (closing_.load(std::memory_order_acquire)) {
        LOG(WARNING) << fmt::format("{} Read aborted due to closing", uuid_);
        ret = Status::Abort("Read aborted due to closing");
        break;
      }

      if (partial_req.req->state == ReadRequestState::kInvalid) {
        LOG(ERROR) << fmt::format("{} Read failed req: {}", uuid_,
                                  partial_req.req->ToStringUnlock());
        CHECK(!partial_req.req->status.ok());
        ret = partial_req.req->status;
        break;
      }

      CHECK(partial_req.req->state == ReadRequestState::kReady);
      size_t ret = partial_req.req->buffer.AppendTo(
          data_buffer->RawIOBuffer(), partial_req.len, partial_req.offset);
      if (ret != partial_req.len) {
        LOG(FATAL) << fmt::format(
            "{} Read buffer append failed partial_req: {}, expected len: {}, "
            "actual len: {}, original buffer:{}, rawbuffer:{}",
            uuid_, partial_req.ToStringUnlock(), partial_req.len, ret,
            partial_req.req->buffer.Describe(),
            data_buffer->RawIOBuffer()->Describe());
      }

      read_size += ret;
    }
  }

  *out_rsize = read_size;
  return ret;
}

Status FileReader::GetAttr(ContextSPtr ctx, Attr* attr) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("FileWriter::GetAttr",
                                                          ctx->GetTraceSpan());
  Status s = vfs_hub_->GetMetaSystem()->GetAttr(SpanScope::GetContext(span),
                                                ino_, attr);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("{} GetAttr failed, status: {}", uuid_,
                                s.ToString());
  }

  return s;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
