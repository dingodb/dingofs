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

#include "client/vfs/data/file.h"

#include <glog/logging.h>
#include <unistd.h>

#include <cstdint>
#include <mutex>
#include <utility>

#include "client/common/const.h"
#include "client/vfs/data/common/async_util.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/callback.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/itrace_span.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("File::" + std::string(__FUNCTION__))

File::File(VFSHub* hub, uint64_t fh, int64_t ino)
    : vfs_hub_(hub),
      fh_(fh),
      ino_(ino),
      file_writer_(std::make_unique<FileWriter>(hub, fh_, ino)) {
  file_reader_ = new FileReader(hub, fh_, ino);
  file_reader_->AcquireRef();
}

// TODO: use condition variable to wait
File::~File() {
  VLOG(12) << "File::~File destroyed, ino: " << ino_ << ", fh: " << fh_;
  while (inflight_flush_.load(std::memory_order_relaxed) > 0) {
    LOG(INFO) << "File::~File wait inflight_flush_ to be 0, ino: " << ino_
              << ", inflight_flush: "
              << inflight_flush_.load(std::memory_order_relaxed);
    sleep(1);
  }

  file_reader_->Close();
  file_reader_->ReleaseRef();

  file_writer_.reset();
}

uint64_t File::GetChunkSize() const { return vfs_hub_->GetFsInfo().chunk_size; }

Status File::PreCheck() {
  Status tmp;
  {
    std::lock_guard<std::mutex> lg(mutex_);
    if (!file_status_.ok()) {
      tmp = file_status_;
    }
  }

  if (!tmp.ok()) {
    LOG(WARNING) << "File::PreCheck failed because file already broken, ino: "
                 << ino_ << ", status: " << tmp.ToString();
  }

  return tmp;
}

Status File::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                   uint64_t offset, uint64_t* out_wsize) {
  DINGOFS_RETURN_NOT_OK(PreCheck());
  return file_writer_->Write(ctx, buf, size, offset, out_wsize);
}

Status File::Read(ContextSPtr ctx, DataBuffer* data_buffer, uint64_t size,
                  uint64_t offset, uint64_t* out_rsize) {
  DINGOFS_RETURN_NOT_OK(PreCheck());
  return file_reader_->Read(ctx, data_buffer, size, offset, out_rsize);
}

void File::Invalidate(int64_t offset, int64_t size) {
  file_reader_->Invalidate(offset, size);
}

void File::FileFlushed(StatusCallback cb, Status status) {
  if (!status.ok()) {
    LOG(WARNING) << "File::FileFlushed failed, ino: " << ino_
                 << ", status: " << status.ToString();
    {
      std::lock_guard<std::mutex> lg(mutex_);
      file_status_ = status;
    }
  }

  cb(status);

  inflight_flush_.fetch_sub(1, std::memory_order_relaxed);

  VLOG(3) << "File::FileFlushed end ino: " << ino_
          << ", status: " << status.ToString() << ", inflight_flush: "
          << inflight_flush_.load(std::memory_order_relaxed);
}

void File::AsyncFlush(StatusCallback cb) {
  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSDataMoudule, METHOD_NAME());
  inflight_flush_.fetch_add(1, std::memory_order_relaxed);
  file_writer_->AsyncFlush(
      [this, span_raw_ptr = span.release(), cb](auto&& ph1) {
        std::unique_ptr<ITraceSpan> flush_span(span_raw_ptr);
        FileFlushed(std::move(cb), std::forward<decltype(ph1)>(ph1));
        flush_span->End();
      });
}

void File::Close() {
  file_reader_->Close();
  file_writer_->Close();
}

Status File::Flush() {
  Status s;
  Synchronizer sync;
  AsyncFlush(sync.AsStatusCallBack(s));
  sync.Wait();
  return s;
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs