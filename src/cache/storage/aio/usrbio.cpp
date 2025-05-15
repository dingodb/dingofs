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
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/aio/usrbio.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <sys/epoll.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <queue>

#include "cache/common/types.h"
#include "cache/storage/aio/aio.h"
#include "cache/storage/aio/usrbio_api.h"
#include "cache/storage/buffer.h"
#include "cache/utils/helper.h"
#include "common/status.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace storage {

using dingofs::utils::ReadLockGuard;
using dingofs::utils::WriteLockGuard;

USRBIO::USRBIO(const std::string& mountpoint, uint32_t blksize,
               uint32_t io_depth_, bool for_read)
    : running_(false),
      mountpoint_(mountpoint),
      blksize_(blksize),
      io_depth_(io_depth_),
      for_read_(for_read),
      ior_(),
      iov_(),
      cqes_(nullptr),
      openfiles_(std::make_unique<Openfiles>()) {}

uint32_t USRBIO::GetIODepth() { return io_depth_; }

Status USRBIO::Init() {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  std::string real_mountpoint;
  Status status = hf3fs::extract_mount_point(mountpoint_, &real_mountpoint);
  if (!status.ok()) {
    LOG(ERROR) << "The mountpoint(" << mountpoint_
               << ") is not format with 3fs filesystem.";
    return status;
  }
  mountpoint_ = real_mountpoint;

  // create read io ring
  status = hf3fs::iorcreate(&ior_, mountpoint_, io_depth_, for_read_);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs write io ring failed: status = "
               << status.ToString();
    return status;
  }

  // create shared memory
  status = hf3fs::iovcreate(&iov_, mountpoint_, io_depth_ * blksize_);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs shared memory failed: status = "
               << status.ToString();
    return status;
  }

  cqes_ = new hf3fs::cqe[io_depth_];
  iov_buffer_ = std::make_unique<IOVBuffer>(&iov_);
  // TODO(Wine93): add option for pool total size when for_read is true
  iov_buffer_->Init(blksize_, io_depth_ * 2);
  return Status::OK();
}

Status USRBIO::Shutdown() {
  if (running_.exchange(false)) {
    hf3fs::iordestroy(&ior_);
    delete[] cqes_;
  }
  return Status::OK();
}

Status USRBIO::PrepareIO(AioClosure* aio) {
  CHECK_EQ(aio->IsRead(), for_read_);

  Status status =
      openfiles_->Open(aio->Fd(), [&](int fd) { return hf3fs::reg_fd(fd); });
  if (!status.ok()) {
    return status;
  }

  char* buffer = iov_buffer_->GetBlock();
  status = hf3fs::prep_io(&ior_, &iov_, for_read_, buffer, aio->Fd(),
                          aio->Offset(), aio->Length(), aio);
  if (!status.ok()) {
    iov_buffer_->ReleaseBlock(buffer);
    openfiles_->Close(aio->Fd(), [&](int fd) { hf3fs::dereg_fd(fd); });
    return status;
  }

  aio->SetCtx(buffer);
  return Status::OK();
}

Status USRBIO::SubmitIO() { return hf3fs::submit_ios(&ior_); }

Status USRBIO::WaitIO(uint32_t timeout_ms, Aios* aios) {
  aios->clear();

  int n = hf3fs::wait_for_ios(&ior_, cqes_, io_depth_, timeout_ms);
  if (n < 0) {
    return Status::Internal("hf3fs_wait_for_ios() failed");
  }

  for (int i = 0; i < n; i++) {
    auto* aio = (AioClosure*)(cqes_[i].userdata);
    int retcode = cqes_[i].result >= 0 ? 0 : -1;
    OnCompleted(aio, retcode);
    aios->emplace_back(aio);
  }
  return Status::OK();
}

void USRBIO::OnCompleted(AioClosure* aio, int retcode) {
  auto status = (retcode == 0) ? Status::OK() : Status::IoError("aio failed");
  aio->status() = status;
  openfiles_->Close(aio->Fd(), [&](int fd) { hf3fs::dereg_fd(fd); });

  auto* ctx = aio->GetCtx();
  CHECK_NOTNULL(ctx);
  char* buffer = static_cast<char*>(ctx);
  if (!for_read_ || !status.ok()) {  // write or read failed
    iov_buffer_->ReleaseBlock(buffer);
    return;
  }

  butil::IOBuf iobuf;
  iobuf.append_user_data(buffer, aio->Length(), [this](void* data) {
    iov_buffer_->ReleaseBlock((char*)data);
  });
  *aio->Buffer() = IOBuffer(iobuf);
}

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
