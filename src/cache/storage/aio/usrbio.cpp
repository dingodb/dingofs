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

#ifdef WITH_LIBUSRBIO

#include "cache/storage/aio/usrbio.h"

namespace dingofs {
namespace cache {

USRBIO::USRBIO(const std::string& mountpoint, uint32_t blksize,
               uint32_t iodepth_, bool for_read)
    : running_(false),
      mountpoint_(mountpoint),
      blksize_(blksize),
      iodepth_(iodepth_),
      for_read_(for_read),
      ior_(),
      iov_(),
      cqes_(nullptr),
      fd_register_(std::make_unique<FdRegister>()) {}

Status USRBIO::Start() {
  if (running_.exchange(true)) {  // already running
    return Status::OK();
  }

  std::string real_mountpoint;
  Status status = USRBIOApi::ExtractMountPoint(mountpoint_, &real_mountpoint);
  if (!status.ok()) {
    LOG(ERROR) << "The mountpoint(" << mountpoint_
               << ") is not format with hf3fs filesystem.";
    return status;
  }
  mountpoint_ = real_mountpoint;

  // create ior
  status = USRBIOApi::IORCreate(&ior_, mountpoint_, iodepth_, for_read_);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs ior failed: status = " << status.ToString();
    return status;
  }

  // create iov
  status = USRBIOApi::IOVCreate(&iov_, mountpoint_, iodepth_ * blksize_);
  if (!status.ok()) {
    LOG(ERROR) << "Create 3fs iov failed: status = " << status.ToString();
    return status;
  }

  cqes_ = new USRBIOApi::Cqe[iodepth_];
  // TODO(Wine93): add option for pool total size when for_read is true
  block_buffer_pool_ = std::make_unique<BlockBufferPool>(
      &iov_, blksize_, for_read_ ? iodepth_ * 2 : iodepth_);
  return Status::OK();
}

Status USRBIO::Shutdown() {
  if (running_.exchange(false)) {
    USRBIOApi::IORDestroy(&ior_);
    USRBIOApi::IOVDestroy(&iov_);
    delete[] cqes_;
  }
  return Status::OK();
}

Status USRBIO::PrepareIO(AioClosure* aio) {
  CHECK_EQ(IsAioRead(aio), for_read_);

  auto status = fd_register_->RegFd(aio->fd);
  if (!status.ok()) {
    return status;
  }

  // FIXME: copy write buffer to shared memory
  char* buffer = block_buffer_pool_->Get();
  status = USRBIOApi::PrepIO(&ior_, &iov_, for_read_, buffer, aio->fd,
                             aio->offset, aio->length, aio);
  if (!status.ok()) {
    block_buffer_pool_->Release(buffer);
    fd_register_->DeRegFd(aio->fd);
    return status;
  }

  aio->ctx = new Context(buffer);
  return Status::OK();
}

Status USRBIO::SubmitIO() { return USRBIOApi::SubmitIOs(&ior_); }

Status USRBIO::WaitIO(uint64_t timeout_ms, std::vector<AioClosure*>* aios) {
  aios->clear();

  int n = USRBIOApi::WaitForIOs(&ior_, cqes_, iodepth_, timeout_ms);
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
  fd_register_->DeRegFd(aio->fd);

  CHECK_NOTNULL(aio->ctx);
  auto* ctx = static_cast<Context*>(aio->ctx);
  HandleBuffer(aio, ctx->buffer);

  delete ctx;
}

void USRBIO::HandleBuffer(AioClosure* aio, char* buffer) {
  if (!for_read_ || !aio->status().ok()) {  // write or read failed
    block_buffer_pool_->Release(buffer);
    return;
  }

  butil::IOBuf iobuf;
  iobuf.append_user_data(buffer, aio->length, [this](void* data) {
    block_buffer_pool_->Release(static_cast<char*>(data));
  });
  *aio->buffer_out = IOBuffer(iobuf);
}

uint32_t USRBIO::GetIODepth() const { return iodepth_; }

}  // namespace cache
}  // namespace dingofs

#endif  // WITH_LIBUSRBIO
