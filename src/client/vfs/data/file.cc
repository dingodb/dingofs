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

#include <cstdint>
#include <mutex>
#include <utility>

#include "client/common/utils.h"
#include "client/vfs/data/chunk.h"
#include "client/vfs/data/common/async_util.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/callback.h"
#include "common/status.h"
#include "options/client/vfs/vfs_option.h"

namespace dingofs {
namespace client {
namespace vfs {

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

Status File::Write(const char* buf, uint64_t size, uint64_t offset,
                   uint64_t* out_wsize) {
  DINGOFS_RETURN_NOT_OK(PreCheck());

  return file_writer_->Write(buf, size, offset, out_wsize);
}

Status File::Read(char* buf, uint64_t size, uint64_t offset,
                  uint64_t* out_rsize) {
  DINGOFS_RETURN_NOT_OK(PreCheck());
  return file_reader_->Read(buf, size, offset, out_rsize);
}

void File::FileFlushed(StatusCallback cb, Status status) {
  if (!status.ok()) {
    LOG(WARNING) << "File::FileFlushed failed, ino: " << ino_
                 << ", status: " << status.ToString();
    file_status_ = status;
  }

  cb(status);

  VLOG(3) << "File::FileFlushed end ino: " << ino_
          << ", status: " << status.ToString();
}

void File::AsyncFlush(StatusCallback cb) {
  file_writer_->AsyncFlush([this, cb](auto&& ph1) {
    FileFlushed(cb, std::forward<decltype(ph1)>(ph1));
  });
}

Status File::Flush() {
  Status s;

  if (!FLAGS_data_use_direct_write) {
    Synchronizer sync;
    AsyncFlush(sync.AsStatusCallBack(s));
    sync.Wait();
  }

  return s;
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs