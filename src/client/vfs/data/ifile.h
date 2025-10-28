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

#ifndef DINGODB_CLIENT_VFS_DATA_IFILE_H_
#define DINGODB_CLIENT_VFS_DATA_IFILE_H_

#include <cstdint>

#include "common/callback.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class IFile {
 public:
  virtual ~IFile() = default;

  virtual Status Write(ContextSPtr ctx, const char* buf, uint64_t size,
                       uint64_t offset, uint64_t* out_wsize) = 0;

  virtual Status Read(ContextSPtr ctx, char* buf, uint64_t size,
                      uint64_t offset, uint64_t* out_rsize) = 0;

  virtual Status Flush() = 0;

  virtual void AsyncFlush(StatusCallback cb) = 0;
};

using IFileUPtr = std::unique_ptr<IFile>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_IFILE_H_