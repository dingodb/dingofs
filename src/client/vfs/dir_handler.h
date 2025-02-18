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

#ifndef DINGOFS_CLIENT_VFS_DIR_HANDLER_H_
#define DINGOFS_CLIENT_VFS_DIR_HANDLER_H_

#include <cstdint>

#include "client/common/status.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class DirHandler {
 public:
  DirHandler() = default;

  virtual ~DirHandler() = default;

  virtual Status Init(bool with_attr) = 0;

  virtual uint64_t Offset() = 0;

  virtual Status Seek(uint64_t offset) = 0;

  virtual bool HasNext() = 0;

  // NOTE: if HasNext() is false, then Next() will undefined behavior
  virtual Status Next(DirEntry* dir_entry) = 0;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DIR_HANDLER_H_