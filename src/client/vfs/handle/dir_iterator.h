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

#ifndef DINGOFS_CLIENT_VFS_DIR_ITERATOR_H_
#define DINGOFS_CLIENT_VFS_DIR_ITERATOR_H_

#include <memory>

#include "common/status.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class DirIterator {
 public:
  DirIterator() = default;

  virtual ~DirIterator() = default;

  virtual Status Seek() = 0;

  virtual bool Valid() = 0;

  virtual DirEntry GetValue(bool with_attr) = 0;

  virtual void Next() = 0;

  virtual std::string Dump() = 0;

  virtual void Load(const std::string& data) = 0;
};

using DirIteratorUPtr = std::unique_ptr<DirIterator>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DIR_ITERATOR_H_