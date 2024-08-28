/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_META_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_META_H_

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/dir_buffer.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::Mutex;
using ::curve::common::ReadLockGuard;
using ::curve::common::RWLock;
using ::curve::common::UniqueLock;
using ::curve::common::WriteLockGuard;
using ::curvefs::base::time::TimeSpec;
using ::curvefs::client::DirBufferHead;
using ::curvefs::client::InodeWrapper;
using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::FsFileType;
using ::curvefs::metaserver::InodeAttr;
using ::curvefs::metaserver::XAttr;

using Ino = fuse_ino_t;
using Request = fuse_req_t;
using FileInfo = struct fuse_file_info;

struct EntryOut {
  EntryOut() = default;

  explicit EntryOut(InodeAttr attr) : attr(attr) {}

  InodeAttr attr;
  double entryTimeout;
  double attrTimeout;
};

struct AttrOut {
  AttrOut() = default;

  explicit AttrOut(InodeAttr attr) : attr(attr) {}

  InodeAttr attr;
  double attrTimeout;
};

struct DirEntry {
  DirEntry() = default;

  DirEntry(Ino ino, const std::string& name, InodeAttr attr)
      : ino(ino), name(name), attr(attr) {}

  Ino ino;
  std::string name;
  InodeAttr attr;
};

struct FileOut {
  FileOut() = default;

  FileOut(FileInfo* fi, InodeAttr attr) : fi(fi), attr(attr), nwritten(0) {}

  FileOut(InodeAttr attr, size_t nwritten)
      : fi(nullptr), attr(attr), nwritten(nwritten) {}

  FileInfo* fi;
  InodeAttr attr;
  size_t nwritten;
};

struct FileHandler {
  uint64_t fh;
  DirBufferHead* buffer;
  TimeSpec mtime;
  bool padding;  // padding buffer
};

class HandlerManager {
 public:
  HandlerManager();

  ~HandlerManager();

  std::shared_ptr<FileHandler> NewHandler();

  std::shared_ptr<FileHandler> FindHandler(uint64_t id);

  void ReleaseHandler(uint64_t id);

 private:
  Mutex mutex_;
  std::shared_ptr<DirBuffer> dirBuffer_;
  std::map<uint64_t, std::shared_ptr<FileHandler>> handlers_;
};

std::string StrMode(uint16_t mode);

std::string StrEntry(EntryOut entryOut);

std::string StrAttr(AttrOut attrOut);

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_META_H_
