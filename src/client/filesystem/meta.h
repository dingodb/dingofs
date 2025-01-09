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

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_META_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_META_H_

#include <map>
#include <memory>
#include <string>

#include "dingofs/metaserver.pb.h"
#include "base/time/time.h"
#include "client/dir_buffer.h"
#include "client/fuse_common.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace filesystem {

using Ino = fuse_ino_t;
using Request = fuse_req_t;
using FileInfo = struct fuse_file_info;

struct EntryOut {
  EntryOut() = default;

  explicit EntryOut(pb::metaserver::InodeAttr attr) : attr(attr) {}

  pb::metaserver::InodeAttr attr;
  double entryTimeout;
  double attrTimeout;
};

struct AttrOut {
  AttrOut() = default;

  explicit AttrOut(pb::metaserver::InodeAttr attr) : attr(attr) {}

  pb::metaserver::InodeAttr attr;
  double attrTimeout;
};

struct DirEntry {
  DirEntry() = default;

  DirEntry(Ino ino, const std::string& name, pb::metaserver::InodeAttr attr)
      : ino(ino), name(name), attr(attr) {}

  Ino ino;
  std::string name;
  pb::metaserver::InodeAttr attr;
};

struct FileOut {
  FileOut() = default;

  FileOut(FileInfo* fi, pb::metaserver::InodeAttr attr)
      : fi(fi), attr(attr), nwritten(0) {}

  FileOut(pb::metaserver::InodeAttr attr, size_t nwritten)
      : fi(nullptr), attr(attr), nwritten(nwritten) {}

  FileInfo* fi;
  pb::metaserver::InodeAttr attr;
  size_t nwritten;
};

struct FileHandler {
  uint64_t fh;
  DirBufferHead* buffer;
  base::time::TimeSpec mtime;
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
  utils::Mutex mutex_;
  std::shared_ptr<DirBuffer> dirBuffer_;
  std::map<uint64_t, std::shared_ptr<FileHandler>> handlers_;
};

std::string StrMode(uint16_t mode);

std::string StrEntry(EntryOut entryOut);

std::string StrAttr(AttrOut attrOut);

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_META_H_
