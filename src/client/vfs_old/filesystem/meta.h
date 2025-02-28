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
 * Project: Dingofs
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_META_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_META_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "base/time/time.h"
#include "client/common/status.h"
#include "client/vfs/handle/dir_iterator.h"
#include "client/vfs/vfs_meta.h"
#include "client/vfs_old/dir_buffer.h"
#include "dingofs/mdsv2.pb.h"
#include "dingofs/metaserver.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace filesystem {

using Ino = vfs::Ino;

struct EntryOut {
  EntryOut() = default;

  explicit EntryOut(pb::metaserver::InodeAttr attr) : attr(attr) {}
  explicit EntryOut(pb::mdsv2::Inode inode) : inode(inode) {}

  pb::metaserver::InodeAttr attr;
  pb::mdsv2::Inode inode;
  double entryTimeout;
  double attrTimeout;
};

struct AttrOut {
  AttrOut() = default;

  explicit AttrOut(pb::metaserver::InodeAttr attr) : attr(attr) {}
  explicit AttrOut(pb::mdsv2::Inode inode) : inode(inode) {}

  pb::metaserver::InodeAttr attr;
  pb::mdsv2::Inode inode;
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

struct FileHandler;

class FsDirIterator : public vfs::DirIterator {
 public:
  FsDirIterator() = default;
  ~FsDirIterator() override = default;

  void Append(vfs::DirEntry& entry) { entries_.push_back(entry); }

  Status Seek() override;
  bool Valid() override;
  vfs::DirEntry GetValue(bool with_attr) override;
  void Next() override;

 private:
  uint64_t offset_{0};

  std::vector<vfs::DirEntry> entries_;
};

struct FileHandler {
  uint64_t fh;
  // for file
  int32_t flags;

  DirBufferHead* buffer;
  base::time::TimeSpec mtime;
  bool padding;  // padding buffer
  // for read dir
  bool dir_handler_init{false};
  std::unique_ptr<FsDirIterator> dir_iterator;
};

class HandlerManager {
 public:
  HandlerManager();

  ~HandlerManager();

  std::shared_ptr<FileHandler> NewHandler();

  std::shared_ptr<FileHandler> FindHandler(uint64_t id);

  void ReleaseHandler(uint64_t id);

  void SaveAllHandlers(const std::string& path);

  void LoadAllHandlers(const std::string& path);

 private:
  utils::Mutex mutex_;
  std::shared_ptr<DirBuffer> dirBuffer_;
  std::map<uint64_t, std::shared_ptr<FileHandler>> handlers_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_META_H_
