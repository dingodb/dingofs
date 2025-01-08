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
 * Created Date: 2023-03-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <memory>
#include <string>

#include "base/timer/timer.h"
#include "client/common/config.h"
#include "client/filesystem/attr_watcher.h"
#include "client/filesystem/defer_sync.h"
#include "client/filesystem/dir_cache.h"
#include "client/filesystem/dir_parent_watcher.h"
#include "client/filesystem/dir_quota_manager.h"
#include "client/filesystem/entry_watcher.h"
#include "client/filesystem/error.h"
#include "client/filesystem/fs_stat_manager.h"
#include "client/filesystem/lookup_cache.h"
#include "client/filesystem/meta.h"
#include "client/filesystem/openfile.h"
#include "client/filesystem/package.h"
#include "client/filesystem/rpc_client.h"

namespace dingofs {
namespace client {
namespace filesystem {

struct FileSystemMember {
  FileSystemMember(std::shared_ptr<DeferSync> deferSync,
                   std::shared_ptr<OpenFiles> openFiles,
                   std::shared_ptr<AttrWatcher> attrWatcher,
                   std::shared_ptr<EntryWatcher> entry_watcher)
      : deferSync(deferSync),
        openFiles(openFiles),
        attrWatcher(attrWatcher),
        entry_watcher(entry_watcher) {}

  std::shared_ptr<DeferSync> deferSync;
  std::shared_ptr<OpenFiles> openFiles;
  std::shared_ptr<AttrWatcher> attrWatcher;
  std::shared_ptr<EntryWatcher> entry_watcher;
};

class FileSystem {
 public:
  FileSystem(uint32_t fs_id, common::FileSystemOption option,
             ExternalMember member);

  void Run();

  void Destory();

  // fuse request
  DINGOFS_ERROR Lookup(Request req, Ino parent, const std::string& name,
                       EntryOut* entry_out);

  DINGOFS_ERROR GetAttr(Request req, Ino ino, AttrOut* attr_out);

  DINGOFS_ERROR OpenDir(Request req, Ino ino, FileInfo* fi);

  DINGOFS_ERROR ReadDir(Request req, Ino ino, FileInfo* fi,
                        std::shared_ptr<DirEntryList>* entries);

  DINGOFS_ERROR ReleaseDir(Request req, Ino ino, FileInfo* fi);

  DINGOFS_ERROR Open(Request req, Ino ino, FileInfo* fi);

  DINGOFS_ERROR Release(Request req, Ino ino, FileInfo* fi);

  // fuse reply: we control all replies to vfs layer in same entrance.
  void ReplyError(Request req, DINGOFS_ERROR code);

  void ReplyEntry(Request req, EntryOut* entry_out);

  void ReplyAttr(Request req, AttrOut* attr_out);

  void ReplyReadlink(Request req, const std::string& link);

  void ReplyOpen(Request req, FileInfo* fi);

  void ReplyOpen(Request req, FileOut* file_out);

  void ReplyData(Request req, struct fuse_bufvec* bufv,
                 enum fuse_buf_copy_flags flags);

  void ReplyWrite(Request req, FileOut* file_out);

  void ReplyBuffer(Request req, const char* buf, size_t size);

  void ReplyStatfs(Request req, const struct statvfs* stbuf);

  void ReplyXattr(Request req, size_t size);

  void ReplyCreate(Request req, EntryOut* entry_out, FileInfo* fi);

  void AddDirEntry(Request req, DirBufferHead* buffer, DirEntry* dir_entry);

  void AddDirEntryPlus(Request req, DirBufferHead* buffer, DirEntry* dir_entry);

  // utility: file handler
  std::shared_ptr<FileHandler> NewHandler();

  std::shared_ptr<FileHandler> FindHandler(uint64_t fh);

  void ReleaseHandler(uint64_t fh);

  // utility: others
  FileSystemMember BorrowMember();

  // ----------- dispatch request  -----------
  void UpdateFsQuotaUsage(int64_t add_space, int64_t add_inode);
  void UpdateDirQuotaUsage(Ino ino, int64_t add_space, int64_t add_inode);

  bool CheckFsQuota(int64_t add_space, int64_t add_inode);
  // This only check dir quota
  bool CheckDirQuota(Ino ino, int64_t add_space, int64_t add_inode);
  // This will first check fs stat, then check dir quota
  bool CheckQuota(Ino ino, int64_t add_space, int64_t add_inode);

  // This will check ino and its parent has dir quota or not
  // if find, then return the nearest dir quota ino
  bool NearestDirQuota(Ino ino, Ino& out_quota_ino);

  pb::metaserver::Quota GetFsQuota();

 private:
  FRIEND_TEST(FileSystemTest, Attr2Stat);
  FRIEND_TEST(FileSystemTest, Entry2Param);
  FRIEND_TEST(FileSystemTest, SetEntryTimeout);
  FRIEND_TEST(FileSystemTest, SetAttrTimeout);

  // utility: convert to system type.
  void Attr2Stat(pb::metaserver::InodeAttr* attr, struct stat* stat);

  void Entry2Param(EntryOut* entry_out, fuse_entry_param* e);

  // utility: set entry/attribute timeout
  void SetEntryTimeout(EntryOut* entry_out);

  void SetAttrTimeout(AttrOut* attr_out);

  uint32_t fs_id_;
  common::FileSystemOption option_;
  ExternalMember member;
  std::shared_ptr<DeferSync> deferSync_;
  std::shared_ptr<LookupCache> negative_;
  std::shared_ptr<DirCache> dirCache_;
  std::shared_ptr<OpenFiles> openFiles_;
  std::shared_ptr<AttrWatcher> attrWatcher_;
  std::shared_ptr<EntryWatcher> entry_watcher_;
  std::shared_ptr<HandlerManager> handlerManager_;
  std::shared_ptr<RPCClient> rpc_;

  std::shared_ptr<DirParentWatcher> dir_parent_watcher_;
  // NOTE: filesytem own this timer, when destroy or stop, first stop
  // stat_timer_, then stop fs_stat_manager_  and dir_quota_manager_
  std::shared_ptr<base::timer::Timer> stat_timer_;
  std::shared_ptr<FsStatManager> fs_stat_manager_;
  std::shared_ptr<DirQuotaManager> dir_quota_manager_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_
