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
#include "client/vfs_old/common/config.h"
#include "client/vfs_old/filesystem/attr_watcher.h"
#include "client/vfs_old/filesystem/defer_sync.h"
#include "client/vfs_old/filesystem/dir_cache.h"
#include "client/vfs_old/filesystem/dir_parent_watcher.h"
#include "client/vfs_old/filesystem/dir_quota_manager.h"
#include "client/vfs_old/filesystem/entry_watcher.h"
#include "client/vfs_old/filesystem/error.h"
#include "client/vfs_old/filesystem/fs_push_metric_manager.h"
#include "client/vfs_old/filesystem/fs_stat_manager.h"
#include "client/vfs_old/filesystem/lookup_cache.h"
#include "client/vfs_old/filesystem/meta.h"
#include "client/vfs_old/filesystem/openfile.h"
#include "client/vfs_old/filesystem/package.h"
#include "client/vfs_old/filesystem/rpc_client.h"

namespace dingofs {
namespace client {

// forward declaration
namespace vfs {
class VFSOld;
}

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
  FileSystem(uint32_t fs_id, std::string fs_name,
             common::FileSystemOption option, ExternalMember member);

  void Run();

  void Destory();

  // fuse request
  DINGOFS_ERROR Lookup(Ino parent, const std::string& name,
                       EntryOut* entry_out);

  DINGOFS_ERROR GetAttr(Ino ino, AttrOut* attr_out);

  DINGOFS_ERROR OpenDir(Ino ino, uint64_t* fh);

  DINGOFS_ERROR ReadDir(Ino ino, uint64_t fh,
                        std::shared_ptr<DirEntryList>* entries);

  DINGOFS_ERROR ReleaseDir(uint64_t fh);

  DINGOFS_ERROR Open(Ino ino);

  DINGOFS_ERROR Release(Ino ino);

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

  // --------- below is related to reply kernel in fuse ----------

  // NOTE* : all the below function should be called out of lock

  void BeforeReplyEntry(pb::metaserver::InodeAttr& attr);

  void BeforeReplyAttr(pb::metaserver::InodeAttr& attr);

  void BeforeReplyCreate(pb::metaserver::InodeAttr& attr);

  void BeforeReplyOpen(pb::metaserver::InodeAttr& attr);

  void BeforeReplyWrite(pb::metaserver::InodeAttr& attr);

  void BeforeReplyAddDirEntryPlus(pb::metaserver::InodeAttr& attr);
  // --------- end is related to reply kernel in fuse ----------

 private:
  friend class vfs::VFSOld;

  uint32_t fs_id_;
  std::string fs_name_;
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
  std::unique_ptr<FsPushMetricManager> fs_push_metrics_manager_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_
