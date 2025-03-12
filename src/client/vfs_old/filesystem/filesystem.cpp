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

#include "client/vfs_old/filesystem/filesystem.h"

#include <cstdint>
#include <memory>

#include "base/timer/timer_impl.h"
#include "client/common/dynamic_config.h"
#include "client/vfs_old/filesystem/attr_watcher.h"
#include "client/vfs_old/filesystem/dir_cache.h"
#include "client/vfs_old/filesystem/dir_parent_watcher.h"
#include "client/vfs_old/filesystem/fs_stat_manager.h"
#include "client/vfs_old/filesystem/utils.h"
#include "dingofs/metaserver.pb.h"

namespace dingofs {
namespace client {
namespace filesystem {

using base::time::TimeSpec;
using base::timer::TimerImpl;
using common::FileSystemOption;

using pb::metaserver::InodeAttr;
using pb::metaserver::Quota;

USING_FLAG(stat_timer_thread_num);

FileSystem::FileSystem(uint32_t fs_id, std::string fs_name,
                       FileSystemOption option, ExternalMember member)
    : fs_id_(fs_id), fs_name_(fs_name), option_(option), member(member) {
  deferSync_ = std::make_shared<DeferSync>(option.deferSyncOption);
  negative_ = std::make_shared<LookupCache>(option.lookupCacheOption);
  dirCache_ = std::make_shared<DirCache>(option.dirCacheOption);
  openFiles_ = std::make_shared<OpenFiles>(option_.openFilesOption, deferSync_);
  attrWatcher_ = std::make_shared<AttrWatcher>(option_.attrWatcherOption,
                                               openFiles_, dirCache_);
  entry_watcher_ = std::make_shared<EntryWatcher>(option_.nocto_suffix);
  handlerManager_ = std::make_shared<HandlerManager>();
  rpc_ = std::make_shared<RPCClient>(option.rpcOption, member);
}

void FileSystem::Run() {
  deferSync_->Start();
  dirCache_->Start();

  dir_parent_watcher_ =
      std::make_shared<DirParentWatcherImpl>(member.inodeManager);

  stat_timer_ =
      std::make_shared<base::timer::TimerImpl>(FLAGS_stat_timer_thread_num);
  fs_stat_manager_ =
      std::make_shared<FsStatManager>(fs_id_, member.meta_client, stat_timer_);
  dir_quota_manager_ = std::make_shared<DirQuotaManager>(
      fs_id_, member.meta_client, dir_parent_watcher_, stat_timer_);
  fs_push_metrics_manager_ = std::make_unique<FsPushMetricManager>(
      fs_name_, member.mds_client, stat_timer_);

  // must start before fs_stat_manager_ and dir_quota_manager_ and
  // fs_push_metrics_manager_
  stat_timer_->Start();

  fs_stat_manager_->Start();
  dir_quota_manager_->Start();
  fs_push_metrics_manager_->Start();
}

void FileSystem::Destory() {
  openFiles_->CloseAll();
  deferSync_->Stop();
  dirCache_->Stop();

  stat_timer_->Stop();
  fs_stat_manager_->Stop();
  dir_quota_manager_->Stop();
  fs_push_metrics_manager_->Stop();
}

// handler*
std::shared_ptr<FileHandler> FileSystem::NewHandler() {
  return handlerManager_->NewHandler();
}

std::shared_ptr<FileHandler> FileSystem::FindHandler(uint64_t fh) {
  return handlerManager_->FindHandler(fh);
}

void FileSystem::ReleaseHandler(uint64_t fh) {
  return handlerManager_->ReleaseHandler(fh);
}

FileSystemMember FileSystem::BorrowMember() {
  return FileSystemMember(deferSync_, openFiles_, attrWatcher_, entry_watcher_);
}

// fuse request*
DINGOFS_ERROR FileSystem::Lookup(Ino parent, const std::string& name,
                                 EntryOut* entry_out) {
  if (name.size() > option_.maxNameLength) {
    return DINGOFS_ERROR::NAMETOOLONG;
  }

  bool yes = negative_->Get(parent, name);
  if (yes) {
    return DINGOFS_ERROR::NOTEXIST;
  }

  auto rc = rpc_->Lookup(parent, name, entry_out);
  if (rc == DINGOFS_ERROR::OK) {
    negative_->Delete(parent, name);
  } else if (rc == DINGOFS_ERROR::NOTEXIST) {
    negative_->Put(parent, name);
  }
  return rc;
}

DINGOFS_ERROR FileSystem::GetAttr(Ino ino, AttrOut* attr_out) {
  InodeAttr attr;
  auto rc = rpc_->GetAttr(ino, &attr);
  if (rc == DINGOFS_ERROR::OK) {
    *attr_out = AttrOut(attr);
  }
  return rc;
}

DINGOFS_ERROR FileSystem::OpenDir(Ino ino, uint64_t* fh) {
  InodeAttr attr;
  DINGOFS_ERROR rc = rpc_->GetAttr(ino, &attr);
  if (rc != DINGOFS_ERROR::OK) {
    return rc;
  }

  // revalidate directory cache
  std::shared_ptr<DirEntryList> entries;
  bool yes = dirCache_->Get(ino, &entries);
  if (yes) {
    if (entries->GetMtime() != AttrMtime(attr)) {
      dirCache_->Drop(ino);
    }
  }

  auto handler = NewHandler();
  handler->mtime = AttrMtime(attr);
  *fh = handler->fh;
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FileSystem::ReadDir(Ino ino, uint64_t fh,
                                  std::shared_ptr<DirEntryList>* entries) {
  bool yes = dirCache_->Get(ino, entries);
  if (yes) {
    return DINGOFS_ERROR::OK;
  }

  DINGOFS_ERROR rc = rpc_->ReadDir(ino, entries);
  if (rc != DINGOFS_ERROR::OK) {
    return rc;
  }

  (*entries)->SetMtime(FindHandler(fh)->mtime);
  dirCache_->Put(ino, *entries);
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FileSystem::ReleaseDir(uint64_t fh) {
  ReleaseHandler(fh);
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FileSystem::Open(Ino ino) {
  std::shared_ptr<InodeWrapper> inode;
  bool yes = openFiles_->IsOpened(ino, &inode);
  if (yes) {
    openFiles_->Open(ino, inode);
    // fi->keep_cache = 1;
    return DINGOFS_ERROR::OK;
  }

  DINGOFS_ERROR rc = rpc_->Open(ino, &inode);
  if (rc != DINGOFS_ERROR::OK) {
    return rc;
  }

  TimeSpec mtime;
  yes = attrWatcher_->GetMtime(ino, &mtime);
  if (!yes) {
    // It is rare which only arise when attribute evited for attr-watcher.
    LOG(WARNING) << "open(" << ino << "): stale file handler"
                 << ": attribute not found in wacther";
    return DINGOFS_ERROR::STALE;
  } else if (mtime != InodeMtime(inode)) {
    LOG(WARNING) << "open(" << ino << "): stale file handler"
                 << ", cache(" << mtime << ") vs remote(" << InodeMtime(inode)
                 << ")";
    return DINGOFS_ERROR::STALE;
  }

  openFiles_->Open(ino, inode);
  return DINGOFS_ERROR::OK;
}

DINGOFS_ERROR FileSystem::Release(Ino ino) {
  openFiles_->Close(ino);
  return DINGOFS_ERROR::OK;
}

void FileSystem::UpdateFsQuotaUsage(int64_t add_space, int64_t add_inode) {
  fs_stat_manager_->UpdateFsQuotaUsage(add_space, add_inode);
}

void FileSystem::UpdateDirQuotaUsage(Ino ino, int64_t add_space,
                                     int64_t add_inode) {
  dir_quota_manager_->UpdateDirQuotaUsage(ino, add_space, add_inode);
}

bool FileSystem::CheckFsQuota(int64_t add_space, int64_t add_inode) {
  return fs_stat_manager_->CheckFsQuota(add_space, add_inode);
}

bool FileSystem::CheckDirQuota(Ino ino, int64_t add_space, int64_t add_inode) {
  return dir_quota_manager_->CheckDirQuota(ino, add_space, add_inode);
}

bool FileSystem::CheckQuota(Ino ino, int64_t add_space, int64_t add_inode) {
  if (!fs_stat_manager_->CheckFsQuota(add_space, add_inode)) {
    return false;
  }

  return dir_quota_manager_->CheckDirQuota(ino, add_space, add_inode);
}

bool FileSystem::NearestDirQuota(Ino ino, Ino& out_quota_ino) {
  return dir_quota_manager_->NearestDirQuota(ino, out_quota_ino);
}

Quota FileSystem::GetFsQuota() { return fs_stat_manager_->GetFsQuota(); }

void FileSystem::BeforeReplyEntry(pb::metaserver::InodeAttr& attr) {
  AttrWatcherGuard watcher(attrWatcher_, &attr, ReplyType::ATTR, true);
  DirParentWatcherGuard parent_watcher(dir_parent_watcher_, attr);
}

void FileSystem::BeforeReplyAttr(pb::metaserver::InodeAttr& attr) {
  AttrWatcherGuard watcher(attrWatcher_, &attr, ReplyType::ATTR, true);
}

void FileSystem::BeforeReplyCreate(pb::metaserver::InodeAttr& attr) {
  AttrWatcherGuard watcher(attrWatcher_, &attr, ReplyType::ATTR, true);
}

void FileSystem::BeforeReplyOpen(pb::metaserver::InodeAttr& attr) {
  AttrWatcherGuard watcher(attrWatcher_, &attr, ReplyType::ONLY_LENGTH, true);
}

void FileSystem::BeforeReplyWrite(pb::metaserver::InodeAttr& attr) {
  AttrWatcherGuard watcher(attrWatcher_, &attr, ReplyType::ONLY_LENGTH, true);
}

void FileSystem::BeforeReplyAddDirEntryPlus(pb::metaserver::InodeAttr& attr) {
  AttrWatcherGuard watcher(attrWatcher_, &attr, ReplyType::ONLY_LENGTH, true);
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
