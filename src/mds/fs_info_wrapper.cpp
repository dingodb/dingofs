
/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: dingo
 * @Date: Fri Jul 23 16:37:33 CST 2021
 * @Author: wuhanqing
 */

#include "mds/fs_info_wrapper.h"

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>

#include "utils/string.h"

namespace dingofs {
namespace mds {

using utils::GenUuid;

FsInfoWrapper::FsInfoWrapper(const pb::mds::CreateFsRequest* request,
                             uint64_t fs_id, uint64_t root_inode_id) {
  pb::mds::FsInfo fs_info;
  fs_info.set_fsname(request->fsname());
  fs_info.set_fsid(fs_id);
  fs_info.set_status(pb::mds::FsStatus::NEW);
  fs_info.set_rootinodeid(root_inode_id);
  fs_info.set_capacity(request->capacity());
  fs_info.set_block_size(request->block_size());
  fs_info.set_chunk_size(request->chunk_size());
  fs_info.set_mountnum(0);
  fs_info.set_txsequence(0);
  fs_info.set_txowner("");
  fs_info.set_uuid(GenUuid());
  if (request->has_recycletimehour()) {
    fs_info.set_recycletimehour(request->recycletimehour());
  }

  auto* storage_info = fs_info.mutable_storage_info();
  *storage_info = request->storage_info();

  fs_info.set_owner(request->owner());
  fsInfo_ = std::move(fs_info);
}

bool FsInfoWrapper::IsMountPointExist(const pb::mds::Mountpoint& mp) const {
  return std::find_if(fsInfo_.mountpoints().begin(),
                      fsInfo_.mountpoints().end(),
                      [mp](const pb::mds::Mountpoint& mount_point) {
                        return mp.path() == mount_point.path() &&
                               mp.hostname() == mount_point.hostname();
                      }) != fsInfo_.mountpoints().end();
}

bool FsInfoWrapper::IsMountPointConflict(const pb::mds::Mountpoint& mp) const {
  bool cto = (fsInfo_.mountpoints_size() ? false : mp.cto());

  bool exist =
      std::find_if(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                   [&](const pb::mds::Mountpoint& mount_point) {
                     if (mount_point.has_cto() && mount_point.cto()) {
                       cto = true;
                     }

                     return mp.path() == mount_point.path() &&
                            mp.hostname() == mount_point.hostname();
                   }) != fsInfo_.mountpoints().end();

  // NOTE:
  // 1. if mount point exist (exist = true), conflict
  // 2. if existing mount point enableCto is diffrent from newcomer, conflict
  return exist || (cto != mp.cto());
}

void FsInfoWrapper::AddMountPoint(const pb::mds::Mountpoint& mp) {
  // TODO(wuhanqing): sort after add ?
  auto* p = fsInfo_.add_mountpoints();
  *p = mp;

  fsInfo_.set_mountnum(fsInfo_.mountnum() + 1);
}

pb::mds::FSStatusCode FsInfoWrapper::DeleteMountPoint(
    const pb::mds::Mountpoint& mp) {
  auto iter =
      std::find_if(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                   [mp](const pb::mds::Mountpoint& mount_point) {
                     return mp.path() == mount_point.path() &&
                            mp.hostname() == mount_point.hostname() &&
                            mp.port() == mount_point.port();
                   });

  bool found = iter != fsInfo_.mountpoints().end();
  if (found) {
    fsInfo_.mutable_mountpoints()->erase(iter);
    fsInfo_.set_mountnum(fsInfo_.mountnum() - 1);
    return pb::mds::FSStatusCode::OK;
  }

  return pb::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST;
}

std::vector<pb::mds::Mountpoint> FsInfoWrapper::MountPoints() const {
  if (fsInfo_.mountpoints_size() == 0) {
    return {};
  }

  return {fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end()};
}

}  // namespace mds
}  // namespace dingofs
