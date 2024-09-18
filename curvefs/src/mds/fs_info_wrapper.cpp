
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
 * @Project: curve
 * @Date: Fri Jul 23 16:37:33 CST 2021
 * @Author: wuhanqing
 */

#include "curvefs/src/mds/fs_info_wrapper.h"

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>

#include "curvefs/src/base/string/string.h"

namespace curvefs {
namespace mds {

using ::curvefs::base::string::GenUuid;

FsInfoWrapper::FsInfoWrapper(const ::curvefs::mds::CreateFsRequest* request,
                             uint64_t fs_id, uint64_t root_inode_id) {
  FsInfo fs_info;
  fs_info.set_fsname(request->fsname());
  fs_info.set_fsid(fs_id);
  fs_info.set_status(FsStatus::NEW);
  fs_info.set_rootinodeid(root_inode_id);
  fs_info.set_blocksize(request->blocksize());
  fs_info.set_mountnum(0);
  fs_info.set_enablesumindir(request->enablesumindir());
  fs_info.set_txsequence(0);
  fs_info.set_txowner("");
  fs_info.set_uuid(GenUuid());
  if (request->has_recycletimehour()) {
    fs_info.set_recycletimehour(request->recycletimehour());
  }

  const auto& detail = request->fsdetail();
  fs_info.set_allocated_detail(new FsDetail(detail));

  switch (request->fstype()) {
    case FSType::TYPE_S3:
      fs_info.set_fstype(FSType::TYPE_S3);
      fs_info.set_capacity(request->capacity());
      break;
    case FSType::TYPE_VOLUME:
      fs_info.set_fstype(FSType::TYPE_VOLUME);
      fs_info.set_capacity(detail.volume().volumesize());
      break;
    case FSType::TYPE_HYBRID:
      fs_info.set_fstype(FSType::TYPE_HYBRID);
      // TODO(huyao): set capacity for hybrid fs
      fs_info.set_capacity(
          std::min(detail.volume().volumesize(), request->capacity()));
      break;
  }

  fs_info.set_owner(request->owner());
  fsInfo_ = std::move(fs_info);
}

bool FsInfoWrapper::IsMountPointExist(const Mountpoint& mp) const {
  return std::find_if(fsInfo_.mountpoints().begin(),
                      fsInfo_.mountpoints().end(),
                      [mp](const Mountpoint& mount_point) {
                        return mp.path() == mount_point.path() &&
                               mp.hostname() == mount_point.hostname();
                      }) != fsInfo_.mountpoints().end();
}

bool FsInfoWrapper::IsMountPointConflict(const Mountpoint& mp) const {
  bool cto = (fsInfo_.mountpoints_size() ? false : mp.cto());

  bool exist =
      std::find_if(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                   [&](const Mountpoint& mount_point) {
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

void FsInfoWrapper::AddMountPoint(const Mountpoint& mp) {
  // TODO(wuhanqing): sort after add ?
  auto* p = fsInfo_.add_mountpoints();
  *p = mp;

  fsInfo_.set_mountnum(fsInfo_.mountnum() + 1);

  if (fsInfo_.enablesumindir() && fsInfo_.mountnum() > 1) {
    fsInfo_.set_enablesumindir(false);
  }
}

FSStatusCode FsInfoWrapper::DeleteMountPoint(const Mountpoint& mp) {
  auto iter =
      std::find_if(fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end(),
                   [mp](const Mountpoint& mount_point) {
                     return mp.path() == mount_point.path() &&
                            mp.hostname() == mount_point.hostname() &&
                            mp.port() == mount_point.port();
                   });

  bool found = iter != fsInfo_.mountpoints().end();
  if (found) {
    fsInfo_.mutable_mountpoints()->erase(iter);
    fsInfo_.set_mountnum(fsInfo_.mountnum() - 1);
    return FSStatusCode::OK;
  }

  return FSStatusCode::MOUNT_POINT_NOT_EXIST;
}

std::vector<Mountpoint> FsInfoWrapper::MountPoints() const {
  if (fsInfo_.mountpoints_size() == 0) {
    return {};
  }

  return {fsInfo_.mountpoints().begin(), fsInfo_.mountpoints().end()};
}

void FsInfoWrapper::SetVolumeSize(uint64_t size) {
  fsInfo_.mutable_detail()->mutable_volume()->set_volumesize(size);
}

}  // namespace mds
}  // namespace curvefs
