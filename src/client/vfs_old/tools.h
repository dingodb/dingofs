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

#ifndef DINGOFS_CLIENT_VFS_OLD_TOOLS_H_
#define DINGOFS_CLIENT_VFS_OLD_TOOLS_H_

#include <glog/logging.h>
#include <sys/stat.h>

#include <cstdint>
#include <ctime>

#include "client/vfs/vfs_meta.h"
#include "dingofs/mds.pb.h"
#include "dingofs/metaserver.pb.h"
#include "stub/common/common.h"

namespace dingofs {
namespace client {
namespace vfs {

const uint32_t kMaxHostNameLength = 255u;

static Attr GenerateVirtualInodeAttr(Ino ino) {
  Attr attr;

  attr.ino = ino;
  attr.mode = S_IFREG | 0444;
  attr.nlink = 1;
  attr.length = 0;

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  attr.atime = now.tv_sec;
  attr.atime_ns = now.tv_nsec;
  attr.mtime = now.tv_sec;
  attr.mtime_ns = now.tv_nsec;
  attr.ctime = now.tv_sec;
  attr.ctime_ns = now.tv_nsec;

  attr.type = FileType::kFile;

  return attr;
}

static FileType FsFileTypePBToFileType(
    pb::metaserver::FsFileType fs_file_type) {
  switch (fs_file_type) {
    case pb::metaserver::FsFileType::TYPE_DIRECTORY:
      return FileType::kDirectory;
    case pb::metaserver::FsFileType::TYPE_SYM_LINK:
      return FileType::kSymlink;
    case pb::metaserver::FsFileType::TYPE_S3:
      return FileType::kFile;
    default:
      CHECK(false) << "Unknown fs_file_type: " << fs_file_type;
  }
}

static Attr InodeAttrPBToAttr(pb::metaserver::InodeAttr& inode_attr) {
  Attr attr;
  attr.ino = inode_attr.inodeid();
  attr.mode = inode_attr.mode();
  attr.nlink = inode_attr.nlink();
  attr.uid = inode_attr.uid();
  attr.gid = inode_attr.gid();
  attr.length = inode_attr.length();
  attr.rdev = inode_attr.rdev();
  attr.atime = inode_attr.atime();
  attr.atime_ns = inode_attr.atime_ns();
  attr.mtime = inode_attr.mtime();
  attr.mtime_ns = inode_attr.mtime_ns();
  attr.ctime = inode_attr.ctime();
  attr.ctime_ns = inode_attr.ctime_ns();
  attr.type = FsFileTypePBToFileType(inode_attr.type());
  for (int i = 0; i < inode_attr.parent_size(); i++) {
    attr.parents.push_back(inode_attr.parent(i));
  }
  return attr;
}

static int SetHostPortInMountPoint(pb::mds::Mountpoint& out) {
  char hostname[kMaxHostNameLength];
  int ret = gethostname(hostname, kMaxHostNameLength);
  if (ret < 0) {
    LOG(ERROR) << "GetHostName failed, ret = " << ret;
    return ret;
  }
  out.set_hostname(hostname);
  out.set_port(stub::common::ClientDummyServerInfo::GetInstance().GetPort());
  return 0;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_OLD_TOOLS_H_