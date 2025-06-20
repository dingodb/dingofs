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

#include "client/vfs_legacy/filesystem/utils.h"

#include <glog/logging.h>

namespace dingofs {
namespace client {
namespace filesystem {

using base::time::TimeSpec;

bool IsDir(const pb::metaserver::InodeAttr& attr) {
  return attr.type() == pb::metaserver::FsFileType::TYPE_DIRECTORY;
}

bool IsS3File(const pb::metaserver::InodeAttr& attr) {
  return attr.type() == pb::metaserver::FsFileType::TYPE_S3;
}

bool IsVolmeFile(const pb::metaserver::InodeAttr& attr) {
  return attr.type() == pb::metaserver::FsFileType::TYPE_FILE;
}

bool IsSymLink(const pb::metaserver::InodeAttr& attr) {
  return attr.type() == pb::metaserver::FsFileType::TYPE_SYM_LINK;
}

TimeSpec AttrMtime(const pb::metaserver::InodeAttr& attr) {
  return base::time::TimeSpec(attr.mtime(), attr.mtime_ns());
}

TimeSpec AttrCtime(const pb::metaserver::InodeAttr& attr) {
  return TimeSpec(attr.ctime(), attr.ctime_ns());
}

TimeSpec InodeMtime(const std::shared_ptr<InodeWrapper> inode) {
  pb::metaserver::InodeAttr attr;
  inode->GetInodeAttr(&attr);
  return AttrMtime(attr);
}

TimeSpec Now() {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  return TimeSpec(now.tv_sec, now.tv_nsec);
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs
