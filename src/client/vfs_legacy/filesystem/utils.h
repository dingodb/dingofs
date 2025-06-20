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

#ifndef DINGOFS_SRC_CLIENT_FILESYSTEM_UTILS_H_
#define DINGOFS_SRC_CLIENT_FILESYSTEM_UTILS_H_

#include <memory>

#include "dingofs/metaserver.pb.h"
#include "base/time/time.h"
#include "client/vfs_legacy/inode_wrapper.h"

namespace dingofs {
namespace client {
namespace filesystem {

// directory
bool IsDir(const pb::metaserver::InodeAttr& attr);

// file which data is stored in s3
bool IsS3File(const pb::metaserver::InodeAttr& attr);

// file which data is stored in volume
bool IsVolmeFile(const pb::metaserver::InodeAttr& attr);

// symbol link
bool IsSymLink(const pb::metaserver::InodeAttr& attr);

base::time::TimeSpec AttrMtime(const pb::metaserver::InodeAttr& attr);

base::time::TimeSpec AttrCtime(const pb::metaserver::InodeAttr& attr);

base::time::TimeSpec InodeMtime(const std::shared_ptr<InodeWrapper> inode);

base::time::TimeSpec Now();

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FILESYSTEM_UTILS_H_
