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
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CLIENT_FILESYSTEM_HELPER_META_H_
#define DINGOFS_TEST_CLIENT_FILESYSTEM_HELPER_META_H_

#include <gmock/gmock.h>

#include <memory>
#include <string>

#include "absl/strings/str_format.h"
#include "proto/metaserver.pb.h"
#include "client/filesystem/filesystem.h"
#include "client/filesystem/meta.h"

namespace dingofs {
namespace client {
namespace filesystem {

using ::absl::StrFormat;
using ::dingofs::base::time::TimeSpec;
using ::dingofs::pb::metaserver::Dentry;
using ::dingofs::pb::metaserver::FsFileType;
using ::dingofs::pb::metaserver::InodeAttr;

struct AttrOption {
 public:
  AttrOption() = default;
  AttrOption type(FsFileType type);
  AttrOption mode(uint32_t mode);
  AttrOption nlink(uint32_t nlink);
  AttrOption uid(uint32_t uid);
  AttrOption gid(uint32_t gid);
  AttrOption length(uint64_t length);
  AttrOption rdev(uint64_t rdev);
  AttrOption atime(uint64_t seconds, uint32_t naoSeconds);
  AttrOption mtime(uint64_t seconds, uint32_t naoSeconds);
  AttrOption ctime(uint64_t seconds, uint32_t naoSeconds);

 private:
  friend InodeAttr MkAttr(Ino ino, AttrOption option);

 private:
  FsFileType type_;
  uint32_t mode_;
  uint32_t nlink_;
  uint32_t uid_;
  uint32_t gid_;
  uint64_t length_;
  uint64_t rdev_;
  TimeSpec atime_;
  TimeSpec mtime_;
  TimeSpec ctime_;
};

class InodeOption {
 public:
  InodeOption() = default;
  InodeOption mtime(uint64_t seconds, uint32_t naoSeconds);
  InodeOption metaClient(
      std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient);

 private:
  friend std::shared_ptr<InodeWrapper> MkInode(Ino ino, InodeOption option);

  TimeSpec mtime_;
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;
};

InodeAttr MkAttr(Ino ino, AttrOption option = AttrOption());

std::shared_ptr<InodeWrapper> MkInode(Ino ino,
                                      InodeOption option = InodeOption());

Dentry MkDentry(Ino ino, const std::string& name);

DirEntry MkDirEntry(Ino ino, const std::string& name,
                    InodeAttr attr = MkAttr(0));

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_FILESYSTEM_HELPER_META_H_
