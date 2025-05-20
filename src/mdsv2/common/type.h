// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_COMMON_TYPE_H_
#define DINGOFS_MDSV2_COMMON_TYPE_H_

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"

namespace dingofs {
namespace mdsv2 {

using Ino = uint64_t;
using AttrType = pb::mdsv2::Inode;
using DentryType = pb::mdsv2::Dentry;
using ChunkType = pb::mdsv2::Chunk;
using FsInfoType = pb::mdsv2::FsInfo;
using TrashSliceList = pb::mdsv2::TrashSliceList;

inline std::string DescribeAttr(const AttrType& attr) {
  auto parents_func = [](const auto& parents) {
    std::string result;
    for (const auto& parent : parents) {
      if (!result.empty()) {
        result += ",";
      }
      result += std::to_string(parent);
    }
    return result;
  };

  return fmt::format("{}:{}:{}:{}:{}:{}:{}:{} v{} p{} t{}:{}:{}", attr.fs_id(), attr.ino(),
                     pb::mdsv2::FileType_Name(attr.type()), attr.nlink(), attr.mode(), attr.uid(), attr.gid(),
                     attr.length(), attr.version(), parents_func(attr.parents()), attr.ctime(), attr.mtime(),
                     attr.atime());
}

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_TYPE_H_