// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_HELPER_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_HELPER_H_

#include "client/meta/vfs_meta.h"
#include "mdsv2/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class Helper {
 public:
  static Slice ToSlice(const mdsv2::SliceEntry& slice) {
    Slice out_slice;

    out_slice.id = slice.id();
    out_slice.offset = slice.offset();
    out_slice.length = slice.len();
    out_slice.compaction = slice.compaction_version();
    out_slice.is_zero = slice.zero();
    out_slice.size = slice.size();

    return out_slice;
  }

  static mdsv2::SliceEntry ToSlice(const Slice& slice) {
    pb::mdsv2::Slice out_slice;

    out_slice.set_id(slice.id);
    out_slice.set_offset(slice.offset);
    out_slice.set_len(slice.length);
    out_slice.set_compaction_version(slice.compaction);
    out_slice.set_zero(slice.is_zero);
    out_slice.set_size(slice.size);

    return out_slice;
  }
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_HELPER_H_