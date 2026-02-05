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

#include "mds/filesystem/parent_memo.h"

namespace dingofs {
namespace mds {

static const std::string kParentMemoTotalCountMetricsName = "dingofs_{}_parent_memo_total_count";

ParentMemo::ParentMemo(uint64_t fs_id)
    : fs_id_(fs_id), total_count_(fmt::format(kParentMemoTotalCountMetricsName, fs_id)) {}

void ParentMemo::Remeber(Ino ino, Ino parent) {
  parent_map_.withWLock(
      [this, ino, parent](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map[ino] = parent;
          total_count_ << 1;
        } else {
          it->second = parent;
        }
      },
      ino);
}

void ParentMemo::Forget(Ino ino) {
  parent_map_.withWLock([ino](Map& map) mutable { map.erase(ino); }, ino);
}

bool ParentMemo::GetParent(Ino ino, Ino& parent) {
  bool found = false;
  parent_map_.withRLock(
      [ino, &parent, &found](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          found = true;
          parent = it->second;
        }
      },
      ino);

  return found;
}

size_t ParentMemo::Size() {
  size_t size = 0;
  parent_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t ParentMemo::Bytes() { return Size() * (sizeof(Ino) + sizeof(Ino)); }

void ParentMemo::DescribeByJson(Json::Value& value) { value["count"] = Size(); }

void ParentMemo::Summary(Json::Value& value) {
  value["name"] = "parentmemo";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
}

}  // namespace mds
}  // namespace dingofs