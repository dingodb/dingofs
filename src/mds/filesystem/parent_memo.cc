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

#include <vector>

#include "brpc/reloadable_flags.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

static const std::string kParentMemoTotalCountMetricsName = "dingofs_{}_parent_memo_{}";

// 0: no limit
DEFINE_uint32(mds_parent_memo_cache_max_count, 1 * 1024 * 1024, "parent memo cache max count");
DEFINE_validator(mds_parent_memo_cache_max_count, brpc::PassValidate);

ParentMemo::ParentMemo(uint64_t fs_id)
    : fs_id_(fs_id),
      total_count_(fmt::format(kParentMemoTotalCountMetricsName, fs_id, "total_count")),
      clean_count_(fmt::format(kParentMemoTotalCountMetricsName, fs_id, "clean_count")) {}

void ParentMemo::Remeber(Ino ino, Ino parent) {
  parent_map_.withWLock(
      [this, ino, parent](Map& map) mutable {
        auto it = map.find(ino);
        if (it == map.end()) {
          map[ino] = {parent, utils::Timestamp()};
          total_count_ << 1;
        } else {
          it->second.parent = parent;
          it->second.last_active_time_s = utils::Timestamp();
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
          parent = it->second.parent;
        }
      },
      ino);

  return found;
}

void ParentMemo::CleanExpired(uint64_t expire_s) {
  if (Size() < FLAGS_mds_parent_memo_cache_max_count) return;

  std::vector<Ino> delete_inos;
  parent_map_.iterate([&](const Map& map) {
    for (const auto& [ino, value] : map) {
      if (value.last_active_time_s < expire_s) {
        delete_inos.push_back(ino);
      }
    }
  });

  for (const auto& ino : delete_inos) {
    Forget(ino);
  }

  clean_count_ << delete_inos.size();

  LOG(INFO) << fmt::format("[parentmemo.{}] clean expired, stat({}|{}|{}).", fs_id_, Size(), delete_inos.size(),
                           clean_count_.get_value());
}

size_t ParentMemo::Size() {
  size_t size = 0;
  parent_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t ParentMemo::Bytes() { return Size() * (sizeof(Ino) + sizeof(Ino)); }

void ParentMemo::DescribeByJson(Json::Value& value) {
  value["count"] = Size();
  value["cache_clean"] = clean_count_.get_value();
}

void ParentMemo::Summary(Json::Value& value) {
  value["name"] = "parentmemo";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
}

}  // namespace mds
}  // namespace dingofs