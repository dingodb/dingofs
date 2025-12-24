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

#include "client/vfs/metasystem/mds/modify_time_memo.h"

#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

void ModifyTimeMemo::Remember(Ino ino) {
  modify_time_map_.withWLock(
      [ino](Map& map) mutable { map[ino] = utils::TimestampNs(); }, ino);
}

void ModifyTimeMemo::Forget(Ino ino) {
  modify_time_map_.withWLock([ino](Map& map) mutable { map.erase(ino); }, ino);
}

void ModifyTimeMemo::ForgetExpired(uint64_t expire_time_ns) {
  modify_time_map_.iterateWLock([expire_time_ns](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (it->second < expire_time_ns) {
        auto temp_it = it++;
        map.erase(temp_it);
      } else {
        ++it;
      }
    }
  });
}

uint64_t ModifyTimeMemo::Get(Ino ino) {
  uint64_t modify_time_ns = 0;
  modify_time_map_.withRLock(
      [ino, &modify_time_ns](Map& map) {
        auto it = map.find(ino);
        if (it != map.end()) {
          modify_time_ns = it->second;
        }
      },
      ino);

  return modify_time_ns;
}

bool ModifyTimeMemo::ModifiedSince(Ino ino, uint64_t timestamp) {
  return Get(ino) > timestamp;
}

bool ModifyTimeMemo::Dump(Json::Value& value) {
  Json::Value items = Json::arrayValue;
  modify_time_map_.iterate([&value, &items](const Map& map) {
    for (const auto& [ino, modify_time_ns] : map) {
      Json::Value item;
      item["ino"] = ino;
      item["modify_time_ns"] = modify_time_ns;

      items.append(item);
    }
  });

  value["modify_time_memo"] = items;

  return true;
}

bool ModifyTimeMemo::Load(const Json::Value& value) {
  const Json::Value& items = value["modify_time_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.modify_time_memo] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    uint64_t modify_time_ns = item["modify_time_ns"].asUInt64();

    // put
    modify_time_map_.withWLock(
        [ino, modify_time_ns](Map& map) mutable { map[ino] = modify_time_ns; },
        ino);
  }

  return true;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs