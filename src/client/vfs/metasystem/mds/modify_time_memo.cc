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

#include "common/logging.h"
#include "fmt/format.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

void ModifyTimeMemo::Remember(Ino ino) {
  shard_map_.withWLock(
      [ino](Map& map) mutable { map[ino] = utils::TimestampNs(); }, ino);
}

void ModifyTimeMemo::Forget(Ino ino) {
  shard_map_.withWLock([ino](Map& map) mutable { map.erase(ino); }, ino);
}

void ModifyTimeMemo::CleanExpired(uint64_t expire_time_ns) {
  std::vector<Ino> expired_inos;
  shard_map_.iterate([this, expire_time_ns, &expired_inos](Map& map) {
    for (auto& [ino, modify_time_ns] : map) {
      if (modify_time_ns < expire_time_ns) {
        expired_inos.push_back(ino);
      }
    }
  });

  // erase
  for (const auto& ino : expired_inos) {
    Forget(ino);
    clean_count_ << 1;
    LOG_DEBUG << fmt::format(
        "[meta.modify_time_memo] clean expired modify time memo ino({}).", ino);
  }
}

uint64_t ModifyTimeMemo::Get(Ino ino) {
  uint64_t modify_time_ns = 0;
  shard_map_.withRLock(
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

size_t ModifyTimeMemo::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t ModifyTimeMemo::Bytes() {
  return Size() * (sizeof(Ino) + sizeof(uint64_t));
}

void ModifyTimeMemo::Summary(Json::Value& value) {
  value["name"] = "modifytimememo";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["clean_count"] = clean_count_.get_value();
}

bool ModifyTimeMemo::Dump(Json::Value& value) {
  Json::Value items = Json::arrayValue;
  shard_map_.iterate([&value, &items](const Map& map) {
    for (const auto& [ino, modify_time_ns] : map) {
      Json::Value item;
      item["ino"] = ino;
      item["modify_time_ns"] = modify_time_ns;

      items.append(item);
    }
  });

  value["modify_time_memo"] = items;

  LOG(INFO) << fmt::format(
      "[meta.modify_time_memo] dump modify time memo count({}).", items.size());

  return true;
}

bool ModifyTimeMemo::Load(const Json::Value& value) {
  if (value.isNull()) return true;

  const Json::Value& items = value["modify_time_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "[meta.modify_time_memo] value is not an array.";
    return false;
  }

  for (const auto& item : items) {
    Ino ino = item["ino"].asUInt64();
    uint64_t modify_time_ns = item["modify_time_ns"].asUInt64();

    // put
    shard_map_.withWLock(
        [ino, modify_time_ns](Map& map) mutable { map[ino] = modify_time_ns; },
        ino);
  }

  LOG(INFO) << fmt::format(
      "[meta.modify_time_memo] load modify time memo count({}).", items.size());

  return true;
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs