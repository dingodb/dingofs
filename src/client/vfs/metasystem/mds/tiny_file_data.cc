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

#include "client/vfs/metasystem/mds/tiny_file_data.h"

#include "common/logging.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

DataBufferSPtr TinyFileDataCache::Get(Ino ino) {
  DataBufferSPtr data_buffer;
  shard_map_.withWLock(
      [ino, &data_buffer](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          data_buffer = it->second;
        }
      },
      ino);

  return data_buffer;
}

DataBufferSPtr TinyFileDataCache::Create(Ino ino) {
  DataBufferSPtr data_buffer;
  shard_map_.withWLock(
      [this, ino, &data_buffer](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) map.erase(it);

        data_buffer = DataBuffer::New(ino, true);
        map.emplace(ino, data_buffer);
        total_count_ << 1;
      },
      ino);

  return data_buffer;
}

DataBufferSPtr TinyFileDataCache::GetOrCreate(Ino ino) {
  DataBufferSPtr data_buffer;
  shard_map_.withWLock(
      [this, ino, &data_buffer](Map& map) mutable {
        auto it = map.find(ino);
        if (it != map.end()) {
          data_buffer = it->second;
        } else {
          data_buffer = DataBuffer::New(ino);
          map.emplace(ino, data_buffer);
          total_count_ << 1;
        }
      },
      ino);

  return data_buffer;
}

void TinyFileDataCache::Delete(Ino ino) {
  shard_map_.withWLock([ino](Map& map) { map.erase(ino); }, ino);
}

size_t TinyFileDataCache::Size() {
  size_t size = 0;
  shard_map_.iterate([&size](Map& map) { size += map.size(); });
  return size;
}

size_t TinyFileDataCache::Bytes() {
  size_t bytes = 0;
  shard_map_.iterate([&bytes](Map& map) {
    for (auto& [_, data_buffer] : map) {
      bytes += data_buffer->Bytes();
    }
  });

  return bytes;
}

void TinyFileDataCache::CleanExpired(uint64_t expire_s) {
  shard_map_.iterateWLock([&](Map& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (it->second->LastActiveTimeS() < expire_s) {
        auto temp = it++;
        map.erase(temp);
        clean_count_ << 1;
        LOG_DEBUG << fmt::format(
            "[meta.tinyfiledatacache.{}] clean expired tiny file data.",
            temp->first);

      } else {
        ++it;
      }
    }
  });
}

void TinyFileDataCache::Summary(Json::Value& value) {
  value["name"] = "tinyfiledatacache";
  value["count"] = Size();
  value["bytes"] = Bytes();
  value["total_count"] = total_count_.get_value();
  value["clean_count"] = clean_count_.get_value();
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs