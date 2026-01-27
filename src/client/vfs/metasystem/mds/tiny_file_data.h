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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_TINY_FILE_DATA_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_TINY_FILE_DATA_H_

#include <fmt/format.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/vfs_meta.h"
#include "common/io_buffer.h"
#include "common/options/client.h"
#include "glog/logging.h"
#include "json/value.h"
#include "utils/shards.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

class DataBuffer;
using DataBufferSPtr = std::shared_ptr<DataBuffer>;

class DataBuffer {
 public:
  DataBuffer(Ino ino) : ino_(ino) {
    data_.reserve(FLAGS_vfs_tiny_file_max_size / 2);
    last_active_time_s_ = utils::Timestamp();
  }
  DataBuffer(Ino ino, bool is_complete) : ino_(ino), is_complete_(is_complete) {
    data_.reserve(FLAGS_vfs_tiny_file_max_size / 2);
    last_active_time_s_ = utils::Timestamp();
  }
  DataBuffer(Ino ino, std::string&& data) : ino_(ino), data_(std::move(data)) {}
  ~DataBuffer() = default;

  struct WriteOp {
    uint64_t offset;
    uint64_t size;
    std::string data;
  };

  static DataBufferSPtr New(Ino ino) {
    return std::make_shared<DataBuffer>(ino);
  }
  static DataBufferSPtr New(Ino ino, bool is_complete) {
    return std::make_shared<DataBuffer>(ino, is_complete);
  }

  static DataBufferSPtr New(Ino ino, std::string&& data) {
    return std::make_shared<DataBuffer>(ino, std::move(data));
  }

  bool Write(const char* data, uint64_t offset, uint64_t size) {
    utils::WriteLockGuard lk(lock_);

    last_active_time_s_ = utils::Timestamp();

    if (offset + size > FLAGS_vfs_tiny_file_max_size) {
      out_of_range_ = true;
      return false;
    }

    if (!is_complete_) {
      write_ops_.push_back(WriteOp{offset, size, std::string(data, size)});
      return true;
    }

    if (offset + size > data_.size()) {
      data_.resize(offset + size, '\0');
    }

    std::copy(data, data + size, data_.data() + offset);

    return true;
  }

  void Put(std::string& data, uint64_t version) {
    utils::WriteLockGuard lk(lock_);

    if (version <= version_) return;

    data_ = std::move(data);
    version_ = version;
    is_complete_ = true;

    // apply write ops
    for (auto& write_op : write_ops_) {
      if (write_op.offset + write_op.size > data_.size()) {
        data_.resize(write_op.offset + write_op.size, '\0');
      }

      std::copy(write_op.data.data(), write_op.data.data() + write_op.size,
                data_.data() + write_op.offset);
    }

    write_ops_.clear();
  }

  void Copy(std::string& out_data) {
    utils::ReadLockGuard lk(lock_);

    out_data = data_;
  }

  bool Read(uint64_t offset, uint64_t size, IOBuffer& io_buffer) {
    utils::ReadLockGuard lk(lock_);

    CHECK(offset < data_.size()) << fmt::format(
        "offset out of range, offset({}) data_size({}).", offset, data_.size());

    if (offset + size > data_.size()) {
      size = data_.size() - offset;
    }

    io_buffer.AppendUserData(data_.data() + offset, size, nullptr);

    last_active_time_s_ = utils::Timestamp();

    return true;
  }

  size_t Size() const {
    utils::ReadLockGuard lk(lock_);
    return data_.size();
  }
  size_t Bytes() const {
    utils::ReadLockGuard lk(lock_);
    return data_.capacity() + (write_ops_.capacity() * sizeof(WriteOp));
  }

  bool IsComplete() const {
    utils::ReadLockGuard lk(lock_);
    return is_complete_;
  }

  bool IsOutOfRange() const {
    utils::ReadLockGuard lk(lock_);
    return out_of_range_;
  }

  uint64_t Version() const {
    utils::ReadLockGuard lk(lock_);
    return version_;
  }

  uint64_t LastActiveTimeS() const {
    utils::ReadLockGuard lk(lock_);
    return last_active_time_s_;
  }

 private:
  mutable utils::RWLock lock_;

  const Ino ino_;
  std::string data_;

  std::vector<WriteOp> write_ops_;

  bool is_complete_{false};
  // whether write out of range
  bool out_of_range_{false};

  uint64_t version_{0};

  uint64_t last_active_time_s_{0};
};

class TinyFileDataCache {
 public:
  TinyFileDataCache() = default;
  ~TinyFileDataCache() = default;

  DataBufferSPtr Get(Ino ino);
  DataBufferSPtr Create(Ino ino);
  DataBufferSPtr GetOrCreate(Ino ino);

  void Delete(Ino ino);

  size_t Size();
  size_t Bytes();

  void CleanExpired(uint64_t expire_s);

  void Summary(Json::Value& value);

 private:
  using Map = absl::flat_hash_map<Ino, DataBufferSPtr>;

  constexpr static size_t kShardNum = 32;
  utils::Shards<Map, kShardNum> shard_map_;

  // metrics
  bvar::Adder<uint64_t> total_count_{"meta_tiny_file_data_cache_total_count"};
  bvar::Adder<uint64_t> clean_count_{"meta_tiny_file_data_cache_clean_count"};
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_TINY_FILE_DATA_H_