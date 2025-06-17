/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-10-31
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_METASERVER_SUPERPARTITION_BUILDER_BUILDER_H_
#define DINGOFS_TEST_METASERVER_SUPERPARTITION_BUILDER_BUILDER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>

#include "common/const.h"
#include "fs/ext4_filesystem_impl.h"
#include "metaserver/storage/config.h"
#include "metaserver/storage/rocksdb_storage.h"
#include "metaserver/storage/storage.h"
#include "metaserver/superpartition/super_partition.h"
#include "utils/string.h"

namespace dingofs {
namespace metaserver {
namespace superpartition {

using ::dingofs::metaserver::storage::KVStorage;
using ::dingofs::metaserver::storage::RocksDBStorage;
using ::dingofs::metaserver::storage::StorageOptions;
using ::dingofs::utils::GenUuid;

using PcloseDeleter = int (*)(FILE*);

static bool RunShell(const std::string& cmd, std::string* ret) {
  std::array<char, 128> buffer;
  std::unique_ptr<FILE, PcloseDeleter> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe) {
    return false;
  } else if (ret == nullptr) {
    return true;
  }

  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    *ret += buffer.data();
  }
  return true;
}

class KVStorageBuilder {
  StorageOptions DefaultOption() {
    data_dir_ = "." + GenUuid();
    auto local_fs = ::dingofs::fs::Ext4FileSystemImpl::getInstance();
    auto options = StorageOptions();
    options.dataDir = absl::StrFormat("%s/%s", data_dir_, "rocksdb.db");
    options.maxMemoryQuotaBytes = 2 * kGiB;
    options.maxDiskQuotaBytes = 10 * kMiB;
    options.compression = false;
    options.localFileSystem = local_fs.get();
    return options;
  }

 public:
  KVStorageBuilder() : options_(DefaultOption()) {}

  std::shared_ptr<KVStorage> Build() {
    kv_ = std::make_shared<RocksDBStorage>(options_);
    CHECK(RunShell("mkdir -p " + data_dir_, nullptr));
    CHECK(kv_->Open());
    return kv_;
  }

  std::string GetDataDir() { return data_dir_; }

  void Cleanup() {
    CHECK(std::system(("rm -rf " + data_dir_).c_str()) == 0);
    CHECK(RunShell("rm -rf " + data_dir_, nullptr));
  }

 private:
  std::string data_dir_;
  StorageOptions options_;
  std::shared_ptr<KVStorage> kv_;
};

class SuperPartitionBuilder {
 public:
  SuperPartitionBuilder() = default;

  ~SuperPartitionBuilder() { kv_builder_.Cleanup(); }

  std::shared_ptr<SuperPartition> Build() {
    return std::make_shared<SuperPartition>(kv_builder_.Build());
  }

 private:
  KVStorageBuilder kv_builder_;
};

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_METASERVER_SUPERPARTITION_BUILDER_BUILDER_H_
