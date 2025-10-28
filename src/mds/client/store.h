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

#ifndef DINGOFS_MDS_CLIENT_STORE_H_
#define DINGOFS_MDS_CLIENT_STORE_H_

#include <cstdint>
#include <string>

#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {
namespace client {

class StoreClient {
 public:
  StoreClient() = default;
  ~StoreClient() = default;

  bool Init(const std::string& coor_addr);

  bool CreateMetaTable(const std::string& name);
  bool CreateFsStatsTable(const std::string& name);
  bool DropMetaTable();
  bool DropFsStatsTable();
  bool DropFsMetaTable(uint32_t fs_id);

  // print fs dentry tree
  void PrintDentryTree(uint32_t fs_id, bool is_details);

  bool UpdateFsS3Info(const std::string& fs_name, const S3Info& s3_info);
  bool UpdateFsRadosInfo(const std::string& fs_name, const RadosInfo& rados_info);

 private:
  KVStorageSPtr kv_storage_;
};

class StoreCommandRunner {
 public:
  StoreCommandRunner() = default;
  ~StoreCommandRunner() = default;

  struct Options {
    uint32_t fs_id{0};
    std::string fs_name;
    std::string meta_table_name;
    std::string fsstats_table_name;

    S3Info s3_info;
    RadosInfo rados_info;
  };

  static bool Run(const Options& options, const std::string& coor_addr, const std::string& cmd);
};

}  // namespace client
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_CLIENT_STORE_H_