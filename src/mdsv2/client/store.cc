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

#include "mdsv2/client/store.h"

#include <cstdint>
#include <ostream>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/filesystem/fs_utils.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/storage/dingodb_storage.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

bool StoreClient::Init(const std::string& coor_addr) {
  CHECK(!coor_addr.empty()) << "coor addr is empty.";

  kv_storage_ = DingodbStorage::New();
  CHECK(kv_storage_ != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseCoorAddr(coor_addr);
  if (store_addrs.empty()) {
    return false;
  }

  return kv_storage_->Init(store_addrs);
}

bool StoreClient::CreateMetaTable(const std::string& name) {
  int64_t table_id = 0;
  Range range = MetaCodec::GetMetaTableRange();
  KVStorage::TableOption option = {.start_key = range.start, .end_key = range.end};
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create meta table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create meta table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateFsStatsTable(const std::string& name) {
  int64_t table_id = 0;
  Range range = MetaCodec::GetFsStatsTableRange();
  KVStorage::TableOption option = {.start_key = range.start, .end_key = range.end};
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create fs stats table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create fs stats table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

static std::string FormatTime(uint64_t time_ns) { return Helper::FormatMsTime(time_ns / 1000000, "%H:%M:%S"); }

static void TraversePrint(FsTreeNode* item, bool is_details, int level) {
  if (item == nullptr) return;

  for (int i = 0; i < level; i++) {
    std::cout << "  ";
  }

  auto& dentry = item->dentry;
  auto& attr = item->attr;

  std::cout << fmt::format("{} [{},{},{}/{},{},{},{},{},{},{},{}]\n", dentry.name(), dentry.ino(),
                           pb::mdsv2::FileType_Name(attr.type()), attr.mode(), Helper::FsModeToString(attr.mode()),
                           attr.nlink(), attr.uid(), attr.gid(), attr.length(), FormatTime(attr.ctime()),
                           FormatTime(attr.mtime()), FormatTime(attr.atime()));

  if (dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
    for (auto* child : item->children) {
      TraversePrint(child, is_details, level + 1);
    }
  }
}

void StoreClient::PrintDentryTree(uint32_t fs_id, bool is_details) {
  if (fs_id == 0) {
    std::cout << "fs_id is invalid." << std::endl;
    return;
  }

  FsUtils fs_utils(OperationProcessor::New(kv_storage_));

  FsTreeNode* root = fs_utils.GenFsTree(fs_id);
  if (root == nullptr) {
    return;
  }

  std::cout << "############ name [ino,type,mode,nlink,uid,gid,size,ctime,mtime,atime] ############\n";
  TraversePrint(root, is_details, 0);

  FreeFsTree(root);
}

bool StoreCommandRunner::Run(const Options& options, const std::string& coor_addr, const std::string& cmd) {
  static std::set<std::string> mds_cmd = {
      Helper::ToLowerCase("CreateMetaTable"),
      Helper::ToLowerCase("CreateFsStatsTable"),
      Helper::ToLowerCase("CreateAllTable"),
      "tree",
  };

  if (mds_cmd.count(cmd) == 0) return false;

  if (coor_addr.empty()) {
    std::cout << "coordinator address is empty." << '\n';
    return false;
  }

  dingofs::mdsv2::client::StoreClient store_client;
  if (!store_client.Init(coor_addr)) {
    std::cout << "init store client fail." << '\n';
    return -1;
  }

  if (cmd == Helper::ToLowerCase("CreateMetaTable")) {
    store_client.CreateMetaTable(options.meta_table_name);

  } else if (cmd == Helper::ToLowerCase("CreateFsStatsTable")) {
    store_client.CreateFsStatsTable(options.fsstats_table_name);

  } else if (cmd == Helper::ToLowerCase("CreateAllTable")) {
    store_client.CreateMetaTable(options.meta_table_name);
    store_client.CreateFsStatsTable(options.fsstats_table_name);

  } else if (cmd == Helper::ToLowerCase("tree")) {
    store_client.PrintDentryTree(options.fs_id, true);
  }

  return true;
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs