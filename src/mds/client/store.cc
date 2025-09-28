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

#include "mds/client/store.h"

#include <cstdint>
#include <iostream>
#include <ostream>
#include <string>

#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/filesystem/fs_utils.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dingodb_storage.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {
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
    std::cerr << fmt::format("create meta table fail, error: {}.", status.error_str()) << '\n';
    return false;
  }

  std::cout << fmt::format("create meta table success, start_key({}), end_key({}).",
                           Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key))
            << '\n';

  return true;
}

bool StoreClient::CreateFsStatsTable(const std::string& name) {
  int64_t table_id = 0;
  Range range = MetaCodec::GetFsStatsTableRange();
  KVStorage::TableOption option = {.start_key = range.start, .end_key = range.end};
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    std::cerr << fmt::format("create fs stats table fail, error: {}.", status.error_str()) << '\n';
    return false;
  }

  std::cout << fmt::format("create fs stats table success, start_key({}), end_key({}).",
                           Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key))
            << '\n';

  return true;
}

bool StoreClient::DropMetaTable() {
  Range range = MetaCodec::GetMetaTableRange();
  auto status = kv_storage_->DropTable(range);
  if (!status.ok()) {
    std::cerr << fmt::format("drop meta table fail, error: {}.", status.error_str()) << '\n';
    return false;
  }

  std::cout << "drop meta table success." << '\n';
  return true;
}

bool StoreClient::DropFsStatsTable() {
  Range range = MetaCodec::GetFsStatsTableRange();
  auto status = kv_storage_->DropTable(range);
  if (!status.ok()) {
    std::cerr << fmt::format("drop fs stats table fail, error: {}.", status.error_str()) << '\n';
    return false;
  }

  std::cout << "drop fs stats table success." << '\n';
  return true;
}

bool StoreClient::DropFsMetaTable(uint32_t fs_id) {
  Range range = MetaCodec::GetFsMetaTableRange(fs_id);
  auto status = kv_storage_->DropTable(range);
  if (!status.ok()) {
    std::cerr << fmt::format("drop fs meta table fail, error: {}.", status.error_str()) << '\n';
    return false;
  }

  std::cout << fmt::format("drop fs meta table success, fs_id({}).", fs_id) << '\n';
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
                           pb::mds::FileType_Name(attr.type()), attr.mode(), Helper::FsModeToString(attr.mode()),
                           attr.nlink(), attr.uid(), attr.gid(), attr.length(), FormatTime(attr.ctime()),
                           FormatTime(attr.mtime()), FormatTime(attr.atime()));

  if (dentry.type() == pb::mds::FileType::DIRECTORY) {
    for (auto* child : item->children) {
      TraversePrint(child, is_details, level + 1);
    }
  }
}

void StoreClient::PrintDentryTree(uint32_t fs_id, bool is_details) {
  if (fs_id == 0) {
    std::cerr << "fs_id is invalid.\n";
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
      Helper::ToLowerCase("DropMetaTable"),
      Helper::ToLowerCase("DropFsStatsTable"),
      Helper::ToLowerCase("DropFsMetaTable"),
      "tree",
  };

  if (mds_cmd.count(cmd) == 0) return false;

  if (coor_addr.empty()) {
    std::cerr << "coordinator address is empty." << '\n';
    return false;
  }

  dingofs::mds::client::StoreClient store_client;
  if (!store_client.Init(coor_addr)) {
    std::cerr << "init store client fail." << '\n';
    return false;
  }

  if (cmd == Helper::ToLowerCase("CreateMetaTable")) {
    store_client.CreateMetaTable(options.meta_table_name);

  } else if (cmd == Helper::ToLowerCase("CreateFsStatsTable")) {
    store_client.CreateFsStatsTable(options.fsstats_table_name);

  } else if (cmd == Helper::ToLowerCase("CreateAllTable")) {
    store_client.CreateMetaTable(options.meta_table_name);
    store_client.CreateFsStatsTable(options.fsstats_table_name);

  } else if (cmd == Helper::ToLowerCase("DropMetaTable")) {
    store_client.DropMetaTable();

  } else if (cmd == Helper::ToLowerCase("DropFsStatsTable")) {
    store_client.DropFsStatsTable();

  } else if (cmd == Helper::ToLowerCase("DropFsMetaTable")) {
    if (options.fs_id == 0) {
      std::cerr << "fs_id is invalid." << '\n';
      return true;
    }
    store_client.DropFsMetaTable(options.fs_id);

  } else if (cmd == Helper::ToLowerCase("tree")) {
    store_client.PrintDentryTree(options.fs_id, true);
  }

  return true;
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs