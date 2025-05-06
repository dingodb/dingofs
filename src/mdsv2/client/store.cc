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

bool StoreClient::CreateLockTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetLockTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create lock table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create lock table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateHeartbeatTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetHeartbeatTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create heartbeat table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create heartbeat table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateFsTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFsTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create fs table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create fs table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateFsQuotaTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetQuotaTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create fs quota table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create fs quota table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateFsStatsTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFsStatsTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create fs stats table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create fs stats table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateFileSessionTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetFileSessionTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create file session table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create file session table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateTrashChunkTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetTrashChunkTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create trash chunk table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create trash chunk table success, start_key({}), end_key({}).",
                                 Helper::StringToHex(option.start_key), Helper::StringToHex(option.end_key));

  return true;
}

bool StoreClient::CreateDelFileTable(const std::string& name) {
  int64_t table_id = 0;
  KVStorage::TableOption option;
  MetaDataCodec::GetDelFileTableRange(option.start_key, option.end_key);
  auto status = kv_storage_->CreateTable(name, option, table_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("create del file table fail, error: {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("create del file table success, start_key({}), end_key({}).",
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
  auto& inode = item->inode;

  std::cout << fmt::format("{} [{},{},{}/{},{},{},{},{},{},{},{}]\n", dentry.name(), dentry.ino(),
                           pb::mdsv2::FileType_Name(inode.type()), inode.mode(), Helper::FsModeToString(inode.mode()),
                           inode.nlink(), inode.uid(), inode.gid(), inode.length(), FormatTime(inode.ctime()),
                           FormatTime(inode.mtime()), FormatTime(inode.atime()));

  if (dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
    for (auto* child : item->children) {
      TraversePrint(child, is_details, level + 1);
    }
  }
}

void StoreClient::PrintDentryTree(uint32_t fs_id, bool is_details) {
  FsUtils fs_utils(kv_storage_);

  FsTreeNode* root = fs_utils.GenFsTree(fs_id);
  if (root == nullptr) {
    return;
  }

  std::cout << "############ name [ino,type,mode,nlink,uid,gid,size,ctime,mtime,atime] ############\n";
  TraversePrint(root, is_details, 0);

  FreeFsTree(root);
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs