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

#include "mdsv2/filesystem/fs_utils.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/type.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace mdsv2 {

DEFINE_int32(fs_scan_batch_size, 10000, "fs scan batch size");

static const uint32_t kBatchGetSize = 1000;

void FreeFsTree(FsTreeNode* root) {
  if (root == nullptr) {
    return;
  }

  for (FsTreeNode* child : root->children) {
    FreeFsTree(child);
  }

  delete root;
}

void FreeMap(std::multimap<uint64_t, FsTreeNode*>& node_map) {
  for (auto [_, node] : node_map) {
    delete node;
  }
}

static Status GetFsInfo(KVStorageSPtr kv_storage, const std::string& fs_name, pb::mdsv2::FsInfo& fs_info) {
  std::string fs_key = MetaCodec::EncodeFSKey(fs_name);
  std::string value;
  Status status = kv_storage->Get(fs_key, value);
  if (!status.ok()) {
    return Status(pb::error::ENOT_FOUND, fmt::format("not found fs({}), {}.", fs_name, status.error_str()));
  }

  fs_info = MetaCodec::DecodeFSValue(value);

  return Status::OK();
}

static FsTreeNode* GenFsTreeStruct(KVStorageSPtr kv_storage, uint32_t fs_id,
                                   std::multimap<uint64_t, FsTreeNode*>& node_map) {
  Range range;
  MetaCodec::GetDentryTableRange(fs_id, range.start_key, range.end_key);

  // scan dentry/attr table
  auto txn = kv_storage->NewTxn();
  std::vector<KeyValue> kvs;
  Status status;
  uint64_t count = 0;
  do {
    kvs.clear();
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[fsutils] scan dentry table fail, {}.", status.error_str());
      break;
    }

    for (const auto& kv : kvs) {
      uint32_t fs_id = 0;
      uint64_t ino = 0;

      if (kv.key.size() == MetaCodec::InodeKeyLength()) {
        MetaCodec::DecodeInodeKey(kv.key, fs_id, ino);
        const AttrType attr = MetaCodec::DecodeInodeValue(kv.value);

        // DINGO_LOG(INFO) << fmt::format("attr({}).", attr.ShortDebugString());
        auto it = node_map.find(ino);
        if (it == node_map.end()) {
          node_map.insert({ino, new FsTreeNode{.attr = attr}});
        }
        while (it != node_map.end() && it->first == ino) {
          it->second->attr = attr;
          ++it;
        }

      } else {
        // dentry
        uint64_t parent = 0;
        std::string name;
        MetaCodec::DecodeDentryKey(kv.key, fs_id, parent, name);
        pb::mdsv2::Dentry dentry = MetaCodec::DecodeDentryValue(kv.value);

        // DINGO_LOG(INFO) << fmt::format("dentry({}).", dentry.ShortDebugString());

        FsTreeNode* item = new FsTreeNode{.dentry = dentry};
        auto it = node_map.find(dentry.ino());
        if (it != node_map.end()) {
          item->attr = it->second->attr;
          if (it->second->dentry.name().empty()) {
            delete it->second;
            node_map.erase(it);
          }
        }
        node_map.insert({dentry.ino(), item});

        it = node_map.find(parent);
        if (it != node_map.end()) {
          it->second->children.push_back(item);
        } else {
          if (parent != 0) {
            DINGO_LOG(ERROR) << fmt::format("[fsutils] not found parent({}) for dentry({}/{})", parent, fs_id, name);
          }
        }
      }
    }

    count += kvs.size();
  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsutils] scan dentry table fail, {}.", status.error_str());
    return nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("[fsutils] scan dentry table kv num({}).", count);

  auto it = node_map.find(1);
  if (it == node_map.end()) {
    DINGO_LOG(ERROR) << "[fsutils] not found root node.";
    return nullptr;
  }

  return it->second;
}

static void LabeledOrphan(FsTreeNode* node) {
  if (node == nullptr) return;

  node->is_orphan = false;
  for (FsTreeNode* child : node->children) {
    child->is_orphan = false;
    if (child->dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
      LabeledOrphan(child);
    }
  }
}

static void FreeOrphan(std::multimap<uint64_t, FsTreeNode*>& node_map) {
  for (auto it = node_map.begin(); it != node_map.end();) {
    if (it->second->is_orphan) {
      DINGO_LOG(INFO) << fmt::format("free orphan dentry({}) attr({}).", it->second->dentry.ShortDebugString(),
                                     it->second->attr.ShortDebugString());
      delete it->second;
      it = node_map.erase(it);
    } else {
      ++it;
    }
  }
}

FsTreeNode* FsUtils::GenFsTree(uint32_t fs_id) {
  std::multimap<uint64_t, FsTreeNode*> node_map;
  FsTreeNode* root = GenFsTreeStruct(kv_storage_, fs_id, node_map);

  LabeledOrphan(root);

  FreeOrphan(node_map);

  return root;
}

static std::string FormatTime(uint64_t time_ns) { return Helper::FormatTime(time_ns / 1000000000, "%H:%M:%S"); }

void FsUtils::GenFsTreeJson(FsTreeNode* node, nlohmann::json& doc) {
  CHECK(node != nullptr) << "node is null";

  const auto& dentry = node->dentry;
  auto& attr = node->attr;
  doc["ino"] = attr.ino();
  doc["name"] = dentry.name();
  doc["type"] = dentry.type() == pb::mdsv2::FileType::DIRECTORY ? "directory" : "file";
  if (fs_info_.partition_policy().type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    doc["node"] = fs_info_.partition_policy().mono().mds_id();
  } else {
    doc["node"] = (dentry.type() == pb::mdsv2::FileType::DIRECTORY) ? hash_router_->GetMDS(attr.ino())
                                                                    : hash_router_->GetMDS(dentry.parent());
  }

  // mode,nlink,uid,gid,size,ctime,mtime,atime
  doc["description"] =
      fmt::format("{},{}/{},{},{},{},{},{},{},{}", attr.version(), attr.mode(), Helper::FsModeToString(attr.mode()),
                  attr.nlink(), attr.uid(), attr.gid(), attr.length(), FormatTime(attr.ctime()),
                  FormatTime(attr.mtime()), FormatTime(attr.atime()));

  nlohmann::json children;
  for (FsTreeNode* child : node->children) {
    nlohmann::json child_doc;
    GenFsTreeJson(child, child_doc);
    children.push_back(child_doc);
  }

  doc["children"] = children;
}

std::string FsUtils::GenFsTreeJsonString() {
  CHECK(!fs_info_.fs_name().empty()) << "fs_info is empty";

  std::multimap<uint64_t, FsTreeNode*> node_map;
  FsTreeNode* root = GenFsTreeStruct(kv_storage_, fs_info_.fs_id(), node_map);
  if (root == nullptr) {
    FreeMap(node_map);
    return "gen fs tree struct fail";
  }

  nlohmann::json doc;
  GenFsTreeJson(root, doc);

  FreeMap(node_map);

  return doc.dump();
}

Status FsUtils::GenRootDirJsonString(std::string& result) {
  const uint32_t fs_id = fs_info_.fs_id();

  auto txn = kv_storage_->NewTxn();

  std::string value;
  auto status = txn->Get(MetaCodec::EncodeInodeKey(fs_id, kRootIno), value);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsutils] get root inode fail, {}.", status.error_str());
    return status;
  }

  auto attr = MetaCodec::DecodeInodeValue(value);

  nlohmann::json doc = nlohmann::json::array();

  nlohmann::json item;
  item["ino"] = attr.ino();
  item["name"] = "/";
  item["type"] = "directory";
  if (fs_info_.partition_policy().type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    item["node"] = fs_info_.partition_policy().mono().mds_id();
  } else {
    item["node"] = hash_router_->GetMDS(attr.ino());
  }

  // mode,nlink,uid,gid,size,ctime,mtime,atime
  item["description"] =
      fmt::format("{},{}/{},{},{},{},{},{},{},{}", attr.version(), attr.mode(), Helper::FsModeToString(attr.mode()),
                  attr.nlink(), attr.uid(), attr.gid(), attr.length(), FormatTime(attr.ctime()),
                  FormatTime(attr.mtime()), FormatTime(attr.atime()));

  doc.push_back(item);

  result = doc.dump();
  return Status::OK();
}

Status FsUtils::GenDirJsonString(Ino parent, std::string& result) {
  if (parent == kRootParentIno) {
    return GenRootDirJsonString(result);
  }

  const uint32_t fs_id = fs_info_.fs_id();
  Range range;
  MetaCodec::EncodeDentryRange(fs_id, parent, range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();

  std::vector<KeyValue> kvs;
  auto status = txn->Scan(range, 100000, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsutils] scan dentry table fail, {}.", status.error_str());
    return status;
  }

  if (kvs.empty()) {
    return Status(pb::error::ENOT_FOUND, "not found kv");
  }

  // add child dentry
  std::map<Ino, DentryType> dentries;
  for (size_t i = 1; i < kvs.size(); ++i) {
    const auto& kv = kvs.at(i);
    auto dentry = MetaCodec::DecodeDentryValue(kv.value);
    dentries.insert(std::make_pair(dentry.ino(), dentry));
  }

  // batch get inode attrs
  std::map<Ino, AttrType> attrs;
  uint32_t count = 0;
  std::vector<std::string> keys;
  keys.reserve(kBatchGetSize);
  for (auto& [ino, dentry] : dentries) {
    keys.push_back(MetaCodec::EncodeInodeKey(fs_id, ino));

    if (++count == dentries.size() || keys.size() == kBatchGetSize) {
      std::vector<KeyValue> inode_kvs;
      status = txn->BatchGet(keys, inode_kvs);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[fsutils] batch get inode attrs fail, {}.", status.error_str());
        return status;
      }

      if (inode_kvs.size() != keys.size()) {
        DINGO_LOG(WARNING) << fmt::format("[fsutils] batch get inode attrs size({}) not match keys size({}).",
                                          inode_kvs.size(), keys.size());
      }

      for (const auto& kv : inode_kvs) {
        auto attr = MetaCodec::DecodeInodeValue(kv.value);
        attrs.insert(std::make_pair(attr.ino(), attr));
      }

      keys.clear();
    }
  }

  // gen json
  nlohmann::json doc = nlohmann::json::array();
  for (auto& [ino, dentry] : dentries) {
    auto it = attrs.find(ino);
    if (it == attrs.end()) {
      DINGO_LOG(ERROR) << fmt::format("[fsutils] not found attr for dentry({}/{})", dentry.ino(), dentry.name());
      continue;
    }

    const auto& attr = it->second;

    nlohmann::json item;
    item["ino"] = dentry.ino();
    item["name"] = dentry.name();
    item["type"] = dentry.type() == pb::mdsv2::FileType::DIRECTORY ? "directory" : "file";
    if (fs_info_.partition_policy().type() == pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
      item["node"] = fs_info_.partition_policy().mono().mds_id();
    } else {
      item["node"] = (dentry.type() == pb::mdsv2::FileType::DIRECTORY) ? hash_router_->GetMDS(dentry.ino())
                                                                       : hash_router_->GetMDS(dentry.parent());
    }

    // mode,nlink,uid,gid,size,ctime,mtime,atime
    item["description"] =
        fmt::format("{},{}/{},{},{},{},{},{},{},{}", attr.version(), attr.mode(), Helper::FsModeToString(attr.mode()),
                    attr.nlink(), attr.uid(), attr.gid(), attr.length(), FormatTime(attr.ctime()),
                    FormatTime(attr.mtime()), FormatTime(attr.atime()));

    doc.push_back(item);
  }

  result = doc.dump();

  return Status::OK();
}

}  // namespace mdsv2
}  // namespace dingofs