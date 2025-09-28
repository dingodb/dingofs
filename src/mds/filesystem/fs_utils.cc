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

#include "mds/filesystem/fs_utils.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mds/common/codec.h"
#include "mds/common/constant.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace mds {

DEFINE_uint32(mds_scan_batch_size, 10000, "fs scan batch size");

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

static void FreeMap(std::multimap<uint64_t, FsTreeNode*>& node_map) {
  for (auto [_, node] : node_map) {
    delete node;
  }
}

static FsTreeNode* GenFsTreeStruct(OperationProcessorSPtr operation_processor, uint32_t fs_id,
                                   std::multimap<uint64_t, FsTreeNode*>& node_map) {
  uint64_t count = 0;
  Trace trace;
  ScanFsMetaTableOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    uint32_t fs_id = 0;
    uint64_t ino = 0;

    if (MetaCodec::IsInodeKey(key)) {
      MetaCodec::DecodeInodeKey(key, fs_id, ino);
      const AttrEntry attr = MetaCodec::DecodeInodeValue(value);

      // DINGO_LOG(INFO) << fmt::format("attr({}).", attr.ShortDebugString());
      auto it = node_map.find(ino);
      if (it == node_map.end()) {
        node_map.insert({ino, new FsTreeNode{.attr = attr}});
      }
      while (it != node_map.end() && it->first == ino) {
        it->second->attr = attr;
        ++it;
      }

    } else if (MetaCodec::IsDentryKey(key)) {
      // dentry
      uint64_t parent = 0;
      std::string name;
      MetaCodec::DecodeDentryKey(key, fs_id, parent, name);
      pb::mds::Dentry dentry = MetaCodec::DecodeDentryValue(value);

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
          LOG(ERROR) << fmt::format("[fsutils] not found parent({}) for dentry({}/{})", parent, fs_id, name);
        }
      }
    }

    ++count;

    return true;
  });

  auto status = operation_processor->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[fsutils] scan dentry table fail, {}.", status.error_str());
    return nullptr;
  }

  auto it = node_map.find(kRootIno);
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
    if (child->dentry.type() == pb::mds::FileType::DIRECTORY) {
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
  FsTreeNode* root = GenFsTreeStruct(operation_processor_, fs_id, node_map);

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
  doc["type"] = dentry.type() == pb::mds::FileType::DIRECTORY ? "directory" : "file";
  if (fs_info_.partition_policy().type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    doc["node"] = fs_info_.partition_policy().mono().mds_id();
  } else {
    doc["node"] = (dentry.type() == pb::mds::FileType::DIRECTORY) ? hash_router_->GetMDS(attr.ino())
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
  FsTreeNode* root = GenFsTreeStruct(operation_processor_, fs_info_.fs_id(), node_map);
  if (root == nullptr) {
    FreeMap(node_map);
    return "gen fs tree struct fail";
  }

  nlohmann::json doc;
  GenFsTreeJson(root, doc);

  FreeMap(node_map);

  return doc.dump();
}

Status FsUtils::GenRootDirJsonString(std::string& output) {
  const uint32_t fs_id = fs_info_.fs_id();

  Trace trace;
  GetInodeAttrOperation operation(trace, fs_id, kRootIno);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  nlohmann::json doc = nlohmann::json::array();

  nlohmann::json item;
  item["ino"] = attr.ino();
  item["name"] = "/";
  item["type"] = "directory";
  if (fs_info_.partition_policy().type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
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

  output = doc.dump();

  return Status::OK();
}

Status FsUtils::GenDirJsonString(Ino parent, std::string& output) {
  if (parent == kRootParentIno) {
    return GenRootDirJsonString(output);
  }

  const uint32_t fs_id = fs_info_.fs_id();

  std::map<Ino, DentryEntry> dentries;
  Trace trace;
  ScanDentryOperation operation(trace, fs_id, parent, [&](const DentryEntry& dentry) -> bool {
    dentries.insert(std::make_pair(dentry.ino(), dentry));

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) return status;

  // batch get inode attrs
  std::map<Ino, AttrEntry> attrs;
  uint32_t count = 0;
  std::vector<Ino> inoes;
  inoes.reserve(kBatchGetSize);
  for (auto& [ino, dentry] : dentries) {
    inoes.push_back(ino);

    if (++count == dentries.size() || inoes.size() == kBatchGetSize) {
      BatchGetInodeAttrOperation operation(trace, fs_id, inoes);
      status = operation_processor_->RunAlone(&operation);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[fsutils] batch get inode attrs fail, {}.", status.error_str());
        return status;
      }
      auto& result = operation.GetResult();

      if (result.attrs.size() != inoes.size()) {
        DINGO_LOG(WARNING) << fmt::format("[fsutils] batch get attrs size({}) not match ino size({}).",
                                          result.attrs.size(), inoes.size());
      }

      for (const auto& attr : result.attrs) {
        attrs.insert(std::make_pair(attr.ino(), attr));
      }

      inoes.clear();
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
    item["type"] = dentry.type() == pb::mds::FileType::DIRECTORY ? "directory" : "file";
    if (fs_info_.partition_policy().type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
      item["node"] = fs_info_.partition_policy().mono().mds_id();
    } else {
      item["node"] = (dentry.type() == pb::mds::FileType::DIRECTORY) ? hash_router_->GetMDS(dentry.ino())
                                                                     : hash_router_->GetMDS(dentry.parent());
    }

    // mode,nlink,uid,gid,size,ctime,mtime,atime
    item["description"] =
        fmt::format("{},{}/{},{},{},{},{},{},{},{}", attr.version(), attr.mode(), Helper::FsModeToString(attr.mode()),
                    attr.nlink(), attr.uid(), attr.gid(), attr.length(), FormatTime(attr.ctime()),
                    FormatTime(attr.mtime()), FormatTime(attr.atime()));

    doc.push_back(item);
  }

  output = doc.dump();

  return Status::OK();
}

Status FsUtils::GetChunks(uint32_t fs_id, Ino ino, std::vector<ChunkEntry>& chunks) {
  Trace trace;
  ScanChunkOperation operation(trace, fs_id, ino);
  Status status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();
  chunks = std::move(result.chunks);

  return Status::OK();
}

}  // namespace mds
}  // namespace dingofs