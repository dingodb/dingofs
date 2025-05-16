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

#include <fmt/format.h>

#include <cstdint>
#include <map>

#include "fmt/core.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace mdsv2 {

DEFINE_int32(fs_scan_batch_size, 10000, "fs scan batch size");

void FreeFsTree(FsTreeNode* root) {
  if (root == nullptr) {
    return;
  }

  for (FsTreeNode* child : root->children) {
    FreeFsTree(child);
  }

  delete root;
}

static FsTreeNode* GenFsTreeStruct(KVStorageSPtr kv_storage, uint32_t fs_id,
                                   std::multimap<uint64_t, FsTreeNode*>& node_map) {
  Range range;
  MetaCodec::GetDentryTableRange(fs_id, range.start_key, range.end_key);

  // scan dentry/attr table
  auto txn = kv_storage->NewTxn();
  std::vector<KeyValue> kvs;
  uint64_t count = 0;
  do {
    kvs.clear();
    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("scan dentry table fail, {}.", status.error_str());
      return nullptr;
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
        uint64_t parent_ino = 0;
        std::string name;
        MetaCodec::DecodeDentryKey(kv.key, fs_id, parent_ino, name);
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

        it = node_map.find(parent_ino);
        if (it != node_map.end()) {
          it->second->children.push_back(item);
        } else {
          if (parent_ino != 0) {
            DINGO_LOG(ERROR) << fmt::format("not found parent({}) for dentry({}/{})", parent_ino, fs_id, name);
          }
        }
      }
    }

    count += kvs.size();
  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  DINGO_LOG(INFO) << fmt::format("scan dentry table kv num({}).", count);

  auto it = node_map.find(1);
  if (it == node_map.end()) {
    DINGO_LOG(ERROR) << "not found root node.";
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

void GenFsTreeJson(FsTreeNode* node, nlohmann::json& doc) {
  doc["ino"] = node->dentry.ino();
  doc["name"] = node->dentry.name();
  doc["type"] = node->dentry.type() == pb::mdsv2::FileType::DIRECTORY ? "directory" : "file";
  // mode,nlink,uid,gid,size,ctime,mtime,atime
  auto& attr = node->attr;
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

std::string FsUtils::GenFsTreeJsonString(uint32_t fs_id) {
  std::multimap<uint64_t, FsTreeNode*> node_map;
  FsTreeNode* root = GenFsTreeStruct(kv_storage_, fs_id, node_map);

  nlohmann::json doc;
  GenFsTreeJson(root, doc);

  return doc.dump();
}

}  // namespace mdsv2
}  // namespace dingofs