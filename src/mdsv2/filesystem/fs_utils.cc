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

#include "fmt/core.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/storage/dingodb_storage.h"
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

// generate file inode map by scan file inode table
static bool GenFileInodeMap(KVStoragePtr kv_storage, uint32_t fs_id,
                            std::map<uint64_t, pb::mdsv2::Inode>& file_inode_map) {
  Range range;
  MetaDataCodec::GetFileInodeTableRange(fs_id, range.start_key, range.end_key);

  auto txn = kv_storage->NewTxn();
  std::vector<KeyValue> kvs;

  do {
    kvs.clear();
    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("scan file inode table fail, {}.", status.error_str());
      return false;
    }

    for (const auto& kv : kvs) {
      uint64_t ino = 0;
      uint32_t fs_id;
      MetaDataCodec::DecodeInodeKey(kv.key, fs_id, ino);
      pb::mdsv2::Inode inode = MetaDataCodec::DecodeInodeValue(kv.value);

      file_inode_map.insert({inode.ino(), inode});
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  DINGO_LOG(INFO) << fmt::format("scan file inode table kv num({}).", file_inode_map.size());

  return true;
}

static FsTreeNode* GenFsTreeStruct(KVStoragePtr kv_storage, uint32_t fs_id,
                                   std::map<uint64_t, FsTreeNode*>& inode_map) {
  // get all file inode
  std::map<uint64_t, pb::mdsv2::Inode> file_inode_map;
  if (!GenFileInodeMap(kv_storage, fs_id, file_inode_map)) {
    return nullptr;
  }

  Range range;
  MetaDataCodec::GetDentryTableRange(fs_id, range.start_key, range.end_key);

  // scan dentry table
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

      if (kv.key.size() == MetaDataCodec::InodeKeyLength()) {
        // dir inode
        MetaDataCodec::DecodeInodeKey(kv.key, fs_id, ino);
        pb::mdsv2::Inode inode = MetaDataCodec::DecodeInodeValue(kv.value);

        DINGO_LOG(INFO) << fmt::format("dir inode({}).", inode.ShortDebugString());
        auto it = inode_map.find(ino);
        if (it != inode_map.end()) {
          it->second->inode = inode;
        } else {
          inode_map.insert({ino, new FsTreeNode{.inode = inode}});
        }

      } else {
        // dentry
        uint64_t parent_ino = 0;
        std::string name;
        MetaDataCodec::DecodeDentryKey(kv.key, fs_id, parent_ino, name);
        pb::mdsv2::Dentry dentry = MetaDataCodec::DecodeDentryValue(kv.value);

        DINGO_LOG(INFO) << fmt::format("dentry({}).", dentry.ShortDebugString());

        FsTreeNode* item = nullptr;
        auto it = inode_map.find(dentry.ino());
        if (it != inode_map.end()) {
          item = it->second;
          item->dentry = dentry;
        } else {
          item = new FsTreeNode{.dentry = dentry};
          inode_map.insert({dentry.ino(), item});
        }

        if (dentry.type() == pb::mdsv2::FileType::FILE || dentry.type() == pb::mdsv2::FileType::SYM_LINK) {
          auto it = file_inode_map.find(dentry.ino());
          if (it != file_inode_map.end()) {
            item->inode = it->second;
          } else {
            DINGO_LOG(ERROR) << fmt::format("not found file inode({}) for dentry({}/{})", dentry.ino(), fs_id, name);
          }
        }

        it = inode_map.find(parent_ino);
        if (it != inode_map.end()) {
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

  auto it = inode_map.find(1);
  if (it == inode_map.end()) {
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

static void FreeOrphan(std::map<uint64_t, FsTreeNode*>& inode_map) {
  for (auto it = inode_map.begin(); it != inode_map.end();) {
    if (it->second->is_orphan) {
      delete it->second;
      it = inode_map.erase(it);
    } else {
      ++it;
    }
  }
}

FsTreeNode* FsUtils::GenFsTree(uint32_t fs_id) {
  std::map<uint64_t, FsTreeNode*> inode_map;
  FsTreeNode* root = GenFsTreeStruct(kv_storage_, fs_id, inode_map);

  LabeledOrphan(root);

  FreeOrphan(inode_map);

  return root;
}

static std::string FormatTime(uint64_t time_ns) { return Helper::FormatTime(time_ns / 1000000000, "%H:%M:%S"); }

void GenFsTreeJson(FsTreeNode* node, nlohmann::json& doc) {
  doc["ino"] = node->dentry.ino();
  doc["name"] = node->dentry.name();
  doc["type"] = node->dentry.type() == pb::mdsv2::FileType::DIRECTORY ? "directory" : "file";
  // mode,nlink,uid,gid,size,ctime,mtime,atime
  auto& inode = node->inode;
  doc["description"] =
      fmt::format("{},{}/{},{},{},{},{},{},{},{}", inode.version(), inode.mode(), Helper::FsModeToString(inode.mode()),
                  inode.nlink(), inode.uid(), inode.gid(), inode.length(), FormatTime(inode.ctime()),
                  FormatTime(inode.mtime()), FormatTime(inode.atime()));

  nlohmann::json children;
  for (FsTreeNode* child : node->children) {
    nlohmann::json child_doc;
    GenFsTreeJson(child, child_doc);
    children.push_back(child_doc);
  }

  doc["children"] = children;
}

std::string FsUtils::GenFsTreeJsonString(uint32_t fs_id) {
  std::map<uint64_t, FsTreeNode*> inode_map;
  FsTreeNode* root = GenFsTreeStruct(kv_storage_, fs_id, inode_map);

  nlohmann::json doc;
  GenFsTreeJson(root, doc);

  return doc.dump();
}

}  // namespace mdsv2
}  // namespace dingofs