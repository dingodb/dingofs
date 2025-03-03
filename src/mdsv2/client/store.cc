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

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/filesystem/codec.h"
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

struct TreeNode {
  bool is_orphan{true};
  pb::mdsv2::Dentry dentry;
  pb::mdsv2::Inode inode;

  std::vector<TreeNode*> children;
};

bool GenFileInodeMap(KVStoragePtr kv_storage, uint32_t fs_id, std::map<uint64_t, pb::mdsv2::Inode>& file_inode_map) {
  Range range;
  MetaDataCodec::GetFileInodeTableRange(fs_id, range.start_key, range.end_key);

  std::vector<KeyValue> kvs;
  auto status = kv_storage->Scan(range, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("scan file inode table fail, {}.", status.error_str());
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("scan file inode table kv num({}).", kvs.size());

  for (const auto& kv : kvs) {
    uint64_t ino = 0;
    int fs_id;
    MetaDataCodec::DecodeFileInodeKey(kv.key, fs_id, ino);
    pb::mdsv2::Inode inode = MetaDataCodec::DecodeFileInodeValue(kv.value);

    file_inode_map.insert({inode.ino(), inode});
  }

  return true;
}

TreeNode* GenDentryTree(KVStoragePtr kv_storage, uint32_t fs_id, std::map<uint64_t, TreeNode*>& tree_inode_map) {
  Range range;
  MetaDataCodec::GetDentryTableRange(fs_id, range.start_key, range.end_key);

  std::vector<KeyValue> kvs;
  auto status = kv_storage->Scan(range, kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("scan dentry table fail, {}.", status.error_str());
    return nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("scan dentry table kv num({}).", kvs.size());

  std::map<uint64_t, pb::mdsv2::Inode> file_inode_map;
  if (!GenFileInodeMap(kv_storage, fs_id, file_inode_map)) {
    return nullptr;
  }

  TreeNode* root = new TreeNode();
  root->dentry.set_ino(1);
  root->dentry.set_name("/");
  root->dentry.set_parent_ino(0);
  root->dentry.set_type(pb::mdsv2::FileType::DIRECTORY);

  tree_inode_map.insert({0, root});
  for (const auto& kv : kvs) {
    int fs_id = 0;
    uint64_t ino = 0;

    if (kv.key.size() == MetaDataCodec::DirInodeKeyLength()) {
      MetaDataCodec::DecodeDirInodeKey(kv.key, fs_id, ino);
      pb::mdsv2::Inode inode = MetaDataCodec::DecodeDirInodeValue(kv.value);

      DINGO_LOG(INFO) << fmt::format("dir inode({}).", inode.ShortDebugString());
      auto it = tree_inode_map.find(ino);
      if (it != tree_inode_map.end()) {
        it->second->inode = inode;
      } else {
        tree_inode_map.insert({ino, new TreeNode{.inode = inode}});
      }

    } else {
      uint64_t parent_ino = 0;
      std::string name;
      MetaDataCodec::DecodeDentryKey(kv.key, fs_id, parent_ino, name);
      pb::mdsv2::Dentry dentry = MetaDataCodec::DecodeDentryValue(kv.value);

      TreeNode* item = nullptr;
      auto it = tree_inode_map.find(dentry.ino());
      if (it != tree_inode_map.end()) {
        item = it->second;
        item->dentry = dentry;
      } else {
        item = new TreeNode{.dentry = dentry};
        tree_inode_map.insert({dentry.ino(), item});
      }

      if (dentry.type() == pb::mdsv2::FileType::FILE || dentry.type() == pb::mdsv2::FileType::SYM_LINK) {
        auto it = file_inode_map.find(dentry.ino());
        if (it != file_inode_map.end()) {
          item->inode = it->second;
        } else {
          DINGO_LOG(ERROR) << fmt::format("not found file inode({}) for dentry({}/{})", dentry.ino(), fs_id, name);
        }
      }

      it = tree_inode_map.find(parent_ino);
      if (it != tree_inode_map.end()) {
        it->second->children.push_back(item);
      } else {
        DINGO_LOG(ERROR) << fmt::format("not found parent({}) for dentry({}/{})", parent_ino, fs_id, name);
      }
    }
  }

  return root;
}

void TraverseFunc(TreeNode* item) {
  for (TreeNode* child : item->children) {
    child->is_orphan = false;
    if (child->dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
      TraverseFunc(child);
    }
  }
}

std::string FormatTime(uint64_t time_ns) { return Helper::FormatMsTime(time_ns / 1000000, "%H:%M:%S"); }

void TraversePrint(TreeNode* item, bool is_details, int level) {
  for (TreeNode* child : item->children) {
    for (int i = 0; i < level; i++) {
      std::cout << "  ";
    }

    auto& inode = child->inode;

    std::cout << fmt::format("{}({}) type({}) gid({}) uid({}) mode({}) nlink({}) time({}/{}/{})", child->dentry.name(),
                             child->dentry.ino(), pb::mdsv2::FileType_Name(inode.type()), inode.gid(), inode.uid(),
                             inode.mode(), inode.nlink(), FormatTime(inode.ctime()), FormatTime(inode.mtime()),
                             FormatTime(inode.atime()))
              << '\n';
    if (child->dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
      TraversePrint(child, is_details, level + 1);
    }
  }
}

void StoreClient::PrintDentryTree(uint32_t fs_id, bool is_details) {
  std::map<uint64_t, TreeNode*> tree_inode_map;
  TreeNode* root = GenDentryTree(kv_storage_, fs_id, tree_inode_map);
  if (root == nullptr) {
    return;
  }

  TraversePrint(root, is_details, 0);

  // release memory
  for (auto [ino, item] : tree_inode_map) {
    delete item;
  }
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs