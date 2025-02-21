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
  kv_storage_ = DingodbStorage::New();
  CHECK(kv_storage_ != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseCoorAddr(coor_addr);
  if (store_addrs.empty()) {
    return false;
  }

  return kv_storage_->Init(store_addrs);
}

struct Item {
  bool is_orphan{true};
  pb::mdsv2::Dentry dentry;
  pb::mdsv2::Inode inode;

  std::vector<Item*> children;
};

void TraverseFunc(Item* item) {
  for (Item* child : item->children) {
    item->is_orphan = false;
    if (child->dentry.type() == pb::mdsv2::FileType::DIRECTORY) {
      TraverseFunc(child);
    }
  }
}

void StoreClient::TraverseDentryTree(uint32_t fs_id) {
  Range range;
  MetaDataCodec::GetDentryTableRange(fs_id, range.start_key, range.end_key);

  std::vector<KeyValue> kvs;
  auto status = kv_storage_->Scan(range, kvs);

  Item* root = new Item;
  root->dentry.set_ino(1);
  root->dentry.set_name("/");
  root->dentry.set_parent_ino(0);
  root->dentry.set_type(pb::mdsv2::FileType::DIRECTORY);

  std::map<uint64_t, Item*> item_map;
  item_map.insert({0, root});
  for (const auto& kv : kvs) {
    int fs_id = 0;
    uint64_t ino = 0;

    if (kv.key.size() == MetaDataCodec::DirInodeKeyLength()) {
      MetaDataCodec::DecodeDirInodeKey(kv.key, fs_id, ino);
      pb::mdsv2::Inode inode = MetaDataCodec::DecodeDirInodeValue(kv.value);

      auto it = item_map.find(ino);
      if (it != item_map.end()) {
        it->second->inode = inode;
      } else {
        item_map.insert({ino, new Item{.inode = inode}});
      }

    } else {
      uint64_t parent_ino = 0;
      std::string name;
      MetaDataCodec::DecodeDentryKey(kv.key, fs_id, parent_ino, name);
      pb::mdsv2::Dentry dentry = MetaDataCodec::DecodeDentryValue(kv.value);

      Item* item = new Item{.dentry = dentry};
      item_map.insert({dentry.ino(), item});

      auto it = item_map.find(parent_ino);
      if (it != item_map.end()) {
        it->second->children.push_back(item);
      } else {
        DINGO_LOG(ERROR) << fmt::format("not found parent({}) for dentry({}/{})", parent_ino, fs_id, name);
      }
    }
  }

  // traverse tree
  TraverseFunc(root);

  for (auto [ino, item] : item_map) {
    if (item->is_orphan) {
      DINGO_LOG(ERROR) << fmt::format("dentry({}/{}) is orphan.", ino, item->dentry.name());
    }
  }

  // release memory
  for (auto [ino, item] : item_map) {
    delete item;
  }
}

void StoreClient::PrintDentryTree(uint32_t fs_id, bool is_details) { TraverseDentryTree(fs_id); }

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs