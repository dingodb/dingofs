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

#ifndef DINGOFS_MDV2_FILESYSTEM_MUTATION_PROCESSOR_H_
#define DINGOFS_MDV2_FILESYSTEM_MUTATION_PROCESSOR_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

struct Operation {
  enum OpType {
    kCreateInode = 0,
    kDeleteInode = 1,
    kUpdateInodeNlink = 2,
    kUpdateInodeAttr = 3,
    kUpdateInodeXAttr = 4,
    kUpdateInodeChunk = 5,
    kCreateDentry = 6,
    kDeleteDentry = 7,
  };

  static std::string OpTypeName(OpType op_type) {
    switch (op_type) {
      case OpType::kCreateInode:
        return "CreateInode";
      case OpType::kDeleteInode:
        return "DeleteInode";
      case OpType::kUpdateInodeNlink:
        return "UpdateInodeNlink";
      case OpType::kUpdateInodeAttr:
        return "UpdateInodeAttr";
      case OpType::kUpdateInodeXAttr:
        return "UpdateInodeXAttr";
      case OpType::kUpdateInodeChunk:
        return "UpdateInodeChunk";
      case OpType::kCreateDentry:
        return "CreateDentry";
      case OpType::kDeleteDentry:
        return "DeleteDentry";
      default:
        return "Unknown";
    }
  }

  struct CreateInode {
    pb::mdsv2::Inode inode;
  };

  struct DeleteInode {
    int64_t ino;
  };

  struct UpdateInodeNlink {
    uint64_t ino;
    int64_t delta;
    uint64_t time;
  };

  struct UpdateInodeAttr {
    uint32_t to_set{0};
    pb::mdsv2::Inode inode;
  };

  struct UpdateInodeXAttr {
    std::map<std::string, std::string> xattrs;
  };

  struct UpdateInodeChunk {
    uint64_t chunk_index;
    pb::mdsv2::SliceList slice_list;
  };

  struct CreateDentry {
    pb::mdsv2::Dentry dentry;
    uint64_t time;
  };

  struct UpdateDentry {
    pb::mdsv2::Dentry dentry;
    uint64_t time;
  };

  struct DeleteDentry {
    pb::mdsv2::Dentry dentry;
    uint64_t time;
  };

  struct ChangedResult {
    uint64_t version{0};
    uint32_t nlink{0};
  };

  uint64_t txn_id;
  std::string key;

  OpType op_type;
  CreateInode* create_inode{nullptr};
  DeleteInode* delete_inode{nullptr};
  UpdateInodeNlink* update_inode_nlink{nullptr};
  UpdateInodeAttr* update_inode_attr{nullptr};
  UpdateInodeXAttr* update_inode_xattr{nullptr};
  UpdateInodeChunk* update_inode_chunk{nullptr};
  CreateDentry* create_dentry{nullptr};
  UpdateDentry* update_dentry{nullptr};
  DeleteDentry* delete_dentry{nullptr};

  bthread::CountdownEvent* event{nullptr};
  Status status;
  ChangedResult result;
  Trace* trace{nullptr};

  Operation(OpType op_type, uint64_t txn_id, const std::string& key, bthread::CountdownEvent* event, Trace* trace)
      : op_type(op_type), txn_id(txn_id), key(key), event(event), trace(trace) {};
  ~Operation() {
    delete create_inode;
    delete delete_inode;
    delete update_inode_nlink;
    delete update_inode_attr;
    delete update_inode_chunk;
    delete create_dentry;
    delete update_dentry;
    delete delete_dentry;
  }

  void SetCreateInode(pb::mdsv2::Inode&& inode) {
    create_inode = new CreateInode();
    create_inode->inode = std::move(inode);
  }

  void SetDeleteInode(int64_t ino) {
    delete_inode = new DeleteInode();
    delete_inode->ino = ino;
  }

  void SetUpdateInodeNlink(uint64_t ino, int64_t delta, uint64_t time) {
    update_inode_nlink = new UpdateInodeNlink();
    update_inode_nlink->ino = ino;
    update_inode_nlink->delta = delta;
    update_inode_nlink->time = time;
  }

  void SetUpdateInodeAttr(const pb::mdsv2::Inode& inode, uint32_t to_set) {
    update_inode_attr = new UpdateInodeAttr();
    update_inode_attr->to_set = to_set;
    update_inode_attr->inode = inode;
  }

  void SetUpdateInodeXAttr(const std::map<std::string, std::string>& xattrs) {
    update_inode_xattr = new UpdateInodeXAttr();
    update_inode_xattr->xattrs = xattrs;
  }

  void SetUpdateInodeChunk(uint64_t chunk_index, const pb::mdsv2::SliceList& slice_list) {
    update_inode_chunk = new UpdateInodeChunk();
    update_inode_chunk->chunk_index = chunk_index;
    update_inode_chunk->slice_list = slice_list;
  }

  void SetCreateDentry(pb::mdsv2::Dentry&& dentry, uint64_t time) {
    create_dentry = new CreateDentry();
    create_dentry->dentry = std::move(dentry);
    create_dentry->time = time;
  }

  void SetUpdateDentry(pb::mdsv2::Dentry&& dentry, uint64_t time) {
    update_dentry = new UpdateDentry();
    update_dentry->dentry = std::move(dentry);
    update_dentry->time = time;
  }

  void SetDeleteDentry(pb::mdsv2::Dentry&& dentry, uint64_t time) {
    delete_dentry = new DeleteDentry();
    delete_dentry->dentry = std::move(dentry);
    delete_dentry->time = time;
  }
};

// for one target(file inode, dir inode, dentry) mutation
struct TargetMutation {
  std::string key;
  std::vector<Operation*> operations;
};

// for one transaction mutation
struct TxnMutation {
  uint32_t fs_id{0};
  uint64_t txn_id{0};
  std::vector<Operation*> operations;
};

struct MixMutation {
  uint32_t fs_id{0};
  std::vector<Operation*> operations;
};

class MutationProcessor;
using MutationProcessorPtr = std::shared_ptr<MutationProcessor>;

// process mutation
class MutationProcessor {
 public:
  MutationProcessor(KVStoragePtr kv_storage);
  ~MutationProcessor();

  struct Key {
    uint32_t fs_id{0};
    uint64_t txn_id{0};

    bool operator<(const Key& other) const {
      if (fs_id != other.fs_id) {
        return fs_id < other.fs_id;
      }

      return txn_id < other.txn_id;
    }
  };

  static MutationProcessorPtr New(KVStoragePtr kv_storage) { return std::make_shared<MutationProcessor>(kv_storage); }

  bool Init();
  bool Destroy();

  bool Commit(MixMutation& mix_mutation);

 private:
  void ProcessMutation();
  void LaunchExecuteTxnMutation(const TxnMutation& txn_mutation);

  static std::map<Key, TxnMutation> GroupingByTxn(std::vector<MixMutation>& mutations);
  static std::map<std::string, TargetMutation> GroupingByTarget(TxnMutation& mutation);

  void ExecuteTxnMutation(TxnMutation& txn_mutation);
  Status ExecuteUpdateInodeTxnMutation(TxnMutation& txn_mutation);
  Status ExecuteCreateInodeTxnMutation(TxnMutation& txn_mutation);
  Status ExecuteDeleteInodeTxnMutation(TxnMutation& txn_mutation);
  Status ExecuteDentryTxnMutation(TxnMutation& txn_mutation);

  static void ProcessFileInodeOperations(std::vector<Operation*>& operations, pb::mdsv2::Inode& inode,
                                         KeyValue::OpType& op_type);
  static Status ProcessDentryOperations(TxnUPtr& txn, pb::mdsv2::Inode& parent_inode, TargetMutation& target_mutation);

  bthread_t tid_{0};
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  std::atomic<bool> is_stop_{false};

  butil::MPSCQueue<MixMutation> mutations_;

  // persistence store
  KVStoragePtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_MUTATION_PROCESSOR_H_