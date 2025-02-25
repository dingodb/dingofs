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

#ifndef DINGOFS_MDV2_FILESYSTEM_MUTATION_MERGER_H_
#define DINGOFS_MDV2_FILESYSTEM_MUTATION_MERGER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

struct Mutation {
  using OpType = KeyValue::OpType;

  enum class Type {
    kFileInode,
    kDirInode,
    kParentWithDentry,
  };

  struct InodeOperation {
    OpType op_type{OpType::kPut};
    pb::mdsv2::Inode inode;
  };

  struct DentryOperation {
    OpType op_type{OpType::kPut};
    pb::mdsv2::Dentry dentry;
  };

  struct Notification {
    bthread::CountdownEvent* count_down_event{nullptr};
    Status* status{nullptr};
  };

  Mutation() = default;
  Mutation(uint32_t fs_id, const InodeOperation& inode_op, bthread::CountdownEvent* count_down_event, Status* status)
      : type(inode_op.inode.type() == pb::mdsv2::FileType::DIRECTORY ? Type::kDirInode : Type::kFileInode),
        fs_id(fs_id),
        inode_op(inode_op),
        notification({count_down_event, status}) {}

  Mutation(uint32_t fs_id, const InodeOperation& inode_op, const DentryOperation& dentry_op,
           bthread::CountdownEvent* count_down_event, Status* status)
      : type(Type::kParentWithDentry),
        fs_id(fs_id),
        inode_op(inode_op),
        dentry_op(dentry_op),
        notification({count_down_event, status}) {}

  Type type;
  uint32_t fs_id;
  // maybe file inode or parent inode
  InodeOperation inode_op;
  DentryOperation dentry_op;

  Notification notification;
};

struct MergeMutation {
  Mutation::Type type;

  uint32_t fs_id;
  Mutation::InodeOperation inode_op;
  std::vector<Mutation::DentryOperation> dentry_ops;

  std::vector<Mutation::Notification> notifications;

  std::string ToString() const;
};

class MutationMerger;
using MutationMergerPtr = std::shared_ptr<MutationMerger>;

class MutationMerger {
 public:
  MutationMerger(KVStoragePtr kv_storage);
  ~MutationMerger();

  static MutationMergerPtr New(KVStoragePtr kv_storage) { return std::make_shared<MutationMerger>(kv_storage); }

  struct Key {
    uint32_t fs_id{0};
    uint64_t ino{0};

    bool operator<(const Key& other) const {
      if (fs_id != other.fs_id) {
        return fs_id < other.fs_id;
      }

      return ino < other.ino;
    }
  };

  bool Init();
  bool Destroy();

  bool CommitMutation(Mutation& mutation);
  bool CommitMutation(std::vector<Mutation>& mutations);

  // for unit test
  static void TestMerge(std::vector<Mutation>& mutations, std::map<Key, MergeMutation>& out_merge_mutations) {
    Merge(mutations, out_merge_mutations);
  }

 private:
  void ProcessMutation();

  void LaunchExecuteMutation(const MergeMutation& merge_mutation);
  void ExecuteMutation(const MergeMutation& merge_mutation);

  static void Merge(std::vector<Mutation>& mutations, std::map<Key, MergeMutation>& out_merge_mutations);

  bthread_t tid_;
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  std::atomic<bool> is_stop_{false};

  butil::MPSCQueue<Mutation> mutations_;

  // persistence store
  KVStoragePtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_MUTATION_MERGER_H_