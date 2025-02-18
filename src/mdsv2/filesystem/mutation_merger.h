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
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

struct Mutation {
  enum class Type {
    kFileInode,
    kDirInode,
    kParentInodeWithDentry,
  };

  struct Notification {
    bthread::CountdownEvent* count_down_event;
    Status* status;
  };

  Type type;

  uint32_t fs_id;
  pb::mdsv2::Inode inode;
  pb::mdsv2::Dentry dentry;

  Notification notification;
};

struct MergeMutation {
  Mutation::Type type;

  uint32_t fs_id;
  pb::mdsv2::Inode inode;
  std::vector<pb::mdsv2::Dentry> dentries;

  std::vector<Mutation::Notification> notifications;
};

class MutationMerger {
 public:
  MutationMerger() = default;
  ~MutationMerger() = default;

  bool Init();
  bool Destroy();

  bool CommitMutation(Mutation& mutation);
  bool CommitMutation(std::vector<Mutation>& mutations);

 private:
  void ProcessMutation();

  static void Merge(std::vector<Mutation>& mutations, std::map<uint64_t, MergeMutation>& out_merge_mutations);

  void LaunchExecuteMutation(const MergeMutation& merge_mutation);
  void ExecuteMutation(const MergeMutation& merge_mutation);

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