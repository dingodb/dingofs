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

#include "mdsv2/filesystem/mutation_merger.h"

#include <atomic>
#include <cstdint>
#include <vector>

#include "bthread/bthread.h"
#include "fmt/core.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/codec.h"

namespace dingofs {
namespace mdsv2 {

bool MutationMerger::Init() {
  struct Param {
    MutationMerger* self{nullptr};
  };

  Param* param = new Param({this});

  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid_, &attr,
          [](void* arg) -> void* {
            Param* param = reinterpret_cast<Param*>(arg);
            param->self->ProcessMutation();

            delete param;
            return nullptr;
          },
          param) != 0) {
    delete param;
    DINGO_LOG(FATAL) << "[mutationmerge] start background thread fail.";
  }

  return true;
}

bool MutationMerger::Destroy() {
  is_stop_.store(true);

  if (bthread_stop(tid_) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[mutationmerge] bthread_stop fail.");
  }

  if (bthread_join(tid_, nullptr) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[mutationmerge] bthread_join fail.");
  }

  return true;
}

void MutationMerger::ProcessMutation() {
  while (true) {
    bthread_mutex_lock(&mutex_);
    bthread_cond_wait(&cond_, &mutex_);

    std::vector<Mutation> mutations;
    mutations.reserve(32);
    for (;;) {
      Mutation mutation;
      if (!mutations_.Dequeue(mutation)) {
        break;
      }

      mutations.push_back(mutation);
    }

    if (is_stop_.load(std::memory_order_relaxed) && mutations.empty()) {
      bthread_mutex_unlock(&mutex_);
      break;
    }

    bthread_mutex_unlock(&mutex_);

    // todo: process mutations
    std::map<uint64_t, MergeMutation> out_merge_mutations;
    Merge(mutations, out_merge_mutations);
    for (auto& [ino, merge_mutation] : out_merge_mutations) {
      LaunchExecuteMutation(merge_mutation);
    }
  }
}

bool MutationMerger::CommitMutation(Mutation& mutation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  mutations_.Enqueue(mutation);

  return true;
}

bool MutationMerger::CommitMutation(std::vector<Mutation>& mutations) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  for (auto& mutation : mutations) {
    mutations_.Enqueue(mutation);
  }

  return true;
}

void MergeDirInode(const Mutation& mutation, MergeMutation& merge_mutation) {
  const auto& inode = mutation.inode;
  auto& merge_inode = merge_mutation.inode;

  if (inode.atime() > merge_inode.atime()) {
    merge_inode.set_atime(inode.atime());
  }

  if (inode.mtime() > merge_inode.mtime()) {
    merge_inode.set_mtime(inode.mtime());
  }

  if (inode.ctime() > merge_inode.ctime()) {
    merge_inode.set_ctime(inode.ctime());
  }
}

void MergeFileInode(const Mutation& mutation, MergeMutation& merge_mutation) {
  const auto& inode = mutation.inode;
  auto& merge_inode = merge_mutation.inode;

  if (inode.atime() > merge_inode.atime()) {
    merge_inode.set_atime(inode.atime());
  }

  if (inode.mtime() > merge_inode.mtime()) {
    merge_inode.set_mtime(inode.mtime());
  }

  if (inode.ctime() > merge_inode.ctime()) {
    merge_inode.set_ctime(inode.ctime());
  }

  merge_inode.set_length(inode.length());
  merge_inode.set_mode(inode.mode());
  *merge_inode.mutable_chunks() = inode.chunks();
  *merge_inode.mutable_xattrs() = inode.xattrs();
}

void MutationMerger::Merge(std::vector<Mutation>& mutations, std::map<uint64_t, MergeMutation>& out_merge_mutations) {
  for (auto& mutation : mutations) {
    auto it = out_merge_mutations.find(mutation.inode.ino());
    if (it == out_merge_mutations.end()) {
      MergeMutation merge_mutation = {mutation.type, mutation.fs_id, mutation.inode};
      merge_mutation.dentries.push_back(mutation.dentry);
      merge_mutation.notifications.push_back(mutation.notification);

      out_merge_mutations.insert({mutation.inode.ino(), merge_mutation});
    } else {
      MergeMutation& merge_mutation = it->second;
      merge_mutation.notifications.push_back(mutation.notification);
      switch (mutation.type) {
        case Mutation::Type::kFileInode:
          MergeFileInode(mutation, merge_mutation);
          break;

        case Mutation::Type::kDirInode:
          MergeDirInode(mutation, merge_mutation);
          break;

        case Mutation::Type::kParentInodeWithDentry:
          MergeDirInode(mutation, merge_mutation);
          merge_mutation.dentries.push_back(mutation.dentry);
          break;

        default:
          DINGO_LOG(FATAL) << "unknown mutation type.";
          break;
      }
    }
  }
}

void MutationMerger::LaunchExecuteMutation(const MergeMutation& merge_mutation) {
  struct Params {
    MutationMerger* self{nullptr};
    MergeMutation merge_mutation;
  };

  Params* params = new Params({this, merge_mutation});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_SMALL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);
            MutationMerger* self = params->self;
            self->ExecuteMutation(params->merge_mutation);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    DINGO_LOG(FATAL) << "[mutationmerge] start background thread fail.";
  }
}

void MutationMerger::ExecuteMutation(const MergeMutation& merge_mutation) {
  Status status;
  switch (merge_mutation.type) {
    case Mutation::Type::kFileInode: {
      const auto& inode = merge_mutation.inode;

      KeyValue kv;
      kv.key = MetaDataCodec::EncodeFileInodeKey(merge_mutation.fs_id, inode.ino());
      kv.value = MetaDataCodec::EncodeFileInodeValue(inode);

      KVStorage::WriteOption option;
      auto status = kv_storage_->Put(option, kv);
      if (!status.ok()) {
        status = Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
      }
    } break;

    case Mutation::Type::kParentInodeWithDentry: {
      const auto& parent_inode = merge_mutation.inode;

      std::vector<KeyValue> kvs;
      kvs.reserve(merge_mutation.dentries.size() + 1);

      KeyValue kv;
      kv.key = MetaDataCodec::EncodeFileInodeKey(merge_mutation.fs_id, parent_inode.ino());
      kv.value = MetaDataCodec::EncodeFileInodeValue(parent_inode);
      kvs.push_back(kv);

      for (const auto& dentry : merge_mutation.dentries) {
        KeyValue kv;
        kv.key = MetaDataCodec::EncodeDentryKey(merge_mutation.fs_id, parent_inode.ino(), dentry.name());
        kv.value = MetaDataCodec::EncodeDentryValue(dentry);

        kvs.push_back(kv);
      }

      KVStorage::WriteOption option;
      auto status = kv_storage_->Put(option, kvs);
      if (!status.ok()) {
        status = Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
      }
    } break;

    default:
      DINGO_LOG(FATAL) << "unknown mutation type.";
      break;
  }

  for (const auto& notification : merge_mutation.notifications) {
    *notification.status = status;
    notification.count_down_event->signal();
  }
}

}  // namespace mdsv2
}  // namespace dingofs
