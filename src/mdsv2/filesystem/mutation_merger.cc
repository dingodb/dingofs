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
#include <string>
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "dingofs/error.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/codec.h"

namespace dingofs {
namespace mdsv2 {

// DEFINE_uint32(process_mutation_batch_size, 64, "process mutation batch size.");

static std::string MutationTypeName(Mutation::Type type) {
  switch (type) {
    case Mutation::Type::kFileInode:
      return "FileInode";
    case Mutation::Type::kDirInode:
      return "DirInode";
    case Mutation::Type::kParentWithDentry:
      return "ParentWithDentry";
    default:
      return "Unknown";
  }

  return "Unknown";
}

static std::string MutationOpTypeName(Mutation::OpType op_type) {
  switch (op_type) {
    case Mutation::OpType::kPut:
      return "Put";
    case Mutation::OpType::kDelete:
      return "Delete";
    default:
      return "Unknown";
  }

  return "Unknown";
}

std::string MergeMutation::ToString() const {
  std::string dentry_op_str;
  for (const auto& dentry_op : dentry_ops) {
    dentry_op_str += fmt::format("{}/{}:", MutationOpTypeName(dentry_op.op_type), dentry_op.dentry.ShortDebugString());
  }

  return fmt::format("{} {} inode_op({}/{}), dentry_ops({}), notifications({})", MutationTypeName(type), fs_id,
                     MutationOpTypeName(inode_op.op_type), inode_op.inode.ShortDebugString(), dentry_op_str,
                     notifications.size());
}

MutationMerger::MutationMerger(KVStoragePtr kv_storage) : kv_storage_(kv_storage) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

MutationMerger::~MutationMerger() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

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
    return false;
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
  std::vector<Mutation> mutations;
  mutations.reserve(64);

  while (true) {
    mutations.clear();

    Mutation mutation;
    while (!mutations_.Dequeue(mutation)) {
      bthread_mutex_lock(&mutex_);
      bthread_cond_wait(&cond_, &mutex_);
      bthread_mutex_unlock(&mutex_);
    }

    do {
      mutations.push_back(mutation);
    } while (mutations_.Dequeue(mutation));

    if (is_stop_.load(std::memory_order_relaxed) && mutations.empty()) {
      break;
    }

    std::map<Key, MergeMutation> merge_mutations;
    Merge(mutations, merge_mutations);
    for (auto& [_, merge_mutation] : merge_mutations) {
      LaunchExecuteMutation(merge_mutation);
    }
  }
}

bool MutationMerger::CommitMutation(Mutation& mutation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  mutations_.Enqueue(mutation);

  bthread_cond_signal(&cond_);

  return true;
}

bool MutationMerger::CommitMutation(std::vector<Mutation>& mutations) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  for (auto& mutation : mutations) {
    mutations_.Enqueue(std::move(mutation));
  }

  bthread_cond_signal(&cond_);

  return true;
}

void MergeInode(const Mutation& mutation, MergeMutation& merge_mutation) {
  const auto& inode = mutation.inode_op.inode;
  auto& merge_inode = merge_mutation.inode_op.inode;

  if (inode.atime() > merge_inode.atime()) {
    merge_inode.set_atime(inode.atime());
    merge_inode.set_mtime(inode.mtime());
    merge_inode.set_ctime(inode.ctime());

    merge_inode.set_uid(inode.uid());
    merge_inode.set_gid(inode.gid());

    merge_inode.set_nlink(inode.nlink());

    merge_inode.set_length(inode.length());
    merge_inode.set_mode(inode.mode());
    merge_inode.set_symlink(inode.symlink());
    merge_inode.set_rdev(inode.rdev());
    merge_inode.set_dtime(inode.dtime());
    merge_inode.set_openmpcount(inode.openmpcount());

    *merge_inode.mutable_parent_inos() = inode.parent_inos();

    *merge_inode.mutable_chunks() = inode.chunks();
    *merge_inode.mutable_xattrs() = inode.xattrs();
  }
}

void MutationMerger::Merge(std::vector<Mutation>& mutations, std::map<Key, MergeMutation>& merge_mutations) {
  for (auto& mutation : mutations) {
    const auto& inode_op = mutation.inode_op;
    Key key = {mutation.fs_id, inode_op.inode.ino()};

    auto it = merge_mutations.find(key);
    if (it == merge_mutations.end()) {
      MergeMutation merge_mutation = {mutation.type, mutation.fs_id, inode_op};
      if (mutation.type == Mutation::Type::kParentWithDentry) {
        merge_mutation.dentry_ops.push_back(mutation.dentry_op);
      }

      merge_mutation.notifications.push_back(mutation.notification);

      merge_mutations.insert({key, std::move(merge_mutation)});
      continue;
    }

    MergeMutation& merge_mutation = it->second;

    // if delete, ignore subsequent operations
    if (merge_mutation.inode_op.op_type == Mutation::OpType::kDelete) {
      *mutation.notification.status = Status(pb::error::EINODE_DELETED, "inode deleted.");
      merge_mutation.notifications.push_back(mutation.notification);
      continue;
    }

    merge_mutation.notifications.push_back(mutation.notification);

    merge_mutation.inode_op.op_type = inode_op.op_type;

    if (inode_op.op_type == Mutation::OpType::kPut) {
      switch (mutation.type) {
        case Mutation::Type::kFileInode:
          MergeInode(mutation, merge_mutation);
          break;

        case Mutation::Type::kDirInode:
          MergeInode(mutation, merge_mutation);
          break;

        case Mutation::Type::kParentWithDentry:
          MergeInode(mutation, merge_mutation);
          merge_mutation.dentry_ops.push_back(mutation.dentry_op);
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

            params->self->ExecuteMutation(params->merge_mutation);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    DINGO_LOG(FATAL) << "[mutationmerge] start background thread fail.";
  }
}

void MutationMerger::ExecuteMutation(const MergeMutation& merge_mutation) {
  DINGO_LOG(INFO) << "[merge] execute mutation start, " << merge_mutation.ToString();

  uint64_t start_us = Helper::TimestampUs();

  Status status;
  switch (merge_mutation.type) {
    case Mutation::Type::kFileInode: {
      const auto& inode = merge_mutation.inode_op.inode;

      KeyValue kv;
      kv.opt_type = merge_mutation.inode_op.op_type;
      kv.key = MetaDataCodec::EncodeFileInodeKey(merge_mutation.fs_id, inode.ino());
      kv.value = MetaDataCodec::EncodeFileInodeValue(inode);

      KVStorage::WriteOption option;
      auto status = kv_storage_->Put(option, kv);
      if (!status.ok()) {
        status = Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
      }
    } break;

    case Mutation::Type::kDirInode: {
      const auto& inode = merge_mutation.inode_op.inode;

      KeyValue kv;
      kv.opt_type = merge_mutation.inode_op.op_type;
      kv.key = MetaDataCodec::EncodeDirInodeKey(merge_mutation.fs_id, inode.ino());
      kv.value = MetaDataCodec::EncodeDirInodeValue(inode);

      KVStorage::WriteOption option;
      auto status = kv_storage_->Put(option, kv);
      if (!status.ok()) {
        status = Status(pb::error::EBACKEND_STORE, fmt::format("put inode fail, {}", status.error_str()));
      }
    } break;

    case Mutation::Type::kParentWithDentry: {
      const auto& parent_inode = merge_mutation.inode_op.inode;

      std::vector<KeyValue> kvs;
      kvs.reserve(merge_mutation.dentry_ops.size() + 1);

      KeyValue kv;
      kv.opt_type = merge_mutation.inode_op.op_type;
      kv.key = MetaDataCodec::EncodeDirInodeKey(merge_mutation.fs_id, parent_inode.ino());
      kv.value = MetaDataCodec::EncodeDirInodeValue(parent_inode);
      kvs.push_back(kv);

      for (const auto& dentry_op : merge_mutation.dentry_ops) {
        KeyValue kv;
        kv.opt_type = dentry_op.op_type;
        kv.key = MetaDataCodec::EncodeDentryKey(merge_mutation.fs_id, parent_inode.ino(), dentry_op.dentry.name());
        kv.value = MetaDataCodec::EncodeDentryValue(dentry_op.dentry);

        kvs.push_back(std::move(kv));
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

  DINGO_LOG(INFO) << fmt::format("[merge] execute mutation finish, elapsed_time({}) {}.",
                                 Helper::TimestampUs() - start_us, merge_mutation.ToString());

  for (const auto& notification : merge_mutation.notifications) {
    *notification.status = status;
    notification.count_down_event->signal();
  }
}

}  // namespace mdsv2
}  // namespace dingofs
