
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

#include "mdsv2/filesystem/mutation_processor.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_uint32(process_mutation_batch_size, 64, "process mutation batch size.");
DEFINE_uint32(txn_max_retry_times, 5, "txn max retry times.");

DEFINE_uint32(merge_mutation_delay_us, 0, "merge mutation delay us.");

static void SetError(TargetMutation& target_mutation, Status& status) {
  for (auto* operation : target_mutation.operations) {
    if (operation->status.ok()) {
      operation->status = status;
    }
  }
}

static void SetError(TxnMutation& txn_mutation, Status& status) {
  for (auto* operation : txn_mutation.operations) {
    if (operation->status.ok()) {
      operation->status = status;
    }
  }
}

MutationProcessor::MutationProcessor(KVStoragePtr kv_storage) : kv_storage_(kv_storage) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

MutationProcessor::~MutationProcessor() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool MutationProcessor::Init() {
  struct Param {
    MutationProcessor* self{nullptr};
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
    tid_ = 0;
    delete param;
    LOG(FATAL) << "[mutation] start background thread fail.";
    return false;
  }

  return true;
}

bool MutationProcessor::Destroy() {
  is_stop_.store(true);

  if (tid_ > 0) {
    bthread_cond_signal(&cond_);

    if (bthread_stop(tid_) != 0) {
      LOG(ERROR) << fmt::format("[mutation] bthread_stop fail.");
    }

    if (bthread_join(tid_, nullptr) != 0) {
      LOG(ERROR) << fmt::format("[mutation] bthread_join fail.");
    }
  }

  return true;
}

bool MutationProcessor::Commit(MixMutation& mix_mutation) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  // check notification
  for (auto* operation : mix_mutation.operations) {
    CHECK(operation->event != nullptr) << "count down event is null.";
    CHECK(operation->trace != nullptr) << "trace is null.";

    operation->trace->UpdateLastTime();
  }

  mutations_.Enqueue(mix_mutation);

  bthread_cond_signal(&cond_);

  return true;
}

void MutationProcessor::ProcessMutation() {
  std::vector<MixMutation> mix_mutations;
  mix_mutations.reserve(FLAGS_process_mutation_batch_size);

  while (true) {
    mix_mutations.clear();

    MixMutation mix_mutation;
    while (!mutations_.Dequeue(mix_mutation) && !is_stop_.load(std::memory_order_relaxed)) {
      bthread_mutex_lock(&mutex_);
      bthread_cond_wait(&cond_, &mutex_);
      bthread_mutex_unlock(&mutex_);
    }

    if (is_stop_.load(std::memory_order_relaxed) && mix_mutation.operations.empty()) {
      break;
    }

    if (FLAGS_merge_mutation_delay_us > 0) {
      bthread_usleep(FLAGS_merge_mutation_delay_us);
    }

    do {
      mix_mutations.push_back(mix_mutation);
    } while (mutations_.Dequeue(mix_mutation));

    auto txn_mutation_map = GroupingByTxn(mix_mutations);
    for (auto& [_, txn_mutation] : txn_mutation_map) {
      LaunchExecuteTxnMutation(txn_mutation);
    }
  }
}

void MutationProcessor::LaunchExecuteTxnMutation(const TxnMutation& txn_mutation) {
  struct Params {
    MutationProcessor* self{nullptr};
    TxnMutation txn_mutation;
  };

  Params* params = new Params({.self = this, .txn_mutation = txn_mutation});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_SMALL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);

            params->self->ExecuteTxnMutation(params->txn_mutation);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    LOG(FATAL) << "[mutation] start background thread fail.";
  }
}

bool IsFileInodeTxn(Operation::OpType op_type) {
  switch (op_type) {
    case Operation::OpType::kCreateInode:
    case Operation::OpType::kDeleteInode:
    case Operation::OpType::kUpdateInodeNlink:
    case Operation::OpType::kUpdateInodeAttr:
    case Operation::OpType::kUpdateInodeXAttr:
    case Operation::OpType::kUpdateInodeChunk:
      return true;

    case Operation::OpType::kCreateDentry:
    case Operation::OpType::kDeleteDentry:
      return false;

    default:
      LOG(FATAL) << fmt::format("unknown operation type({}).", Operation::OpTypeName(op_type));
      break;
  }
}

void MutationProcessor::ExecuteTxnMutation(TxnMutation& txn_mutation) {
  auto op_type = txn_mutation.operations.front()->op_type;
  bool is_file_txn = IsFileInodeTxn(op_type);

  for (auto* operation : txn_mutation.operations) {
    operation->trace->SetPendingTime(is_file_txn);
  }

  uint64_t start_time = Helper::TimestampUs();
  LOG(INFO) << fmt::format("[mutation] txn({}/{}) mutation.", txn_mutation.fs_id, txn_mutation.txn_id);

  Status status;
  switch (op_type) {
    case Operation::OpType::kCreateInode:
      status = ExecuteCreateInodeTxnMutation(txn_mutation);
      break;

    case Operation::OpType::kDeleteInode:
      status = ExecuteDeleteInodeTxnMutation(txn_mutation);
      break;

    case Operation::OpType::kUpdateInodeNlink:
    case Operation::OpType::kUpdateInodeAttr:
    case Operation::OpType::kUpdateInodeXAttr:
    case Operation::OpType::kUpdateInodeChunk:
      status = ExecuteUpdateInodeTxnMutation(txn_mutation);
      break;

    case Operation::OpType::kCreateDentry:
    case Operation::OpType::kDeleteDentry:
      status = ExecuteDentryTxnMutation(txn_mutation);
      break;

    default:
      LOG(FATAL) << fmt::format("unknown operation type({}).", Operation::OpTypeName(op_type));
      break;
  }

  LOG(INFO) << fmt::format("[mutation] txn({}/{}) mutation finish, elapsed({}us)  error({} {}).", txn_mutation.fs_id,
                           txn_mutation.txn_id, Helper::TimestampUs() - start_time, status.error_code(),
                           status.error_str());

  if (!status.ok()) {
    SetError(txn_mutation, status);
  }

  // notify operation finish
  for (auto* operation : txn_mutation.operations) {
    operation->event->signal();
  }
}

static void SetAttr(const pb::mdsv2::Inode& inode, uint32_t to_set, pb::mdsv2::Inode& dst_inode) {
  if (to_set & kSetAttrMode) {
    dst_inode.set_mode(inode.mode());
  }

  if (to_set & kSetAttrUid) {
    dst_inode.set_uid(inode.uid());
  }

  if (to_set & kSetAttrGid) {
    dst_inode.set_gid(inode.gid());
  }

  if (to_set & kSetAttrLength) {
    dst_inode.set_length(inode.length());
  }

  if (to_set & kSetAttrAtime) {
    dst_inode.set_atime(inode.atime());
  }

  if (to_set & kSetAttrMtime) {
    dst_inode.set_mtime(inode.mtime());
  }

  if (to_set & kSetAttrCtime) {
    dst_inode.set_ctime(inode.ctime());
  }

  if (to_set & kSetAttrNlink) {
    dst_inode.set_nlink(inode.nlink());
  }
}

void MutationProcessor::ProcessFileInodeOperations(std::vector<Operation*>& operations, pb::mdsv2::Inode& inode,
                                                   KeyValue::OpType& op_type) {
  bool exist_inode = (inode.ino() != 0);
  for (auto& operation : operations) {
    switch (operation->op_type) {
      case Operation::OpType::kUpdateInodeNlink: {
        if (exist_inode) {
          auto* param = operation->update_inode_nlink;
          inode.set_nlink(inode.nlink() + param->delta);
          inode.set_ctime(std::max(inode.ctime(), param->time));
          inode.set_mtime(std::max(inode.mtime(), param->time));

        } else {
          operation->status = Status(pb::error::ENOT_FOUND, "inode not found");
        }
      } break;

      case Operation::OpType::kUpdateInodeAttr: {
        if (exist_inode) {
          auto* param = operation->update_inode_attr;
          SetAttr(param->inode, param->to_set, inode);

        } else {
          operation->status = Status(pb::error::ENOT_FOUND, "inode not found");
        }
      } break;

      case Operation::OpType::kUpdateInodeXAttr: {
        if (exist_inode) {
          auto* param = operation->update_inode_xattr;
          for (const auto& [key, value] : param->xattrs) {
            (*inode.mutable_xattrs())[key] = value;
          }

        } else {
          operation->status = Status(pb::error::ENOT_FOUND, "inode not found");
        }
      } break;

      case Operation::OpType::kUpdateInodeChunk: {
        if (exist_inode) {
          auto* param = operation->update_inode_chunk;
          auto it = inode.mutable_chunks()->find(param->chunk_index);
          if (it == inode.chunks().end()) {
            inode.mutable_chunks()->insert({param->chunk_index, param->slice_list});
          } else {
            it->second.MergeFrom(param->slice_list);
          }

        } else {
          operation->status = Status(pb::error::ENOT_FOUND, "inode not found");
        }

      } break;

      case Operation::OpType::kDeleteInode: {
        op_type = KeyValue::OpType::kDelete;
        exist_inode = false;
      } break;

      default:
        LOG(FATAL) << fmt::format("unknown operation type({}).", static_cast<int>(operation->op_type));
        break;
    }
  }
}

Status MutationProcessor::ExecuteCreateInodeTxnMutation(TxnMutation& txn_mutation) {
  CHECK(txn_mutation.operations.size() == 1)
      << fmt::format("create inode mutation map size({}) is wrong.", txn_mutation.operations.size());

  auto* operation = txn_mutation.operations.front();
  CHECK(operation->op_type == Operation::OpType::kCreateInode)
      << fmt::format("op type({}) is wrong.", Operation::OpTypeName(operation->op_type));

  auto& param = operation->create_inode;

  LOG(INFO) << fmt::format("[mutation] txn({}/{}), create inode({}).", txn_mutation.fs_id, txn_mutation.txn_id,
                           param->inode.ShortDebugString());

  std::string value = MetaDataCodec::EncodeInodeValue(param->inode);
  // trace txn
  auto* trace = operation->trace;
  auto& trace_txn = trace->GetFileTxn();

  uint32_t retry = 0;
  Status status;
  do {
    auto txn = kv_storage_->NewTxn();

    status = txn->Put(operation->key, value);
    CHECK(status.ok()) << fmt::format("put inode fail, key({}), error({} {}).", operation->key, status.error_code(),
                                      status.error_str());

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.txn_id = txn_mutation.txn_id;
  trace_txn.retry = retry;

  return status;
}

Status MutationProcessor::ExecuteDeleteInodeTxnMutation(TxnMutation& txn_mutation) {
  CHECK(txn_mutation.operations.size() == 1)
      << fmt::format("delete inode mutation map size({}) is wrong.", txn_mutation.operations.size());

  auto* operation = txn_mutation.operations.front();
  CHECK(operation->op_type == Operation::OpType::kDeleteInode)
      << fmt::format("op type({}) is wrong.", Operation::OpTypeName(operation->op_type));

  auto& param = operation->delete_inode;
  LOG(INFO) << fmt::format("[mutation] txn({}/{}), delete inode({}).", txn_mutation.fs_id, txn_mutation.txn_id,
                           param->ino);

  // trace txn
  auto* trace = operation->trace;
  auto& trace_txn = trace->GetFileTxn();

  uint32_t retry = 0;
  Status status;
  do {
    auto txn = kv_storage_->NewTxn();

    status = txn->Delete(operation->key);
    CHECK(status.ok()) << fmt::format("delete inode fail, key({}), error({} {}).", operation->key, status.error_code(),
                                      status.error_str());

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.txn_id = txn_mutation.txn_id;
  trace_txn.retry = retry;

  return status;
}

Status MutationProcessor::ExecuteUpdateInodeTxnMutation(TxnMutation& txn_mutation) {
  auto target_mutation_map = GroupingByTarget(txn_mutation);
  CHECK(target_mutation_map.size() == 1) << fmt::format("file inode target mutation map size({}) is wrong.",
                                                        target_mutation_map.size());

  auto it = target_mutation_map.begin();
  auto& target_mutation = it->second;

  // trace txn
  Trace::Txn trace_txn;

  pb::mdsv2::Inode inode;
  uint32_t retry = 0;
  Status status;
  do {
    auto txn = kv_storage_->NewTxn();

    std::string value;
    status = txn->Get(target_mutation.key, value);
    if (!status.ok() && status.error_code() != pb::error::ENOT_FOUND) {
      return status;
    }

    KeyValue::OpType op_type = KeyValue::OpType::kPut;
    inode = value.empty() ? pb::mdsv2::Inode() : MetaDataCodec::DecodeInodeValue(value);
    inode.set_version(inode.version() + 1);
    ProcessFileInodeOperations(target_mutation.operations, inode, op_type);

    LOG(INFO) << fmt::format("[mutation] {} file inode({}).", KeyValue::OpTypeName(op_type), inode.ShortDebugString());

    status = (op_type == KeyValue::OpType::kPut) ? txn->Put(target_mutation.key, MetaDataCodec::EncodeInodeValue(inode))
                                                 : txn->Delete(target_mutation.key);
    CHECK(status.ok()) << fmt::format("update inode fail, key({}), error({} {}).", target_mutation.key,
                                      status.error_code(), status.error_str());
    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.txn_id = txn_mutation.txn_id;
  trace_txn.retry = retry;
  for (auto* operation : txn_mutation.operations) {
    operation->result.version = inode.version();
    operation->result.nlink = inode.nlink();
    operation->trace->GetFileTxn() = trace_txn;
  }

  return status;
}

Status MutationProcessor::ExecuteDentryTxnMutation(TxnMutation& txn_mutation) {
  auto target_mutation_map = GroupingByTarget(txn_mutation);
  CHECK(!target_mutation_map.empty()) << "dentry target mutation map is empty.";

  Trace::Txn trace_txn;

  pb::mdsv2::Inode parent_inode;
  uint32_t retry = 0;
  Status status;
  do {
    auto txn = kv_storage_->NewTxn();

    std::string value;
    std::string parent_key = MetaDataCodec::EncodeInodeKey(txn_mutation.fs_id, txn_mutation.txn_id);
    auto status = txn->Get(parent_key, value);
    if (!status.ok()) {
      break;
    }

    parent_inode = MetaDataCodec::DecodeInodeValue(value);
    parent_inode.set_version(parent_inode.version() + 1);

    bool is_update_parent = false;
    for (auto& [_, target_mutation] : target_mutation_map) {
      auto status = ProcessDentryOperations(txn, parent_inode, target_mutation);
      if (!status.ok()) {
        SetError(target_mutation, status);
      } else {
        is_update_parent = true;
      }
    }

    if (is_update_parent) {
      status = txn->Put(parent_key, MetaDataCodec::EncodeInodeValue(parent_inode));
      CHECK(status.ok()) << fmt::format("put fail, key({}), error({} {}).", parent_key, status.error_code(),
                                        status.error_str());
    }

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.txn_id = txn_mutation.txn_id;
  trace_txn.retry = retry;
  for (auto* operation : txn_mutation.operations) {
    operation->result.version = parent_inode.version();
    operation->result.nlink = parent_inode.nlink();
    operation->trace->GetTxn() = trace_txn;
  }

  return status;
}

Status MutationProcessor::ProcessDentryOperations(TxnUPtr& txn, pb::mdsv2::Inode& parent_inode,
                                                  TargetMutation& target_mutation) {
  bool has_value = false;
  pb::mdsv2::Dentry dentry;

  int change_nlink = 0;
  uint64_t change_time = 0;

  KeyValue::OpType op_type = KeyValue::OpType::kPut;
  for (auto* operation : target_mutation.operations) {
    switch (operation->op_type) {
      case Operation::OpType::kCreateDentry: {
        if (has_value) {
          operation->status = Status(pb::error::EEXISTED, "dentry already exist.");
        } else {
          auto& param = operation->create_dentry;
          dentry = param->dentry;
          has_value = true;

          change_nlink += 1;
          change_time = std::max(change_time, param->time);
        }
      } break;

      case Operation::OpType::kDeleteDentry: {
        auto& param = operation->delete_dentry;
        dentry = param->dentry;

        has_value = false;
        op_type = KeyValue::OpType::kDelete;

        change_nlink -= 1;
        change_time = std::max(change_time, param->time);
      } break;

      default:
        LOG(FATAL) << fmt::format("unknown operation type({}).", static_cast<int>(operation->op_type));
        break;
    }
  }

  LOG(INFO) << fmt::format("[mutation] txn({}/{}) {} dentry({}).", dentry.fs_id(), dentry.parent_ino(),
                           KeyValue::OpTypeName(op_type), dentry.ShortDebugString());

  auto status = (op_type == KeyValue::OpType::kPut)
                    ? txn->Put(target_mutation.key, MetaDataCodec::EncodeDentryValue(dentry))
                    : txn->Delete(target_mutation.key);
  if (!status.ok()) {
    return status;
  }

  parent_inode.set_nlink(parent_inode.nlink() + change_nlink);
  parent_inode.set_atime(std::max(parent_inode.atime(), change_time));
  parent_inode.set_mtime(std::max(parent_inode.mtime(), change_time));

  return Status::OK();
}

std::map<MutationProcessor::Key, TxnMutation> MutationProcessor::GroupingByTxn(
    std::vector<MixMutation>& mix_mutations) {
  std::map<Key, TxnMutation> mutation_map;

  for (auto& mix_mutation : mix_mutations) {
    for (auto* operation : mix_mutation.operations) {
      Key key = {.fs_id = mix_mutation.fs_id, .txn_id = operation->txn_id};

      auto it = mutation_map.find(key);
      if (it == mutation_map.end()) {
        mutation_map.insert(
            {key, {.fs_id = mix_mutation.fs_id, .txn_id = operation->txn_id, .operations = {operation}}});
      } else {
        it->second.operations.push_back(operation);
      }
    }
  }

  return std::move(mutation_map);
}

std::map<std::string, TargetMutation> MutationProcessor::GroupingByTarget(TxnMutation& txn_mutation) {
  std::map<std::string, TargetMutation> target_mutation_map;

  for (auto& operation : txn_mutation.operations) {
    const std::string& key = operation->key;

    auto it = target_mutation_map.find(key);
    if (it == target_mutation_map.end()) {
      std::vector<Operation*> operations;
      operations.push_back(operation);
      TargetMutation target_mutation = {.key = key, .operations = operations};
      target_mutation_map.insert({key, target_mutation});

    } else {
      it->second.operations.push_back(operation);
    }
  }

  return std::move(target_mutation_map);
}

}  // namespace mdsv2
}  // namespace dingofs