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

#include "mdsv2/filesystem/quota.h"

#include <fmt/format.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mdsv2/filesystem/codec.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

const std::string kQuotaWorkerSetName = "QUOTA_WORKER_SET";

DEFINE_uint32(quota_worker_num, 128, "quota service worker num");
DEFINE_uint32(quota_worker_max_pending_num, 1024, "quota service worker num");

DECLARE_uint32(txn_max_retry_times);
DECLARE_int32(fs_scan_batch_size);

void QuotaTask::Run() {
  Context ctx;
  switch (type) {
    case Type::kSetFsQuota: {
      quota_processor->SetFsQuota(ctx, fs_id, quota);
    } break;

    case Type::kDeleteFsQuota: {
      quota_processor->DeleteFsQuota(ctx, fs_id);
    } break;

    case Type::kFlushFsUsage: {
      quota_processor->FlushFsUsage(ctx, fs_id, usage);
    } break;

    case Type::kSetDirQuota: {
      quota_processor->SetDirQuota(ctx, fs_id, ino, quota);
    } break;

    case Type::kDeleteDirQuota: {
      quota_processor->DeleteDirQuota(ctx, fs_id, ino);
    } break;

    case Type::kFlushDirUsages: {
      quota_processor->FlushDirUsages(ctx, fs_id, usages);
    } break;

    default:
      DINGO_LOG(FATAL) << "unknown quota task type: " << static_cast<int>(type);
      break;
  }
}

QuotaProcessor::QuotaProcessor(KVStoragePtr kv_storage) : kv_storage_(kv_storage) {
  worker_set_ =
      ExecqWorkerSet::NewUnique(kQuotaWorkerSetName, FLAGS_quota_worker_num, FLAGS_quota_worker_max_pending_num);
}

bool QuotaProcessor::Init() {
  if (!worker_set_->Init()) {
    DINGO_LOG(ERROR) << "[quota] init quota worker set fail.";
    return false;
  }

  return true;
}

void QuotaProcessor::Destroy() {
  if (worker_set_ != nullptr) {
    worker_set_->Destroy();
  }
}

QuotaProcessorPtr QuotaProcessor::GetSelfPtr() { return std::dynamic_pointer_cast<QuotaProcessor>(shared_from_this()); }

Status QuotaProcessor::SetFsQuota(Context& ctx, uint32_t fs_id, const Quota& quota) {
  const std::string key = MetaDataCodec::EncodeFsQuotaKey(fs_id);

  auto& trace_txn = ctx.GetTrace().GetTxn();

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    KVStorage::WriteOption option;
    txn->Put(key, MetaDataCodec::EncodeFsQuotaValue(quota));

    auto status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return Status::OK();
}

Status QuotaProcessor::GetFsQuota(Context& ctx, uint32_t fs_id, Quota& quota) {
  const std::string key = MetaDataCodec::EncodeFsQuotaKey(fs_id);

  auto& trace_txn = ctx.GetTrace().GetTxn();

  auto txn = kv_storage_->NewTxn();

  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return status;
  }

  status = txn->Commit();
  trace_txn = txn->GetTrace();
  if (!status.ok()) {
    return status;
  }

  quota = MetaDataCodec::DecodeFsQuotaValue(value);

  return Status::OK();
}

Status QuotaProcessor::FlushFsUsage(Context& ctx, uint32_t fs_id, const Usage& usage) {
  const std::string key = MetaDataCodec::EncodeFsQuotaKey(fs_id);

  auto& trace_txn = ctx.GetTrace().GetTxn();

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    std::string value;
    auto status = txn->Get(key, value);
    if (!status.ok()) {
      return status;
    }

    Quota quota = MetaDataCodec::DecodeFsQuotaValue(value);
    quota.set_used_bytes(quota.used_bytes() + usage.bytes());
    quota.set_used_inodes(quota.used_inodes() + usage.inodes());

    txn->Put(key, MetaDataCodec::EncodeFsQuotaValue(quota));

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return Status::OK();
}

Status QuotaProcessor::DeleteFsQuota(Context& ctx, uint32_t fs_id) {
  const std::string key = MetaDataCodec::EncodeFsQuotaKey(fs_id);

  auto& trace_txn = ctx.GetTrace().GetTxn();

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    txn->Delete(key);

    auto status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return Status::OK();
}

Status QuotaProcessor::SetDirQuota(Context& ctx, uint32_t fs_id, uint64_t ino, const Quota& quota) {
  const std::string key = MetaDataCodec::EncodeDirQuotaKey(fs_id, ino);
  auto& trace_txn = ctx.GetTrace().GetTxn();

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    txn->Put(key, MetaDataCodec::EncodeDirQuotaValue(quota));

    auto status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;

  } while (retry < FLAGS_txn_max_retry_times);

  return Status::OK();
}

Status QuotaProcessor::GetDirQuota(Context& ctx, uint32_t fs_id, uint64_t ino, Quota& quota) {
  const std::string key = MetaDataCodec::EncodeDirQuotaKey(fs_id, ino);
  auto& trace_txn = ctx.GetTrace().GetTxn();

  int retry = 0;
  auto txn = kv_storage_->NewTxn();

  std::string value;
  auto status = txn->Get(key, value);
  if (!status.ok()) {
    return status;
  }

  status = txn->Commit();
  trace_txn = txn->GetTrace();
  if (!status.ok()) {
    return status;
  }

  quota = MetaDataCodec::DecodeDirQuotaValue(value);

  return Status::OK();
}

Status QuotaProcessor::DeleteDirQuota(Context& ctx, uint32_t fs_id, uint64_t ino) {
  const std::string key = MetaDataCodec::EncodeDirQuotaKey(fs_id, ino);

  auto& trace_txn = ctx.GetTrace().GetTxn();

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    txn->Delete(key);

    auto status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;
  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return Status::OK();
}

Status QuotaProcessor::LoadDirQuotas(Context& ctx, uint32_t fs_id, std::map<uint64_t, Quota>& quotas) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  Range range;
  MetaDataCodec::GetDirQuotaRange(fs_id, range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();

  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    auto status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      return status;
    }

    for (auto& kv : kvs) {
      uint32_t fs_id;
      uint64_t ino;
      MetaDataCodec::DecodeDirQuotaKey(kv.key, fs_id, ino);

      Quota quota = MetaDataCodec::DecodeDirQuotaValue(kv.value);
      quotas[ino] = quota;
    }
  } while (kvs.size() <= FLAGS_fs_scan_batch_size);

  auto status = txn->Commit();
  trace_txn = txn->GetTrace();

  return status;
}

Status QuotaProcessor::FlushDirUsages(Context& ctx, uint32_t fs_id, const std::map<uint64_t, Usage>& usages) {
  auto& trace_txn = ctx.GetTrace().GetTxn();

  // generate all keys
  std::vector<std::string> keys;
  keys.reserve(usages.size());
  for (const auto& [ino, usage] : usages) {
    keys.push_back(MetaDataCodec::EncodeDirQuotaKey(fs_id, ino));
  }

  int retry = 0;
  do {
    auto txn = kv_storage_->NewTxn();

    std::vector<KeyValue> kvs;
    auto status = txn->BatchGet(keys, kvs);
    if (!status.ok()) {
      return status;
    }

    for (auto& kv : kvs) {
      uint32_t fs_id;
      uint64_t ino;
      MetaDataCodec::DecodeDirQuotaKey(kv.key, fs_id, ino);

      Quota quota = MetaDataCodec::DecodeDirQuotaValue(kv.value);
      auto it = usages.find(ino);
      if (it != usages.end()) {
        quota.set_used_bytes(quota.used_bytes() + it->second.bytes());
        quota.set_used_inodes(quota.used_inodes() + it->second.inodes());

        txn->Put(kv.key, MetaDataCodec::EncodeDirQuotaValue(quota));
      }
    }

    status = txn->Commit();
    trace_txn = txn->GetTrace();
    if (status.error_code() != pb::error::ESTORE_MAYBE_RETRY) {
      break;
    }

    ++retry;

  } while (retry < FLAGS_txn_max_retry_times);

  trace_txn.retry = retry;

  return Status::OK();
}

bool QuotaProcessor::AsyncSetFsQuota(uint32_t fs_id, const Quota& quota) {
  auto task = std::make_shared<QuotaTask>(GetSelfPtr());
  task->type = QuotaTask::Type::kSetFsQuota;
  task->fs_id = fs_id;
  task->quota = quota;
  bool ret = worker_set_->ExecuteLeastQueue(task);
  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] execute task fail.", fs_id);
  }

  return ret;
}

bool QuotaProcessor::AsyncFlushFsUsage(uint32_t fs_id, const Usage& usage) {
  auto task = std::make_shared<QuotaTask>(GetSelfPtr());
  task->type = QuotaTask::Type::kFlushFsUsage;
  task->fs_id = fs_id;
  task->usage = usage;
  bool ret = worker_set_->ExecuteLeastQueue(task);
  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] execute task fail.", fs_id);
  }

  return ret;
}

bool QuotaProcessor::AsyncDeleteFsQuota(uint32_t fs_id) {
  auto task = std::make_shared<QuotaTask>(GetSelfPtr());
  task->type = QuotaTask::Type::kDeleteFsQuota;
  task->fs_id = fs_id;
  bool ret = worker_set_->ExecuteLeastQueue(task);
  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] execute task fail.", fs_id);
  }

  return ret;
}

bool QuotaProcessor::AsyncSetDirQuota(uint32_t fs_id, uint64_t ino, const Quota& quota) {
  auto task = std::make_shared<QuotaTask>(GetSelfPtr());
  task->type = QuotaTask::Type::kSetDirQuota;
  task->fs_id = fs_id;
  task->ino = ino;
  task->quota = quota;
  bool ret = worker_set_->ExecuteLeastQueue(task);
  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}.{}] execute task fail.", fs_id, ino);
  }

  return ret;
}

bool QuotaProcessor::AsyncDeleteDirQuota(uint32_t fs_id, uint64_t ino) {
  auto task = std::make_shared<QuotaTask>(GetSelfPtr());
  task->type = QuotaTask::Type::kDeleteDirQuota;
  task->fs_id = fs_id;
  task->ino = ino;
  bool ret = worker_set_->ExecuteLeastQueue(task);
  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}.{}] execute task fail.", fs_id, ino);
  }

  return ret;
}

bool QuotaProcessor::AsyncFlushDirUsages(uint32_t fs_id, const std::map<uint64_t, Usage>& usages) {
  auto task = std::make_shared<QuotaTask>(GetSelfPtr());
  task->type = QuotaTask::Type::kFlushDirUsages;
  task->fs_id = fs_id;
  task->usages = usages;
  bool ret = worker_set_->ExecuteLeastQueue(task);
  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] execute task fail.", fs_id);
  }

  return ret;
}

}  // namespace mdsv2
}  // namespace dingofs
