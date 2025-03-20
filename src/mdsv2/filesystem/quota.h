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

#ifndef DINGOFS_MDV2_FILESYSTEM_QUOTA_H_
#define DINGOFS_MDV2_FILESYSTEM_QUOTA_H_

#include <cstdint>
#include <fstream>
#include <memory>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class QuotaProcessor;
using QuotaProcessorPtr = std::shared_ptr<QuotaProcessor>;

class QuotaTask : public TaskRunnable {
 public:
  using Quota = pb::mdsv2::Quota;
  using Usage = pb::mdsv2::Usage;
  QuotaTask(QuotaProcessorPtr quota_processor) : quota_processor(quota_processor) {}
  ~QuotaTask() override = default;

  enum class Type {
    kSetFsQuota = 0,
    kDeleteFsQuota = 1,
    kFlushFsUsage = 2,
    kSetDirQuota = 3,
    kDeleteDirQuota = 4,
    kFlushDirUsages = 5,
  };

  std::string Type() override { return "QUOTA"; }

  void Run() override;

  enum Type type;
  uint32_t fs_id;
  uint64_t ino;
  Quota quota;
  Usage usage;
  std::map<uint64_t, Usage> usages;
  QuotaProcessorPtr quota_processor;
};

class QuotaProcessor : public std::enable_shared_from_this<QuotaProcessor> {
 public:
  QuotaProcessor(KVStoragePtr kv_storage);
  ~QuotaProcessor() = default;

  using Quota = pb::mdsv2::Quota;
  using Usage = pb::mdsv2::Usage;

  static QuotaProcessorPtr New(KVStoragePtr kv_storage) { return std::make_shared<QuotaProcessor>(kv_storage); }

  bool Init();
  void Destroy();

  QuotaProcessorPtr GetSelfPtr();

  Status SetFsQuota(Context& ctx, uint32_t fs_id, const Quota& quota);
  Status GetFsQuota(Context& ctx, uint32_t fs_id, Quota& quota);
  Status FlushFsUsage(Context& ctx, uint32_t fs_id, const Usage& usage);
  Status DeleteFsQuota(Context& ctx, uint32_t fs_id);

  Status SetDirQuota(Context& ctx, uint32_t fs_id, uint64_t ino, const Quota& quota);
  Status GetDirQuota(Context& ctx, uint32_t fs_id, uint64_t ino, Quota& quota);
  Status DeleteDirQuota(Context& ctx, uint32_t fs_id, uint64_t ino);
  Status LoadDirQuotas(Context& ctx, uint32_t fs_id, std::map<uint64_t, Quota>& quotas);
  Status FlushDirUsages(Context& ctx, uint32_t fs_id, const std::map<uint64_t, Usage>& usages);

  bool AsyncSetFsQuota(uint32_t fs_id, const Quota& quota);
  bool AsyncFlushFsUsage(uint32_t fs_id, const Usage& usage);
  bool AsyncDeleteFsQuota(uint32_t fs_id);

  bool AsyncSetDirQuota(uint32_t fs_id, uint64_t ino, const Quota& quota);
  bool AsyncDeleteDirQuota(uint32_t fs_id, uint64_t ino);
  bool AsyncFlushDirUsages(uint32_t fs_id, const std::map<uint64_t, Usage>& usages);

 private:
  // persistence store dentry/inode
  KVStoragePtr kv_storage_;

  WorkerSetUPtr worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_QUOTA_H_