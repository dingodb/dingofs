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

#include "mdsv2/quota/quota.h"

#include <atomic>

#include "fmt/format.h"
#include "glog/logging.h"
#include "mdsv2/common/constant.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/filesystem/store_operation.h"

namespace dingofs {
namespace mdsv2 {
namespace quota {

void Quota::UpdateUsage(int64_t byte_delta, int64_t inode_delta) {
  byte_delta_.fetch_add(byte_delta, std::memory_order_release);
  inode_delta_.fetch_add(inode_delta, std::memory_order_release);

  DINGO_LOG(INFO) << fmt::format(
      "[quota.{}] update usage, byte_delta({}) inode_delta({}).", ino_,
      byte_delta, inode_delta);
}

bool Quota::Check(int64_t byte_delta, int64_t inode_delta) {
  utils::ReadLockGuard lk(rwlock_);

  int64_t used_bytes = quota_.used_bytes() +
                       byte_delta_.load(std::memory_order_acquire) + byte_delta;
  int64_t used_inodes = quota_.used_inodes() +
                        inode_delta_.load(std::memory_order_acquire) +
                        inode_delta;

  if ((quota_.max_bytes() > 0 && used_bytes > quota_.max_bytes()) ||
      (quota_.max_inodes() > 0 && used_inodes > quota_.max_inodes())) {
    DINGO_LOG(DEBUG) << fmt::format(
        "[quota] check fail, bytes({}/{}/{}), inodes({}/{}/{}).",
        quota_.max_bytes(), quota_.used_bytes(), used_bytes,
        quota_.max_inodes(), quota_.used_inodes(), used_inodes);
    return false;
  }

  return true;
}

UsageEntry Quota::GetUsage() {
  UsageEntry usage;

  usage.set_bytes(byte_delta_.load(std::memory_order_acquire));
  usage.set_inodes(inode_delta_.load(std::memory_order_acquire));

  return usage;
}

QuotaEntry Quota::GetQuota() {
  utils::ReadLockGuard lk(rwlock_);

  return quota_;
}

QuotaEntry Quota::GetQuotaAndDelta() {
  utils::ReadLockGuard lk(rwlock_);

  QuotaEntry quota = quota_;
  quota.set_used_bytes(quota.used_bytes() +
                       byte_delta_.load(std::memory_order_acquire));
  quota.set_used_inodes(quota.used_inodes() +
                        inode_delta_.load(std::memory_order_acquire));

  return quota;
}

void Quota::Refresh(const QuotaEntry& quota, const UsageEntry& minus_usage) {
  utils::WriteLockGuard lk(rwlock_);

  if (minus_usage.bytes() != 0) {
    byte_delta_.fetch_sub(minus_usage.bytes(), std::memory_order_release);
  }
  if (minus_usage.inodes() != 0) {
    inode_delta_.fetch_sub(minus_usage.inodes(), std::memory_order_release);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[quota.{}] refresh quota, bytes({}/{}/{}) inodes({}/{}/{}).", ino_,
      byte_delta_.load(), quota.used_bytes(), quota.max_bytes(),
      inode_delta_.load(), quota.used_inodes(), quota.max_inodes());

  quota_ = quota;
}

void DirQuotaMap::UpsertQuota(Ino ino, const QuotaEntry& quota) {
  utils::WriteLockGuard lk(rwlock_);

  auto it = quota_map_.find(ino);
  if (it == quota_map_.end()) {
    quota_map_[ino] = std::make_shared<Quota>(ino, quota);
  } else {
    // update existing quota
    it->second->Refresh(quota, {});
  }
}

void DirQuotaMap::UpdateUsage(Ino ino, int64_t byte_delta,
                              int64_t inode_delta) {
  Ino curr_ino = ino;
  while (true) {
    auto quota = GetQuota(curr_ino);
    if (quota != nullptr) {
      quota->UpdateUsage(byte_delta, inode_delta);
    }

    if (curr_ino == kRootIno) {
      break;
    }

    Ino parent;
    if (!parent_memo_->GetParent(curr_ino, parent)) {
      DINGO_LOG(WARNING) << fmt::format("[quota] not found parent, ino({}).",
                                        curr_ino);
      break;
    }

    curr_ino = parent;
  }
}
void DirQuotaMap::DeleteQuota(Ino ino) {
  utils::WriteLockGuard lk(rwlock_);

  quota_map_.erase(ino);
}

bool DirQuotaMap::CheckQuota(Ino ino, int64_t byte_delta, int64_t inode_delta) {
  auto quota = GetNearestQuota(ino);
  if (quota == nullptr) {
    return true;
  }

  return quota->Check(byte_delta, inode_delta);
}

QuotaSPtr DirQuotaMap::GetNearestQuota(Ino ino) {
  Ino curr_ino = ino;
  while (true) {
    auto quota = GetQuota(curr_ino);
    if (quota != nullptr) {
      return quota;
    }

    if (curr_ino == kRootIno) {
      break;
    }

    Ino parent;
    if (!parent_memo_->GetParent(curr_ino, parent)) {
      break;
    }

    curr_ino = parent;
  }
  if (curr_ino != kRootIno) {
    DINGO_LOG(WARNING) << fmt::format("[quota] not found parent, ino({}).",
                                      curr_ino);
  }

  return nullptr;
}

std::vector<QuotaSPtr> DirQuotaMap::GetAllQuota() {
  utils::ReadLockGuard lk(rwlock_);

  std::vector<QuotaSPtr> quotas;
  quotas.reserve(quota_map_.size());
  for (const auto& [_, quota] : quota_map_) {
    quotas.push_back(quota);
  }

  return quotas;
}

void DirQuotaMap::RefreshAll(
    const std::unordered_map<Ino, QuotaEntry>& quota_map) {
  utils::WriteLockGuard lk(rwlock_);

  for (auto it = quota_map_.begin(); it != quota_map_.end();) {
    const auto& ino = it->first;

    auto new_it = quota_map.find(ino);
    if (new_it == quota_map.end()) {
      // delete not exist quota
      it = quota_map_.erase(it);
    } else {
      // update existing quota
      it->second->Refresh(new_it->second, {});

      ++it;
    }
  }

  // add new quotas
  for (const auto& [ino, quota_entry] : quota_map) {
    if (quota_map_.find(ino) == quota_map_.end()) {
      quota_map_[ino] = std::make_shared<Quota>(ino, quota_entry);
    }
  }
}

QuotaSPtr DirQuotaMap::GetQuota(Ino ino) {
  utils::ReadLockGuard lk(rwlock_);

  auto it = quota_map_.find(ino);
  return (it != quota_map_.end()) ? it->second : nullptr;
}

bool QuotaManager::Init() {
  CHECK(fs_id_ > 0) << fmt::format("[quota] fs_id({}) is 0.", fs_id_);

  auto status = LoadQuota();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] init load quota fail, status({}).",
                                    status.error_str());
    return false;
  }

  return true;
}

void QuotaManager::Destroy() {
  auto status = FlushUsage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[quota] destroy flush usage fail, status({}).", status.error_str());
  }
}

void QuotaManager::UpdateFsUsage(int64_t byte_delta, int64_t inode_delta) {
  fs_quota_.UpdateUsage(byte_delta, inode_delta);
}

void QuotaManager::UpdateDirUsage(Ino parent, int64_t byte_delta,
                                  int64_t inode_delta) {
  dir_quota_map_.UpdateUsage(parent, byte_delta, inode_delta);
}

bool QuotaManager::CheckQuota(Ino ino, int64_t byte_delta,
                              int64_t inode_delta) {
  if (!fs_quota_.Check(byte_delta, inode_delta)) {
    return false;
  }

  return dir_quota_map_.CheckQuota(ino, byte_delta, inode_delta);
}

QuotaSPtr QuotaManager::GetNearestDirQuota(Ino ino) {
  return dir_quota_map_.GetNearestQuota(ino);
}

Status QuotaManager::SetFsQuota(Trace& trace, const QuotaEntry& quota) {
  SetFsQuotaOperation operation(trace, fs_id_, quota);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] set fs quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  fs_quota_.Refresh(quota, {});

  return Status::OK();
}

Status QuotaManager::GetFsQuota(Trace& trace, bool is_bypass_cache,
                                QuotaEntry& quota) {
  if (!is_bypass_cache) {
    quota = fs_quota_.GetQuotaAndDelta();

  } else {
    GetFsQuotaOperation operation(trace, fs_id_);

    auto status = operation_processor_->RunAlone(&operation);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[quota] get fs quota fail, status({}).",
                                      status.error_str());
      return status;
    }

    auto& result = operation.GetResult();
    quota = result.quota;
  }

  return Status::OK();
}

Status QuotaManager::DeleteFsQuota(Trace& trace) {
  DeleteFsQuotaOperation operation(trace, fs_id_);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] delete fs quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  return Status::OK();
}

Status QuotaManager::SetDirQuota(Trace& trace, Ino ino,
                                 const QuotaEntry& quota) {
  SetDirQuotaOperation operation(trace, fs_id_, ino, quota);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] set dir quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  dir_quota_map_.UpsertQuota(ino, quota);

  return Status::OK();
}

Status QuotaManager::GetDirQuota(Trace& trace, Ino ino, QuotaEntry& quota) {
  GetDirQuotaOperation operation(trace, fs_id_, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] get dir quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  quota = result.quota;

  return Status::OK();
}

Status QuotaManager::DeleteDirQuota(Trace& trace, Ino ino) {
  DeleteDirQuotaOperation operation(trace, fs_id_, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[quota] delete dir quota fail, status({}).", status.error_str());
    return status;
  }

  dir_quota_map_.DeleteQuota(ino);

  return Status::OK();
}

Status QuotaManager::LoadDirQuotas(
    Trace& trace, std::map<Ino, QuotaEntry>& quota_entry_map) {  // NOLINT
  auto quotas = dir_quota_map_.GetAllQuota();
  for (auto& quota : quotas) {
    quota_entry_map[quota->GetIno()] = quota->GetQuotaAndDelta();
  }

  return Status::OK();
}

Status QuotaManager::LoadQuota() {
  // load fs quota
  auto status = LoadFsQuota();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] load fs quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  // load all dir quotas
  status = LoadAllDirQuota();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[quota] load all dir quota fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

Status QuotaManager::FlushUsage() {
  // flush fs usage
  auto status = FlushFsUsage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] flush fs usage fail, status({}).",
                                    status.error_str());
    return status;
  }

  // flush all dir usage
  status = FlushDirUsage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[quota] flush all dir usage fail, status({}).", status.error_str());
    return status;
  }

  return Status::OK();
}

Status QuotaManager::FlushFsUsage() {
  Trace trace;

  auto usage = fs_quota_.GetUsage();
  if (usage.bytes() == 0 && usage.inodes() == 0) {
    return Status::OK();
  }

  FlushFsUsageOperation operation(trace, fs_id_, usage);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] flush fs quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  auto& result = operation.GetResult();

  fs_quota_.Refresh(result.quota, usage);

  return Status::OK();
}

Status QuotaManager::FlushDirUsage() {
  auto quotas = dir_quota_map_.GetAllQuota();

  std::map<uint64_t, UsageEntry> usages;
  for (auto& quota : quotas) {
    auto usage = quota->GetUsage();
    DINGO_LOG(INFO) << fmt::format(
        "[quota] flush dir usage, ino({}), bytes({}), inodes({}).",
        quota->GetIno(), usage.bytes(), usage.inodes());
    if (usage.bytes() == 0 && usage.inodes() == 0) {
      continue;
    }

    usages[quota->GetIno()] = usage;
  }

  if (usages.empty()) {
    return Status::OK();
  }

  Trace trace;
  FlushDirUsagesOperation operation(trace, fs_id_, usages);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] flush fs quota fail, status({}).",
                                    status.error_str());
    return status;
  }
  auto& result = operation.GetResult();

  for (auto& quota : quotas) {
    Ino ino = quota->GetIno();
    auto it = result.quotas.find(ino);
    if (it == result.quotas.end()) {
      continue;
    }

    auto& new_quota = it->second;

    auto usage_it = usages.find(ino);
    CHECK(usage_it != usages.end())
        << fmt::format("[quota] usage not found for ino({}).", ino);
    auto& usage = usage_it->second;

    quota->Refresh(new_quota, usage);
  }

  return Status::OK();
}

Status QuotaManager::LoadFsQuota() {
  Trace trace;
  GetFsQuotaOperation operation(trace, fs_id_);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    if (status.error_code() == pb::error::ENOT_FOUND) {
      return Status::OK();
    }

    DINGO_LOG(ERROR) << fmt::format("[quota] get fs quota fail, status({}).",
                                    status.error_str());
    return status;
  }

  auto& result = operation.GetResult();

  fs_quota_.Refresh(result.quota, UsageEntry{});

  return Status::OK();
}

Status QuotaManager::LoadAllDirQuota() {
  Trace trace;
  LoadDirQuotasOperation operation(trace, fs_id_);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota] load dir quotas fail, status({}).",
                                    status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  dir_quota_map_.RefreshAll(result.quotas);

  return Status::OK();
}

}  // namespace quota
}  // namespace mdsv2
}  // namespace dingofs
