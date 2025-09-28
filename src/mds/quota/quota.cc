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

#include "mds/quota/quota.h"

#include <cstdint>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/constant.h"
#include "mds/common/logging.h"
#include "mds/filesystem/store_operation.h"

namespace dingofs {
namespace mds {
namespace quota {

static const uint32_t kMaxNotFoundCount = 30;

Quota::Quota(uint32_t fs_id, Ino ino, const QuotaEntry& quota) : fs_id_(fs_id), ino_(ino), quota_(quota) {
  last_time_ns_ = Helper::TimestampNs();

  DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] create quota, detail({}).", fs_id_, ino_, quota.ShortDebugString());
}

UsageEntry Quota::GetDeltaAccumulatedUsage(uint64_t& timepoint) {
  int64_t bytes = 0, inodes = 0;
  for (auto& usage : delta_usages_) {
    bytes += usage.bytes();
    inodes += usage.inodes();
    timepoint = usage.time_ns();
  }

  UsageEntry usage;
  usage.set_bytes(bytes);
  usage.set_inodes(inodes);

  return usage;
}

UsageEntry Quota::GetTotalAccumulatedUsage(uint64_t& timepoint) {
  auto usage = GetDeltaAccumulatedUsage(timepoint);
  usage.set_bytes(usage.bytes() + quota_.used_bytes());
  usage.set_inodes(usage.inodes() + quota_.used_inodes());

  return usage;
}

UsageEntry Quota::CompactDeltaUsage(uint64_t timepoint) {
  UsageEntry usage;
  while (!delta_usages_.empty()) {
    const auto& delta_usage = delta_usages_.front();
    if (delta_usage.time_ns() > timepoint) break;
    usage.set_bytes(usage.bytes() + delta_usage.bytes());
    usage.set_inodes(usage.inodes() + delta_usage.inodes());
    delta_usages_.pop_front();
  }

  return usage;
}

void Quota::UpdateUsage(int64_t byte_delta, int64_t inode_delta, const std::string& reason) {
  DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] update usage, byte_delta({}) inode_delta({}) reason({}).", fs_id_, ino_,
                                 byte_delta, inode_delta, reason);

  {
    utils::WriteLockGuard lk(rwlock_);

    uint64_t now_ns = Helper::TimestampNs();
    uint64_t time_ns = std::max({now_ns, last_time_ns_}) + 1;
    last_time_ns_ = time_ns;

    UsageEntry usage;
    usage.set_bytes(byte_delta);
    usage.set_inodes(inode_delta);
    usage.set_time_ns(time_ns);
    delta_usages_.push_back(std::move(usage));
  }
}

bool Quota::Check(int64_t byte_delta, int64_t inode_delta) {
  utils::ReadLockGuard lk(rwlock_);

  uint64_t timepoint;
  auto usage = GetTotalAccumulatedUsage(timepoint);

  int64_t used_bytes = usage.bytes() + byte_delta;
  int64_t used_inodes = usage.inodes() + inode_delta;

  if ((quota_.max_bytes() > 0 && used_bytes > quota_.max_bytes()) ||
      (quota_.max_inodes() > 0 && used_inodes > quota_.max_inodes())) {
    DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] check fail, bytes({}/{}/{}), inodes({}/{}/{}).", fs_id_, ino_,
                                   byte_delta, usage.bytes(), quota_.max_bytes(), inode_delta, usage.inodes(),
                                   quota_.max_inodes());
    return false;
  }

  return true;
}

std::vector<UsageEntry> Quota::GetUsage() {
  utils::ReadLockGuard lk(rwlock_);

  std::vector<UsageEntry> usages;
  usages.reserve(delta_usages_.size());
  for (auto& usage : delta_usages_) {
    usages.push_back(usage);
  }

  return usages;
}

QuotaEntry Quota::GetQuota() {
  utils::ReadLockGuard lk(rwlock_);

  return quota_;
}

QuotaEntry Quota::GetAccumulatedQuota() {
  utils::ReadLockGuard lk(rwlock_);

  uint64_t timepoint;
  auto usage = GetDeltaAccumulatedUsage(timepoint);

  QuotaEntry quota = quota_;
  quota.set_used_bytes(quota.used_bytes() + usage.bytes());
  quota.set_used_inodes(quota.used_inodes() + usage.inodes());

  return quota;
}

void Quota::Refresh(const QuotaEntry& quota, uint64_t timepoint, const std::string& reason) {
  bool is_change = false, is_change_uuid = false;
  QuotaEntry old_quota;
  UsageEntry compact_usage;
  {
    utils::WriteLockGuard lk(rwlock_);

    old_quota = quota_;
    compact_usage = CompactDeltaUsage(timepoint);
    if (compact_usage.bytes() != 0 || compact_usage.inodes() != 0) is_change = true;

    if (quota.uuid() == quota.uuid()) {
      if (quota_.version() < quota.version()) {
        quota_ = quota;
        is_change = true;
      }

    } else {
      is_change_uuid = true;
      if (quota_.create_time_ns() < quota.create_time_ns()) {
        quota_ = quota;
        is_change = true;
      }
    }
  }

  if (is_change) {
    DINGO_LOG(INFO) << fmt::format(
        "[quota.{}.{}] refresh quota({}->{}), timepoint({}) change_uuid({}) bytes({}/{}/{}) inodes({}/{}/{}) "
        "reason({}).",
        fs_id_, ino_, old_quota.version(), quota.version(), timepoint, is_change_uuid, compact_usage.bytes(),
        quota.used_bytes(), quota.max_bytes(), compact_usage.inodes(), quota.used_inodes(), quota.max_inodes(), reason);
  }
}

void DirQuotaMap::UpsertQuota(Ino ino, const QuotaEntry& quota, const std::string& reason) {
  utils::WriteLockGuard lk(rwlock_);

  auto it = quota_map_.find(ino);
  if (it == quota_map_.end()) {
    quota_map_[ino] = Quota::New(fs_id_, ino, quota);
  } else {
    // update existing quota
    it->second->Refresh(quota, 0, reason);
  }
}

void DirQuotaMap::UpdateUsage(Ino ino, int64_t byte_delta, int64_t inode_delta, const std::string& reason) {
  Ino curr_ino = ino;
  while (true) {
    auto quota = GetQuota(curr_ino);
    if (quota != nullptr) quota->UpdateUsage(byte_delta, inode_delta, reason);

    if (curr_ino == kRootIno) break;

    Ino parent;
    if (!GetParent(curr_ino, parent)) break;

    curr_ino = parent;
  }
}

void DirQuotaMap::DeleteQuota(Ino ino, const std::string& uuid) {
  utils::WriteLockGuard lk(rwlock_);

  auto it = quota_map_.find(ino);
  if (it == quota_map_.end()) return;
  if (it->second->UUID() != uuid) return;

  quota_map_.erase(ino);
}

bool DirQuotaMap::CheckQuota(Ino ino, int64_t byte_delta, int64_t inode_delta) {
  // not check quota if fs has no quota
  if (!HasQuota()) return true;

  Ino curr_ino = ino;
  while (true) {
    auto quota = GetQuota(curr_ino);
    if (quota != nullptr) {
      if (!quota->Check(byte_delta, inode_delta)) return false;
    }

    if (curr_ino == kRootIno) break;

    Ino parent;
    if (!GetParent(curr_ino, parent)) break;

    curr_ino = parent;
  }

  return true;
}

QuotaSPtr DirQuotaMap::GetNearestQuota(Ino ino) {
  Ino curr_ino = ino;
  while (true) {
    auto quota = GetQuota(curr_ino);
    if (quota != nullptr) return quota;

    if (curr_ino == kRootIno) break;

    Ino parent;
    if (!GetParent(curr_ino, parent)) break;

    curr_ino = parent;
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

void DirQuotaMap::Refresh(const std::unordered_map<Ino, QuotaEntry>& quota_map, const std::string& reason) {
  utils::WriteLockGuard lk(rwlock_);

  for (auto it = quota_map_.begin(); it != quota_map_.end();) {
    const auto& ino = it->first;
    auto& quota = it->second;

    auto new_it = quota_map.find(ino);
    if (new_it == quota_map.end()) {
      if (quota->IncNotFoundCount() >= kMaxNotFoundCount) {
        it = quota_map_.erase(it);
        DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] not found in store, clean it.", fs_id_, ino);
      }
    } else {
      // update existing quota
      quota->Refresh(new_it->second, 0, reason);

      ++it;
    }
  }

  // add new quotas
  for (const auto& [ino, quota_entry] : quota_map) {
    if (quota_map_.find(ino) == quota_map_.end()) {
      quota_map_[ino] = Quota::New(fs_id_, ino, quota_entry);
    }
  }
}

QuotaSPtr DirQuotaMap::GetQuota(Ino ino) {
  utils::ReadLockGuard lk(rwlock_);

  auto it = quota_map_.find(ino);
  return (it != quota_map_.end()) ? it->second : nullptr;
}

bool DirQuotaMap::GetParent(Ino ino, Ino& parent) {
  if (parent_memo_->GetParent(ino, parent)) {
    return true;
  }

  // if not found, try to get parent from store
  Trace trace;
  GetInodeAttrOperation operation(trace, fs_id_, ino);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[quota.{}.{}] query parent fail, status({}).", fs_id_, ino, status.error_str());
    return false;
  }

  auto& result = operation.GetResult();
  auto& attr = result.attr;

  CHECK(attr.parents().size() == 1) << fmt::format("[quota.{}.{}] dir should only one parent, attr({}).", fs_id_, ino,
                                                   attr.ShortDebugString());

  parent = attr.parents().at(0);

  parent_memo_->Remeber(ino, parent);

  DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] query parent finish, parent({}).", fs_id_, ino, parent);

  return true;
}

bool DirQuotaMap::HasQuota() {
  utils::ReadLockGuard lk(rwlock_);

  return !quota_map_.empty();
}

void UpdateDirUsageTask::Run() { quota_manager_->UpdateDirUsage(parent_, byte_delta_, inode_delta_, reason_); }

void DeleteDirQuotaTask::Run() {
  class Trace trace;
  quota_manager_->DeleteDirQuota(trace, ino_);
}

QuotaManagerSPtr QuotaManager::GetSelfPtr() { return std::dynamic_pointer_cast<QuotaManager>(shared_from_this()); }

bool QuotaManager::Init() {
  CHECK(fs_info_ != nullptr) << "[quota] fs_info is nullptr.";
  const uint32_t fs_id = fs_info_->GetFsId();

  auto status = LoadQuota();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] init load quota fail, status({}).", fs_id, status.error_str());
    return false;
  }

  return true;
}

void QuotaManager::Destroy() {
  const uint32_t fs_id = fs_info_->GetFsId();
  auto status = FlushUsage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] destroy flush usage fail, status({}).", fs_id, status.error_str());
  }
}

void QuotaManager::UpdateFsUsage(int64_t byte_delta, int64_t inode_delta, const std::string& reason) {
  fs_quota_.UpdateUsage(byte_delta, inode_delta, reason);
}

void QuotaManager::UpdateDirUsage(Ino parent, int64_t byte_delta, int64_t inode_delta, const std::string& reason) {
  dir_quota_map_.UpdateUsage(parent, byte_delta, inode_delta, reason);
}

void QuotaManager::AsyncUpdateDirUsage(Ino parent, int64_t byte_delta, int64_t inode_delta, const std::string& reason) {
  const uint32_t fs_id = fs_info_->GetFsId();

  auto task = UpdateDirUsageTask::New(GetSelfPtr(), parent, byte_delta, inode_delta, reason);

  if (!worker_set_->ExecuteHash(parent, task)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[quota.{}] async update dir usage fail, parent({}), byte_delta({}), inode_delta({}) reason({}).", fs_id,
        parent, byte_delta, inode_delta, reason);
  }
}

bool QuotaManager::CheckQuota(Trace& trace, Ino ino, int64_t byte_delta, int64_t inode_delta) {
  if (!fs_quota_.Check(byte_delta, inode_delta)) {
    return false;
  }

  bool ret = dir_quota_map_.CheckQuota(ino, byte_delta, inode_delta);

  trace.RecordElapsedTime("check_quota");

  return ret;
}

QuotaSPtr QuotaManager::GetNearestDirQuota(Ino ino) { return dir_quota_map_.GetNearestQuota(ino); }

Status QuotaManager::SetFsQuota(Trace& trace, const QuotaEntry& quota) {
  const uint32_t fs_id = fs_info_->GetFsId();

  DINGO_LOG(INFO) << fmt::format("[quota.{}] set fs quota, quota({}).", fs_id, quota.ShortDebugString());

  SetFsQuotaOperation operation(trace, fs_id, quota);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] set fs quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();

  fs_quota_.Refresh(result.quota, 0, "set-fs-quota");

  return Status::OK();
}

Status QuotaManager::GetFsQuota(Trace& trace, bool is_bypass_cache, QuotaEntry& quota) {
  const uint32_t fs_id = fs_info_->GetFsId();

  if (!is_bypass_cache) {
    quota = fs_quota_.GetAccumulatedQuota();

    return Status::OK();
  }

  GetFsQuotaOperation operation(trace, fs_id);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] get fs quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  quota = result.quota;

  return Status::OK();
}

Status QuotaManager::DeleteFsQuota(Trace& trace) {
  const uint32_t fs_id = fs_info_->GetFsId();

  DINGO_LOG(INFO) << fmt::format("[quota.{}] delete fs quota.", fs_id);

  DeleteFsQuotaOperation operation(trace, fs_id);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] delete fs quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  return Status::OK();
}

Status QuotaManager::SetDirQuota(Trace& trace, Ino ino, const QuotaEntry& quota, bool is_lead) {
  const uint32_t fs_id = fs_info_->GetFsId();

  DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] set dir quota, quota({}).", fs_id, ino, quota.ShortDebugString());

  if (!is_lead) {
    dir_quota_map_.UpsertQuota(ino, quota, "follow-set-dir-quota");
    return Status::OK();
  }

  SetDirQuotaOperation operation(trace, fs_id, ino, quota);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}.{}] set dir quota fail, status({}).", fs_id, ino, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();

  dir_quota_map_.UpsertQuota(ino, result.quota, "set-dir-quota");

  auto mds_ids = fs_info_->GetMdsIds();
  for (auto mds_id : mds_ids) {
    notify_buddy_->AsyncNotify(notify::SetDirQuotaMessage::Create(mds_id, fs_id, ino, result.quota));
  }

  return Status::OK();
}

Status QuotaManager::GetDirQuota(Trace& trace, Ino ino, QuotaEntry& quota) {
  const uint32_t fs_id = fs_info_->GetFsId();

  GetDirQuotaOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}.{}] get dir quota fail, status({}).", fs_id, ino, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  quota = result.quota;

  return Status::OK();
}

Status QuotaManager::DeleteDirQuota(Trace& trace, Ino ino) {
  const uint32_t fs_id = fs_info_->GetFsId();

  DINGO_LOG(INFO) << fmt::format("[quota.{}.{}] delete dir quota.", fs_id, ino);

  DeleteDirQuotaOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    if (status.error_code() != pb::error::ENOT_FOUND) {
      DINGO_LOG(ERROR) << fmt::format("[quota.{}.{}] delete dir quota fail, status({}).", fs_id, ino,
                                      status.error_str());
    }
    return status;
  }

  auto& result = operation.GetResult();

  const auto& uuid = result.quota.uuid();
  dir_quota_map_.DeleteQuota(ino, uuid);

  auto mds_ids = fs_info_->GetMdsIds();
  for (auto mds_id : mds_ids) {
    notify_buddy_->AsyncNotify(notify::DeleteDirQuotaMessage::Create(mds_id, fs_id, ino, uuid));
  }

  return Status::OK();
}

Status QuotaManager::DeleteDirQuotaByNotified(Ino ino, const std::string& uuid) {
  dir_quota_map_.DeleteQuota(ino, uuid);

  return Status::OK();
}

void QuotaManager::AsyncDeleteDirQuota(Ino ino) {
  const uint32_t fs_id = fs_info_->GetFsId();

  auto task = DeleteDirQuotaTask::New(GetSelfPtr(), ino);

  if (!worker_set_->ExecuteHash(ino, task)) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}.{}] async delete dir quota fail.", fs_id, ino);
  }
}

Status QuotaManager::LoadDirQuotas(Trace&, std::map<Ino, QuotaEntry>& quota_entry_map) {
  auto quotas = dir_quota_map_.GetAllQuota();
  for (auto& quota : quotas) {
    quota_entry_map[quota->INo()] = quota->GetAccumulatedQuota();
  }

  return Status::OK();
}

Status QuotaManager::LoadQuota() {
  const uint32_t fs_id = fs_info_->GetFsId();

  // load fs quota
  auto status = LoadFsQuota();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] load fs quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  // load all dir quotas
  status = LoadAllDirQuota();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] load all dir quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  return Status::OK();
}

Status QuotaManager::FlushUsage() {
  const uint32_t fs_id = fs_info_->GetFsId();

  // flush fs usage
  auto status = FlushFsUsage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] flush fs usage fail, status({}).", fs_id, status.error_str());
    return status;
  }

  // flush all dir usage
  status = FlushDirUsage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] flush all dir usage fail, status({}).", fs_id, status.error_str());
    return status;
  }

  return Status::OK();
}

Status QuotaManager::FlushFsUsage() {
  const uint32_t fs_id = fs_info_->GetFsId();

  Trace trace;

  auto usages = fs_quota_.GetUsage();
  if (usages.empty()) return Status::OK();

  FlushFsUsageOperation operation(trace, fs_id, usages);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) return status;

  auto& result = operation.GetResult();

  uint64_t timepoint = usages.back().time_ns();
  fs_quota_.Refresh(result.quota, timepoint, "flush-fs-usage");

  return Status::OK();
}

Status QuotaManager::FlushDirUsage() {
  using UsageEntrySet = std::vector<UsageEntry>;

  const uint32_t fs_id = fs_info_->GetFsId();

  // ino->usage
  std::map<uint64_t, UsageEntrySet> usage_map;
  auto quotas = dir_quota_map_.GetAllQuota();
  for (auto& quota : quotas) {
    auto usages = quota->GetUsage();
    if (usages.empty()) continue;

    usage_map[quota->INo()] = usages;
  }

  if (usage_map.empty()) return Status::OK();

  Trace trace;
  FlushDirUsagesOperation operation(trace, fs_id, usage_map);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] flush fs quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  for (auto& quota : quotas) {
    Ino ino = quota->INo();
    auto it = result.quotas.find(ino);
    if (it == result.quotas.end()) {
      continue;
    }
    auto& new_quota = it->second;

    auto usage_it = usage_map.find(ino);
    if (usage_it == usage_map.end()) continue;

    uint64_t timepoint = usage_it->second.back().time_ns();
    quota->Refresh(new_quota, timepoint, "flush-dir-usage");
  }

  return Status::OK();
}

Status QuotaManager::LoadFsQuota() {
  const uint32_t fs_id = fs_info_->GetFsId();
  Trace trace;
  GetFsQuotaOperation operation(trace, fs_id);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    if (status.error_code() == pb::error::ENOT_FOUND) {
      return Status::OK();
    }

    DINGO_LOG(ERROR) << fmt::format("[quota.{}] get fs quota fail, status({}).", fs_id, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();

  fs_quota_.Refresh(result.quota, 0, "load-fs-quota");

  return Status::OK();
}

Status QuotaManager::LoadAllDirQuota() {
  const uint32_t fs_id = fs_info_->GetFsId();
  Trace trace;
  LoadDirQuotasOperation operation(trace, fs_id);
  operation.SetIsolationLevel(Txn::kReadCommitted);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[quota.{}] load dir quotas fail, status({}).", fs_id, status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  if (!result.quotas.empty()) {
    dir_quota_map_.Refresh(result.quotas, "load-all-dir-quota");
  }

  return Status::OK();
}

}  // namespace quota
}  // namespace mds
}  // namespace dingofs
