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

#include "mdsv2/service/service_helper.h"

#include "brpc/reloadable_flags.h"
#include "gflags/gflags.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_int64(service_log_threshold_time_us, 1000L, "service log threshold time us");
BRPC_VALIDATE_GFLAG(service_log_threshold_time_us, brpc::PositiveInteger);

DEFINE_int32(log_print_max_length, 512, "log print max length");
BRPC_VALIDATE_GFLAG(log_print_max_length, brpc::PositiveInteger);

void ServiceHelper::SetError(pb::error::Error* error, const Status& status) {
  SetError(error, status.error_code(), status.error_str());
}

void ServiceHelper::SetError(pb::error::Error* error, int errcode, const std::string& errmsg) {
  error->set_errcode(static_cast<pb::error::Errno>(errcode));
  error->set_errmsg(errmsg);
}

void ServiceHelper::SetResponseInfo(const Trace& trace, pb::mdsv2::ResponseInfo* info) {
  auto* mut_time = info->mutable_time();
  const auto& time = trace.GetTime();
  mut_time->set_file_pending_time_us(time.file_pending_time_us);
  mut_time->set_pending_time_us(time.pending_time_us);

  auto* mut_cache = info->mutable_cache();
  const auto& cache = trace.GetCache();
  mut_cache->set_is_hit_partition(cache.is_hit_partition);
  mut_cache->set_is_hit_inode(cache.is_hit_inode);

  const auto& txn = trace.GetTxn();
  if (txn.txn_id != 0) {
    auto* mut_txn = info->add_txns();
    mut_txn->set_txn_id(txn.txn_id);
    mut_txn->set_is_one_pc(txn.is_one_pc);
    mut_txn->set_is_conflict(txn.is_conflict);
    mut_txn->set_read_time_us(txn.read_time_us);
    mut_txn->set_write_time_us(txn.write_time_us);
    mut_txn->set_retry(txn.retry);
  }

  const auto& file_txn = trace.GetFileTxn();
  if (file_txn.txn_id != 0) {
    auto* mut_txn = info->add_txns();
    mut_txn->set_txn_id(file_txn.txn_id);
    mut_txn->set_is_one_pc(file_txn.is_one_pc);
    mut_txn->set_is_conflict(file_txn.is_conflict);
    mut_txn->set_read_time_us(file_txn.read_time_us);
    mut_txn->set_write_time_us(file_txn.write_time_us);
    mut_txn->set_retry(file_txn.retry);
  }
}

}  // namespace mdsv2
}  // namespace dingofs
