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

}  // namespace mdsv2
}  // namespace dingofs
