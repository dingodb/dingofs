/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-06-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_CLOSURE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_CLOSURE_H_

#include <absl/strings/str_format.h>
#include <brpc/closure_guard.h>
#include <glog/logging.h>

#include "cache/common/const.h"
#include "cache/common/proto.h"
#include "cache/utils/context.h"

namespace dingofs {
namespace cache {

template <typename T, typename U>
class ServiceClosure : public google::protobuf::Closure {
 public:
  ServiceClosure(ContextSPtr ctx, const std::string& method_name,
                 google::protobuf::Closure* done, const T* request, U* response)
      : ctx_(ctx),
        method_name_(method_name),
        done_(done),
        request_(request),
        response_(response) {}

  ~ServiceClosure() override = default;

  void Run() override {
    std::unique_ptr<ServiceClosure<T, U>> self_guard(this);
    brpc::ClosureGuard done_guard(done_);

    auto ctx = ctx_;
    NEXT_STEP(kSendResponse);
    // TODO: metric guard

    if (response_->status() != PBBlockCacheErrCode::BlockCacheOk) {
      LOG(ERROR) << absl::StrFormat(
          "BlockCacheService [%s] reqeust failed: request = %s, response = %s",
          method_name_, request_->ShortDebugString().substr(0, 512),
          response_->ShortDebugString().substr(0, 512));
    }
  }

 private:
  ContextSPtr ctx_;
  std::string method_name_;
  google::protobuf::Closure* done_;
  const T* request_;
  U* response_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_CLOSURE_H_
