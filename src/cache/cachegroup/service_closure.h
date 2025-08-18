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

#include "cache/blockcache/cache_store.h"
#include "cache/utils/context.h"
#include "cache/utils/step_timer.h"

namespace dingofs {
namespace cache {

static const std::string kModule = "service";

inline std::string ToString(const pb::cache::PutRequest* request) {
  return absl::StrFormat("put(%s,%zu)",
                         BlockKey(request->block_key()).Filename(),
                         request->block_size());
}

inline std::string ToString(const pb::cache::RangeRequest* request) {
  return absl::StrFormat("range(%s,%lld,%zu)",
                         BlockKey(request->block_key()).Filename(),
                         request->offset(), request->length());
}

inline std::string ToString(const pb::cache::CacheRequest* request) {
  return absl::StrFormat("cache(%s,%zu)",
                         BlockKey(request->block_key()).Filename(),
                         request->block_size());
}

inline std::string ToString(const pb::cache::PrefetchRequest* request) {
  return absl::StrFormat("prefetch(%s,%zu)",
                         BlockKey(request->block_key()).Filename(),
                         request->block_size());
}

template <typename T, typename U, typename... Args>
class ServiceClosure : public google::protobuf::Closure {
 public:
  ServiceClosure(ContextSPtr ctx, google::protobuf::Closure* done,
                 const T* request, U* response, Status& status,
                 StepTimer& timer)
      : ctx_(ctx),
        done_(done),
        request_(request),
        response_(response),
        status_(status),
        timer_(timer) {}

  ~ServiceClosure() override = default;

  void Run() override {
    std::unique_ptr<ServiceClosure<T, U>> self_guard(this);
    TraceLogGuard log(ctx_, status_, timer_, kModule, "%s", ToString(request_));

    if (response_->status() != pb::cache::BlockCacheOk) {
      LOG(ERROR) << absl::StrFormat(
          "[%s] BlockCacheService [%s] reqeust failed: request = %s, response "
          "= %s, status = %s",
          ctx_->TraceId(), ToString(request_),
          request_->ShortDebugString().substr(0, 512),
          response_->ShortDebugString().substr(0, 512),
          pb::cache::BlockCacheErrCode_Name(response_->status()));
    }

    timer_.NextStep("send_response");
    done_->Run();

    timer_.Stop();
  }

 private:
  ContextSPtr ctx_;
  google::protobuf::Closure* done_;
  const T* request_;
  U* response_;
  Status& status_;
  StepTimer& timer_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_SERVICE_CLOSURE_H_
