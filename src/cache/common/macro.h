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
 * Created Date: 2025-06-03
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_MACRO_H_
#define DINGOFS_SRC_CACHE_COMMON_MACRO_H_

#include <absl/strings/str_format.h>

#include "butil/memory/scope_guard.h"

namespace dingofs {
namespace cache {

#define DECLARE_RPC_METHOD(method)                         \
  void method(google::protobuf::RpcController* controller, \
              const pb::cache::method##Request* request,   \
              pb::cache::method##Response* response,       \
              google::protobuf::Closure* done) override

#define DEFINE_RPC_METHOD(classname, method)                          \
  void classname::method(google::protobuf::RpcController* controller, \
                         const pb::cache::method##Request* request,   \
                         pb::cache::method##Response* response,       \
                         google::protobuf::Closure* done)

#define CHECK_RUNNING(service_name)               \
  CHECK(running_.load(std::memory_order_relaxed)) \
      << (service_name) << " is not running."

#define CHECK_DOWN(service_name)                   \
  CHECK(!running_.load(std::memory_order_relaxed)) \
      << (service_name) << " is not down.";

#define DCHECK_RUNNING(service_name)               \
  DCHECK(running_.load(std::memory_order_relaxed)) \
      << (service_name) << " is not running."

#define LOG_SYSERR(code, ...) \
  LOG(ERROR) << absl::StrFormat(__VA_ARGS__) << ": " << ::strerror(code);

#define LOG_CTX(severity) LOG(severity) << ctx->StrTraceId() << " "
#define VLOG_CTX(verboselevel) VLOG(verboselevel) << ctx->StrTraceId() << " "
#define LOG_EVERY_SECOND_CTX(severity) \
  LOG_EVERY_SECOND(severity) << ctx->StrTraceId() << " "
#define LOG_EVERY_N_CTX(severity, n) \
  LOG_EVERY_N(severity, n) << ctx->StrTraceId() << " "

#define GENERIC_LOG_PUT_ERROR(to)                               \
  do {                                                          \
    LOG(ERROR) << ctx->StrTraceId() << " Put block to " << (to) \
               << " failed: key = " << key.Filename()           \
               << ", length = " << block.size                   \
               << ", status = " << status.ToString();           \
  } while (0);

#define GENERIC_LOG_RANGE_ERROR(from)                                 \
  do {                                                                \
    LOG(ERROR) << ctx->StrTraceId() << " Range block from " << (from) \
               << " failed: key = " << key.Filename()                 \
               << ", offset = " << offset << ", length = " << length  \
               << ", status = " << status.ToString();                 \
  } while (0);

#define GENERIC_LOG_CACHE_ERROR(to)                               \
  do {                                                            \
    LOG(ERROR) << ctx->StrTraceId() << " Cache block to " << (to) \
               << " failed: key = " << key.Filename()             \
               << ", length = " << block.size                     \
               << ", status = " << status.ToString();             \
  } while (0);

#define GENERIC_LOG_PREFETCH_ERROR(to)                                 \
  do {                                                                 \
    LOG(ERROR) << ctx->StrTraceId() << " Prefetch block into " << (to) \
               << " failed: key = " << key.Filename()                  \
               << ", length = " << length                              \
               << ", status = " << status.ToString();                  \
  } while (0);

#define GENERIC_LOG_STAGE_ERROR()                                         \
  do {                                                                    \
    LOG(ERROR) << ctx->StrTraceId()                                       \
               << " Stage block to disk failed: key = " << key.Filename() \
               << ", length = " << block.size                             \
               << ", status = " << status.ToString();                     \
  } while (0);

#define GENERIC_LOG_LOAD_ERROR()                                           \
  do {                                                                     \
    LOG(ERROR) << ctx->StrTraceId()                                        \
               << " Load block from disk failed: key = " << key.Filename() \
               << ", offset = " << offset << ", length = " << length       \
               << ", status = " << status.ToString();                      \
  } while (0);

#define GENERIC_LOG_UPLOAD_ERROR()                                            \
  do {                                                                        \
    LOG(ERROR) << ctx->StrTraceId()                                           \
               << " Upload block to storage failed: key = " << key.Filename() \
               << ", size = " << block.size                                   \
               << ", status = " << status.ToString();                         \
  } while (0);

#define GENERIC_LOG_DOWNLOAD_ERROR()                            \
  do {                                                          \
    LOG(ERROR) << ctx->StrTraceId()                             \
               << " Download block from storage failed: key = " \
               << key.Filename() << ", offset = " << offset     \
               << ", length = " << length                       \
               << ", status = " << status.ToString();           \
  } while (0);

#define ON_SCOPE_EXIT BRPC_SCOPE_EXIT

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_CONST_H_
