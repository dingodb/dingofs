/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-06-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_SENDER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_SENDER_H_

#include <memory>

#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class RequestSender {
 public:
  explicit RequestSender(Connection* conn);

  Status Send(RDMABuffer* request);

 private:
  bool ShouldSignal();

  Connection* conn_;
  std::atomic<uint64_t> seq_num_;
};

using RequestSenderUPtr = std::unique_ptr<RequestSender>;

class ResponseSender {
 public:
  explicit ResponseSender(Connection* conn);

  Status Send(RDMABuffer* response, const Attachment& attachment = {});

 private:
  Status CheckRegion(const IOBuffer& buffer, const Region& region);
  Status PrepWorkRequest(const Attachment& attachment, SendWorkRequest* wr);
  void PrepWorkRequest(InflightContext* ctx, RDMABuffer* response,
                       SendWorkRequest* wr);

  Connection* conn_;
};

using ResponseSenderUPtr = std::unique_ptr<ResponseSender>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SENDER_H_
