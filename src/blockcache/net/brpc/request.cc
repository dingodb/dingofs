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

#include "blockcache/net/brpc/request.h"

#include "blockcache/net/brpc/call.h"

namespace dingofs {
namespace blockcache {

Future<Status> BrpcRequest::Reply(ReplyCode code, std::string_view payload) {
  MarkReplied();
  call_->response->set_code(code);
  call_->response->set_payload(payload.data(), payload.size());
  call_->RunDone();
  return MakeReadyFuture<Status>(Status::OK());
}

Future<Status> BrpcRequest::FetchBody(BufferView dst) {
  const butil::IOBuf& body = call_->cntl->request_attachment();
  if (dst.data == nullptr || dst.size < body.size()) {
    return MakeReadyFuture<Status>(
        Status::OutOfRange("Fail to fetch a body: the buffer is too small"));
  }
  // The one copy on this path: brpc's blocks are unaligned and unregistered.
  body.copy_to(dst.data, body.size());
  return MakeReadyFuture<Status>(Status::OK());
}

Future<Status> BrpcRequest::DoReplyWithBody(ReplyCode code,
                                            std::string_view payload,
                                            BufferView body) {
  MarkReplied();
  ServerCall* call = call_;
  call->response->set_code(code);
  call->response->set_payload(payload.data(), payload.size());

  // Taken before the reply: brpc may release the body inside RunDone() below.
  call->body_started = true;
  Future<Status> released = call->body_done.GetFuture();

  // The buffer becomes the attachment: no copy; `meta` 0 is right for tcp.
  call->cntl->response_attachment().append_user_data_with_meta(
      body.data, body.size, [call](void*) { call->OnBulkReleased(); }, 0);
  call->RunDone();
  return released;
}

}  // namespace blockcache
}  // namespace dingofs
