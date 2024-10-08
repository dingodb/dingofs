/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_CHUNK_CLOSURE_H_
#define SRC_CHUNKSERVER_CHUNK_CLOSURE_H_

#include <brpc/channel.h>
#include <brpc/closure_guard.h>

#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

using curve::chunkserver::ChunkOpRequest;

/**
 * 携带op request的所有上下文的closure，通过braft::Task传递给raft处理，
 * 调用会有两个地方：
 * 1.op request正常的被raft处理，最后on apply的时候会调用返回
 * 2.op request被打包给raft处理之后，但是还没有来得及处理就出错了，例如leader
 *   step down变为了非leader，那么会明确的提前向client返回错误
 */
class ChunkClosure : public braft::Closure {
 public:
  explicit ChunkClosure(std::shared_ptr<ChunkOpRequest> request)
      : request_(request) {}

  ~ChunkClosure() = default;

  void Run() override;

 public:
  // 包含了op request 的上下文信息
  std::shared_ptr<ChunkOpRequest> request_;
};

class ScanChunkClosure : public google::protobuf::Closure {
 public:
  ScanChunkClosure(ChunkRequest* request, ChunkResponse* response)
      : request_(request), response_(response) {}

  ~ScanChunkClosure() = default;

  void Run() override;

 public:
  ChunkRequest* request_;
  ChunkResponse* response_;
};

class SendScanMapClosure : public google::protobuf::Closure {
 public:
  SendScanMapClosure(FollowScanMapRequest* request,
                     FollowScanMapResponse* response, uint64_t timeout,
                     uint32_t retry, uint64_t retryIntervalUs,
                     brpc::Controller* cntl, brpc::Channel* channel)
      : request_(request),
        response_(response),
        rpcTimeoutMs_(timeout),
        retry_(retry),
        retryIntervalUs_(retryIntervalUs),
        cntl_(cntl),
        channel_(channel) {}

  ~SendScanMapClosure() = default;

  void Run() override;

 private:
  void Guard();

 public:
  FollowScanMapRequest* request_;
  FollowScanMapResponse* response_;
  uint64_t rpcTimeoutMs_;
  uint32_t retry_;
  uint64_t retryIntervalUs_;
  brpc::Controller* cntl_;
  brpc::Channel* channel_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_CLOSURE_H_
