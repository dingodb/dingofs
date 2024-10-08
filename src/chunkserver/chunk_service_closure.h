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
 * Created Date: Thursday June 20th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_
#define SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_

#include <braft/raft.h>
#include <brpc/closure_guard.h>

#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/inflight_throttle.h"
#include "src/common/timeutility.h"

namespace curve {
namespace chunkserver {

// chunk service层的闭包，对rpc的闭包再做一层封装，用于请求返回时统计metric信息
class ChunkServiceClosure : public braft::Closure {
 public:
  explicit ChunkServiceClosure(
      std::shared_ptr<InflightThrottle> inflightThrottle,
      const ChunkRequest* request, ChunkResponse* response,
      google::protobuf::Closure* done)
      : inflightThrottle_(inflightThrottle),
        request_(request),
        response_(response),
        brpcDone_(done),
        receivedTimeUs_(common::TimeUtility::GetTimeofDayUs()) {
    // closure创建的什么加1，closure调用的时候减1
    if (nullptr != inflightThrottle_) {
      inflightThrottle_->Increment();
    }
    // 统计请求数量
    OnRequest();
  }

  ~ChunkServiceClosure() = default;

  /**
   * 该闭包的guard生命周期结束时会调用该函数
   * 该函数内目前主要是对读写请求返回结果的一些metric统计
   * 后面如果有类似的场景（在service请求结束时做一些处理）可以在内部添加逻辑
   */
  void Run() override;

 private:
  /**
   * 统计请求数量和速率
   */
  void OnRequest();
  /**
   * 记录请求处理的结果，例如请求是否出错、请求的延时等
   */
  void OnResonse();

 private:
  // inflight流控
  std::shared_ptr<InflightThrottle> inflightThrottle_;
  // rpc请求的request
  const ChunkRequest* request_;
  // rpc请求的response
  ChunkResponse* response_;
  // rpc请求回调
  google::protobuf::Closure* brpcDone_;
  // 接受到请求的时间
  uint64_t receivedTimeUs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_
