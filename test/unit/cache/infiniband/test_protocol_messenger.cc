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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/messenger.h"
#include "cache/infiniband/protocol.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {
namespace {

namespace pb_cache = ::dingofs::pb::cache;

constexpr uint64_t kCorrelationId = 0x12345678;

class BlockingClosure : public ::google::protobuf::Closure {
 public:
  void Wait() { event_.wait(); }
  void Run() override { event_.signal(); }

 private:
  bthread::CountdownEvent event_{1};
};

struct OwnedRdmaBuffer {
  explicit OwnedRdmaBuffer(size_t capacity) : storage(capacity) {
    buffer.data = storage.data();
    buffer.capacity = static_cast<uint32_t>(storage.size());
  }

  std::vector<char> storage;
  RdmaBuffer buffer;
};

class TestBlockCacheService final : public pb_cache::BlockCacheService {
 public:
  void Put(::google::protobuf::RpcController* /*controller*/,
           const pb_cache::PutRequest* request, pb_cache::PutResponse* response,
           ::google::protobuf::Closure* done) override {
    put_called++;
    last_block_size = request->block_size();
    response->set_status(pb_cache::BlockCacheOk);
    done->Run();
  }

  void Range(::google::protobuf::RpcController* /*controller*/,
             const pb_cache::RangeRequest* /*request*/,
             pb_cache::RangeResponse* response,
             ::google::protobuf::Closure* done) override {
    response->set_status(pb_cache::BlockCacheOk);
    done->Run();
  }

  void Cache(::google::protobuf::RpcController* /*controller*/,
             const pb_cache::CacheRequest* /*request*/,
             pb_cache::CacheResponse* response,
             ::google::protobuf::Closure* done) override {
    response->set_status(pb_cache::BlockCacheOk);
    done->Run();
  }

  void Prefetch(::google::protobuf::RpcController* /*controller*/,
                const pb_cache::PrefetchRequest* /*request*/,
                pb_cache::PrefetchResponse* response,
                ::google::protobuf::Closure* done) override {
    response->set_status(pb_cache::BlockCacheOk);
    done->Run();
  }

  void Ping(::google::protobuf::RpcController* /*controller*/,
            const pb_cache::PingRequest* /*request*/,
            pb_cache::PingResponse* /*response*/,
            ::google::protobuf::Closure* done) override {
    done->Run();
  }

  int put_called{0};
  uint64_t last_block_size{0};
};

void PrepareRequestController(Controller* cntl, std::string method_name,
                              google::protobuf::Message* request) {
  cntl->correlation_id() = kCorrelationId;
  cntl->request_meta().set_service_name(
      pb_cache::BlockCacheService::descriptor()->full_name());
  cntl->request_meta().set_method_name(std::move(method_name));
  cntl->request() = request;
}

TEST(ProtocolTest, ParsesRequestBodyBeforeDispatch) {
  TestBlockCacheService service;
  Messenger messenger;
  ASSERT_TRUE(messenger.AddService(&service).ok());

  pb_cache::PutRequest request;
  request.set_block_size(123);

  Controller outgoing;
  PrepareRequestController(&outgoing, "Put", &request);

  OwnedRdmaBuffer frame(1024);
  ASSERT_TRUE(Protocol::SerializeRequest(&outgoing, &frame.buffer).ok());

  Controller incoming;
  ASSERT_TRUE(Protocol::ParseRequest(&incoming, &frame.buffer, messenger).ok());

  auto* parsed_request =
      dynamic_cast<pb_cache::PutRequest*>(&incoming.request());
  ASSERT_NE(nullptr, parsed_request);
  EXPECT_EQ(123, parsed_request->block_size());

  std::fill(frame.storage.begin() + Protocol::kHeaderSize, frame.storage.end(),
            '\0');

  BlockingClosure done;
  ASSERT_TRUE(messenger.Dispatch(&incoming, &done).ok());
  done.Wait();

  auto* parsed_response =
      dynamic_cast<pb_cache::PutResponse*>(&incoming.response());
  ASSERT_NE(nullptr, parsed_response);
  EXPECT_EQ(pb_cache::BlockCacheOk, parsed_response->status());
  EXPECT_EQ(1, service.put_called);
  EXPECT_EQ(123, service.last_block_size);
}

TEST(ProtocolTest, ResponseRoundTripParsesIntoBoundResponse) {
  pb_cache::PutResponse response;
  response.set_status(pb_cache::BlockCacheErrNotFound);

  Controller outgoing;
  outgoing.correlation_id() = kCorrelationId;
  outgoing.response() = &response;

  OwnedRdmaBuffer frame(1024);
  ASSERT_TRUE(Protocol::SerializeResponse(&outgoing, &frame.buffer).ok());

  pb_cache::PutResponse parsed;
  Controller incoming;
  incoming.response() = &parsed;
  ASSERT_TRUE(Protocol::ParseResponse(&incoming, &frame.buffer).ok());
  EXPECT_EQ(kCorrelationId, incoming.correlation_id());
  EXPECT_EQ(pb_cache::BlockCacheErrNotFound, parsed.status());
}

TEST(ProtocolTest, UnknownServiceAndMethodBecomeResponseErrors) {
  TestBlockCacheService service;
  Messenger messenger;
  ASSERT_TRUE(messenger.AddService(&service).ok());

  pb_cache::PingRequest request;
  Controller outgoing;
  outgoing.correlation_id() = kCorrelationId;
  outgoing.request_meta().set_service_name("missing.Service");
  outgoing.request_meta().set_method_name("Ping");
  outgoing.request() = &request;

  OwnedRdmaBuffer frame(1024);
  ASSERT_TRUE(Protocol::SerializeRequest(&outgoing, &frame.buffer).ok());

  Controller incoming;
  auto status = Protocol::ParseRequest(&incoming, &frame.buffer, messenger);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(incoming.CanRespond());
  EXPECT_NE(0, incoming.response_meta().error_code());

  PrepareRequestController(&outgoing, "MissingMethod", &request);
  ASSERT_TRUE(Protocol::SerializeRequest(&outgoing, &frame.buffer).ok());

  Controller missing_method;
  status = Protocol::ParseRequest(&missing_method, &frame.buffer, messenger);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(missing_method.CanRespond());
  EXPECT_NE(0, missing_method.response_meta().error_code());
}

TEST(ProtocolTest, InvalidFrameCannotRespond) {
  TestBlockCacheService service;
  Messenger messenger;
  ASSERT_TRUE(messenger.AddService(&service).ok());

  OwnedRdmaBuffer frame(Protocol::kHeaderSize);
  frame.buffer.length = Protocol::kHeaderSize;
  auto* header = reinterpret_cast<Protocol::MessageHeader*>(frame.buffer.data);
  header->magic = 0;

  Controller incoming;
  auto status = Protocol::ParseRequest(&incoming, &frame.buffer, messenger);
  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(incoming.CanRespond());
}

TEST(ProtocolTest, BodyParseFailureCanRespondWithError) {
  TestBlockCacheService service;
  Messenger messenger;
  ASSERT_TRUE(messenger.AddService(&service).ok());

  pb_cache::PutRequest request;
  request.set_block_size(123);

  Controller outgoing;
  PrepareRequestController(&outgoing, "Put", &request);

  OwnedRdmaBuffer frame(1024);
  ASSERT_TRUE(Protocol::SerializeRequest(&outgoing, &frame.buffer).ok());

  auto* header =
      reinterpret_cast<const Protocol::MessageHeader*>(frame.buffer.data);
  char* body = frame.buffer.data + Protocol::kHeaderSize + header->meta_len;
  std::fill(body, body + header->data_len, static_cast<char>(0xff));

  Controller incoming;
  auto status = Protocol::ParseRequest(&incoming, &frame.buffer, messenger);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(incoming.CanRespond());
  EXPECT_NE(0, incoming.response_meta().error_code());
}

}  // namespace
}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
