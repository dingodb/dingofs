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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include <google/protobuf/arena.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string_view>
#include <vector>

#include "cache/infiniband/common.h"
#include "cache/infiniband/protocol.h"
#include "cache/infiniband/service.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

using pb::infiniband::ErrorCode;
using pb::infiniband::RequestMeta;

namespace {
// Minimal concrete service so RequestParser can resolve a real descriptor.
class StubBlockCacheService : public pb::cache::BlockCacheService {
 public:
  void Put(google::protobuf::RpcController*, const pb::cache::PutRequest*,
           pb::cache::PutResponse*, google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Range(google::protobuf::RpcController*, const pb::cache::RangeRequest*,
             pb::cache::RangeResponse*,
             google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Cache(google::protobuf::RpcController*, const pb::cache::CacheRequest*,
             pb::cache::CacheResponse*,
             google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Prefetch(google::protobuf::RpcController*,
                const pb::cache::PrefetchRequest*, pb::cache::PrefetchResponse*,
                google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Ping(google::protobuf::RpcController*, const pb::cache::PingRequest*,
            pb::cache::PingResponse*,
            google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
};
}  // namespace

// Owns the backing storage that an RDMABuffer points at.
class ScopedBuffer {
 public:
  explicit ScopedBuffer(uint32_t capacity) : storage_(capacity, 0) {
    buf_.data = storage_.data();
    buf_.capacity = capacity;
    buf_.length = 0;
  }
  RDMABuffer* get() { return &buf_; }

 private:
  std::vector<char> storage_;
  RDMABuffer buf_;
};

class ProtocolTest : public ::testing::Test {};

TEST_F(ProtocolTest, RequestRoundTrip) {
  ScopedBuffer sb(4096);

  RequestMeta meta;
  meta.set_service_name("dingofs.pb.cache.BlockCacheService");
  meta.set_method_name("Put");
  meta.set_attachment_size(42);

  RequestMeta payload;  // any protobuf message works as the data body
  payload.set_service_name("payload-marker");

  ASSERT_TRUE(Protocol::SerializeRequest(0x1234, meta, payload, sb.get()).ok());
  EXPECT_GT(sb.get()->length, Protocol::kHeaderSize);

  {
    uint64_t id = 0;
    ASSERT_TRUE(Protocol::PeekCorrelationId(sb.get(), &id).ok());
    EXPECT_EQ(id, 0x1234u);
  }

  uint64_t id = 0;
  RequestMeta parsed_meta;
  std::string_view view;
  ASSERT_TRUE(Protocol::ParseRequest(sb.get(), &id, &parsed_meta, &view).ok());
  EXPECT_EQ(id, 0x1234u);
  EXPECT_EQ(parsed_meta.service_name(), "dingofs.pb.cache.BlockCacheService");
  EXPECT_EQ(parsed_meta.method_name(), "Put");
  EXPECT_EQ(parsed_meta.attachment_size(), 42u);

  RequestMeta parsed_payload;
  ASSERT_TRUE(parsed_payload.ParseFromArray(view.data(), view.size()));
  EXPECT_EQ(parsed_payload.service_name(), "payload-marker");
}

TEST_F(ProtocolTest, ResponseRoundTrip) {
  {  // success carries a response body
    ScopedBuffer sb(4096);
    pb::infiniband::ResponseMeta meta;
    meta.set_error_code(ErrorCode::Ok);
    meta.set_attachment_size(5);
    RequestMeta payload;
    payload.set_service_name("resp-body");

    ASSERT_TRUE(Protocol::SerializeResponse(7, meta, &payload, sb.get()).ok());

    uint64_t id = 0;
    pb::infiniband::ResponseMeta parsed_meta;
    std::string_view view;
    ASSERT_TRUE(
        Protocol::ParseResponse(sb.get(), &id, &parsed_meta, &view).ok());
    EXPECT_EQ(id, 7u);
    EXPECT_EQ(parsed_meta.error_code(), ErrorCode::Ok);
    EXPECT_EQ(parsed_meta.attachment_size(), 5u);

    RequestMeta parsed_payload;
    ASSERT_TRUE(parsed_payload.ParseFromArray(view.data(), view.size()));
    EXPECT_EQ(parsed_payload.service_name(), "resp-body");
  }

  {  // error response has no body (data_len == 0)
    ScopedBuffer sb(4096);
    pb::infiniband::ResponseMeta meta;
    meta.set_error_code(ErrorCode::Unknown);
    meta.set_error_message("boom");

    ASSERT_TRUE(Protocol::SerializeResponse(8, meta, nullptr, sb.get()).ok());

    uint64_t id = 0;
    pb::infiniband::ResponseMeta parsed_meta;
    std::string_view view;
    ASSERT_TRUE(
        Protocol::ParseResponse(sb.get(), &id, &parsed_meta, &view).ok());
    EXPECT_EQ(id, 8u);
    EXPECT_EQ(parsed_meta.error_code(), ErrorCode::Unknown);
    EXPECT_EQ(parsed_meta.error_message(), "boom");
    EXPECT_EQ(view.size(), 0u);
  }
}

TEST_F(ProtocolTest, SerializeRejectsTooSmallBuffer) {
  ScopedBuffer sb(10);  // smaller than the 24-byte header
  RequestMeta meta;
  meta.set_service_name("x");
  RequestMeta payload;
  EXPECT_TRUE(
      Protocol::SerializeRequest(1, meta, payload, sb.get()).IsInvalidParam());
}

TEST_F(ProtocolTest, ParseRejectsBadMagic) {
  ScopedBuffer sb(4096);
  RequestMeta meta;
  meta.set_service_name("svc");
  RequestMeta payload;
  ASSERT_TRUE(Protocol::SerializeRequest(1, meta, payload, sb.get()).ok());

  *reinterpret_cast<uint32_t*>(sb.get()->data) = 0xDEADBEEF;
  uint64_t id = 0;
  EXPECT_TRUE(Protocol::PeekCorrelationId(sb.get(), &id).IsInvalidParam());
}

TEST_F(ProtocolTest, ParseRejectsShortAndIncompleteFrames) {
  {  // shorter than the fixed header
    ScopedBuffer sb(4096);
    sb.get()->length = 10;
    uint64_t id = 0;
    EXPECT_TRUE(Protocol::PeekCorrelationId(sb.get(), &id).IsInvalidParam());
  }

  {  // header present but the declared meta/data bytes are missing
    ScopedBuffer sb(4096);
    RequestMeta meta;
    meta.set_service_name("svc");
    RequestMeta payload;
    payload.set_service_name("body");
    ASSERT_TRUE(Protocol::SerializeRequest(1, meta, payload, sb.get()).ok());

    sb.get()->length = Protocol::kHeaderSize;  // truncate the body
    uint64_t id = 0;
    RequestMeta parsed;
    std::string_view view;
    EXPECT_TRUE(
        Protocol::ParseRequest(sb.get(), &id, &parsed, &view).IsInvalidParam());
  }
}

TEST_F(ProtocolTest, RequestSerializerEmbedsRegions) {
  ScopedBuffer sb(4096);

  Region write_region;
  write_region.addr = 0x1000;
  write_region.length = 64;
  write_region.rkey = 7;

  Region read_region;
  read_region.addr = 0x2000;
  read_region.length = 128;
  read_region.rkey = 9;

  RequestMeta body;
  body.set_service_name("req-body");

  RequestSerializer serializer;
  RequestSerializer::Context ctx;
  ctx.correlation_id = 99;
  ctx.service_name = "svc";
  ctx.method_name = "method";
  ctx.attachment_size = 8;
  ctx.read_regions = {read_region};
  ctx.write_region = write_region;
  ctx.request = &body;

  ASSERT_TRUE(serializer.Serialize(ctx, sb.get()).ok());

  uint64_t id = 0;
  RequestMeta meta;
  std::string_view view;
  ASSERT_TRUE(Protocol::ParseRequest(sb.get(), &id, &meta, &view).ok());
  EXPECT_EQ(id, 99u);
  EXPECT_EQ(meta.service_name(), "svc");
  EXPECT_EQ(meta.method_name(), "method");
  EXPECT_EQ(meta.attachment_size(), 8u);
  ASSERT_EQ(meta.read_regions_size(), 1);
  EXPECT_EQ(meta.read_regions(0).addr(), 0x2000u);
  EXPECT_EQ(meta.write_region().addr(), 0x1000u);
  EXPECT_EQ(meta.write_region().length(), 64u);
  EXPECT_EQ(meta.write_region().rkey(), 7u);
}

TEST_F(ProtocolTest, RequestParserResolvesServiceAndMethod) {
  StubBlockCacheService service;
  ServiceHub hub;
  ASSERT_TRUE(hub.AddService(&service).ok());

  pb::cache::PutRequest put_req;  // empty body is fine for round-trip
  ScopedBuffer sb(4096);
  RequestSerializer serializer;
  RequestSerializer::Context ctx;
  ctx.correlation_id = 77;
  ctx.service_name = service.GetDescriptor()->full_name();
  ctx.method_name = "Put";
  ctx.request = &put_req;
  ASSERT_TRUE(serializer.Serialize(ctx, sb.get()).ok());

  RequestParser parser(&hub);
  RequestParser::Result result;
  google::protobuf::Arena arena;
  RequestParser::Option option;
  option.arena = &arena;
  ASSERT_TRUE(parser.Parse(sb.get(), &result, option).ok());
  EXPECT_EQ(result.correlation_id, 77u);
  EXPECT_EQ(result.service, &service);
  ASSERT_NE(result.method, nullptr);
  EXPECT_EQ(result.method->name(), "Put");
  EXPECT_NE(result.request, nullptr);
  EXPECT_NE(result.response, nullptr);
}

TEST_F(ProtocolTest, RequestParserUnknownServiceFails) {
  ServiceHub hub;  // nothing registered
  ScopedBuffer sb(4096);
  pb::cache::PutRequest put_req;
  RequestSerializer serializer;
  RequestSerializer::Context ctx;
  ctx.correlation_id = 1;
  ctx.service_name = "no.such.Service";
  ctx.method_name = "Put";
  ctx.request = &put_req;
  ASSERT_TRUE(serializer.Serialize(ctx, sb.get()).ok());

  RequestParser parser(&hub);
  RequestParser::Result result;
  google::protobuf::Arena arena;
  RequestParser::Option option;
  option.arena = &arena;
  EXPECT_TRUE(parser.Parse(sb.get(), &result, option).IsNotFound());
}

TEST_F(ProtocolTest, ResponseSerializerParserRoundTrip) {
  ScopedBuffer sb(4096);

  RequestMeta body;
  body.set_service_name("resp-body");

  ResponseSerializer serializer;
  ResponseSerializer::Context ctx;
  ctx.correlation_id = 55;
  ctx.error_code = ErrorCode::Ok;
  ctx.attachment_size = 16;
  ctx.response = &body;
  ASSERT_TRUE(serializer.Serialize(ctx, sb.get()).ok());

  ResponseParser parser;
  ResponseParser::Result result;
  RequestMeta parsed_body;
  ASSERT_TRUE(parser.Parse(sb.get(), &result, &parsed_body).ok());
  EXPECT_EQ(result.correlation_id, 55u);
  EXPECT_EQ(result.error_code, ErrorCode::Ok);
  EXPECT_EQ(result.attachment_size, 16u);
  EXPECT_EQ(parsed_body.service_name(), "resp-body");
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
