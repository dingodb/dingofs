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

#include "cache/node/service.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <string>

#include "cache/common/block_handle_helper.h"
#include "cache/infiniband/controller.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

class CountClosure : public google::protobuf::Closure {
 public:
  void Run() override { ++count; }

  int count{0};
};

pb::cache::BlockHandle RawHandle(uint64_t id = 100) {
  return ToHandlePB(BlockHandle(1, BlockKey(id, 0, 4096)));
}

void FillHandle(pb::cache::BlockHandle* handle, uint64_t id = 100) {
  *handle = RawHandle(id);
}

}  // namespace

class BlockCacheServiceImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_cache_store_ = FLAGS_cache_store;
    saved_id_ = FLAGS_id;

    FLAGS_cache_store = "memory";
    FLAGS_id = "044d4698-7bd4-4e44-9e94-aee6312ff06f";
  }

  void TearDown() override {
    FLAGS_cache_store = saved_cache_store_;
    FLAGS_id = saved_id_;
  }

 private:
  std::string saved_cache_store_;
  std::string saved_id_;
};

TEST_F(BlockCacheServiceImplTest, PutReturnsCacheDownWhenNodeIsDown) {
  CacheNode node;
  BlockCacheServiceImpl service(ServiceType::kBRPC, &node);
  brpc::Controller controller;
  controller.request_attachment().append("block", 5);

  pb::cache::PutRequest request;
  FillHandle(request.mutable_handle());
  request.set_block_size(5);
  pb::cache::PutResponse response;
  CountClosure done;

  service.Put(&controller, &request, &response, &done);

  EXPECT_EQ(response.status(), pb::cache::BlockCacheErrUnknown);
  EXPECT_EQ(done.count, 1);
}

TEST_F(BlockCacheServiceImplTest, RangeReturnsCacheDownWhenNodeIsDown) {
  CacheNode node;
  BlockCacheServiceImpl service(ServiceType::kBRPC, &node);
  brpc::Controller controller;

  pb::cache::RangeRequest request;
  FillHandle(request.mutable_handle());
  request.set_offset(0);
  request.set_length(4);
  request.set_block_size(4096);
  pb::cache::RangeResponse response;
  CountClosure done;

  service.Range(&controller, &request, &response, &done);

  EXPECT_EQ(response.status(), pb::cache::BlockCacheErrUnknown);
  EXPECT_FALSE(response.cache_hit());
  EXPECT_EQ(controller.response_attachment().length(), 0u);
  EXPECT_EQ(done.count, 1);
}

TEST_F(BlockCacheServiceImplTest, CacheRejectsMismatchedBodySize) {
  CacheNode node;
  BlockCacheServiceImpl service(ServiceType::kBRPC, &node);
  brpc::Controller controller;
  controller.request_attachment().append("tiny", 4);

  pb::cache::CacheRequest request;
  FillHandle(request.mutable_handle());
  request.set_block_size(8);
  pb::cache::CacheResponse response;
  CountClosure done;

  service.Cache(&controller, &request, &response, &done);

  EXPECT_EQ(response.status(), pb::cache::BlockCacheErrInvalidParam);
  EXPECT_EQ(done.count, 1);
}

TEST_F(BlockCacheServiceImplTest, CacheUsesRdmaAttachment) {
  CacheNode node;
  BlockCacheServiceImpl service(ServiceType::kRDMA, &node);
  infiniband::Controller controller;
  controller.request_attachment() = IOBuffer("block", 5);

  pb::cache::CacheRequest request;
  FillHandle(request.mutable_handle());
  request.set_block_size(5);
  pb::cache::CacheResponse response;
  CountClosure done;

  service.Cache(&controller, &request, &response, &done);

  EXPECT_EQ(response.status(), pb::cache::BlockCacheErrUnknown);
  EXPECT_EQ(done.count, 1);
}

TEST_F(BlockCacheServiceImplTest, PrefetchReturnsCacheDownWhenNodeIsDown) {
  CacheNode node;
  BlockCacheServiceImpl service(ServiceType::kBRPC, &node);
  brpc::Controller controller;

  pb::cache::PrefetchRequest request;
  FillHandle(request.mutable_handle());
  request.set_block_size(4096);
  pb::cache::PrefetchResponse response;
  CountClosure done;

  service.Prefetch(&controller, &request, &response, &done);

  EXPECT_EQ(response.status(), pb::cache::BlockCacheErrUnknown);
  EXPECT_EQ(done.count, 1);
}

TEST_F(BlockCacheServiceImplTest, PingRunsDone) {
  CacheNode node;
  BlockCacheServiceImpl service(ServiceType::kBRPC, &node);
  brpc::Controller controller;
  pb::cache::PingRequest request;
  pb::cache::PingResponse response;
  CountClosure done;

  service.Ping(&controller, &request, &response, &done);

  EXPECT_EQ(done.count, 1);
}

}  // namespace cache
}  // namespace dingofs
