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

#include "cache/infiniband/service.h"

#include <google/protobuf/service.h>
#include <gtest/gtest.h>

#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

namespace {
// Minimal concrete BlockCacheService so ServiceHub has a real descriptor to
// register and introspect; the RPC bodies are irrelevant to these tests.
class StubBlockCacheService : public pb::cache::BlockCacheService {
 public:
  void Put(google::protobuf::RpcController*, const pb::cache::PutRequest*,
           pb::cache::PutResponse*, google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Range(google::protobuf::RpcController*, const pb::cache::RangeRequest*,
             pb::cache::RangeResponse*, google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Cache(google::protobuf::RpcController*, const pb::cache::CacheRequest*,
             pb::cache::CacheResponse*, google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Prefetch(google::protobuf::RpcController*,
                const pb::cache::PrefetchRequest*, pb::cache::PrefetchResponse*,
                google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
  void Ping(google::protobuf::RpcController*, const pb::cache::PingRequest*,
            pb::cache::PingResponse*, google::protobuf::Closure* done) override {
    if (done) done->Run();
  }
};
}  // namespace

TEST(ServiceHubTest, AddAndGetService) {
  ServiceHub hub;
  StubBlockCacheService service;

  ASSERT_TRUE(hub.AddService(&service).ok());
  EXPECT_TRUE(hub.AddService(&service).IsExist());  // duplicate registration

  std::string name = service.GetDescriptor()->full_name();
  google::protobuf::Service* found = nullptr;
  ASSERT_TRUE(hub.GetService(name, &found).ok());
  EXPECT_EQ(found, &service);

  EXPECT_TRUE(hub.GetService("no.such.Service", &found).IsNotFound());
}

TEST(ServiceHubTest, GetMethod) {
  ServiceHub hub;
  StubBlockCacheService service;
  ASSERT_TRUE(hub.AddService(&service).ok());

  google::protobuf::MethodDescriptor* method = nullptr;
  for (const char* name : {"Put", "Range", "Cache", "Prefetch", "Ping"}) {
    EXPECT_TRUE(hub.GetMethod(&service, name, &method).ok()) << name;
    EXPECT_NE(method, nullptr) << name;
  }

  EXPECT_TRUE(hub.GetMethod(&service, "NoSuchMethod", &method).IsNotFound());
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
