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

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "cache/node/node.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

BlockHandle Handle(uint64_t id = 100) {
  return BlockHandle(1, BlockKey(id, 0, 4096));
}

IOBuffer Buffer(const std::string& data) {
  return IOBuffer(data.data(), data.size());
}

}  // namespace

class CacheNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_cache_store_ = FLAGS_cache_store;
    saved_id_ = FLAGS_id;
    saved_listen_ip_ = FLAGS_listen_ip;
    saved_listen_port_ = FLAGS_listen_port;
    saved_group_weight_ = FLAGS_group_weight;

    FLAGS_cache_store = "memory";
    FLAGS_id = "044d4698-7bd4-4e44-9e94-aee6312ff06f";
    FLAGS_listen_ip = "127.0.0.1";
    FLAGS_listen_port = 9300;
    FLAGS_group_weight = 20;
  }

  void TearDown() override {
    FLAGS_cache_store = saved_cache_store_;
    FLAGS_id = saved_id_;
    FLAGS_listen_ip = saved_listen_ip_;
    FLAGS_listen_port = saved_listen_port_;
    FLAGS_group_weight = saved_group_weight_;
  }

 private:
  std::string saved_cache_store_;
  std::string saved_id_;
  std::string saved_listen_ip_;
  int32_t saved_listen_port_{0};
  uint32_t saved_group_weight_{0};
};

TEST_F(CacheNodeTest, ShutdownBeforeStartIsOk) {
  CacheNode node;
  EXPECT_TRUE(node.Shutdown().ok());
}

TEST_F(CacheNodeTest, RequestsReturnCacheDownBeforeStart) {
  CacheNode node;

  EXPECT_TRUE(node.Put(Handle(), Buffer("block")).IsCacheDown());

  bool cache_hit = true;
  IOBuffer out;
  EXPECT_TRUE(node.Range(Handle(), 0, 4, &out, 4096, &cache_hit).IsCacheDown());
  EXPECT_FALSE(cache_hit);

  EXPECT_TRUE(node.AsyncCache(Handle(), Buffer("block")).IsCacheDown());
  EXPECT_TRUE(node.AsyncPrefetch(Handle(), 4096).IsCacheDown());
}

TEST_F(CacheNodeTest, StreamOperatorUsesFlags) {
  CacheNode node;

  std::ostringstream oss;
  oss << node;

  EXPECT_NE(oss.str().find("044d4698-7bd4-4e44-9e94-aee6312ff06f"),
            std::string::npos);
  EXPECT_NE(oss.str().find("127.0.0.1"), std::string::npos);
  EXPECT_NE(oss.str().find("weight=20"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
