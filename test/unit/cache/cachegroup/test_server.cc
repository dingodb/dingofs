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

#include "cache/cachegroup/server.h"

#include <gtest/gtest.h>

#include <string>

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

class CacheGroupServerTest : public ::testing::Test {
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

TEST_F(CacheGroupServerTest, ShutdownBeforeStartIsOk) {
  Server server;

  EXPECT_TRUE(server.Shutdown().ok());
}

}  // namespace cache
}  // namespace dingofs
