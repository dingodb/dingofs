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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <sstream>

#include "cache/remotecache/request.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

class RequestTest : public ::testing::Test {};

TEST_F(RequestTest, MakeRequest) {
  pb::cache::PutRequest raw;
  raw.set_block_size(1024);

  auto request = MakeRequest("Put", raw);

  EXPECT_EQ(request.method, "Put");
  EXPECT_EQ(request.raw.block_size(), 1024);
  EXPECT_EQ(request.body, nullptr);
}

TEST_F(RequestTest, MakeRequestWithBody) {
  pb::cache::PutRequest raw;
  raw.set_block_size(1024);

  IOBuffer body("hello", 5);
  auto request = MakeRequest("Put", raw, &body);

  EXPECT_EQ(request.method, "Put");
  EXPECT_EQ(request.raw.block_size(), 1024);
  EXPECT_NE(request.body, nullptr);
}

TEST_F(RequestTest, RequestStreamOperator) {
  pb::cache::PutRequest raw;
  raw.set_block_size(1024);

  auto request = MakeRequest("Put", raw);

  std::ostringstream oss;
  oss << request;

  EXPECT_NE(oss.str().find("Request"), std::string::npos);
  EXPECT_NE(oss.str().find("Put"), std::string::npos);
}

TEST_F(RequestTest, ResponseStreamOperator) {
  Response<pb::cache::PutResponse> response;
  response.status = Status::OK();

  std::ostringstream oss;
  oss << response;

  EXPECT_NE(oss.str().find("Response"), std::string::npos);
}

}  // namespace cache
}  // namespace dingofs
