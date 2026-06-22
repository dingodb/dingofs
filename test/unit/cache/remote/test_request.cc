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

#include "cache/remote/request.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "common/io_buffer.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

TEST(RequestTest, MakeRequest) {
  pb::cache::PingRequest raw;
  std::string payload = "attachment";
  IOBuffer attachment(payload.data(), payload.size());
  IOBuffer response_attachment;

  {  // without attachments
    auto request = MakeRequest("Ping", raw);
    EXPECT_EQ(request.method, "Ping");
    EXPECT_EQ(request.request_attachment, nullptr);
    EXPECT_EQ(request.response_attachment, nullptr);
  }

  {  // with attachments
    auto request = MakeRequest("Put", raw, &attachment, &response_attachment);
    EXPECT_EQ(request.method, "Put");
    EXPECT_EQ(request.request_attachment, &attachment);
    EXPECT_EQ(request.response_attachment, &response_attachment);
  }
}

TEST(RequestTest, ResponseHoldsStatus) {
  Response<pb::cache::PingResponse> response;
  EXPECT_TRUE(response.status.ok());  // default Status is OK

  response.status = Status::NotFound("missing");
  EXPECT_TRUE(response.status.IsNotFound());
}

TEST(RequestTest, StreamOperators) {
  {
    auto request = MakeRequest("Range", pb::cache::RangeRequest());
    std::ostringstream oss;
    oss << request;
    EXPECT_NE(oss.str().find("Request{method=Range"), std::string::npos);
  }

  {
    Response<pb::cache::RangeResponse> response;
    std::ostringstream oss;
    oss << response;
    EXPECT_NE(oss.str().find("Response{"), std::string::npos);
  }
}

}  // namespace cache
}  // namespace dingofs
