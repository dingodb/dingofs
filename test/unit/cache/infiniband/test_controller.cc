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

#include "cache/infiniband/controller.h"

#include <gtest/gtest.h>

#include <string>

#include "common/io_buffer.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

TEST(ControllerTest, DefaultState) {
  Controller cntl;
  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorText(), "");
  EXPECT_EQ(cntl.ErrorCode(), pb::infiniband::ErrorCode::Ok);
  EXPECT_EQ(cntl.timeout_ms(), -1);
  EXPECT_EQ(cntl.response_attachment_size(), 0u);
  EXPECT_FALSE(cntl.IsCanceled());
}

TEST(ControllerTest, SetFailedAndErrorCode) {
  Controller cntl;
  cntl.SetFailed("network error");
  EXPECT_TRUE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorText(), "network error");

  cntl.SetErrorCode(pb::infiniband::ErrorCode::Unknown);
  EXPECT_EQ(cntl.ErrorCode(), pb::infiniband::ErrorCode::Unknown);
}

TEST(ControllerTest, Reset) {
  Controller cntl;
  cntl.SetFailed("oops");
  cntl.SetErrorCode(pb::infiniband::ErrorCode::Unknown);
  cntl.set_timeout_ms(1000);
  cntl.set_response_attachment_size(42);

  cntl.Reset();

  EXPECT_FALSE(cntl.Failed());
  EXPECT_EQ(cntl.ErrorText(), "");
  EXPECT_EQ(cntl.ErrorCode(), pb::infiniband::ErrorCode::Ok);
  EXPECT_EQ(cntl.timeout_ms(), -1);
  EXPECT_EQ(cntl.response_attachment_size(), 0u);
}

TEST(ControllerTest, TimeoutAndRegions) {
  Controller cntl;

  cntl.set_timeout_ms(500);
  EXPECT_EQ(cntl.timeout_ms(), 500);

  cntl.set_response_attachment_size(64);
  EXPECT_EQ(cntl.response_attachment_size(), 64u);

  cntl.write_region().set_addr(0x2000);
  cntl.write_region().set_length(128);
  EXPECT_EQ(cntl.write_region().addr(), 0x2000u);
  EXPECT_EQ(cntl.write_region().length(), 128u);

  cntl.read_regions().Add()->set_rkey(9);
  ASSERT_EQ(cntl.read_regions().size(), 1);
  EXPECT_EQ(cntl.read_regions().Get(0).rkey(), 9u);
}

TEST(ControllerTest, CancelIsNoopAndAttachmentsAccessible) {
  Controller cntl;

  cntl.StartCancel();              // no-op for the RDMA controller
  EXPECT_FALSE(cntl.IsCanceled());
  cntl.NotifyOnCancel(nullptr);    // no-op

  std::string req = "request-data";
  std::string resp = "response-data";
  cntl.request_attachment() = IOBuffer(req.data(), req.size());
  cntl.response_attachment() = IOBuffer(resp.data(), resp.size());

  const Controller& c = cntl;
  EXPECT_EQ(c.request_attachment().Size(), req.size());
  EXPECT_EQ(c.response_attachment().Size(), resp.size());
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
