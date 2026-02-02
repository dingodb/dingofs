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

#include "cache/common/error.h"

namespace dingofs {
namespace cache {

class ErrorTest : public ::testing::Test {};

TEST_F(ErrorTest, ToPBErr) {
  EXPECT_EQ(ToPBErr(Status::OK()), pb::cache::BlockCacheOk);
  EXPECT_EQ(ToPBErr(Status::InvalidParam("")),
            pb::cache::BlockCacheErrInvalidParam);
  EXPECT_EQ(ToPBErr(Status::NotFound("")), pb::cache::BlockCacheErrNotFound);
  EXPECT_EQ(ToPBErr(Status::Internal("")), pb::cache::BlockCacheErrFailure);
  EXPECT_EQ(ToPBErr(Status::IoError("")), pb::cache::BlockCacheErrIOError);
  EXPECT_EQ(ToPBErr(Status::Exist("")), pb::cache::BlockCacheErrUnknown);
}

TEST_F(ErrorTest, ToStatus) {
  EXPECT_TRUE(ToStatus(pb::cache::BlockCacheOk).ok());
  EXPECT_TRUE(ToStatus(pb::cache::BlockCacheErrInvalidParam).IsInvalidParam());
  EXPECT_TRUE(ToStatus(pb::cache::BlockCacheErrNotFound).IsNotFound());
  EXPECT_TRUE(ToStatus(pb::cache::BlockCacheErrFailure).IsInternal());
  EXPECT_TRUE(ToStatus(pb::cache::BlockCacheErrIOError).IsIoError());
  EXPECT_TRUE(ToStatus(pb::cache::BlockCacheErrUnknown).IsInternal());
}

TEST_F(ErrorTest, RoundTrip) {
  {
    Status original = Status::OK();
    auto pb_err = ToPBErr(original);
    Status converted = ToStatus(pb_err);
    EXPECT_TRUE(converted.ok());
  }

  {
    Status original = Status::NotFound("test");
    auto pb_err = ToPBErr(original);
    Status converted = ToStatus(pb_err);
    EXPECT_TRUE(converted.IsNotFound());
  }

  {
    Status original = Status::IoError("test");
    auto pb_err = ToPBErr(original);
    Status converted = ToStatus(pb_err);
    EXPECT_TRUE(converted.IsIoError());
  }

  {
    Status original = Status::Internal("test");
    auto pb_err = ToPBErr(original);
    Status converted = ToStatus(pb_err);
    EXPECT_TRUE(converted.IsInternal());
  }
}

TEST_F(ErrorTest, ToPBErrWithMessage) {
  // Verify error messages don't affect error code mapping
  EXPECT_EQ(ToPBErr(Status::NotFound("file not found")),
            pb::cache::BlockCacheErrNotFound);
  EXPECT_EQ(ToPBErr(Status::NotFound("")), pb::cache::BlockCacheErrNotFound);
  EXPECT_EQ(ToPBErr(Status::NotFound("some long error message with details")),
            pb::cache::BlockCacheErrNotFound);
}

TEST_F(ErrorTest, IoErrorVsInternal) {
  // This is the bug fix verification - IoError and Internal should map
  // differently
  auto io_err = ToPBErr(Status::IoError("io error"));
  auto internal_err = ToPBErr(Status::Internal("internal error"));

  EXPECT_NE(io_err, internal_err);
  EXPECT_EQ(io_err, pb::cache::BlockCacheErrIOError);
  EXPECT_EQ(internal_err, pb::cache::BlockCacheErrFailure);
}

TEST_F(ErrorTest, UnknownStatusTypes) {
  // Status types not in the mapping should return Unknown
  EXPECT_EQ(ToPBErr(Status::Exist("exists")), pb::cache::BlockCacheErrUnknown);
  EXPECT_EQ(ToPBErr(Status::OutOfRange("range")),
            pb::cache::BlockCacheErrUnknown);
}

TEST_F(ErrorTest, ToStatusPreservesType) {
  // Verify ToStatus returns correct status types
  auto ok_status = ToStatus(pb::cache::BlockCacheOk);
  EXPECT_TRUE(ok_status.ok());
  EXPECT_FALSE(ok_status.IsNotFound());
  EXPECT_FALSE(ok_status.IsIoError());
  EXPECT_FALSE(ok_status.IsInternal());

  auto not_found = ToStatus(pb::cache::BlockCacheErrNotFound);
  EXPECT_FALSE(not_found.ok());
  EXPECT_TRUE(not_found.IsNotFound());
  EXPECT_FALSE(not_found.IsIoError());

  auto io_error = ToStatus(pb::cache::BlockCacheErrIOError);
  EXPECT_FALSE(io_error.ok());
  EXPECT_TRUE(io_error.IsIoError());
  EXPECT_FALSE(io_error.IsNotFound());
}

}  // namespace cache
}  // namespace dingofs
