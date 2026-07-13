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

#include "common/status.h"

#include <gtest/gtest.h>

#include <cerrno>

namespace dingofs {

TEST(StatusTest, DefaultIsOk) {
  Status s;
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(s.ToString(), "OK");
  EXPECT_EQ(s.ToSysErrNo(), 0);
}

TEST(StatusTest, OKFactoryIsOk) {
  Status s = Status::OK();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(s, Status::OK());
}

TEST(StatusTest, ErrorCarriesMessageAndErrno) {
  Status s = Status::NotExist(ENOENT, "file", "missing");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotExist());
  EXPECT_EQ(s.Errno(), ENOENT);
  EXPECT_EQ(s.ToSysErrNo(), ENOENT);
  EXPECT_EQ(s.ToString(), "NotExist (errno:2) : file: missing");
}

TEST(StatusTest, ErrorWithoutErrnoOmitsErrnoSuffix) {
  Status s = Status::InvalidParam("bad argument");
  EXPECT_EQ(s.ToString(), "InvalidParam: bad argument");
}

TEST(StatusTest, SingleMessageOmitsSeparator) {
  Status s = Status::Internal("boom");
  EXPECT_EQ(s.ToString(), "Internal: boom");
}

TEST(StatusTest, EqualityComparesCodeOnly) {
  Status a = Status::Exist("a-message");
  Status b = Status::Exist("different-message");
  EXPECT_EQ(a, b);
  EXPECT_NE(a, Status::NotExist("a-message"));
}

TEST(StatusTest, CopyConstructorClonesState) {
  Status original = Status::IoError("disk failure");
  Status copy(original);
  EXPECT_EQ(copy.ToString(), original.ToString());
  // Independent storage: mutating one must not affect the other.
  copy = Status::OK();
  EXPECT_EQ(original.ToString(), "IoError: disk failure");
}

TEST(StatusTest, CopyAssignmentClonesState) {
  Status original = Status::Timeout("slow rpc");
  Status copy;
  copy = original;
  EXPECT_EQ(copy.ToString(), original.ToString());
}

TEST(StatusTest, MoveConstructorTransfersState) {
  Status original = Status::NoSpace("disk full");
  Status moved(std::move(original));
  EXPECT_EQ(moved.ToString(), "NoSpace: disk full");
}

TEST(StatusTest, MoveAssignmentTransfersState) {
  Status original = Status::NoPermission("denied");
  Status target;
  target = std::move(original);
  EXPECT_EQ(target.ToString(), "NoPermission: denied");
}

TEST(StatusTest, ToSysErrNoMapsEachCodeToExpectedErrno) {
  EXPECT_EQ(Status::BadFd("").ToSysErrNo(), EBADF);
  EXPECT_EQ(Status::NoSpace("").ToSysErrNo(), ENOSPC);
  EXPECT_EQ(Status::InvalidParam("").ToSysErrNo(), EINVAL);
  EXPECT_EQ(Status::NotEmpty("").ToSysErrNo(), ENOTEMPTY);
  EXPECT_EQ(Status::NotSupport("").ToSysErrNo(), EOPNOTSUPP);
  EXPECT_EQ(Status::NameTooLong("").ToSysErrNo(), ENAMETOOLONG);
  EXPECT_EQ(Status::OutOfRange("").ToSysErrNo(), ERANGE);
  EXPECT_EQ(Status::NoData("").ToSysErrNo(), ENODATA);
  EXPECT_EQ(Status::Stale("").ToSysErrNo(), ESTALE);
  EXPECT_EQ(Status::NoSys("").ToSysErrNo(), ENOSYS);
  EXPECT_EQ(Status::NoPermitted("").ToSysErrNo(), EPERM);
  EXPECT_EQ(Status::Timeout("").ToSysErrNo(), ETIMEDOUT);
  EXPECT_EQ(Status::OutOfMemory("").ToSysErrNo(), ENOMEM);
  EXPECT_EQ(Status::Exist("").ToSysErrNo(), EEXIST);
  EXPECT_EQ(Status::NotFound("").ToSysErrNo(), ENOENT);
  EXPECT_EQ(Status::Deleted("").ToSysErrNo(), ENOENT);
}

TEST(StatusTest, ToStringNamesEachRemainingErrorCode) {
  EXPECT_EQ(Status::Internal("x").ToString(), "Internal: x");
  EXPECT_EQ(Status::Unknown("x").ToString(), "Unknown: x");
  EXPECT_EQ(Status::NoFlush("x").ToString(), "NoFlush: x");
  EXPECT_EQ(Status::MountPointExist("x").ToString(), "MountPointExist: x");
  EXPECT_EQ(Status::MountFailed("x").ToString(), "MountFailed: x");
  EXPECT_EQ(Status::IoError("x").ToString(), "IoError: x");
  EXPECT_EQ(Status::NetError("x").ToString(), "NetError: x");
  EXPECT_EQ(Status::NotDirectory("x").ToString(), "NotDirectory: x");
  EXPECT_EQ(Status::FileTooLarge("x").ToString(), "FileTooLarge: x");
  EXPECT_EQ(Status::EndOfFile("x").ToString(), "EndOfFile: x");
  EXPECT_EQ(Status::Abort("x").ToString(), "Abort: x");
  EXPECT_EQ(Status::CacheDown("x").ToString(), "CacheDown: x");
  EXPECT_EQ(Status::CacheUnhealthy("x").ToString(), "CacheUnhealthy: x");
  EXPECT_EQ(Status::CacheFull("x").ToString(), "CacheFull: x");
  EXPECT_EQ(Status::Stop("x").ToString(), "Stop: x");
  EXPECT_EQ(Status::NotFit("x").ToString(), "NotFit: x");
}

TEST(StatusTest, ToSysErrNoMapsRemainingErrorCodesToEIO) {
  // These codes intentionally share the generic EIO errno.
  EXPECT_EQ(Status::Internal("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::Unknown("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::NoFlush("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::MountPointExist("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::MountFailed("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::IoError("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::NetError("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::Stop("").ToSysErrNo(), EIO);
  EXPECT_EQ(Status::NotFit("").ToSysErrNo(), EIO);
}

TEST(StatusTest, IsXxxPredicatesMatchTheirOwnCodeOnly) {
  Status s = Status::CacheDown("down");
  EXPECT_TRUE(s.IsCacheDown());
  EXPECT_FALSE(s.IsCacheUnhealthy());
  EXPECT_FALSE(s.IsCacheFull());
  EXPECT_FALSE(s.IsNotDirectory());
  EXPECT_FALSE(s.IsFileTooLarge());
  EXPECT_FALSE(s.IsEndOfFile());
}

TEST(StatusTest, DINGOFS_RETURN_NOT_OK_ReturnsOnError) {
  auto fn = []() -> Status {
    DINGOFS_RETURN_NOT_OK(Status::Abort("stop here"));
    ADD_FAILURE() << "must not reach past a non-OK status";
    return Status::OK();
  };
  Status s = fn();
  EXPECT_TRUE(s.IsAbort());
}

TEST(StatusTest, DINGOFS_RETURN_NOT_OK_PassesThroughOnOk) {
  auto fn = []() -> Status {
    DINGOFS_RETURN_NOT_OK(Status::OK());
    return Status::Deleted("reached");
  };
  Status s = fn();
  EXPECT_TRUE(s.IsDeleted());
}

}  // namespace dingofs
