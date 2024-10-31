
// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/client/filesystem/dir_quota.h"

#include <gtest/gtest.h>

namespace curvefs {
namespace client {
namespace filesystem {

class DirQuotaTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(DirQuotaTest, Init) {
  Quota quota;
  DirQuota dir_quota(1, quota);

  EXPECT_EQ(dir_quota.GetIno(), 1);

  Usage usage = dir_quota.GetUsage();
  EXPECT_EQ(usage.bytes(), 0);
  EXPECT_EQ(usage.inodes(), 0);
}

TEST_F(DirQuotaTest, Update) {
  {
    Quota quota;
    DirQuota dir_quota(1, quota);

    uint64_t space = 100;
    uint64_t inodes = 10;
    dir_quota.UpdateUsage(space, inodes);

    Usage usage = dir_quota.GetUsage();
    EXPECT_EQ(usage.bytes(), space);
    EXPECT_EQ(usage.inodes(), inodes);
  }

  {
    Quota quota;
    DirQuota dir_quota(1, quota);

    uint64_t space = 0;
    uint64_t inodes = 10;
    dir_quota.UpdateUsage(space, inodes);

    Usage usage = dir_quota.GetUsage();
    EXPECT_EQ(usage.bytes(), space);
    EXPECT_EQ(usage.inodes(), inodes);
  }

  {
    Quota quota;
    DirQuota dir_quota(1, quota);

    uint64_t space = 100;
    uint64_t inodes = 0;
    dir_quota.UpdateUsage(space, inodes);

    Usage usage = dir_quota.GetUsage();
    EXPECT_EQ(usage.bytes(), space);
    EXPECT_EQ(usage.inodes(), inodes);
  }

  {
    // Update with zero values
    Quota quota;
    DirQuota dir_quota(1, quota);

    uint64_t space = 0;
    uint64_t inodes = 0;
    dir_quota.UpdateUsage(space, inodes);

    Usage usage = dir_quota.GetUsage();
    EXPECT_EQ(usage.bytes(), space);
    EXPECT_EQ(usage.inodes(), inodes);
  }

  {
    // Update with negative values
    Quota quota;
    DirQuota dir_quota(1, quota);

    uint64_t space = -100;
    uint64_t inodes = -10;
    dir_quota.UpdateUsage(space, inodes);

    Usage usage = dir_quota.GetUsage();
    EXPECT_EQ(usage.bytes(), space);
    EXPECT_EQ(usage.inodes(), inodes);
  }

  {
    // Update with multiple times
    Quota quota;
    DirQuota dir_quota(1, quota);

    uint64_t space1 = 100;
    uint64_t inodes1 = 10;
    uint64_t space2 = 200;
    uint64_t inodes2 = 20;

    {
      dir_quota.UpdateUsage(space1, inodes1);

      Usage usage = dir_quota.GetUsage();

      EXPECT_EQ(usage.bytes(), space1);
      EXPECT_EQ(usage.inodes(), inodes1);
    }

    {
      dir_quota.UpdateUsage(space2, inodes2);
      Usage usage = dir_quota.GetUsage();

      EXPECT_EQ(usage.bytes(), space1 + space2);
      EXPECT_EQ(usage.inodes(), inodes1 + inodes2);
    }
  }
}

TEST_F(DirQuotaTest, Refresh) {
  Quota quota;
  quota.set_maxbytes(1000);
  quota.set_maxinodes(100);
  quota.set_usedbytes(500);
  quota.set_usedinodes(50);
  DirQuota dir_quota(1, quota);

  {
    Quota quota = dir_quota.GetQuota();
    EXPECT_EQ(quota.maxbytes(), 1000);
    EXPECT_EQ(quota.maxinodes(), 100);
    EXPECT_EQ(quota.usedbytes(), 500);
    EXPECT_EQ(quota.usedinodes(), 50);
  }

  {
    quota.set_maxbytes(2000);
    quota.set_maxinodes(200);
    quota.set_usedbytes(1000);
    quota.set_usedinodes(100);

    dir_quota.Refresh(quota);

    Quota quota = dir_quota.GetQuota();
    EXPECT_EQ(quota.maxbytes(), 2000);
    EXPECT_EQ(quota.maxinodes(), 200);
    EXPECT_EQ(quota.usedbytes(), 1000);
    EXPECT_EQ(quota.usedinodes(), 100);
  }

  {
    Quota zero_quota;
    zero_quota.set_maxbytes(0);
    zero_quota.set_maxinodes(0);
    zero_quota.set_usedbytes(0);
    zero_quota.set_usedinodes(0);

    dir_quota.Refresh(zero_quota);

    Quota quota = dir_quota.GetQuota();
    EXPECT_EQ(quota.maxbytes(), 0);
    EXPECT_EQ(quota.maxinodes(), 0);
    EXPECT_EQ(quota.usedbytes(), 0);
    EXPECT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(DirQuotaTest, CheckQuota) {
  Quota quota;
  quota.set_maxbytes(1000);
  quota.set_maxinodes(100);
  quota.set_usedbytes(500);
  quota.set_usedinodes(50);
  DirQuota dir_quota(1, quota);

  {
    // Check within limits
    int64_t new_space = 400;
    int64_t new_inodes = 40;

    EXPECT_TRUE(dir_quota.CheckQuota(new_space, new_inodes));
  }

  {
    // Check exceeds space limit
    int64_t new_space = 600;
    int64_t new_inodes = 40;

    EXPECT_FALSE(dir_quota.CheckQuota(new_space, new_inodes));
  }

  {
    // Check exceeds inode limit
    int64_t new_space = 400;
    int64_t new_inodes = 60;

    EXPECT_FALSE(dir_quota.CheckQuota(new_space, new_inodes));
  }

  {
    // Check exceeds both limits
    int64_t new_space = 600;
    int64_t new_inodes = 60;

    EXPECT_FALSE(dir_quota.CheckQuota(new_space, new_inodes));
  }
}

TEST_F(DirQuotaTest, CheckQuotaNoLimits) {
  Quota quota;
  DirQuota dir_quota(1, quota);

  int64_t new_space = 1000;
  int64_t new_inodes = 100;

  EXPECT_TRUE(dir_quota.CheckQuota(new_space, new_inodes));
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs