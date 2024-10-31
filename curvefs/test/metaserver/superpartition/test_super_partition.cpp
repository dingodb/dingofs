/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-10-31
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/base/math/math.h"
#include "curvefs/src/metaserver/superpartition/super_partition_storage.h"
#include "curvefs/test/metaserver/superpartition/builder/builder.h"
#include "gtest/gtest.h"

namespace curvefs {
namespace metaserver {
namespace superpartition {

using ::curvefs::base::math::kGiB;

class SuperPartitionTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(SuperPartitionTest, SetFsQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(10 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 10 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_InvalidParam) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: |maxbytes| and |maxinodes| are not setted
  {
    Quota quota;
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
  }

  // CASE 2: |usedbytes| setted
  {
    Quota quota;
    quota.set_maxbytes(10 * kGiB);
    quota.set_usedbytes(0);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
  }

  // CASE 3: |usedinodes| setted
  {
    Quota quota;
    quota.set_maxbytes(10 * kGiB);
    quota.set_usedinodes(0);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_OnlyMaxBytes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_TRUE(quota.has_maxinodes());
    ASSERT_EQ(quota.maxinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_OnlyMaxInodes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  {
    Quota quota;
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_TRUE(quota.has_maxbytes());
    ASSERT_EQ(quota.maxbytes(), 0);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_MultiTimes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: |maxbytes|=100GiB, |maxinodes|=1000
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 2: only set |maxbytes|
  {
    Quota quota;
    quota.set_maxbytes(200 * kGiB);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 200 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 3: only set |maxinodes|
  {
    Quota quota;
    quota.set_maxinodes(2000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 200 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 2000);
  }

  // CASE 4: both |maxbytes| and |maxbytes| are setted
  {
    Quota quota;
    quota.set_maxbytes(300 * kGiB);
    quota.set_maxinodes(3000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 300 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 3000);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_FsId) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 3: GetFsQuota(200, ...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(200, &quota);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, GetFsQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  for (auto i = 1; i <= 100; i++) {
    Quota quota;
    quota.set_maxbytes(i * 100 * kGiB);
    quota.set_maxinodes(i * 1000);
    auto rc = super_partition->SetFsQuota(i, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  for (auto i = 1; i <= 100; i++) {
    Quota quota;
    auto rc = super_partition->GetFsQuota(i, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), i * 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), i * 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 3: FlushFsUsage()
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(100);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 10 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 100);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_OnlyUsedBytes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage()
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 10 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_OnlyUsedInodes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(...)
  {
    Usage usage;
    usage.set_inodes(100);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 100);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_NotFound) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(200, ...)
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(100);
    auto rc = super_partition->FlushFsUsage(200, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_MultiTimes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(...) x 100
  {
    for (auto i = 1; i <= 500; i++) {
      Usage usage;
      usage.set_bytes(1 * kMiB);
      usage.set_inodes(1);
      auto rc = super_partition->FlushFsUsage(100, usage);
      ASSERT_EQ(rc, MetaStatusCode::OK);
    }
  }

  // CASE 3: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 500 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 500);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_ExceedLimit) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(...)
  {
    Usage usage;
    usage.set_bytes(200 * kGiB);
    usage.set_inodes(2000);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 2000);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_ReduceUsage) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(..., [100 GiB, 1000])
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(..., [-200 GiB, -2000])
  {
    Usage usage;
    usage.set_bytes(-200 * kGiB);
    usage.set_inodes(-2000);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), -200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), -2000);
  }

  // CASE 4: FlushFsUsage(..., [300 GiB, 3000])
  {
    Usage usage;
    usage.set_bytes(300 * kGiB);
    usage.set_inodes(3000);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 5: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 100 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 1000);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_ResetQuota) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(..., [100 GiB, 1000])
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(..., [200 GiB, 2000])
  {
    Usage usage;
    usage.set_bytes(200 * kGiB);
    usage.set_inodes(2000);
    auto rc = super_partition->FlushFsUsage(100, usage);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: SetFsQuota(..., [300 GiB, 3000])
  {
    Quota quota;
    quota.set_maxbytes(300 * kGiB);
    quota.set_maxinodes(3000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetFsQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 300 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 3000);
    ASSERT_EQ(quota.usedbytes(), 200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 2000);
  }
}

//
TEST_F(SuperPartitionTest, SetDirQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(10 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 10 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetDirQuota_InvalidParam) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: |maxbytes| and |maxinodes| are not setted
  {
    Quota quota;
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
  }

  // CASE 2: |usedbytes| setted
  {
    Quota quota;
    quota.set_maxbytes(10 * kGiB);
    quota.set_usedbytes(0);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
  }

  // CASE 3: |usedinodes| setted
  {
    Quota quota;
    quota.set_maxbytes(10 * kGiB);
    quota.set_usedinodes(0);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);
  }
}

TEST_F(SuperPartitionTest, SetDirQuota_OnlyMaxBytes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_TRUE(quota.has_maxinodes());
    ASSERT_EQ(quota.maxinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetDirQuota_OnlyMaxInodes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_TRUE(quota.has_maxbytes());
    ASSERT_EQ(quota.maxbytes(), 0);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }
}

TEST_F(SuperPartitionTest, SetDirQuota_MultiTimes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: |maxbytes|=100GiB, |maxinodes|=1000
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 2: only set |maxbytes|
  {
    Quota quota;
    quota.set_maxbytes(200 * kGiB);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 200 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 3: only set |maxinodes|
  {
    Quota quota;
    quota.set_maxinodes(2000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 200 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 2000);
  }

  // CASE 4: both |maxbytes| and |maxbytes| are setted
  {
    Quota quota;
    quota.set_maxbytes(300 * kGiB);
    quota.set_maxinodes(3000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 300 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 3000);
  }
}

TEST_F(SuperPartitionTest, SetDirQuota_FsId) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, 100, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 3: GetFsQuota(2, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(2, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, SetDirQuota_DirInodeId) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, 100, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: SetDirQuota(1, 200, ...)
  {
    Quota quota;
    quota.set_maxbytes(200 * kGiB);
    quota.set_maxinodes(2000);
    auto rc = super_partition->SetDirQuota(1, 200, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 4: GetFsQuota(1, 200, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 200, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 200 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 2000);
  }
}

TEST_F(SuperPartitionTest, GetDirQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  for (auto i = 1; i <= 100; i++) {
    Quota quota;
    quota.set_maxbytes(i * 100 * kGiB);
    quota.set_maxinodes(i * 1000);
    auto rc = super_partition->SetDirQuota(i, i, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  for (auto i = 1; i <= 100; i++) {
    Quota quota;
    auto rc = super_partition->GetDirQuota(i, i, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), i * 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), i * 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, DeleteDirQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, 100, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
  }

  // CASE 3: DeleteDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->DeleteDirQuota(1, 100);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, DeleteDirQuota_DelNotExistQuota) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }

  // CASE 2: DeleteDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->DeleteDirQuota(1, 100);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
}

TEST_F(SuperPartitionTest, LoadDirQuotas_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  for (auto i = 1; i <= 100; i++) {
    Quota quota;
    quota.set_maxbytes(i * 100 * kGiB);
    quota.set_maxinodes(i * 1000);
    auto rc = super_partition->SetDirQuota(1, i, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: LoadDirQuotas(...)
  Quotas quotas;
  auto rc = super_partition->LoadDirQuotas(1, &quotas);
  ASSERT_EQ(rc, MetaStatusCode::OK);
  ASSERT_EQ(quotas.size(), 100);
  for (auto i = 1; i <= 100; i++) {
    auto quota = quotas[i];
    ASSERT_EQ(quota.maxbytes(), i * 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), i * 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, LoadDirQuotas_Empty) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  Quotas quotas;
  auto rc = super_partition->LoadDirQuotas(1, &quotas);
  ASSERT_EQ(rc, MetaStatusCode::OK);
  ASSERT_EQ(quotas.size(), 0);
}

TEST_F(SuperPartitionTest, LoadDirQuotas_LoadAfterSet) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();
}

TEST_F(SuperPartitionTest, LoadDirQuotas_FsId) {}

}  // namespace superpartition
}  // namespace metaserver
}  // namespace curvefs
