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

#include "proto/metaserver.pb.h"
#include "base/math/math.h"
#include "metaserver/superpartition/access_log.h"
#include "metaserver/superpartition/super_partition_storage.h"
#include "metaserver/superpartition/builder/builder.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace metaserver {
namespace superpartition {

using ::dingofs::base::math::kGiB;

using pb::metaserver::MetaStatusCode;
using pb::metaserver::Quota;
using pb::metaserver::Usage;

class SuperPartitionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    log_dir = ".access_log_" + GenUuid();
    CHECK(RunShell("mkdir -p " + log_dir, nullptr));
    CHECK(InitAccessLog(log_dir));
  }

  static void TearDownTestSuite() {
    ShutdownAccessLog();
    CHECK(RunShell("rm -rf " + log_dir, nullptr));
  }

  void SetUp() override {}
  void TearDown() override {}

 private:
  static std::string log_dir;
};

std::string SuperPartitionTest::log_dir;

TEST_F(SuperPartitionTest, SetFsQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000, _, _ })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(100) => { 100 GiB, 1000, 0, 0 }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_OnlyMaxBytes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, _, _, _ })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(100) => { 100 GiB, 0, 0, 0 }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 0);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_OnlyMaxInodes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { _, 1000, _, _ })
  {
    Quota quota;
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(100) => { 0, 1000, 0, 0 }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 0);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, SetFsQuota_MultiTimes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000, _, _ })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {  // GetFsQuota(100) => { 100 GiB, 1000, 0, 0 }
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 2: only set |maxbytes|
  {  // SetFsQuota(100, { 200 GiB, _, _, _ })
    Quota quota;
    quota.set_maxbytes(200 * kGiB);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }
  {  // GetFsQuota(100) => { 200 GiB, 1000, 0, 0 }
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 200 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
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
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
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
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
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

TEST_F(SuperPartitionTest, DeleteFsQuota_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000, _, _ })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(100) => { 100 GiB, 1000, 0, 0 }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_TRUE(quota.has_usedbytes());
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_TRUE(quota.has_usedinodes());
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 3: DeleteFsQuota(100)
  {
    auto rc = super_partition->DeleteFsQuota(100);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetFsQuota(100) => MetaStatusCode::NOT_FOUND
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetFsQuota(100) => { 100 GiB, 1000, 0, 0 }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 3: FlushFsUsage(100, { 10 MiB, 100 })
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(100);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 10 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 100);
  }

  // CASE 4: GetFsQuota(100) => { 100 GiB, 1000, 10 MiB, 100 }
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

TEST_F(SuperPartitionTest, FlushFsUsage_OnlyFlushUsedBytes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(100, { 10 MiB })
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(10 * kMiB);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 10 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 3: GetFsQuota(100) => { 100 GiB, 1000, 10 MiB, 0 }
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

TEST_F(SuperPartitionTest, FlushFsUsage_OnlyFlushUsedInodes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(100, { 100 })
  {
    Quota quota;
    Usage usage;
    usage.set_inodes(100);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 100);
  }

  // CASE 3: GetFsQuota(100) => { 100 GiB, 1000, 0, 100 }
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

TEST_F(SuperPartitionTest, FlushFsUsage_FsNotFound) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(200, { 10 MiB, 100 }) => NOT_FOUND
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(100);

    auto rc = super_partition->FlushFsUsage(200, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, FlushFsUsage_MultiTimes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(100, { 1 MiB, 1 }) x 500
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(1 * kMiB);
    usage.set_inodes(1);

    for (auto i = 1; i <= 500; i++) {
      auto rc = super_partition->FlushFsUsage(100, usage, &quota);
      ASSERT_EQ(rc, MetaStatusCode::OK);
      ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
      ASSERT_EQ(quota.maxinodes(), 1000);
      ASSERT_EQ(quota.usedbytes(), i * kMiB);
      ASSERT_EQ(quota.usedinodes(), i);
    }
  }

  // CASE 3: GetFsQuota(100) => { 100 GiB, 1000, 500 MiB, 500 }
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

TEST_F(SuperPartitionTest, FlushFsUsage_ExceedQuotaLimit) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(100, { 200 GiB, 2000 })
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(200 * kGiB);
    usage.set_inodes(2000);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 2000);
  }

  // CASE 3: GetFsQuota(100) => { 100 GiB, 1000, 200 GiB, 2000 }
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

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(100, { -200 GiB, -2000 })
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(-200 * kGiB);
    usage.set_inodes(-2000);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), -200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), -2000);
  }

  // CASE 3: GetFsQuota(100) => { 100 GiB, 1000, -200 GiB, -2000 }
  {
    Quota quota;
    auto rc = super_partition->GetFsQuota(100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), -200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), -2000);
  }

  // CASE 4: FlushFsUsage(100, { 300 GiB, 3000 })
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(300 * kGiB);
    usage.set_inodes(3000);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 100 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 1000);
  }

  // CASE 5: GetFsQuota(100) => { 100 GiB, 1000, 100 GiB, 1000 }
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

  // CASE 1: SetFsQuota(100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushFsUsage(100, { 200 GiB, 2000 })
  {
    Quota quota;
    Usage usage;
    usage.set_bytes(200 * kGiB);
    usage.set_inodes(2000);

    auto rc = super_partition->FlushFsUsage(100, usage, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 2000);
  }

  // CASE 3: SetFsQuota(100, { 300 GiB, 3000 })
  {
    Quota quota;
    quota.set_maxbytes(300 * kGiB);
    quota.set_maxinodes(3000);

    auto rc = super_partition->SetFsQuota(100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetFsQuota(100) => { 300 GiB, 3000, 200 GiB, 2000 }
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

TEST_F(SuperPartitionTest, LoadDirQuotas_LoadAfterDelete) {
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

  // CASE 2: LoadDirQuotas(1, ...)
  {
    Quotas quotas;
    auto rc = super_partition->LoadDirQuotas(1, &quotas);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quotas.size(), 1);
    auto quota = quotas[100];
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 3: DeleteDirQuota(1, 100, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->DeleteDirQuota(1, 100);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: LoadDirQuotas(1, ...)
  {
    Quotas quotas;
    auto rc = super_partition->LoadDirQuotas(1, &quotas);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quotas.size(), 0);
  }
}

TEST_F(SuperPartitionTest, LoadDirQuotas_FsId) {
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

  // CASE 2: LoadDirQuotas(1, ...)
  {
    Quotas quotas;
    auto rc = super_partition->LoadDirQuotas(2, &quotas);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quotas.size(), 0);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsage_Basic) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 0);
  }

  // CASE 3: FlushDirUsage(...)
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(100);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetDirUsage(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 10 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 100);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsage_OnlyUsedBytes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsages(...)
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(0);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 10 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 0);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsage_OnlyUsedInodes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsages(...)
  {
    Usage usage;
    usage.set_bytes(0);
    usage.set_inodes(100);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 0);
    ASSERT_EQ(quota.usedinodes(), 100);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsages_NotFound) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, ...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsage(2, ...)
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(0);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(2, usages);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsages_OneDirNotFound) {
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

  // CASE 2: FlushFsUsage(1, {100, 200}, ...)
  {
    Usage usage;
    usage.set_bytes(10 * kMiB);
    usage.set_inodes(0);
    Usages usages;
    usages.insert({100, usage});
    usages.insert({200, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::NOT_FOUND);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsages_MultiTimes) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsages(1, { 100: { MiB, 1 } }) x 500
  {
    Usage usage;
    usage.set_bytes(1 * kMiB);
    usage.set_inodes(1);
    Usages usages;
    usages.insert({100, usage});

    for (auto i = 1; i <= 500; i++) {
      auto rc = super_partition->FlushDirUsages(1, usages);
      ASSERT_EQ(rc, MetaStatusCode::OK);
    }
  }

  // CASE 3: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 500 * kMiB);
    ASSERT_EQ(quota.usedinodes(), 500);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsages_ExceedLimit) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(...)
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsages(...)
  {
    Usage usage;
    usage.set_bytes(200 * kGiB);
    usage.set_inodes(2000);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 2000);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsages_MultiDirs) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, [ 1, 2, 3, ... 10000 ], { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);

    for (auto i = 1; i <= 10000; i++) {
      auto rc = super_partition->SetDirQuota(1, i, quota);
      ASSERT_EQ(rc, MetaStatusCode::OK);
    }
  }

  // CASE 2: FlushDirUsages(1, { [ 1, 2, 3, ... 10000 ]: { i MiB, i } })
  {
    Usages usages;
    for (uint64_t i = 1; i <= 10000; i++) {
      Usage usage;
      usage.set_bytes(i * kGiB);
      usage.set_inodes(i);
      usages.insert({i, usage});
    }

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetDirQuota(1, [ 1, 2, 3, ... 10000 ])
  //         => { 100 GiB, 1000, i * GiB, i }
  {
    for (uint64_t i = 1; i <= 10000; i++) {
      Quota quota;
      auto rc = super_partition->GetDirQuota(1, i, &quota);
      ASSERT_EQ(rc, MetaStatusCode::OK);
      ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
      ASSERT_EQ(quota.maxinodes(), 1000);
      ASSERT_EQ(quota.usedbytes(), i * kGiB);
      ASSERT_EQ(quota.usedinodes(), i);
    }
  }
}

TEST_F(SuperPartitionTest, FlushDirUsages_ReduceUsage) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, 100, [100 GiB, 1000])
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsages(1, { 100: { -200 GiB, -2000 } })
  {
    Usage usage;
    usage.set_bytes(-200 * kGiB);
    usage.set_inodes(-2000);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), -200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), -2000);
  }

  // CASE 4: FlushDirUsages(1, { 100: { -300 GiB, -3000 } })
  {
    Usage usage;
    usage.set_bytes(300 * kGiB);
    usage.set_inodes(3000);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 5: GetDirQuota(...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 100 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 1000);
    ASSERT_EQ(quota.usedbytes(), 100 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 1000);
  }
}

TEST_F(SuperPartitionTest, FlushDirUsage_ResetQuota) {
  auto builder = SuperPartitionBuilder();
  auto super_partition = builder.Build();

  // CASE 1: SetDirQuota(1, 100, { 100 GiB, 1000 })
  {
    Quota quota;
    quota.set_maxbytes(100 * kGiB);
    quota.set_maxinodes(1000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 2: FlushDirUsages(..., [200 GiB, 2000])
  {
    Usage usage;
    usage.set_bytes(200 * kGiB);
    usage.set_inodes(2000);
    Usages usages;
    usages.insert({100, usage});

    auto rc = super_partition->FlushDirUsages(1, usages);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 3: SetDirQuota(1, 100, { 300 GiB, 3000 })
  {
    Quota quota;
    quota.set_maxbytes(300 * kGiB);
    quota.set_maxinodes(3000);
    auto rc = super_partition->SetDirQuota(1, 100, quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
  }

  // CASE 4: GetDirQuota(1, 100, ...)
  {
    Quota quota;
    auto rc = super_partition->GetDirQuota(1, 100, &quota);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(quota.maxbytes(), 300 * kGiB);
    ASSERT_EQ(quota.maxinodes(), 3000);
    ASSERT_EQ(quota.usedbytes(), 200 * kGiB);
    ASSERT_EQ(quota.usedinodes(), 2000);
  }
}

}  // namespace superpartition
}  // namespace metaserver
}  // namespace dingofs
