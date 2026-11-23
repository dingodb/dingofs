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
 * Created Date: 2026-11-23
 * Author: AI
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "blockcache/core/runtime/bootstrap.h"
#include "common/options/cache.h"
#include "common/status.h"
#include "test/unit/blockcache/base/runtime.h"

namespace dingofs {
namespace blockcache {

TEST(BootstrapTest, StartProcessRuntime) {
  const gflags::FlagSaver flag_saver;
  SetRuntimeFlags();

  {
    FLAGS_cpuset = "0-";
    const Status status = StartProcessRuntime();
    EXPECT_TRUE(status.IsInvalidParam()) << status.ToString();
    EXPECT_FALSE(ProcessRuntimeStarted());
    FLAGS_cpuset = "";
  }

  {
    FLAGS_buffer_pool_mb = 0;
    const Status status = StartProcessRuntime();
    EXPECT_TRUE(status.IsInvalidParam()) << status.ToString();
    EXPECT_FALSE(ProcessRuntimeStarted());
    EXPECT_EQ(GetGlobalThreadPool(), nullptr);
    SetRuntimeFlags();
  }

  {
    const Status status = StartProcessRuntime();
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(ProcessRuntimeStarted());
    EXPECT_NE(GetGlobalThreadPool(), nullptr);
    StopProcessRuntime();
    EXPECT_FALSE(ProcessRuntimeStarted());
  }
}

}  // namespace blockcache
}  // namespace dingofs
