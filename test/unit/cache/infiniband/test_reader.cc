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
 * Created Date: 2026-09-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cache/helper/infiniband.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/reader.h"

namespace dingofs {
namespace cache {
namespace infiniband {

using test::MakeRegion;
using test::Rejects;

TEST(BodyReaderTest, CheckSource) {
  const std::string kMissing = "request rdma memory context is missing";
  const std::string kMismatch =
      "request attachment size mismatches advertised rdma regions";

  {  // no regions at all
    EXPECT_TRUE(Rejects(BodyReader::CheckSource({}, 0), kMismatch));
    EXPECT_TRUE(Rejects(BodyReader::CheckSource({}, 8), kMismatch));
  }

  {  // every region needs addr and rkey
    EXPECT_TRUE(
        Rejects(BodyReader::CheckSource({MakeRegion(0, 8, 7)}, 8), kMissing));
    EXPECT_TRUE(Rejects(BodyReader::CheckSource({MakeRegion(0x1000, 8, 0)}, 8),
                        kMissing));
  }

  {  // regions must add up to the advertised size
    EXPECT_TRUE(Rejects(BodyReader::CheckSource({MakeRegion(0x1000, 8, 7)}, 16),
                        kMismatch));
    EXPECT_TRUE(Rejects(BodyReader::CheckSource({MakeRegion(0x1000, 8, 7)}, 4),
                        kMismatch));
  }

  {  // missing context is reported before a size mismatch
    EXPECT_TRUE(Rejects(BodyReader::CheckSource(
                            {MakeRegion(0x1000, 8, 7), MakeRegion(0, 8, 7)}, 4),
                        kMissing));
  }

  EXPECT_TRUE(BodyReader::CheckSource({MakeRegion(0x1000, 8, 7)}, 8).ok());
  EXPECT_TRUE(BodyReader::CheckSource(
                  {MakeRegion(0x1000, 8, 7), MakeRegion(0x2000, 24, 9)}, 32)
                  .ok());
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
