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

#include "cache/remotecache/upstream.h"

#include <gtest/gtest.h>
#include <json/value.h>

#include <string>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

namespace {

BlockHandle Handle(uint64_t id) {
  return BlockHandle(1, BlockKey(id, 0, 4096));
}

IOBuffer Buffer(const std::string& data) {
  return IOBuffer(data.data(), data.size());
}

}  // namespace

TEST(UpstreamTest, DumpEmptyGroup) {
  Upstream upstream;
  Json::Value value;

  EXPECT_TRUE(upstream.Dump(value));
  ASSERT_TRUE(value.isMember("members"));
  EXPECT_TRUE(value["members"].isArray());
  EXPECT_EQ(value["members"].size(), 0u);
}

TEST(UpstreamTest, RequestsReturnNotFoundWithoutPeer) {
  Upstream upstream;
  const std::string data = "remote-cache-payload";
  auto payload = Buffer(data);

  EXPECT_TRUE(upstream.SendPutRequest(Handle(1001), payload).IsNotFound());
  EXPECT_TRUE(upstream.SendCacheRequest(Handle(1002), payload).IsNotFound());
  EXPECT_TRUE(upstream.SendPrefetchRequest(Handle(1003), 4096).IsNotFound());

  std::string out(8, '\0');
  IOBuffer out_buffer(out.data(), out.size());
  bool cache_hit = true;
  EXPECT_TRUE(upstream
                  .SendRangeRequest(Handle(1004), 0, out.size(), &out_buffer,
                                    4096, &cache_hit)
                  .IsNotFound());
  EXPECT_TRUE(cache_hit);
}

TEST(UpstreamVarsRecordGuardTest, RecordsSuccessAndErrors) {
  UpstreamVarsCollector vars;

  {
    Status status = Status::OK();
    UpstreamVarsRecordGuard guard("Put", 4096, status, &vars);
  }
  {
    Status status = Status::NetError("network");
    UpstreamVarsRecordGuard guard("Range", 128, status, &vars);
  }
  {
    Status status = Status::OK();
    UpstreamVarsRecordGuard guard("Cache", 4096, status, &vars);
  }
  {
    Status status = Status::NotFound("no peer");
    UpstreamVarsRecordGuard guard("Prefetch", 4096, status, &vars);
  }
}

}  // namespace cache
}  // namespace dingofs
