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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "client/vfs/components/uid_gid_mapper.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

class CycleSource : public PasswdSource {
 public:
  std::vector<UserRecord> ListUsersWithPrimaryGid() override {
    return {{"alice", 1001, 1001}, {"bob", 1002, 1002}, {"carol", 1003, 0}};
  }
  std::vector<std::pair<std::string, uint32_t>> ListGroups() override {
    return {{"alice", 1001}, {"bob", 1002}};
  }
};

// Source whose ListUsersWithPrimaryGid/ListGroups always return empty so every
// Rebuild() produces an empty snapshot. Outbound lookups therefore always miss
// and pass through, exercising the reader read-lock against the refresher's
// table swap.
class EmptyListSource : public PasswdSource {
 public:
  std::vector<UserRecord> ListUsersWithPrimaryGid() override { return {}; }
  std::vector<std::pair<std::string, uint32_t>> ListGroups() override {
    return {};
  }
};

TEST(UidGidMapperConcurrent, ReadersAndRefresherRace) {
  UidGidMapper m(true, "salt", std::make_unique<CycleSource>());
  m.Refresh();
  // Precompute the real hashed IDs for alice so the full inbound lookup path
  // (uid_reverse_ + uid_name_to_local_) is exercised under contention.
  // Refresh() synchronously primes the snapshot so LocalIdToHashedId below hits
  // the fast path.
  const uint32_t alice_uid_hash = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  const uint32_t alice_gid_hash = m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1001);

  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < 64; ++i) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_relaxed)) {
        // Hit path: exercises reverse_ + name_to_local_ under contention.
        (void)m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
        (void)m.HashedIdToLocalId(UidGidMapper::Kind::kUid, alice_uid_hash);
        (void)m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1001);
        (void)m.HashedIdToLocalId(UidGidMapper::Kind::kGid, alice_gid_hash);
        // Miss path: short-circuits at reverse_ map lookup.
        (void)m.HashedIdToLocalId(UidGidMapper::Kind::kUid, 0xCAFEBABEu);
        (void)m.HashedIdToLocalId(UidGidMapper::Kind::kGid, 0xDEADBEEFu);
      }
    });
  }
  std::thread refresher([&] {
    for (int i = 0; i < 50 && !stop.load(); ++i) {
      m.Refresh();
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop.store(true);
  for (auto& t : readers) t.join();
  refresher.join();
}

TEST(UidGidMapperConcurrent, OutboundMissVsRebuildRace) {
  // ListUsers returns empty so every outbound call misses the snapshot and
  // passes through under the read lock. The refresher repeatedly swaps the
  // tables under the write lock. ASan/TSan pick up any data race between the
  // reader's read-lock and the refresher's table swap; functional check is
  // "no crash".
  UidGidMapper m(true, "salt", std::make_unique<EmptyListSource>());
  m.Refresh();

  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < 32; ++i) {
    readers.emplace_back([&, i] {
      uint32_t seed = static_cast<uint32_t>(i) * 7919u + 17u;
      while (!stop.load(std::memory_order_relaxed)) {
        // Spread queries across uids in [1, kLocalUidMax) to exercise both
        // newly-inserted and previously-inserted entries against the
        // refresher's wipe.
        uint32_t uid = 1 + (seed % (UidGidMapper::kLocalUidMax - 1));
        seed = seed * 1103515245u + 12345u;
        (void)m.LocalIdToHashedId(UidGidMapper::Kind::kUid, uid);
        (void)m.LocalIdToHashedId(UidGidMapper::Kind::kGid, uid);
      }
    });
  }
  std::thread refresher([&] {
    for (int i = 0; i < 200 && !stop.load(); ++i) {
      m.Refresh();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop.store(true);
  for (auto& t : readers) t.join();
  refresher.join();
}

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
