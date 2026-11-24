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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "blockcache/utils/align.h"
#include "blockcache/utils/hash.h"

namespace dingofs {
namespace blockcache {

constexpr std::string_view kNode1 = "044d4698-7bd4-4e44-9e94-aee6312ff06f";
constexpr std::string_view kNode2 = "85a4b352-6e0f-4c1d-9a2b-3f7c8d9e0a1b";
constexpr std::string_view kNode3 = "c3f1e2d4-5b6a-4f8e-9d0c-1a2b3c4d5e6f";

constexpr std::array<std::string_view, 16> kNodes = {
    kNode1,
    kNode2,
    kNode3,
    "6f9e8d7c-1b2a-4c3d-8e5f-0a9b8c7d6e5f",
    "01c7e3a9-2f4b-4d6e-8a1c-5b9d0e2f4a6c",
    "fbd9f999-47a1-452f-a73d-f709f85b7d03",
    "36fde8a7-b707-468a-b72c-ff9d16339a8d",
    "b59a0602-acc7-444c-8843-01f24f735a44",
    "91163553-1f2c-41b8-a8df-16a237f5229d",
    "661cb8a7-071c-481c-b79a-2f42bf44f0c4",
    "4960e378-3cbf-46a4-b97c-86895d562e72",
    "31524ffd-0f55-4d1c-b350-1b5fff843bf7",
    "dcd3ef16-d226-472e-8d8c-de5c17f890c3",
    "67d98013-a871-474c-b877-8a124a499058",
    "76737685-fced-4c34-b6b9-dc697e6dbbec",
    "5a52dbdf-cc82-4e34-821f-1f540ee3a19e",
};

// HashInt() of it is 0x00000000ffffffff: the last point a key can land on.
constexpr uint64_t kLastPointKey = 0xc348c88eb271b06fULL;

constexpr uint64_t kKeys = 100000;

struct Member {
  std::string_view id;
  uint64_t weight;
};

static std::unique_ptr<ConsistentHash> BuildCHash(
    const std::vector<Member>& members) {
  auto chash = std::make_unique<ConsistentHash>();
  for (const Member& member : members) {
    chash->Add(member.id, member.weight);
  }
  chash->Finalize();
  return chash;
}

// The first n of kNodes, each of weight 1.
static std::vector<Member> BuildMembers(uint32_t n) {
  std::vector<Member> members;
  members.reserve(n);
  for (uint32_t i = 0; i < n; i++) {
    members.push_back({.id = kNodes[i], .weight = 1});
  }
  return members;
}

static std::vector<uint64_t> BuildSliceIds(uint64_t n) {
  std::vector<uint64_t> ids;
  ids.reserve(n);
  for (uint64_t i = 0; i < n; i++) {
    ids.push_back(1001 + i);
  }
  return ids;
}

// The keys of n 4 MiB blocks, sixteen to a slice from 1001 on, spread over 64
// bits as BlockHandle::Hash() spreads them: MemberGroup routes a block by it.
static std::vector<uint64_t> BuildBlockKeys(uint64_t n) {
  std::vector<uint64_t> keys;
  keys.reserve(n);
  for (uint64_t i = 0; i < n; i++) {
    const uint64_t id = 1001 + (i / 16);
    const uint64_t index = i % 16;
    keys.push_back(HashInt(HashInt(id) ^ ((index << 32) | (4 * kMiB))));
  }
  return keys;
}

static std::vector<std::string> GetOwners(const ConsistentHash& chash,
                                          const std::vector<uint64_t>& keys) {
  std::vector<std::string> owners;
  owners.reserve(keys.size());
  for (const uint64_t key : keys) {
    owners.emplace_back(chash.GetMember(key));
  }
  return owners;
}

// The share of the keys each of members owns, in their order.
static std::vector<double> GetShares(const std::vector<std::string>& owners,
                                     const std::vector<Member>& members) {
  std::vector<double> shares;
  shares.reserve(members.size());
  for (const Member& member : members) {
    const auto num_owned = std::ranges::count(owners, member.id);
    const double share = static_cast<double>(num_owned) / owners.size();
    shares.push_back(share);
  }
  return shares;
}

TEST(HashTest, HashInt) {
  EXPECT_EQ(HashInt(0), 0);

  {  // it is SplitMix64's mixer: seeded with 0, SplitMix64 outputs
     // HashInt(k * 0x9e3779b97f4a7c15) for k = 1, 2, 3
    constexpr uint64_t kGamma = 0x9e3779b97f4a7c15ULL;
    EXPECT_EQ(HashInt(1 * kGamma), 0xe220a8397b1dcdafULL);
    EXPECT_EQ(HashInt(2 * kGamma), 0x6e789e6aa1b965f4ULL);
    EXPECT_EQ(HashInt(3 * kGamma), 0x06c45d188009454fULL);
  }

  {  // flipping any one bit of a slice id, a high one too, flips about 16 bits
     // of each half
    for (int bit = 0; bit < 64; bit++) {
      uint64_t low = 0;
      uint64_t high = 0;
      for (uint64_t id = 1001; id < 2001; id++) {
        const uint64_t diff = HashInt(id) ^ HashInt(id ^ (uint64_t{1} << bit));
        low += std::popcount(static_cast<uint32_t>(diff));
        high += std::popcount(static_cast<uint32_t>(diff >> 32));
      }
      EXPECT_NEAR(low / 1000.0, 16, 1) << bit;
      EXPECT_NEAR(high / 1000.0, 16, 1) << bit;
    }
  }

  {  // slice ids spread evenly over the low 32 bits, which the ring takes as a
     // key's point
    std::vector<uint64_t> buckets(64, 0);
    for (const uint64_t id : BuildSliceIds(64000)) {
      buckets[static_cast<uint32_t>(HashInt(id)) >> 26]++;
    }
    for (const uint64_t count : buckets) {
      EXPECT_NEAR(count, 1000, 150);
    }
  }
}

TEST(HashTest, HashStr) {
  // HashInt() of the reference values of 64-bit FNV-1a
  EXPECT_EQ(HashStr(""), HashInt(0xcbf29ce484222325ULL));
  EXPECT_EQ(HashStr("a"), HashInt(0xaf63dc4c8601ec8cULL));
  EXPECT_EQ(HashStr("fo"), HashInt(0x08985907b541d342ULL));
  EXPECT_EQ(HashStr("foo"), HashInt(0xdcb27518fed9d577ULL));
  EXPECT_EQ(HashStr("foob"), HashInt(0xdd120e790c2512afULL));
  EXPECT_EQ(HashStr("fooba"), HashInt(0xcac165afa2fef40aULL));
  EXPECT_EQ(HashStr("foobar"), HashInt(0x85944171f73967e8ULL));

  {  // a byte is taken as unsigned
    EXPECT_EQ(HashStr("\xff"), HashInt(0xaf64724c8602eb6eULL));
  }

  {  // a NUL byte is hashed too: the view's length counts, not strlen()
    EXPECT_EQ(HashStr(std::string_view("\0", 1)),
              HashInt(0xaf63bd4c8601b7dfULL));
    EXPECT_EQ(HashStr(std::string_view("foobar\0", 7)),
              HashInt(0x34531ca7168b8f38ULL));
  }

  {  // neighbouring ring strings land far apart in either half, since the ring
     // takes both halves of a hash as points
    const std::string id(kNode1);
    uint64_t low = 0;
    uint64_t high = 0;
    for (uint32_t i = 0; i < 2000; i += 2) {
      const uint64_t diff = HashStr(id + "-" + std::to_string(i)) ^
                            HashStr(id + "-" + std::to_string(i + 2));
      low += std::popcount(static_cast<uint32_t>(diff));
      high += std::popcount(static_cast<uint32_t>(diff >> 32));
    }
    EXPECT_NEAR(low / 1000.0, 16, 1);
    EXPECT_NEAR(high / 1000.0, 16, 1);
  }
}

// Reads the private state of ConsistentHash, as its friend.
class ConsistentHashPeer {
 public:
  static constexpr uint64_t kLightestPoints = ConsistentHash::kLightestPoints;
  static constexpr uint64_t kMaxPoints = ConsistentHash::kMaxPoints;

  static size_t GetTotalPoints(const ConsistentHash& chash) {
    return chash.ring_.size();
  }

  static size_t GetPointsById(const ConsistentHash& chash,
                              std::string_view id) {
    return std::ranges::count_if(chash.ring_, [&](const auto& point) {
      return chash.ids_[point.second] == id;
    });
  }
};

TEST(ConsistentHashTest, Add) {
  const std::vector<Member> members = {{.id = kNode1, .weight = 1},
                                       {.id = kNode2, .weight = 2},
                                       {.id = kNode3, .weight = 1}};
  const std::vector<uint64_t> slices = BuildSliceIds(kKeys);

  {  // only the ratios of the weights count: weights in bytes send every key
     // where small ones do
    const std::vector<Member> bytes = {{.id = kNode1, .weight = 100 * kGiB},
                                       {.id = kNode2, .weight = 200 * kGiB},
                                       {.id = kNode3, .weight = 100 * kGiB}};
    const auto small = BuildCHash(members);
    const auto large = BuildCHash(bytes);
    EXPECT_EQ(GetOwners(*small, slices), GetOwners(*large, slices));
  }

  {  // members own the keys in proportion to their weights: 1/4, 2/4 and 1/4
     // of them, each within 30%
    const auto chash = BuildCHash(members);
    const std::vector<std::string> owners = GetOwners(*chash, slices);
    const std::vector<double> shares = GetShares(owners, members);
    EXPECT_NEAR(shares[0] / 0.25, 1, 0.3);
    EXPECT_NEAR(shares[1] / 0.50, 1, 0.3);
    EXPECT_NEAR(shares[2] / 0.25, 1, 0.3);
  }

  {  // the ring keeps its own copy of an id, so the caller's may change
    std::string id(kNode1);
    ConsistentHash chash;
    chash.Add(id, 1);
    chash.Finalize();
    id.assign(id.size(), 'x');
    EXPECT_EQ(chash.GetMember(1001), kNode1);
  }
}

TEST(ConsistentHashTest, Finalize) {
  {  // the ring is sorted, so the order members were added in makes no
     // difference, but a point two ids share goes to the one added first
    const std::vector<uint64_t> slices = BuildSliceIds(kKeys);
    const auto forward = BuildCHash({{.id = kNode1, .weight = 1},
                                     {.id = kNode2, .weight = 2},
                                     {.id = kNode3, .weight = 1}});
    const auto backward = BuildCHash({{.id = kNode3, .weight = 1},
                                      {.id = kNode2, .weight = 2},
                                      {.id = kNode1, .weight = 1}});
    EXPECT_EQ(GetOwners(*forward, slices), GetOwners(*backward, slices));
  }

  {  // a ring of no members finalizes: MemberGroup builds one while no member
     // is online
    ConsistentHash chash;
    chash.Finalize();
  }

  {  // the lightest member gets kLightestPoints and the others in proportion
    const auto chash = BuildCHash({{.id = kNode1, .weight = 512000 * kMiB},
                                   {.id = kNode2, .weight = 1024000 * kMiB},
                                   {.id = kNode3, .weight = 2048000 * kMiB}});
    EXPECT_EQ(ConsistentHashPeer::GetPointsById(*chash, kNode1),
              ConsistentHashPeer::kLightestPoints);
    EXPECT_EQ(ConsistentHashPeer::GetPointsById(*chash, kNode2),
              ConsistentHashPeer::kLightestPoints * 2);
    EXPECT_EQ(ConsistentHashPeer::GetPointsById(*chash, kNode3),
              ConsistentHashPeer::kLightestPoints * 4);
  }

  {  // weights with no common divisor stay small: 1 TB beside 2 TB, in MiB
    const auto chash = BuildCHash({{.id = kNode1, .weight = 953674 * kMiB},
                                   {.id = kNode2, .weight = 1907349 * kMiB}});
    EXPECT_EQ(ConsistentHashPeer::GetTotalPoints(*chash),
              ConsistentHashPeer::kLightestPoints * 3);
  }

  {  // a ring that would pass kMaxPoints is scaled down to it, and the
     // lightest members, raised to one point each, still fit
    const auto chash = BuildCHash({{.id = kNode1, .weight = 1},
                                   {.id = kNode2, .weight = 1},
                                   {.id = kNode3, .weight = 1000000000}});
    EXPECT_LE(ConsistentHashPeer::GetTotalPoints(*chash),
              ConsistentHashPeer::kMaxPoints);
    EXPECT_EQ(ConsistentHashPeer::GetPointsById(*chash, kNode1), 1u);
    EXPECT_EQ(ConsistentHashPeer::GetPointsById(*chash, kNode2), 1u);
  }

  {  // a cache group of 200 nodes weighted 100 to 6400, as MDS may weigh them,
     // stays within kMaxPoints, and the lightest nodes keep their points
    std::vector<std::string> ids;
    ids.reserve(200);
    for (uint32_t i = 0; i < 200; i++) {
      ids.push_back("node-" + std::to_string(i));
    }

    std::vector<Member> members;
    members.reserve(ids.size());
    for (uint32_t i = 0; i < ids.size(); i++) {
      members.push_back({.id = ids[i], .weight = 100 * (1 + (i * 15 % 64))});
    }

    const auto chash = BuildCHash(members);
    EXPECT_LE(ConsistentHashPeer::GetTotalPoints(*chash),
              ConsistentHashPeer::kMaxPoints);
    for (const uint32_t i : {0, 64, 128, 192}) {  // the ones weighted 100
      EXPECT_GE(ConsistentHashPeer::GetPointsById(*chash, ids[i]), 1u)
          << ids[i];
    }
  }
}

TEST(ConsistentHashTest, GetMember) {
  {  // a lone member owns every key
    const auto chash = BuildCHash({{.id = kNode1, .weight = 1}});
    for (const uint64_t key :
         {uint64_t{0}, uint64_t{1001}, kLastPointKey, uint64_t{UINT64_MAX}}) {
      EXPECT_EQ(chash->GetMember(key), kNode1) << key;
    }
  }

  {  // the ring wraps around: a key past the last point goes to the first
     // point's member, as key 0 does, since HashInt(0) == 0
    const auto chash = BuildCHash({{.id = kNode1, .weight = 1},
                                   {.id = kNode2, .weight = 2},
                                   {.id = kNode3, .weight = 1}});
    ASSERT_EQ(static_cast<uint32_t>(HashInt(kLastPointKey)), UINT32_MAX);
    EXPECT_EQ(chash->GetMember(kLastPointKey), chash->GetMember(0));
  }
}

TEST(ConsistentHashTest, Balance) {
  {  // equal members each own about 1/n of the slices
    const std::vector<uint64_t> slices = BuildSliceIds(kKeys);
    for (const uint32_t n : {2, 3, 4, 8, 16}) {
      const std::vector<Member> members = BuildMembers(n);
      const auto chash = BuildCHash(members);
      const std::vector<std::string> owners = GetOwners(*chash, slices);
      const std::vector<double> shares = GetShares(owners, members);
      const double expected = 1.0 / n;
      for (uint32_t i = 0; i < n; i++) {
        EXPECT_NEAR(shares[i] / expected, 1, 0.3)
            << "member " << i << " of " << n;
      }
    }
  }

  {  // weights 1:64 apart, the most MemberGroup allows, split the blocks 1:64
    const std::vector<Member> members = {{.id = kNode1, .weight = 1},
                                         {.id = kNode2, .weight = 64}};
    const auto chash = BuildCHash(members);
    const std::vector<uint64_t> blocks = BuildBlockKeys(kKeys);
    const std::vector<std::string> owners = GetOwners(*chash, blocks);
    const std::vector<double> shares = GetShares(owners, members);
    const double expected = 1.0 / 65;  // kNode1's share
    EXPECT_NEAR(shares[0] / expected, 1, 0.3);
  }

  {  // keys of other patterns spread as evenly as slice ids: blocks, slice ids
     // of 4 clients from their own batches of 100000, every 1000th slice id,
     // and slice ids in the high 32 bits
    std::vector<std::vector<uint64_t>> patterns(4);
    patterns[0] = BuildBlockKeys(kKeys);
    for (uint64_t i = 0; i < kKeys; i++) {
      patterns[1].push_back(1001 + ((i % 4) * 100000) + (i / 4));
      patterns[2].push_back(1001 + (i * 1000));
      patterns[3].push_back((1001 + i) << 32);
    }

    const std::vector<Member> members = BuildMembers(4);
    const auto chash = BuildCHash(members);
    for (uint32_t p = 0; p < patterns.size(); p++) {
      const std::vector<std::string> owners = GetOwners(*chash, patterns[p]);
      const std::vector<double> shares = GetShares(owners, members);
      for (uint32_t i = 0; i < 4; i++) {
        EXPECT_NEAR(shares[i] / 0.25, 1, 0.3)
            << "pattern " << p << " member " << i;
      }
    }
  }
}

TEST(ConsistentHashTest, Rebalance) {
  const std::vector<uint64_t> slices = BuildSliceIds(kKeys);
  const auto chash = BuildCHash(BuildMembers(4));
  const std::vector<std::string> before = GetOwners(*chash, slices);

  {  // a leaving member's keys spread over every survivor, not one neighbour
    const auto survivors = BuildCHash(BuildMembers(3));
    const std::vector<std::string> after = GetOwners(*survivors, slices);
    std::map<std::string_view, double> parts;
    for (uint64_t i = 0; i < kKeys; i++) {
      if (before[i] == kNodes[3]) {
        parts[after[i]]++;
      }
    }

    const double given = parts[kNodes[0]] + parts[kNodes[1]] + parts[kNodes[2]];
    const double expected = 1.0 / 3;
    for (uint32_t i = 0; i < 3; i++) {
      const double part = parts[kNodes[i]] / given;
      EXPECT_NEAR(part / expected, 1, 0.6) << "survivor " << i;
    }
  }

  {  // a joining member takes about a fifth of every member's keys
    const auto joined = BuildCHash(BuildMembers(5));
    const std::vector<std::string> after = GetOwners(*joined, slices);
    std::map<std::string_view, double> owned;
    std::map<std::string_view, double> taken;
    for (uint64_t i = 0; i < kKeys; i++) {
      owned[before[i]]++;
      if (after[i] == kNodes[4]) {
        taken[before[i]]++;
      }
    }

    const double expected = 1.0 / 5;
    for (uint32_t i = 0; i < 4; i++) {
      const double lost = taken[kNodes[i]] / owned[kNodes[i]];
      EXPECT_NEAR(lost / expected, 1, 0.6) << "member " << i;
    }
  }

  {  // a member grown by a hair keeps every key: its points stay the same
    const auto disks = BuildCHash({{.id = kNode1, .weight = 512000 * kMiB},
                                   {.id = kNode2, .weight = 1024000 * kMiB},
                                   {.id = kNode3, .weight = 2048000 * kMiB}});
    const auto grown = BuildCHash({{.id = kNode1, .weight = 512000 * kMiB},
                                   {.id = kNode2, .weight = 1024000 * kMiB},
                                   {.id = kNode3, .weight = 2048001 * kMiB}});
    EXPECT_EQ(GetOwners(*disks, slices), GetOwners(*grown, slices));
  }
}

}  // namespace blockcache
}  // namespace dingofs
