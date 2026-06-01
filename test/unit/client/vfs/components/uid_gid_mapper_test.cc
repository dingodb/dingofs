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
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client/vfs/components/uid_gid_mapper.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

namespace {

// Shared helper: snapshot is table-driven so tests can configure the
// per-uid/gid presence Refresh() will install. Pgid is always 0 — tests that
// care about the fast-path cookie should use PgidAwareSource further below.
class TableSource : public PasswdSource {
 public:
  explicit TableSource(std::vector<std::pair<std::string, uint32_t>> users = {},
                       std::vector<std::pair<std::string, uint32_t>> groups = {})
      : users_(std::move(users)), groups_(std::move(groups)) {}

  std::vector<UserRecord> ListUsersWithPrimaryGid() override {
    std::vector<UserRecord> out;
    out.reserve(users_.size());
    for (const auto& [name, uid] : users_) {
      out.push_back({name, uid, 0});
    }
    return out;
  }
  std::vector<std::pair<std::string, uint32_t>> ListGroups() override {
    return groups_;
  }

  void SetUsers(std::vector<std::pair<std::string, uint32_t>> v) {
    users_ = std::move(v);
  }
  void SetGroups(std::vector<std::pair<std::string, uint32_t>> v) {
    groups_ = std::move(v);
  }

 private:
  std::vector<std::pair<std::string, uint32_t>> users_;
  std::vector<std::pair<std::string, uint32_t>> groups_;
};

}  // namespace

TEST(UidGidMapperTest, DisabledIsPassthrough) {
  UidGidMapper m(/*enabled=*/false, "salt", std::make_unique<LibcPasswdSource>());
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), 1001u);
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, 0xDEADBEEFu), 0xDEADBEEFu);
}

TEST(UidGidMapperTest, EnabledOutbound_HashesIntoReservedRange) {
  UidGidMapper m(/*enabled=*/true, "salt-X",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}}));
  m.Refresh();
  uint32_t a = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  uint32_t b = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  EXPECT_EQ(a, b);
  EXPECT_GE(a, UidGidMapper::kLocalUidMax);
}

TEST(UidGidMapperTest, EnabledOutbound_DifferentSaltDifferentId) {
  auto make = [] {
    return std::make_unique<TableSource>(
        std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}});
  };
  UidGidMapper m1(true, "salt-A", make());
  m1.Refresh();
  UidGidMapper m2(true, "salt-B", make());
  m2.Refresh();
  EXPECT_NE(m1.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), m2.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001));
}

TEST(UidGidMapperTest, OutboundRoot_AlwaysZero) {
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"root", 0}},
                     std::vector<std::pair<std::string, uint32_t>>{{"root", 0}}));
  m.Refresh();
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 0), 0u);
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 0), 0u);
}

TEST(UidGidMapperTest, OutboundRangeFuzz_AllInReservedRange) {
  std::vector<std::pair<std::string, uint32_t>> users;
  for (uint32_t i = 0; i < 1000; ++i) {
    users.emplace_back("user_" + std::to_string(i), 2000 + i);
  }
  UidGidMapper m(true, "salt-X",
                 std::make_unique<TableSource>(std::move(users)));
  m.Refresh();
  for (uint32_t i = 0; i < 1000; ++i) {
    uint32_t h = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 2000 + i);
    EXPECT_GE(h, UidGidMapper::kLocalUidMax);
  }
}

namespace {
// Convenience: TableSource pre-populated with alice@local_uid in both users
// and groups.
std::unique_ptr<TableSource> MakeAliceSource(uint32_t local_uid) {
  return std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{{"alice", local_uid}},
      std::vector<std::pair<std::string, uint32_t>>{{"alice", local_uid}});
}
}  // namespace

TEST(UidGidMapperTest, InboundRoot_AlwaysZero) {
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, 0), 0u);
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kGid, 0), 0u);
}

TEST(UidGidMapperTest, InboundBelowReserved_Passthrough) {
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, 1001), 1001u);  // raw caller uid stored by old client
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, 9999), 9999u);
}

TEST(UidGidMapperTest, InboundRoundtrip_AliceMapsBackToLocalUid) {
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  uint32_t hashed = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  ASSERT_GE(hashed, UidGidMapper::kLocalUidMax);
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, hashed), 1001u);
}

TEST(UidGidMapperTest, InboundDifferentHost_AliceMapsToHostLocalUid) {
  UidGidMapper writer(true, "salt", MakeAliceSource(1001));
  writer.Refresh();
  uint32_t hashed = writer.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);

  // simulate "host B" where alice=2002, same fs (same salt)
  UidGidMapper reader(true, "salt", MakeAliceSource(2002));
  reader.Refresh();
  EXPECT_EQ(reader.HashedIdToLocalId(UidGidMapper::Kind::kUid, hashed), 2002u);
}

TEST(UidGidMapperTest, InboundReverseHitButGetpwnamMiss_ReturnsStoredId) {
  // writer knows alice; reader knows no one
  UidGidMapper writer(true, "salt", MakeAliceSource(1001));
  writer.Refresh();
  uint32_t hashed = writer.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);

  UidGidMapper reader(true, "salt", std::make_unique<TableSource>());
  reader.Refresh();
  EXPECT_EQ(reader.HashedIdToLocalId(UidGidMapper::Kind::kUid, hashed), hashed);  // bare hash visible
}

TEST(UidGidMapperTest, InboundReverseMiss_ReturnsStoredId) {
  UidGidMapper reader(true, "salt", MakeAliceSource(1001));
  reader.Refresh();
  EXPECT_EQ(reader.HashedIdToLocalId(UidGidMapper::Kind::kUid, 123456789u), 123456789u);
}

TEST(UidGidMapperTest, OutboundUnmappedLocalUid_Passthrough) {
  // The snapshot only knows alice@1001; an outbound lookup for a uid that
  // Refresh() never installed is a pure miss and passes through untranslated.
  // There is no NSS fallback — the id is only translated once a Refresh()
  // (PasswdWatcher-driven in production) installs it.
  auto src = std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}});
  UidGidMapper m(true, "salt", std::move(src));
  m.Refresh();

  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1500), 1500u);
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1500), 1500u);
}

TEST(UidGidMapperTest, OutboundMiss_LaterRebuildFindsIt) {
  // After a miss, the source becomes aware of the user; the next Rebuild
  // installs it into the snapshot so the subsequent lookup hashes it.
  auto src = std::make_unique<TableSource>();
  auto* src_raw = src.get();
  UidGidMapper m(true, "salt", std::move(src));

  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1500), 1500u);  // miss
  src_raw->SetUsers({{"alice", 1500}});
  m.Refresh();
  uint32_t hashed = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1500);
  EXPECT_GE(hashed, UidGidMapper::kLocalUidMax);
}

TEST(UidGidMapperTest, OutboundAboveReserved_Passthrough) {
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 12345), 12345u);
}

TEST(UidGidMapperTest, SetEnabled_HotToggleRoundtrip) {
  auto src = std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}});
  UidGidMapper m(true, "salt", std::move(src));
  m.Refresh();

  uint32_t hashed = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  ASSERT_GE(hashed, UidGidMapper::kLocalUidMax);

  m.SetEnabled(false);
  EXPECT_FALSE(m.IsEnabled());
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), 1001u);          // passthrough
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, hashed), hashed);        // passthrough

  m.SetEnabled(true);
  EXPECT_TRUE(m.IsEnabled());
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), hashed);         // translated again
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, hashed), 1001u);
}

TEST(LibcPasswdSourceTest, ListUsersWithPrimaryGidIncludesRoot) {
  LibcPasswdSource src;
  auto users = src.ListUsersWithPrimaryGid();
  bool found_root = false;
  for (const auto& u : users) {
    if (u.name == "root" && u.uid == 0) {
      found_root = true;
      EXPECT_EQ(u.primary_gid, 0u) << "root's primary gid should be 0";
      break;
    }
  }
  EXPECT_TRUE(found_root) << "expected root user in /etc/passwd";
}

TEST(LibcPasswdSourceTest, ListGroupsIncludesRoot) {
  LibcPasswdSource src;
  auto groups = src.ListGroups();
  bool found_root = false;
  for (auto& [name, gid] : groups) {
    if (name == "root" && gid == 0) { found_root = true; break; }
  }
  EXPECT_TRUE(found_root) << "expected root group in /etc/group";
}

TEST(UidGidMapperTest, Disabled_RefreshNoOps_ThenToggleOnRefreshFills) {
  // Refresh() is a no-op while disabled — the snapshot stays empty, so even
  // after SetEnabled(true) the first outbound still misses and passes through
  // (no NSS fallback). A Refresh() after enabling — which production drives
  // off the enable toggle via the heartbeat — installs the entry and the
  // outbound then translates.
  auto src = std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}});
  UidGidMapper m(false, "salt", std::move(src));
  m.Refresh();
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), 1001u);  // disabled: passthrough

  m.SetEnabled(true);
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), 1001u);  // enabled but snapshot still empty
  m.Refresh();
  EXPECT_GE(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), UidGidMapper::kLocalUidMax);
}

namespace {

// Lookup an entries[] row by (type, name) for tests. Searches `mappings`
// inside each row so this also finds rows whose entry shares a hash with
// another row (collision row's losers).
const Json::Value* FindMapping(const Json::Value& entries, const char* type,
                               const std::string& name) {
  for (const auto& row : entries) {
    if (row["type"].asString() != type) continue;
    for (const auto& m : row["mappings"]) {
      if (m["name"].asString() == name) return &m;
    }
  }
  return nullptr;
}

const Json::Value* FindRowByHash(const Json::Value& entries, const char* type,
                                 uint64_t hash) {
  for (const auto& row : entries) {
    if (row["type"].asString() == type &&
        row["hashed_id"].asUInt64() == hash) {
      return &row;
    }
  }
  return nullptr;
}

}  // namespace

TEST(UidGidMapper, Dump_NoCollision_ProducesEntriesWithSingletonMappings) {
  auto src = std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{
          {"bob", 2002}, {"alice", 1001}},
      std::vector<std::pair<std::string, uint32_t>>{{"dev", 500}});
  UidGidMapper m(true, "salt", std::move(src));
  m.Refresh();

  Json::Value out;
  m.Dump(out);
  const Json::Value& d = out["uid_gid_map"];
  ASSERT_TRUE(d.isObject());
  EXPECT_TRUE(d["enabled"].asBool());
  EXPECT_EQ(d["salt"].asString(), "salt");
  EXPECT_GT(d["last_refresh_time_ms"].asUInt64(), 0u);
  EXPECT_EQ(d["uid_count"].asUInt64(), 2u);
  EXPECT_EQ(d["gid_count"].asUInt64(), 1u);
  EXPECT_FALSE(d["has_collisions"].asBool());

  const Json::Value& entries = d["entries"];
  ASSERT_EQ(entries.size(), 3u);

  // Each row holds exactly one mapping when there is no collision.
  for (const auto& row : entries) {
    ASSERT_EQ(row["mappings"].size(), 1u) << row.toStyledString();
  }

  // Lookups by name across all entries (sort order is by encoded key,
  // so we don't assert index positions).
  const Json::Value* alice = FindMapping(entries, "uid", "alice");
  ASSERT_NE(alice, nullptr);
  EXPECT_EQ((*alice)["local_id"].asUInt(), 1001u);

  const Json::Value* bob = FindMapping(entries, "uid", "bob");
  ASSERT_NE(bob, nullptr);
  EXPECT_EQ((*bob)["local_id"].asUInt(), 2002u);

  const Json::Value* dev = FindMapping(entries, "gid", "dev");
  ASSERT_NE(dev, nullptr);
  EXPECT_EQ((*dev)["local_id"].asUInt(), 500u);
}

TEST(UidGidMapper, Dump_HashCollision_ReportsMappingsWithWinnerAndLoser) {
  constexpr const char* kCollideU1 = "u_18475";
  constexpr const char* kCollideU2 = "u_210073";
  constexpr uint32_t kCollideUHash = 350950541u;

  auto src = std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{
          {kCollideU1, 2002}, {kCollideU2, 1001}});
  UidGidMapper m(true, "test-salt", std::move(src));
  m.Refresh();

  Json::Value out;
  m.Dump(out);
  const Json::Value& d = out["uid_gid_map"];
  EXPECT_TRUE(d["has_collisions"].asBool());

  const Json::Value* row =
      FindRowByHash(d["entries"], "uid", kCollideUHash);
  ASSERT_NE(row, nullptr);
  const Json::Value& mappings = (*row)["mappings"];
  ASSERT_EQ(mappings.size(), 2u);
  // mappings[0] is the winner (smallest local_id).
  EXPECT_EQ(mappings[0]["local_id"].asUInt(), 1001u);
  EXPECT_EQ(mappings[0]["name"].asString(), kCollideU2);
  EXPECT_EQ(mappings[1]["local_id"].asUInt(), 2002u);
  EXPECT_EQ(mappings[1]["name"].asString(), kCollideU1);
}

// Hash-collision fixture: offline birthday search (Python + hashlib) on
// HashToReserved(salt="test-salt", name="u_<i>") found the pair below
// both folding to kCollideUHash. Re-run the search if the algorithm or
// salt convention changes.
TEST(UidGidMapper, Refresh_HashCollision_SmallerLocalUidWins) {
  constexpr const char* kCollideU1 = "u_18475";
  constexpr const char* kCollideU2 = "u_210073";
  constexpr uint32_t kCollideUHash = 350950541u;

  // Deliberately list the larger uid first to prove ListUsers ordering
  // does not influence the outcome — the ascending sort in Refresh()
  // should make the smaller local uid win the reverse table.
  auto src = std::make_unique<TableSource>(
      std::vector<std::pair<std::string, uint32_t>>{
          {kCollideU1, 2002},
          {kCollideU2, 1001},
      });
  UidGidMapper m(true, "test-salt", std::move(src));
  m.Refresh();

  // Sanity: outbound remains a pure function — both names route to the
  // same hash regardless of who owns the reverse slot.
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 2002), kCollideUHash);
  EXPECT_EQ(m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001), kCollideUHash);

  // Reverse table is owned by the smaller local uid (1001 = kCollideU2).
  EXPECT_EQ(m.HashedIdToLocalId(UidGidMapper::Kind::kUid, kCollideUHash), 1001u);
}

// ---- LocalPairToHashed: pair outbound translation ----

TEST(UidGidMapperPair, OutboundDisabled_BothPassthrough) {
  UidGidMapper m(/*enabled=*/false, "salt",
                 std::make_unique<TableSource>());
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1001, 2002, ou, og);
  EXPECT_EQ(ou, 1001u);
  EXPECT_EQ(og, 2002u);
}

TEST(UidGidMapperPair, OutboundEqualInRange_UPG_OneFind) {
  // user alice@1000 + group alice@1000 (UPG) — uid==gid AND uname==gname.
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1000}},
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1000}}));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1000, 1000, ou, og);
  EXPECT_EQ(ou, m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1000));
  EXPECT_EQ(og, m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1000));
  EXPECT_EQ(ou, og);  // UPG: same name → same hash
}

TEST(UidGidMapperPair, OutboundEqualInRange_DifferentNames_TwoFields) {
  // uid==gid==1000 but uname="alice" != gname="developers".
  // Single find on numeric 1000 must return DIFFERENT hashes via two fields.
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1000}},
                     std::vector<std::pair<std::string, uint32_t>>{{"developers", 1000}}));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1000, 1000, ou, og);
  EXPECT_EQ(ou, m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1000));
  EXPECT_EQ(og, m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1000));
  EXPECT_NE(ou, og) << "different names must yield different hashes";
}

TEST(UidGidMapperPair, OutboundEqualInRange_OnlyUidKnown_GidPassthrough) {
  // id 1000 is a uid but no group with gid 1000 exists.
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1000}}));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1000, 1000, ou, og);
  EXPECT_GE(ou, UidGidMapper::kLocalUidMax);
  EXPECT_EQ(og, 1000u);  // passthrough
}

TEST(UidGidMapperPair, OutboundUnequal_TwoFindPath) {
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}},
                     std::vector<std::pair<std::string, uint32_t>>{{"dev", 500}}));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1001, 500, ou, og);
  EXPECT_EQ(ou, m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001));
  EXPECT_EQ(og, m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 500));
}

TEST(UidGidMapperPair, OutboundEqualButOutOfRange_BothPassthrough) {
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}}));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  // uid==gid==0 (root) → both 0
  m.LocalPairToHashed(0, 0, ou, og);
  EXPECT_EQ(ou, 0u);
  EXPECT_EQ(og, 0u);
  // uid==gid==12345 (>= kLocalUidMax) → passthrough
  m.LocalPairToHashed(12345, 12345, ou, og);
  EXPECT_EQ(ou, 12345u);
  EXPECT_EQ(og, 12345u);
}

namespace {

// PgidAwareSource lets tests inject a real paired_local_id cookie per user
// so the fast-path cookie branch can be exercised.
class PgidAwareSource : public PasswdSource {
 public:
  PgidAwareSource(std::vector<UserRecord> users,
                  std::vector<std::pair<std::string, uint32_t>> groups)
      : users_(std::move(users)), groups_(std::move(groups)) {}

  std::vector<UserRecord> ListUsersWithPrimaryGid() override {
    return users_;
  }
  std::vector<std::pair<std::string, uint32_t>> ListGroups() override {
    return groups_;
  }

 private:
  std::vector<UserRecord> users_;
  std::vector<std::pair<std::string, uint32_t>> groups_;
};

}  // namespace

TEST(UidGidMapperPair, OutboundUPG_NumericIdShared_FastPathOneFind) {
  // alice user 1000 with primary gid 1000, AND group X with gid 1000 — UPG by
  // number, different name. fast path: cookie matches, paired_hash != 0 →
  // single find returns out_uid=H("alice"), out_gid=H("X").
  PgidAwareSource src({{"alice", 1000, 1000}}, {{"X", 1000}});
  UidGidMapper m(true, "salt",
                 std::make_unique<PgidAwareSource>(std::move(src)));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1000, 1000, ou, og);
  EXPECT_EQ(ou, m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1000));
  EXPECT_EQ(og, m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1000));
  EXPECT_GE(ou, UidGidMapper::kLocalUidMax);
  EXPECT_GE(og, UidGidMapper::kLocalUidMax);
  EXPECT_NE(ou, og) << "different names must produce different hashes";
}

TEST(UidGidMapperPair, OutboundUPG_PrimaryGidSwitched_CookieMismatchDegrades) {
  // Models the staleness window: snapshot remembers alice with primary gid
  // 1001 (alice_group); admin has just `usermod -g 1000 alice` and snapshot
  // has not refreshed yet. FUSE arrives with (1000, 1000) since alice's
  // effective gid is now 1000.
  //   * /etc/group entries 1001 and 1000 are BOTH already in the snapshot
  //     because group X (gid 1000) existed independently before the switch.
  //   * cookie (1001) != fuse_gid (1000) → fast path bails.
  //   * Per-element path then hits {1000, Gid} directly → H("X").
  //   * Key assertion: out_gid is the H("X") hash, not the raw 1000 number.
  PgidAwareSource src({{"alice", 1000, 1001}},
                      {{"alice_group", 1001}, {"X", 1000}});
  UidGidMapper m(true, "salt",
                 std::make_unique<PgidAwareSource>(std::move(src)));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1000, 1000, ou, og);
  EXPECT_EQ(ou, m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1000));
  EXPECT_EQ(og, m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1000));
  EXPECT_GE(og, UidGidMapper::kLocalUidMax);
  EXPECT_NE(og, 1000u) << "must translate via {1000, Gid} entry, not passthrough";
}

TEST(UidGidMapperPair, OutboundUPG_PairedGroupMissingFromGroupFile) {
  // alice has primary gid 1001 but /etc/group has no entry for 1001 (admin
  // temporarily cleaned the group file). paired_hash falls to 0 → fast path
  // path's paired_hash != 0 guard fails even though the cookie matches → fall
  // through to the per-element path → {1001, Gid} miss → out_gid passthrough.
  PgidAwareSource src({{"alice", 1000, 1001}}, {{"X", 500}});
  UidGidMapper m(true, "salt",
                 std::make_unique<PgidAwareSource>(std::move(src)));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1000, 1001, ou, og);
  EXPECT_GE(ou, UidGidMapper::kLocalUidMax);
  EXPECT_EQ(og, 1001u) << "no group with gid 1001 in snapshot → passthrough";
}

TEST(UidGidMapperPair, OutboundEqualAcrossGate_PerElement) {
  // uid in range, gid out of range — must NOT take single-find branch.
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}}));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.LocalPairToHashed(1001, 70000, ou, og);
  EXPECT_EQ(ou, m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001));
  EXPECT_EQ(og, 70000u);
}

// ---- HashedPairToLocal: pair inbound translation ----

TEST(UidGidMapperPair, InboundDisabled_BothPassthrough) {
  UidGidMapper m(false, "salt", std::make_unique<TableSource>());
  uint32_t ou = 0, og = 0;
  m.HashedPairToLocal(123456u, 654321u, ou, og);
  EXPECT_EQ(ou, 123456u);
  EXPECT_EQ(og, 654321u);
}

TEST(UidGidMapperPair, InboundUPG_EqualHash_OneFind) {
  // UPG: user alice + group alice → both Hash("alice") → equal hashes.
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  uint32_t h_uid = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  uint32_t h_gid = m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 1001);
  ASSERT_EQ(h_uid, h_gid);

  uint32_t ou = 0, og = 0;
  m.HashedPairToLocal(h_uid, h_gid, ou, og);
  EXPECT_EQ(ou, 1001u);
  EXPECT_EQ(og, 1001u);
}

TEST(UidGidMapperPair, InboundUnequalHashes_TwoFindPath) {
  // alice (uid 1001) + developers (gid 500) → different names → different hashes.
  UidGidMapper m(true, "salt",
                 std::make_unique<TableSource>(
                     std::vector<std::pair<std::string, uint32_t>>{{"alice", 1001}},
                     std::vector<std::pair<std::string, uint32_t>>{{"developers", 500}}));
  m.Refresh();
  uint32_t h_uid = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  uint32_t h_gid = m.LocalIdToHashedId(UidGidMapper::Kind::kGid, 500);
  ASSERT_NE(h_uid, h_gid);

  uint32_t ou = 0, og = 0;
  m.HashedPairToLocal(h_uid, h_gid, ou, og);
  EXPECT_EQ(ou, 1001u);
  EXPECT_EQ(og, 500u);
}

TEST(UidGidMapperPair, InboundRoot_AlwaysZero) {
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  m.HashedPairToLocal(0, 0, ou, og);
  EXPECT_EQ(ou, 0u);
  EXPECT_EQ(og, 0u);
}

TEST(UidGidMapperPair, InboundEqualButBelowReserved_BothPassthrough) {
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  uint32_t ou = 0, og = 0;
  // Equal but below reserved range — must NOT take fast path.
  m.HashedPairToLocal(1001, 1001, ou, og);
  EXPECT_EQ(ou, 1001u);
  EXPECT_EQ(og, 1001u);
}

TEST(UidGidMapperPair, InboundAcrossGate_PerElement) {
  // huid in reserved range (hit), hgid below (passthrough).
  UidGidMapper m(true, "salt", MakeAliceSource(1001));
  m.Refresh();
  uint32_t h_uid = m.LocalIdToHashedId(UidGidMapper::Kind::kUid, 1001);
  ASSERT_GE(h_uid, UidGidMapper::kLocalUidMax);

  uint32_t ou = 0, og = 0;
  m.HashedPairToLocal(h_uid, 500, ou, og);
  EXPECT_EQ(ou, 1001u);
  EXPECT_EQ(og, 500u);   // below reserved → passthrough
}

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
