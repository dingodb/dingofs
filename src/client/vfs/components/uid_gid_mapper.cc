// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/components/uid_gid_mapper.h"

#include <fmt/format.h>
#include <grp.h>
#include <openssl/evp.h>
#include <pwd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "common/logging.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {

// MD5(salt + name + salt) folded to uint32, mapped to [kLocalUidMax,
// UINT32_MAX].
static uint32_t HashToReserved(std::string_view salt, std::string_view name) {
  std::array<uint8_t, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_len = 0;
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  CHECK(ctx != nullptr);
  CHECK(EVP_DigestInit_ex(ctx, EVP_md5(), nullptr) == 1);
  CHECK(EVP_DigestUpdate(ctx, salt.data(), salt.size()) == 1);
  CHECK(EVP_DigestUpdate(ctx, name.data(), name.size()) == 1);
  CHECK(EVP_DigestUpdate(ctx, salt.data(), salt.size()) == 1);
  CHECK(EVP_DigestFinal_ex(ctx, digest.data(), &digest_len) == 1);
  EVP_MD_CTX_free(ctx);
  CHECK_GE(digest_len, 16u);

  uint64_t lo = 0, hi = 0;
  std::memcpy(&lo, digest.data(), sizeof(lo));
  std::memcpy(&hi, digest.data() + 8, sizeof(hi));
  uint32_t h = static_cast<uint32_t>(lo ^ hi);
  constexpr uint64_t span =
      static_cast<uint64_t>(UINT32_MAX) - UidGidMapper::kLocalUidMax + 1ULL;
  return UidGidMapper::kLocalUidMax +
         static_cast<uint32_t>(static_cast<uint64_t>(h) % span);
}

std::vector<std::pair<std::string, uint32_t>> LibcPasswdSource::ListGroups() {
  utils::WriteLockGuard lk(nss_lock_);
  std::vector<std::pair<std::string, uint32_t>> out;
  setgrent();
  while (auto* ent = getgrent()) {
    if (ent->gr_name != nullptr) {
      out.emplace_back(ent->gr_name, static_cast<uint32_t>(ent->gr_gid));
    }
  }
  endgrent();
  return out;
}

std::vector<PasswdSource::UserRecord>
LibcPasswdSource::ListUsersWithPrimaryGid() {
  // One sweep returns name + uid + pw_gid (primary gid) so Refresh can build
  // the user→pgid cookie without a second NSS pass. Shares nss_lock_ with the
  // other enumerators since getpwent() / setpwent() / endpwent() reuse the
  // same MT-Unsafe glibc iterator state.
  utils::WriteLockGuard lk(nss_lock_);
  std::vector<UserRecord> out;
  setpwent();
  while (auto* ent = getpwent()) {
    if (ent->pw_name != nullptr) {
      out.push_back({ent->pw_name, static_cast<uint32_t>(ent->pw_uid),
                     static_cast<uint32_t>(ent->pw_gid)});
    }
  }
  endpwent();
  return out;
}

UidGidMapper::UidGidMapper(bool enabled, std::string salt,
                           std::unique_ptr<PasswdSource> passwd)
    : enabled_(enabled), salt_(std::move(salt)), passwd_(std::move(passwd)) {}

void UidGidMapper::Init() {
  if (!IsEnabled()) {
    // Disabled: the snapshot is never consulted and nothing translates, so
    // skip both the priming Refresh and the /etc inotify watch entirely.
    LOG(INFO) << "[uid_gid_mapper] uid/gid map disabled for this fs.";
    return;
  }
  Refresh();
  LOG(INFO) << "[uid_gid_mapper] uid/gid map enabled for this fs.";
  StartWatching("/etc", {"passwd", "group"});
}

void UidGidMapper::StartWatching(std::string dir,
                                 std::vector<std::string> names) {
  watcher_ = std::make_unique<PasswdWatcher>(std::move(dir), std::move(names),
                                             [this] { Refresh(); });
  if (!watcher_->Start()) {
    LOG(WARNING) << "[uid_gid_mapper] passwd watcher failed to start; snapshot "
                    "will not auto-refresh on /etc/passwd changes.";
  }
}

void UidGidMapper::StopWatching() {
  if (watcher_ != nullptr) {
    watcher_->Stop();
  }
}

uint32_t UidGidMapper::Hash(std::string_view name) const {
  return HashToReserved(salt_, name);
}

void UidGidMapper::Refresh() {
  if (!enabled_.load(std::memory_order_relaxed)) return;

  auto users = passwd_->ListUsersWithPrimaryGid();
  auto groups = passwd_->ListGroups();

  // Sort ascending by id so on hash collision the smaller local id wins
  // (first push_back into the reverse vector takes position 0; later
  // duplicates land after it).
  std::sort(users.begin(), users.end(),
            [](const auto& a, const auto& b) { return a.uid < b.uid; });
  std::sort(groups.begin(), groups.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });

  // First a pre-pass over groups builds a gid → group-name index so the user
  // pass below can compute paired_hash without a second NSS sweep. We keep the
  // first (smallest-gid) name on collision, consistent with the reverse table.
  absl::flat_hash_map<uint32_t, std::string> gid_to_gname;
  gid_to_gname.reserve(groups.size());
  for (const auto& [gname, gid] : groups) {
    if (gid == 0 || gid >= kLocalUidMax) continue;
    gid_to_gname.try_emplace(gid, gname);
  }

  absl::flat_hash_map<Key, LocalEntry, KeyHash> fresh_local;
  absl::flat_hash_map<uint32_t, HashedEntry> fresh_hashed;

  // Pass 1: users → {uid, kUid} entry. paired_local_id is pw_gid; paired_hash
  // is H(group-name-for-pw_gid) when that group is present in the snapshot.
  size_t mapped_users = 0;
  for (const auto& u : users) {
    if (u.uid == 0 || u.uid >= kLocalUidMax) continue;

    const uint32_t h = Hash(u.name);
    LocalEntry e;
    e.hash = h;
    e.paired_local_id = u.primary_gid;
    if (u.primary_gid != 0 && u.primary_gid < kLocalUidMax) {
      auto it = gid_to_gname.find(u.primary_gid);
      if (it != gid_to_gname.end()) {
        e.paired_hash = Hash(it->second);
      }
    }
    fresh_local.try_emplace(Key{u.uid, Kind::kUid}, e);

    auto& vec = fresh_hashed[h].uid_locals;
    vec.push_back({u.name, u.uid});
    ++mapped_users;
    if (vec.size() > 1) {
      LOG(ERROR) << fmt::format(
          "[uidgid.refresh] hash collision (user, refresh): keeping "
          "existing='{}' (uid={}) over new='{}' (uid={}) hash={} — smaller "
          "local uid wins",
          vec.front().name, vec.front().local_id, u.name, u.uid, h);
    }
  }

  // Pass 2: groups → {gid, kGid} entry. paired_local_id stays 0 — groups have
  // no canonical "primary user".
  size_t mapped_groups = 0;
  for (const auto& [gname, gid] : groups) {
    if (gid == 0 || gid >= kLocalUidMax) continue;

    const uint32_t h = Hash(gname);
    LocalEntry e;
    e.hash = h;
    fresh_local.try_emplace(Key{gid, Kind::kGid}, e);

    auto& vec = fresh_hashed[h].gid_locals;
    vec.push_back({gname, gid});
    ++mapped_groups;
    if (vec.size() > 1) {
      LOG(ERROR) << fmt::format(
          "[uidgid.refresh] hash collision (group, refresh): keeping "
          "existing='{}' (gid={}) over new='{}' (gid={}) hash={} — smaller "
          "local gid wins",
          vec.front().name, vec.front().local_id, gname, gid, h);
    }
  }

  {
    utils::WriteLockGuard lk(mu_);
    local_table_ = std::move(fresh_local);
    hashed_table_ = std::move(fresh_hashed);
    last_refresh_time_ms_.store(utils::TimestampMs(),
                                std::memory_order_relaxed);
  }

  LOG(INFO) << fmt::format(
      "[uidgid.refresh] snapshot rebuilt: {} users, {} groups mapped "
      "(of {}/{} listed; root and id>={} skipped).",
      mapped_users, mapped_groups, users.size(), groups.size(), kLocalUidMax);
}

uint32_t UidGidMapper::LocalIdToHashedId(Kind kind, uint32_t local_id) const {
  if (!enabled_.load(std::memory_order_relaxed)) return local_id;
  if (local_id == 0) return 0;
  if (local_id >= kLocalUidMax) return local_id;

  utils::ReadLockGuard lk(mu_);
  auto it = local_table_.find(Key{local_id, kind});
  if (it == local_table_.end()) return local_id;
  return it->second.hash;
}

void UidGidMapper::LocalPairToHashed(uint32_t uid, uint32_t gid,
                                     uint32_t& out_uid,
                                     uint32_t& out_gid) const {
  if (!enabled_.load(std::memory_order_relaxed)) {
    out_uid = uid;
    out_gid = gid;
    return;
  }
  auto in_range = [](uint32_t x) { return x != 0 && x < kLocalUidMax; };

  utils::ReadLockGuard lk(mu_);

  // Fast path: uid==gid in mappable range AND the user entry's paired_local_id
  // cookie matches the inbound gid — typical UPG where pw_gid agrees with the
  // process's effective gid. One find returns both hashes.
  if (in_range(uid) && in_range(gid)) {
    auto it = local_table_.find(Key{uid, Kind::kUid});
    if (it != local_table_.end() && it->second.paired_local_id == gid &&
        it->second.paired_hash != 0) {
      out_uid = it->second.hash;
      out_gid = it->second.paired_hash;
      return;
    }
    // Cookie miss (admin switched alice's primary gid; snapshot stale; or the
    // paired group is not in /etc/group). Fall through to the per-element find
    // path — it still consults this same snapshot but resolves gid via the
    // {gid, kGid} entry directly.
  }

  auto translate = [&](uint32_t id, Kind kind) -> uint32_t {
    if (!in_range(id)) {
      return id;
    }
    auto it = local_table_.find(Key{id, kind});
    return it != local_table_.end() ? it->second.hash : id;
  };
  out_uid = translate(uid, Kind::kUid);
  out_gid = translate(gid, Kind::kGid);
}

uint32_t UidGidMapper::HashedIdToLocalId(Kind kind, uint32_t hashed_id) const {
  if (!enabled_.load(std::memory_order_relaxed)) return hashed_id;
  if (hashed_id == 0) return 0;
  if (hashed_id < kLocalUidMax) return hashed_id;

  utils::ReadLockGuard lk(mu_);
  auto it = hashed_table_.find(hashed_id);
  if (it == hashed_table_.end()) return hashed_id;
  const auto& vec =
      (kind == Kind::kUid) ? it->second.uid_locals : it->second.gid_locals;
  if (vec.empty()) return hashed_id;
  return vec.front().local_id;
}

void UidGidMapper::HashedPairToLocal(uint32_t hashed_uid, uint32_t hashed_gid,
                                     uint32_t& out_uid,
                                     uint32_t& out_gid) const {
  if (!enabled_.load(std::memory_order_relaxed)) {
    out_uid = hashed_uid;
    out_gid = hashed_gid;
    return;
  }
  auto in_range = [](uint32_t x) { return x >= kLocalUidMax; };

  utils::ReadLockGuard lk(mu_);
  if (hashed_uid == hashed_gid && in_range(hashed_uid)) {
    auto it = hashed_table_.find(hashed_uid);
    if (it == hashed_table_.end()) {
      out_uid = hashed_uid;
      out_gid = hashed_gid;
      return;
    }
    out_uid = !it->second.uid_locals.empty()
                  ? it->second.uid_locals.front().local_id
                  : hashed_uid;
    out_gid = !it->second.gid_locals.empty()
                  ? it->second.gid_locals.front().local_id
                  : hashed_gid;
    return;
  }
  // Per-element path.
  auto translate = [&](uint32_t hid, Kind kind) -> uint32_t {
    if (!in_range(hid)) {
      return hid;
    }
    auto it = hashed_table_.find(hid);
    if (it == hashed_table_.end()) return hid;
    const auto& vec =
        (kind == Kind::kUid) ? it->second.uid_locals : it->second.gid_locals;
    return vec.empty() ? hid : vec.front().local_id;
  };
  out_uid = translate(hashed_uid, Kind::kUid);
  out_gid = translate(hashed_gid, Kind::kGid);
}

void UidGidMapper::Summary(Json::Value& value) const {
  utils::ReadLockGuard lk(mu_);
  value["name"] = "uidgidmap";
  value["count"] = static_cast<Json::UInt64>(local_table_.size());
}

void UidGidMapper::Dump(Json::Value& value) const {
  Json::Value root = Json::objectValue;
  root["enabled"] = enabled_.load(std::memory_order_relaxed);
  root["salt"] = salt_;
  root["last_refresh_time_ms"] = static_cast<Json::UInt64>(
      last_refresh_time_ms_.load(std::memory_order_relaxed));

  utils::ReadLockGuard lk(mu_);

  uint64_t uid_count = 0;
  uint64_t gid_count = 0;
  for (const auto& kv : local_table_) {
    if (kv.first.kind == Kind::kUid)
      ++uid_count;
    else
      ++gid_count;
  }
  root["uid_count"] = static_cast<Json::UInt64>(uid_count);
  root["gid_count"] = static_cast<Json::UInt64>(gid_count);

  // Dashboard snapshot; row order is not guaranteed.
  Json::Value entries(Json::arrayValue);
  bool any_collision = false;
  auto emit_row = [&](uint32_t hashed_id, const char* type, Kind kind,
                      const std::vector<ReverseEntry>& vec) {
    if (vec.empty()) return;
    Json::Value row;
    row["hashed_id"] = static_cast<Json::UInt64>(hashed_id);
    row["type"] = type;
    Json::Value mappings(Json::arrayValue);
    for (const auto& e : vec) {
      Json::Value m;
      m["name"] = e.name;
      m["local_id"] = e.local_id;
      // For user rows surface the paired_local_id cookie so the dashboard can
      // distinguish "cookie matches process gid" vs "stale snapshot" cases.
      if (kind == Kind::kUid) {
        auto it = local_table_.find(Key{e.local_id, Kind::kUid});
        if (it != local_table_.end()) {
          m["paired_local_id"] = it->second.paired_local_id;
        }
      }
      mappings.append(m);
    }
    row["mappings"] = mappings;
    entries.append(row);
    if (vec.size() > 1) any_collision = true;
  };

  for (const auto& [hashed_id, entry] : hashed_table_) {
    emit_row(hashed_id, "uid", Kind::kUid, entry.uid_locals);
    emit_row(hashed_id, "gid", Kind::kGid, entry.gid_locals);
  }

  root["entries"] = entries;
  root["has_collisions"] = any_collision;
  value["uid_gid_map"] = root;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
