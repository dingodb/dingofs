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

#ifndef DINGOFS_SRC_CLIENT_VFS_COMPONENTS_UID_GID_MAPPER_H_
#define DINGOFS_SRC_CLIENT_VFS_COMPONENTS_UID_GID_MAPPER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "client/vfs/components/passwd_watcher.h"
#include "json/value.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {

class PasswdSource {
 public:
  // Each user record carries the primary gid (pwd struct's pw_gid) so the
  // mapper can populate LocalEntry::paired_local_id without a second NSS pass.
  struct UserRecord {
    std::string name;
    uint32_t    uid;
    uint32_t    primary_gid;
  };

  virtual ~PasswdSource() = default;
  virtual std::vector<UserRecord> ListUsersWithPrimaryGid() = 0;
  virtual std::vector<std::pair<std::string, uint32_t>> ListGroups() = 0;
};

class LibcPasswdSource : public PasswdSource {
 public:
  std::vector<UserRecord> ListUsersWithPrimaryGid() override;
  std::vector<std::pair<std::string, uint32_t>> ListGroups() override;

 private:
  // setpwent/getpwent/endpwent (and the setgr* family) share process-wide
  // glibc iterator state and are MT-Unsafe (POSIX: race:pwent / race:grent).
  // Multiple UidGidMapper::Refresh() invocations can land here concurrently
  // (the startup priming Refresh() at VFSHub::Start(), an inotify-triggered
  // Refresh() from PasswdWatcher, and the heartbeat-driven toggle-on Refresh()
  // when enable_uid_gid_map flips to true), so serialize the NSS-enumerating
  // methods. utils::RWLock is bthread-aware so contention yields the bthread
  // instead of pinning the pthread worker; we only ever use it in write mode.
  utils::RWLock nss_lock_;
};

class UidGidMapper {
 public:
  static constexpr uint32_t kLocalUidMax = 10000;

  enum class Kind : uint8_t { kUid = 0, kGid = 1 };

  UidGidMapper(bool enabled, std::string salt,
               std::unique_ptr<PasswdSource> passwd);

  UidGidMapper(const UidGidMapper&) = delete;
  UidGidMapper& operator=(const UidGidMapper&) = delete;

  // Runtime toggle. Map* fast-path checks IsEnabled() lock-free.
  void SetEnabled(bool v) { enabled_.store(v, std::memory_order_relaxed); }
  bool IsEnabled() const { return enabled_.load(std::memory_order_relaxed); }

  // Synchronously rebuild forward/reverse tables from the PasswdSource.
  // No-op when disabled — snapshot is never consulted in that state, and the
  // outbound miss path will lazily fill entries via NSS fallback once
  // SetEnabled(true) is called.
  void Refresh();

  // Production entry point. When enabled, primes the snapshot once and starts
  // an inotify watch on the local NSS files (/etc passwd, group) so the mapping
  // auto-refreshes the moment useradd/userdel changes them; a failed watch
  // start is logged and tolerated (the snapshot is still primed once). When
  // disabled, does nothing — no snapshot, no watch. Tests that inject a mock
  // PasswdSource skip Init() and so never spawn a real watch.
  void Init();

  // Stops the inotify watch and joins its thread. Idempotent; the destructor
  // also stops it via the watcher_ member.
  void StopWatching();

  // Translation entry points (lock-free fast path when disabled).
  // LocalIdToHashedId: local-host uid/gid -> hashed id used by MDS (sending out).
  // HashedIdToLocalId: hashed id used by MDS -> local-host uid/gid (reading back).
  uint32_t LocalIdToHashedId(Kind kind, uint32_t local_id) const;
  uint32_t HashedIdToLocalId(Kind kind, uint32_t hashed_id) const;

  // Paired translation: translate (uid, gid) under a single read lock.
  // When the inputs are equal and in the mappable range, a single hash-table
  // find serves both; otherwise falls back to two finds (same single lock).
  // The returned hashed values are independent — never reuse one for both.
  void LocalPairToHashed(uint32_t uid, uint32_t gid,
                         uint32_t& out_uid, uint32_t& out_gid) const;
  void HashedPairToLocal(uint32_t hashed_uid, uint32_t hashed_gid,
                         uint32_t& out_uid, uint32_t& out_gid) const;

  // ClientStatService dashboard hooks. Summary() fills a Summary-table row
  // (name/count/...); Dump() emits the full mapper snapshot under
  // value["uid_gid_map"], with one entries[] row per hashed_id (containing
  // mappings[] = [{name, local_id}], multi-element when a hash collision
  // exists; mappings[0] is the winner, smallest local_id).
  void Summary(Json::Value& value) const;
  void Dump(Json::Value& value) const;

 private:
  // Builds the inotify watcher (callback = Refresh) and starts it. Called only
  // by Init(); failure is logged and tolerated.
  void StartWatching(std::string dir, std::vector<std::string> names);

  // Forward table is keyed by (local_id, kind) so a numeric id that is both a
  // user and a group lives as two independent entries — making the user/group
  // domain split explicit in the key, not implicit in a per-kind field.
  struct Key {
    uint32_t local_id;
    Kind     kind;
    bool operator==(const Key& o) const noexcept {
      return local_id == o.local_id && kind == o.kind;
    }
  };
  struct KeyHash {
    size_t operator()(const Key& k) const noexcept {
      // (local_id < kLocalUidMax) fits in 14 bits; shift kind into bit 16 so
      // the two components occupy disjoint bit ranges before mixing.
      uint32_t v = k.local_id | (static_cast<uint32_t>(k.kind) << 16);
      return std::hash<uint32_t>{}(v);
    }
  };

  // Forward entry: H(name) for this (local_id, kind), plus a paired-entity
  // cookie used by LocalPairToHashed's fast path. paired_local_id is the
  // configured pair on the other kind:
  //   - kind == kUid: the user's primary gid from /etc/passwd's pw_gid;
  //   - kind == kGid: 0 (groups have no canonical "primary user" notion).
  // paired_hash caches H(paired group's name) so a cookie-match outbound call
  // returns both hashes in a single find. 0 in either paired field disables
  // the cookie for that entry and degrades to the per-element find path.
  // hash != 0 always when the entry exists (Hash() guarantees output ≥
  // kLocalUidMax); entry absent ≡ "this (id, kind) is unmapped".
  struct LocalEntry {
    uint32_t hash = 0;
    uint32_t paired_local_id = 0;
    uint32_t paired_hash = 0;
  };

  // One reverse hit (name → local id) for a given kind list.
  struct ReverseEntry {
    std::string name;
    uint32_t    local_id;
  };

  // Reverse entry keyed by hashed id; uid_locals/gid_locals are separate
  // sorted-by-local_id lists of names that hash to this hashed_id when used
  // as the respective kind. Collisions within one kind grow that kind's
  // vector; the smallest local_id (front()) wins. Cross-kind same-hash
  // (Hash(uname) == Hash(gname)) is by design — uid/gid share the hash
  // domain and the two vectors keep their results separate.
  struct HashedEntry {
    std::vector<ReverseEntry> uid_locals;
    std::vector<ReverseEntry> gid_locals;
  };

  // Hash a user/group name into the reserved id range. User and group names
  // share one hash domain; the per-kind fields/vectors in LocalEntry and
  // HashedEntry keep their results distinct.
  uint32_t Hash(std::string_view name) const;

  std::atomic<bool> enabled_;
  const std::string salt_;
  std::unique_ptr<PasswdSource> passwd_;

  // Wall-clock timestamp of the last successful Refresh(). 0 = never.
  std::atomic<uint64_t> last_refresh_time_ms_{0};

  mutable utils::RWLock mu_;

  // Local view: (local_id, kind) -> LocalEntry. Populated only by Refresh();
  // local_id is always in [1, kLocalUidMax). A numeric id that is both a user
  // and a group occupies two independent entries here.
  mutable absl::flat_hash_map<Key, LocalEntry, KeyHash> local_table_;

  // MDS view: hashed_id -> {uid_locals, gid_locals reverse-hit lists}.
  mutable absl::flat_hash_map<uint32_t /*hashed_id*/, HashedEntry> hashed_table_;

  // Declared last so it is destroyed first: ~PasswdWatcher() joins the watch
  // thread before the snapshot tables / passwd_ that its Refresh() callback
  // touches are torn down. Null until StartWatching() is called.
  std::unique_ptr<PasswdWatcher> watcher_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_COMPONENTS_UID_GID_MAPPER_H_
