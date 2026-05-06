// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_MDS_COMMON_TRASH_H_
#define DINGOFS_MDS_COMMON_TRASH_H_

#include <cstdint>
#include <string>

#include "mds/common/type.h"

namespace dingofs {
namespace mds {

// Trash root inode. Mirrors the client-facing `dingofs::kTrashIno` in
// common/const.h; the static_asserts below enforce they stay in sync.
// Odd number = directory in DingoFS convention; far above kInoStartId
// (2e10) to avoid colliding with normal inodes. Note 0x7FFFFFFF00000001
// is taken by kStatsIno in common/const.h.
constexpr Ino kTrashInodeId = 0x7FFFFFFF00000003ULL;

constexpr const char* kTrashName = ".trash";

// First trash sub-directory inode. Increment by 2 to keep odd (directory).
constexpr Ino kTrashSubInodeStart = kTrashInodeId + 2;  // 0x7FFFFFFF00000005

// Check if an inode belongs to the trash range.
inline bool IsTrashInode(Ino ino) { return ino >= kTrashInodeId; }

// True iff `parent` is a sub-trash hour bucket (its dentries are TrashDentry
// values). kTrashInodeId itself holds bucket-pointer dentries which remain
// plain Dentry; only entries under sub-trash buckets carry quota_chain.
inline bool IsTrashBucketChild(Ino parent) { return parent >= kTrashSubInodeStart; }

// Format a UTC hour bucket name from a unix timestamp (seconds): "2026-04-05-14"
std::string FormatTrashBucketName(uint64_t timestamp_s);

// Parse a bucket name back to unix timestamp (seconds). Returns 0 on failure.
uint64_t ParseTrashBucketName(const std::string& name);

// Build trash entry name: "{parent_ino}-{file_ino}-{original_name}"
// Truncated to 255 bytes if needed.
std::string BuildTrashEntryName(Ino parent_ino, Ino file_ino, const std::string& original_name);

// Parse trash entry name. Returns false on parse failure.
bool ParseTrashEntryName(const std::string& trash_name, Ino& parent_ino, Ino& file_ino, std::string& original_name);

// Build the virtual inode attr for kTrashInodeId. kTrashInodeId has no KV record
// (its dentries live under it, but the node itself is synthesized on demand).
//
// `fs_create_time_ns` pins atime/mtime/ctime to the filesystem's creation time
// so the synthesized attr is stable across calls (kernel/client attr caches
// hit; cross-MDS responses are bytewise identical). Callers without easy
// access to fs_info — e.g. RenameOperation populating a placeholder when
// rename's old/new_parent is kTrashInodeId — can pass 0; in those paths the
// synthesized attr is never written to KV nor returned to clients, so
// timestamp accuracy is moot.
AttrEntry BuildTrashInodeAttr(uint32_t fs_id, uint64_t fs_create_time_ns = 0);

// Build the inode attr for an hour-bucket sub-trash directory at `sub_trash_ino`.
// Parent is kTrashInodeId. Timestamps are nanoseconds to match the codebase.
AttrEntry BuildSubTrashBucketAttr(uint32_t fs_id, Ino sub_trash_ino);

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_TRASH_H_
