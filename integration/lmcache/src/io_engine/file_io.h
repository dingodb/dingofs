// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace dingofs {

// Sync mode for file writes.
enum class SyncMode : int {
  NONE = 0,    // No fdatasync (rely on filesystem guarantees)
  ALWAYS = 1,  // fdatasync after every write (default)
};

namespace io {

// Convert a cache key to a filesystem path.
// Replaces "/" with "-SEP-" and appends ".data" (compatible with LMCache
// FSConnector).
std::string key_to_path(const std::string& base_path, const std::string& key);

// Get the filesystem block size for the given path (via statvfs).
size_t get_block_size(const std::string& path);

// Check whether a file exists.
bool file_exists(const std::string& path);

// Read up to `len` bytes from file into `buf`.
// If `use_odirect` is true, opens with O_DIRECT (buf must be aligned).
// Returns the number of bytes actually read.
size_t file_read(const std::string& path, void* buf, size_t len,
                 bool use_odirect);

// Atomically write `len` bytes from `buf` to the file for `key`.
// Writes to a temp file first, optionally calls fdatasync, then renames.
// If `use_odirect` is true, opens with O_DIRECT (buf must be aligned).
// `sync_mode` controls whether fdatasync is called before rename.
void file_write(const std::string& base_path, const std::string& key,
                const void* buf, size_t len, bool use_odirect,
                SyncMode sync_mode);

// Create directory and all parents (like mkdir -p).
void ensure_directory(const std::string& path);

}  // namespace io
}  // namespace dingofs
