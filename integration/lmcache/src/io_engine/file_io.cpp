// SPDX-License-Identifier: Apache-2.0

#include "file_io.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>
#include <string>

namespace dingofs {
namespace io {

std::string key_to_path(const std::string& base_path,
                        const std::string& key) {
  // Replace "/" with "-SEP-" in key, matching LMCache FSConnector convention
  std::string safe_key;
  safe_key.reserve(key.size() + 32);
  for (char c : key) {
    if (c == '/') {
      safe_key += "-SEP-";
    } else {
      safe_key += c;
    }
  }
  return base_path + "/" + safe_key + ".data";
}

size_t get_block_size(const std::string& path) {
  struct statvfs st;
  if (statvfs(path.c_str(), &st) != 0) {
    throw std::runtime_error("statvfs failed on " + path + ": " +
                             std::strerror(errno));
  }
  return st.f_bsize;
}

bool file_exists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

// Helper: read exactly from fd, retrying on EINTR
static ssize_t read_all(int fd, void* buf, size_t len) {
  size_t total = 0;
  char* ptr = static_cast<char*>(buf);
  while (total < len) {
    ssize_t n = ::read(fd, ptr + total, len - total);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (n == 0) break;  // EOF
    total += static_cast<size_t>(n);
  }
  return static_cast<ssize_t>(total);
}

// Helper: write all to fd, retrying on EINTR
static ssize_t write_all(int fd, const void* buf, size_t len) {
  size_t total = 0;
  const char* ptr = static_cast<const char*>(buf);
  while (total < len) {
    ssize_t n = ::write(fd, ptr + total, len - total);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (n == 0) break;
    total += static_cast<size_t>(n);
  }
  return static_cast<ssize_t>(total);
}

size_t file_read(const std::string& path, void* buf, size_t len,
                 bool use_odirect) {
  int flags = O_RDONLY;
#ifdef O_DIRECT
  if (use_odirect) {
    flags |= O_DIRECT;
  }
#else
  (void)use_odirect;
#endif

  int fd = ::open(path.c_str(), flags);
  if (fd < 0) {
    throw std::runtime_error("open failed for read: " + path + ": " +
                             std::strerror(errno));
  }

  ssize_t n = read_all(fd, buf, len);
  int saved_errno = errno;
  ::close(fd);

  if (n < 0) {
    throw std::runtime_error("read failed: " + path + ": " +
                             std::strerror(saved_errno));
  }

  return static_cast<size_t>(n);
}

void file_write(const std::string& base_path, const std::string& key,
                const void* buf, size_t len, bool use_odirect,
                SyncMode sync_mode) {
  std::string final_path = key_to_path(base_path, key);
  std::string tmp_path = final_path + ".tmp";

  int flags = O_CREAT | O_WRONLY | O_TRUNC;
#ifdef O_DIRECT
  if (use_odirect) {
    flags |= O_DIRECT;
  }
#else
  (void)use_odirect;
#endif

  int fd = ::open(tmp_path.c_str(), flags, 0644);
  if (fd < 0) {
    throw std::runtime_error("open failed for write: " + tmp_path + ": " +
                             std::strerror(errno));
  }

  ssize_t n = write_all(fd, buf, len);
  if (n < 0 || static_cast<size_t>(n) != len) {
    int saved_errno = errno;
    ::close(fd);
    ::unlink(tmp_path.c_str());
    throw std::runtime_error("write failed: " + tmp_path + ": " +
                             std::strerror(saved_errno));
  }

  // Optionally ensure data reaches storage before rename
  if (sync_mode == SyncMode::ALWAYS) {
    if (fdatasync(fd) != 0) {
      int saved_errno = errno;
      ::close(fd);
      ::unlink(tmp_path.c_str());
      throw std::runtime_error("fdatasync failed: " + tmp_path + ": " +
                               std::strerror(saved_errno));
    }
  }

  ::close(fd);

  // Atomic rename
  if (::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
    int saved_errno = errno;
    ::unlink(tmp_path.c_str());
    throw std::runtime_error("rename failed: " + tmp_path + " -> " +
                             final_path + ": " + std::strerror(saved_errno));
  }
}

void ensure_directory(const std::string& path) {
  // Simple recursive mkdir
  std::string current;
  for (size_t i = 0; i < path.size(); ++i) {
    current += path[i];
    if (path[i] == '/' || i == path.size() - 1) {
      if (!current.empty() && current != "/") {
        ::mkdir(current.c_str(), 0755);
      }
    }
  }
  // Final check
  struct stat st;
  if (stat(path.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
    throw std::runtime_error("failed to create directory: " + path);
  }
}

}  // namespace io
}  // namespace dingofs
