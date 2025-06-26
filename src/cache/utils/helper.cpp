/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-05-21
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/helper.h"

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <butil/file_util.h>

#include <numeric>

#include "base/filepath/filepath.h"
#include "cache/common/const.h"
#include "cache/storage/filesystem_base.h"

namespace dingofs {
namespace cache {

static const uint64_t kIOAlignedBlockSize = 4096;
static const std::string kTempFileSuffix = ".tmp";

// sys conf
uint64_t Helper::GetSysPageSize() { return sysconf(_SC_PAGESIZE); }

uint64_t Helper::GetIOAlignedBlockSize() { return kIOAlignedBlockSize; }

bool Helper::IsAligned(uint64_t n, uint64_t m) { return n % m == 0; }

// time
int64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// filepath
std::string Helper::ParentDir(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return "/";
  }

  std::string parent = path.substr(0, index);
  if (parent.empty()) {
    return "/";
  }
  return parent;
}

std::string Helper::Filename(const std::string& path) {
  size_t index = path.find_last_of('/');
  if (index == std::string::npos) {
    return path;
  }
  return path.substr(index + 1, path.length());
}

bool Helper::HasSuffix(const std::string& path, const std::string& suffix) {
  return absl::EndsWith(path, suffix);
}

std::string Helper::PathJoin(const std::vector<std::string>& subpaths) {
  return absl::StrJoin(subpaths, "/");
}

std::string Helper::TempFilepath(const std::string& filepath) {
  return filepath + kTempFileSuffix;
}

bool Helper::IsTempFilepath(const std::string& filepath) {
  return base::filepath::HasSuffix(filepath, kTempFileSuffix);
}

// filesystem
Status Helper::Walk(const std::string& dir, WalkFunc walk_func) {
  return FileSystemBase::GetInstance().Walk(dir, walk_func);
}

Status Helper::MkDirs(const std::string& dir) {
  return FileSystemBase::GetInstance().MkDirs(dir);
}

bool Helper::FileExists(const std::string& filepath) {
  return FileSystemBase::GetInstance().FileExists(filepath);
}

Status Helper::ReadFile(const std::string& filepath, std::string* content) {
  if (!FileExists(filepath)) {
    return Status::NotFound("file not found");
  } else if (butil::ReadFileToString(butil::FilePath(filepath), content),
             4 * kMiB) {
    return Status::OK();
  }
  return Status::IoError("read file failed");
}

Status Helper::WriteFile(const std::string& filepath,
                         const std::string& content) {
  int rc = butil::WriteFile(butil::FilePath(filepath), content.data(),
                            content.size());
  if (rc == static_cast<int>(content.size())) {
    return Status::OK();
  }
  return Status::IoError("write file failed");
}

Status Helper::RemoveFile(const std::string& filepath) {
  return FileSystemBase::GetInstance().RemoveFile(filepath);
}

Status Helper::StatFS(const std::string& dir, FSStat* stat) {
  return FileSystemBase::GetInstance().StatFS(dir, stat);
}

bool Helper::IsFile(const struct stat* stat) { return S_ISREG(stat->st_mode); }
bool Helper::IsDir(const struct stat* stat) { return S_ISDIR(stat->st_mode); }
bool Helper::IsLink(const struct stat* stat) { return S_ISLNK(stat->st_mode); }

// others
std::vector<uint64_t> Helper::NormalizeByGcd(
    const std::vector<uint64_t>& nums) {
  uint64_t gcd = 0;
  std::vector<uint64_t> out;
  for (const auto& num : nums) {
    out.push_back(num);
    gcd = std::gcd(gcd, num);
  }
  CHECK_NE(gcd, 0);

  for (auto& num : out) {
    num = num / gcd;
  }
  return out;
}

void Helper::DeleteBuffer(void* data) { delete[] static_cast<char*>(data); }

bool Helper::IsAligned(const IOBuffer& buffer) {
  auto aligned_block_size = GetIOAlignedBlockSize();
  const auto& iovec = buffer.Fetch();
  for (const auto& vec : iovec) {
    if (!IsAligned(reinterpret_cast<std::uintptr_t>(vec.iov_base),
                   aligned_block_size)) {
      return false;
    } else if (!IsAligned(vec.iov_len, aligned_block_size)) {
      return false;
    }
  }
  return true;
}

}  // namespace cache
}  // namespace dingofs
