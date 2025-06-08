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

#include <butil/file_util.h>

#include "base/filepath/filepath.h"

namespace dingofs {
namespace cache {

static const uint64_t kIOAlignedBlockSize = 4096;
static const std::string kTempFileSuffix = ".tmp";

uint64_t Helper::GetSysPageSize() { return sysconf(_SC_PAGESIZE); }

uint64_t Helper::GetIOAlignedBlockSize() { return kIOAlignedBlockSize; }

bool Helper::IsAligned(uint64_t n, uint64_t m) { return n % m == 0; }

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

std::string Helper::TempFilepath(const std::string& filepath) {
  return filepath + kTempFileSuffix;
}

bool Helper::IsTempFilepath(const std::string& filepath) {
  return base::filepath::HasSuffix(filepath, kTempFileSuffix);
}

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

}  // namespace cache
}  // namespace dingofs
