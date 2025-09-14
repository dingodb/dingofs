/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_BASE_H_
#define DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_BASE_H_

#include <butil/file_util.h>
#include <butil/files/file_path.h>
#include <sys/stat.h>

#include "cache/common/const.h"
#include "cache/storage/filesystem.h"

namespace dingofs {
namespace cache {

using CheckStatusFunc = std::function<Status(Status)>;

class BaseFileSystem : public FileSystem {
 public:
  explicit BaseFileSystem(CheckStatusFunc check_status_func = nullptr);

  static BaseFileSystem& GetInstance();

  Status Start() override;
  Status Shutdown() override;

  Status MkDirs(const std::string& path) override;

  Status Walk(const std::string& prefix, WalkFunc func) override;

  // NOTE: not support in base class
  Status WriteFile(ContextSPtr ctx, const std::string& path,
                   const IOBuffer& buffer,
                   WriteOption option = WriteOption()) override;

  // NOTE: not support in base class
  Status ReadFile(ContextSPtr ctx, const std::string& path, off_t offset,
                  size_t length, IOBuffer* buffer,
                  ReadOption option = ReadOption()) override;

  Status RemoveFile(const std::string& path) override;

  Status Link(const std::string& from, const std::string& to) override;

  bool FileExists(const std::string& path) override;

  Status StatFile(const std::string& path, FileInfo* info) override;

  Status StatFS(const std::string& path, FSStat* stat) override;

 protected:
  Status CheckStatus(Status status);

 private:
  CheckStatusFunc check_status_func_;
};

class FSUtil {
 public:
  static Status MkDirs(const std::string& dir);

  static Status Walk(const std::string& dir, WalkFunc walk_func);

  static Status WriteFile(const std::string& filepath,
                          const std::string& content);

  static Status ReadFile(const std::string& filepath, std::string* content);

  static Status RemoveFile(const std::string& filepath);

  static bool FileExists(const std::string& filepath);

  static Status StatFS(const std::string& dir, FSStat* stat);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_STORAGE_FILESYSTEM_BASE_H_
