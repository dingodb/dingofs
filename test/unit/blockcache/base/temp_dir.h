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

#ifndef DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_TEMP_DIR_H_
#define DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_TEMP_DIR_H_

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>

namespace dingofs {
namespace blockcache {

// A fresh directory under the system temp dir, removed with everything in it
// when the object goes away -- including whatever a test made unreadable or
// unwritable to inject a failure.
class TempDir {
 public:
  TempDir() : path_(NextPath()) {
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  ~TempDir() {
    MakeAccessible(path_);
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const std::filesystem::path& path() const { return path_; }

 private:
  static std::filesystem::path NextPath() {
    static std::atomic<uint64_t> seq{0};
    return std::filesystem::temp_directory_path() /
           ("dingofs_blockcache_test_" + std::to_string(::getpid()) + "_" +
            std::to_string(seq++));
  }

  // Gives the owner full access to `dir` and every directory below it, so
  // remove_all() can get everywhere.
  static void MakeAccessible(const std::filesystem::path& dir) {
    std::error_code ec;
    std::filesystem::permissions(dir, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::add, ec);
    std::filesystem::directory_iterator it(dir, ec);
    for (; !ec && it != std::filesystem::directory_iterator();
         it.increment(ec)) {
      std::error_code type_ec;
      if (!it->is_symlink(type_ec) && it->is_directory(type_ec)) {
        MakeAccessible(it->path());
      }
    }
  }

  std::filesystem::path path_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_TEMP_DIR_H_
