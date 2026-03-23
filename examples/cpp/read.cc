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

// read.cc — write a file then read it back via the C API.

#include <fcntl.h>

#include <cstring>
#include <iostream>
#include <string>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // Setup: create directory
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  // Write file
  int wfd = dingofs_open(h, "/demo_dir/hello.txt", O_WRONLY | O_CREAT | O_TRUNC,
                         0644);
  if (wfd < 0) {
    std::cerr << "[FAIL] dingofs_open(write): " << strerror(-wfd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }

  const char* data = "Hello DingoFS SDK!";
  const size_t data_len = strlen(data);

  ssize_t wn = dingofs_write(h, wfd, data, data_len);
  if (wn < 0) {
    std::cerr << "[FAIL] dingofs_write: " << strerror(static_cast<int>(-wn))
              << "\n";
    dingofs_close(h, wfd);
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::cout << "[ OK ] dingofs_write  bytes=" << wn << "\n";

  DINGOFS_CHECK(dingofs_flush(h, wfd), "dingofs_flush");
  DINGOFS_CHECK(dingofs_close(h, wfd), "dingofs_close(write)");

  // Read back
  int rfd = dingofs_open(h, "/demo_dir/hello.txt", O_RDONLY, 0);
  if (rfd < 0) {
    std::cerr << "[FAIL] dingofs_open(read): " << strerror(-rfd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }

  char buf[256] = {};
  ssize_t rn = dingofs_read(h, rfd, buf, sizeof(buf) - 1);
  if (rn < 0) {
    std::cerr << "[FAIL] dingofs_read: " << strerror(static_cast<int>(-rn))
              << "\n";
    dingofs_close(h, rfd);
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::cout << "[ OK ] dingofs_read  bytes=" << rn << "\n";

  std::string content(buf, static_cast<size_t>(rn));
  std::cout << "content: [" << content << "]\n";

  if (content != std::string(data, data_len)) {
    std::cerr << "[FAIL] content mismatch\n";
    dingofs_close(h, rfd);
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::cout << "[ OK ] content verified\n";

  DINGOFS_CHECK(dingofs_close(h, rfd), "dingofs_close(read)");

  // Cleanup
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "dingofs_unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
