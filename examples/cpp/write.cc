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

// write.cc — open/write/flush/fsync/close a file, then unlink via the C API.

#include <fcntl.h>

#include <cstring>
#include <iostream>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // Create parent directory
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  // Open (create) file
  int fd = dingofs_open(h, "/demo_dir/hello.txt", O_WRONLY | O_CREAT | O_TRUNC,
                        0644);
  if (fd < 0) {
    std::cerr << "[FAIL] dingofs_open: " << strerror(-fd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::cout << "[ OK ] dingofs_open  fd=" << fd << "\n";

  // Write
  const char* data = "Hello DingoFS SDK!";
  ssize_t wn = dingofs_write(h, fd, data, strlen(data));
  if (wn < 0) {
    std::cerr << "[FAIL] dingofs_write: " << strerror(static_cast<int>(-wn))
              << "\n";
    dingofs_close(h, fd);
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::cout << "[ OK ] dingofs_write  bytes=" << wn << "\n";

  // Flush + Fsync + Close
  DINGOFS_CHECK(dingofs_flush(h, fd), "dingofs_flush");
  DINGOFS_CHECK(dingofs_fsync(h, fd, 0), "dingofs_fsync");
  DINGOFS_CHECK(dingofs_close(h, fd), "dingofs_close");

  // Cleanup
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "dingofs_unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
