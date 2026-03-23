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

// symlink.cc — create a symlink, readlink, lstat, then unlink via the C API.

#include <fcntl.h>
#include <sys/stat.h>

#include <iostream>
#include <string>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // Setup: create directory + target file
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  int fd = dingofs_open(h, "/demo_dir/target.txt", O_WRONLY | O_CREAT | O_TRUNC,
                        0644);
  if (fd < 0) {
    std::cerr << "[FAIL] dingofs_open: " << strerror(-fd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "dingofs_close");

  // Create symlink: link.txt -> target.txt (absolute path)
  const std::string target = std::string(MOUNT_POINT) + "/demo_dir/target.txt";
  DINGOFS_CHECK(dingofs_symlink(h, target.c_str(), "/demo_dir/link.txt"),
                "dingofs_symlink");

  // ReadLink
  char rbuf[512] = {};
  ssize_t rn =
      dingofs_readlink(h, "/demo_dir/link.txt", rbuf, sizeof(rbuf) - 1);
  if (rn < 0) {
    std::cerr << "[FAIL] dingofs_readlink: " << strerror(static_cast<int>(-rn))
              << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  rbuf[rn] = '\0';
  std::cout << "[ OK ] dingofs_readlink  target: " << rbuf << "\n";

  // lstat — should show S_ISLNK
  struct stat lst {};
  DINGOFS_CHECK(dingofs_lstat(h, "/demo_dir/link.txt", &lst),
                "dingofs_lstat(symlink)");
  std::cout << "symlink mode: 0" << std::oct << lst.st_mode << std::dec
            << "  is_lnk=" << S_ISLNK(lst.st_mode) << "\n";

  // Cleanup
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/link.txt"),
                "dingofs_unlink(link)");
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/target.txt"),
                "dingofs_unlink(target)");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
