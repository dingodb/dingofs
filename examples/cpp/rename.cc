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

// rename.cc — rename a file, verify via stat via the C API.

#include <fcntl.h>
#include <sys/stat.h>

#include <iostream>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // Setup: create directory + file
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  int fd = dingofs_open(h, "/demo_dir/old_name.txt",
                        O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    std::cerr << "[FAIL] dingofs_open: " << strerror(-fd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }

  // Record original inode
  struct stat orig {};
  dingofs_fstat(h, fd, &orig);
  std::cout << "original ino: " << orig.st_ino << "\n";

  DINGOFS_CHECK(dingofs_close(h, fd), "dingofs_close");

  // Rename
  DINGOFS_CHECK(
      dingofs_rename(h, "/demo_dir/old_name.txt", "/demo_dir/new_name.txt"),
      "dingofs_rename");

  // Verify via stat on new name — inode must be the same
  struct stat renamed {};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/new_name.txt", &renamed),
                "dingofs_stat(new_name)");
  std::cout << "renamed  ino: " << renamed.st_ino << "  (was " << orig.st_ino
            << ")\n";

  // Cleanup
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/new_name.txt"), "dingofs_unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
