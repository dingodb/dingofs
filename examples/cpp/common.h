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

#pragma once

#include <cstdio>
#include <cstring>
#include <iostream>

#include "dingofs/libdingofs.h"

// ── Default cluster config
// ────────────────────────────────────────────────────

#define MDS_ADDRS "10.232.10.5:8801"
#define FS_NAME "test03"
#define MOUNT_POINT "/dingofs/client/mnt/test03"

// ── DINGOFS_CHECK macro
// ─────────────────────────────────────────────────────── For functions that
// return 0 on success, -errno on failure.
#define DINGOFS_CHECK(ret, msg)                                       \
  do {                                                                \
    int _r = static_cast<int>(ret);                                   \
    if (_r < 0) {                                                     \
      (void)(std::cerr << "[FAIL] " << (msg) << ": " << strerror(-_r) \
                       << "\n");                                      \
      return 1;                                                       \
    }                                                                 \
    (void)(std::cout << "[ OK ] " << (msg) << "\n");                  \
  } while (0)

// ── make_client
// ─────────────────────────────────────────────────────────────── Allocate and
// configure a client handle (not yet mounted). Caller must call dingofs_mount()
// then dingofs_umount() + dingofs_delete().
static inline uintptr_t make_client() {
  uintptr_t h = dingofs_new();
  (void)dingofs_conf_set(h, "log.dir", "/tmp/dingofs-cpp-demo-log");
  (void)dingofs_conf_set(h, "log.level", "WARNING");
  return h;
}
