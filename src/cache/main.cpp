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
 * Created Date: 2025-02-26
 * Author: Jingli Chen (Wine93)
 */

#include <gflags/gflags.h>

#include <cstring>
#include <iostream>

#include "cache/help.h"
#include "cache/server.h"
#include "cache/version.h"

static int parse_option(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--version") == 0) {
      std::cout << dingofs::cache::Version() << "\n";
      return 1;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      std::cout << dingofs::cache::Usage() << "\n";
      return 1;
    }
  }

  gflags::ParseCommandLineNonHelpFlags(&argc, &argv, false);
  return 0;
}

int main(int argc, char** argv) {
  int rc = parse_option(argc, argv);
  if (rc != 0) {
    return rc;
  }

  return dingofs::cache::Run();
}
