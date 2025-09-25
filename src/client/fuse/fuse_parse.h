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
#include <unistd.h>

#include <cstring>
#include <string>
#include <unordered_map>

#include "client/fuse/fuse_common.h"
#include "utils/string.h"

using dingofs::utils::TrimSpace;

inline void PrintOptionHelp(const char* o, const char* msg) {
  printf("    -o %-20s%s\n", o, msg);
}

inline void ExtraOptionsHelp() {
  printf("\nExtra options:\n");
  PrintOptionHelp("fsname", "[required] name of filesystem to be mounted");
  PrintOptionHelp("fstype",
                  "[required] type of filesystem to be mounted (s3/volume)");
  PrintOptionHelp("conf", "[required] path of config file");
  printf("    --mdsAddr              mdsAddr of dingofs cluster\n");
}

inline std::string MatchAnyPattern(
    const std::unordered_map<std::string, char**>& patterns, const char* src) {
  size_t src_len = strlen(src);
  for (const auto& pair : patterns) {
    const auto& pattern = pair.first;
    if (pattern.length() < src_len &&
        strncmp(pattern.c_str(), src, pattern.length()) == 0) {
      return pattern;
    }
  }
  return {};
}

inline void ParseOption(int argc, char** argv, int* parsed_argc_p,
                        char** parsed_argv, struct MountOption* opts) {
  // add support for parsing option value with comma(,)
  std::unordered_map<std::string, char**> patterns = {
      {"--mdsaddr=", &opts->mds_addr}};
  for (int i = 0, j = 0; j < argc; j++) {
    std::string p = MatchAnyPattern(patterns, argv[j]);
    int p_len = p.length();
    int src_len = strlen(argv[j]);
    if (p_len) {
      if (*patterns[p]) {
        free(*patterns[p]);
      }
      *patterns[p] =
          reinterpret_cast<char*>(malloc(sizeof(char) * (src_len - p_len + 1)));
      memcpy(*patterns[p], argv[j] + p_len, src_len - p_len);
      (*patterns[p])[src_len - p_len] = '\0';
      *parsed_argc_p = *parsed_argc_p - 1;
    } else {
      parsed_argv[i] =
          reinterpret_cast<char*>(malloc(sizeof(char) * (src_len + 1)));
      memcpy(parsed_argv[i], argv[j], src_len);
      parsed_argv[i][src_len] = '\0';
      i++;
    }
  }
}

inline void FreeParsedArgv(char** parsed_argv, int alloc_size) {
  for (int i = 0; i < alloc_size; i++) {
    free(parsed_argv[i]);
  }
  free((char*)parsed_argv);
}
