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

#include "utils/uuid.h"

#include <uuid/uuid.h>

namespace dingofs {
namespace utils {

#define BUFF_LEN 36

std::string GenerateUUID() {
  uuid_t out;
  char buf[BUFF_LEN];
  uuid_generate(out);
  uuid_unparse_lower(out, buf);
  std::string str(&buf[0], BUFF_LEN);
  return str;
}

std::string GenerateUUIDTime() {
  uuid_t out;
  char buf[BUFF_LEN];
  uuid_generate_time(out);
  uuid_unparse_lower(out, buf);
  std::string str(&buf[0], BUFF_LEN);
  return str;
}

std::string GenerateUUIDRandom() {
  uuid_t out;
  char buf[BUFF_LEN];
  uuid_generate_random(out);
  uuid_unparse_lower(out, buf);
  std::string str(&buf[0], BUFF_LEN);
  return str;
}
}  // namespace utils
}  // namespace dingofs
