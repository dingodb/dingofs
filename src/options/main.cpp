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
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#include <iostream>

#include "options/dingo_cache.h"

using dingofs::options::DingoCacheOption;

int main(/*int argc, char** argv*/) {
  DingoCacheOption option;
  bool succ =
      option.Parse("/home/wine93/work/dingofs/conf/v2/dingo-cache.toml");
  if (!succ) {
    std::cerr << "parse failed." << std::endl;
    return -1;
  }

  {
    auto o = option.block_cache_option();
    std::cout << "stage: " << o.stage() << std::endl;
  }

  {
    auto o = option.block_cache_option().disk_cache_option();
    std::cout << "cache_dir: " << o.cache_dir()[0] << std::endl;
    std::cout << "drop_page_cache: " << o.drop_page_cache() << std::endl;
    std::cout << "ioring_iodepth: " << o.ioring_iodepth() << std::endl;
  }

  {
    auto o = option.cache_group_option();
    std::cout << "listen_ip: " << o.listen_ip() << std::endl;
    std::cout << "listen_port: " << o.listen_port() << std::endl;
  }
  return 0;
}