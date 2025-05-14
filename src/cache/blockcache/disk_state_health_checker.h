// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_

#include <bthread/mutex.h>

#include <memory>
#include <shared_mutex>

#include "base/timer/timer_impl.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_state_machine.h"
#include "cache/common/common.h"
#include "options/cache/block_cache.h"

namespace dingofs {
namespace cache {
namespace blockcache {

class DiskStateHealthChecker {
 public:
  DiskStateHealthChecker(std::shared_ptr<DiskCacheLayout> layout,
                         std::shared_ptr<DiskStateMachine> disk_state_machine);

  virtual ~DiskStateHealthChecker() = default;

  virtual bool Start();

  virtual bool Stop();

 private:
  void RunCheck();

  void ProbeDisk();

  bthread::Mutex mutex_;
  bool running_{false};
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<DiskStateMachine> disk_state_machine_;
  std::unique_ptr<base::timer::TimerImpl> timer_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_
