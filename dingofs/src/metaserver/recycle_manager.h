/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * @Project: dingo
 * @Date: 2022-08-25 15:39:12
 * @Author: chenwei
 */
#include <list>
#include <memory>

#include "metaserver/recycle_cleaner.h"

#ifndef DINGOFS_SRC_METASERVER_RECYCLE_MANAGER_H_
#define DINGOFS_SRC_METASERVER_RECYCLE_MANAGER_H_
namespace dingofs {
namespace metaserver {
struct RecycleManagerOption {
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient;
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient;
  uint32_t scanPeriodSec;
  uint32_t scanLimit;
};

class RecycleManager {
 public:
  RecycleManager() {
    isStop_ = true;
    inProcessingCleaner_ = nullptr;
  }

  static RecycleManager& GetInstance() {
    static RecycleManager instance_;
    return instance_;
  }

  void Init(const RecycleManagerOption& opt);

  void Add(uint32_t partitionId, const std::shared_ptr<RecycleCleaner>& cleaner,
           copyset::CopysetNode* copysetNode);

  void Remove(uint32_t partitionId);

  void Run();

  void Stop();

  void ScanLoop();

 private:
  std::list<std::shared_ptr<RecycleCleaner>> recycleCleanerList_;
  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient_;
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;
  dingofs::utils::RWLock rwLock_;
  std::shared_ptr<RecycleCleaner> inProcessingCleaner_;
  utils::Atomic<bool> isStop_;
  utils::Thread thread_;
  utils::InterruptibleSleeper sleeper_;
  uint32_t scanPeriodSec_;
  uint32_t scanLimit_;
};
}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_SRC_METASERVER_RECYCLE_MANAGER_H_
