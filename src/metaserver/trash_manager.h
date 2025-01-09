/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: dingo
 * Created Date: 2021-08-31
 * Author: xuchaojie
 */

#ifndef DINGOFS_SRC_METASERVER_TRASH_MANAGER_H_
#define DINGOFS_SRC_METASERVER_TRASH_MANAGER_H_

#include <list>
#include <map>
#include <memory>

#include "metaserver/trash.h"
#include "utils/concurrent/concurrent.h"
#include "utils/interruptible_sleeper.h"

namespace dingofs {
namespace metaserver {

class TrashManager {
 public:
  TrashManager() : isStop_(true) { LOG(INFO) << "TrashManager"; }

  static TrashManager& GetInstance() {
    static TrashManager instance_;
    return instance_;
  }

  void Add(uint32_t partitionId, const std::shared_ptr<Trash>& trash) {
    dingofs::utils::WriteLockGuard lg(rwLock_);
    trash->Init(options_);
    trashs_.emplace(partitionId, trash);
    LOG(INFO) << "add partition to trash manager, partitionId = "
              << partitionId;
  }

  void Remove(uint32_t partitionId);

  void Init(const TrashOption& options) { options_ = options; }

  int Run();

  void Fini();

  void ScanEveryTrash();

  void ListItems(std::list<TrashItem>* items);

 private:
  void ScanLoop();

  TrashOption options_;

  utils::Thread recycleThread_;

  utils::Atomic<bool> isStop_;

  utils::InterruptibleSleeper sleeper_;

  std::map<uint32_t, std::shared_ptr<Trash>> trashs_;
  dingofs::utils::RWLock rwLock_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_TRASH_MANAGER_H_
