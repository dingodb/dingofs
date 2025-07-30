
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

#ifndef DINGOFS_CLIENT_VFS_BACKGROUND_PERIODIC_FLUSH_MANAGER_H_
#define DINGOFS_CLIENT_VFS_BACKGROUND_PERIODIC_FLUSH_MANAGER_H_

#include <gflags/gflags_declare.h>
#include <sys/types.h>

#include <map>
#include <mutex>

#include "client/vfs/background/iperiodic_flush_manager.h"
#include "client/vfs/handle/handle_manager.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;

class PeriodicFlushManager : public IPeriodicFlushManager {
 public:
  PeriodicFlushManager(VFSHub* hub) : vfs_hub_(hub) {}

  ~PeriodicFlushManager() override = default;

  void Start() override;

  void Stop() override;

  void SubmitToFlush(HandleSPtr handle) override;

 private:
  void FlushHandleDone(Status s, uint64_t seq_id, HandleSPtr handle);
  void FlushHandle(uint64_t fh);

  VFSHub* vfs_hub_{nullptr};

  std::mutex mutex_;
  bool stopped_{true};
  // fh -> weak_ptr<Handle>
  std::map<uint64_t, std::weak_ptr<Handle>> handles_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_BACKGROUND_PERIODIC_FLUSH_MANAGER_H_