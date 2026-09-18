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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_READER_REGISTRY_TASK_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_READER_REGISTRY_TASK_H_
#include <vector>

#include "client/vfs/components/maintenance_manager.h"
namespace dingofs {
namespace client {
namespace vfs {
class FileReader;
class ReaderRegistry;

// Registered reader shrink operation. Registry outlives OnStop completion.
class ReaderRegistryTask final : public MaintenanceTask {
 public:
  explicit ReaderRegistryTask(ReaderRegistry* registry);
  bool RunOnce(size_t budget) override;
  void OnStop() override;

 private:
  ReaderRegistry* registry_;
  size_t shard_{0};
  size_t next_{0};
  std::vector<FileReader*> snapshot_;
};
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif
