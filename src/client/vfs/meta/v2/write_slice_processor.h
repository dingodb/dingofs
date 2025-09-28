// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_WRITE_SLICE_PROCESSOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_WRITE_SLICE_PROCESSOR_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "client/meta/vfs_meta.h"
#include "client/vfs/meta/v2/mds_client.h"
#include "mds/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

using mds::Ino;

class WriteSliceProcessor;
using WriteSliceProcessorSPtr = std::shared_ptr<WriteSliceProcessor>;

struct WriteSliceOperation {
  ContextSPtr ctx;
  Ino ino;
  uint64_t index;
  uint64_t fh;
  std::vector<Slice> slices;

  DoneClosure done;
};

using WriteSliceOperationSPtr = std::shared_ptr<WriteSliceOperation>;

struct BatchOperation {
  Ino ino;
  std::vector<WriteSliceOperationSPtr> operations;
};

class WriteSliceProcessor
    : public std::enable_shared_from_this<WriteSliceProcessor> {
 public:
  WriteSliceProcessor(MDSClientSPtr mds_client);
  ~WriteSliceProcessor();

  WriteSliceProcessor(const WriteSliceProcessor&) = delete;
  WriteSliceProcessor& operator=(const WriteSliceProcessor&) = delete;
  WriteSliceProcessor(WriteSliceProcessor&&) = delete;
  WriteSliceProcessor& operator=(WriteSliceProcessor&&) = delete;

  static WriteSliceProcessorSPtr New(MDSClientSPtr mds_client) {
    return std::make_shared<WriteSliceProcessor>(mds_client);
  }

  WriteSliceProcessorSPtr GetSelfPtr() { return shared_from_this(); }

  bool Init();
  bool Destroy();

  bool AsyncRun(WriteSliceOperationSPtr operation);

 private:
  void ProcessOperation();
  static std::map<Ino, BatchOperation> Grouping(
      std::vector<WriteSliceOperationSPtr>& operations);
  void LaunchExecuteBatchOperation(const BatchOperation& batch_operation);
  void ExecuteBatchOperation(BatchOperation& batch_operation);

  // consumer thread
  bthread_t tid_{0};
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  std::atomic<bool> is_stop_{false};

  MDSClientSPtr mds_client_;

  butil::MPSCQueue<WriteSliceOperationSPtr> operations_;

  //   WorkerSPtr async_worker_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_WRITE_SLICE_PROCESSOR_H_