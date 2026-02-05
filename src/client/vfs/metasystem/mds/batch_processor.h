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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_MDS_BATCH_PROCESSOR_H_
#define DINGOFS_SRC_CLIENT_VFS_META_MDS_BATCH_PROCESSOR_H_

#include <memory>
#include <string>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "client/vfs/metasystem/mds/chunk.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "mds/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

using mds::Ino;

class Operation;
using OperationSPtr = std::shared_ptr<Operation>;

struct BatchOperation;

class Operation {
 public:
  Operation(ContextSPtr& ctx) : ctx_(ctx) { time_ns_ = utils::TimestampNs(); };
  virtual ~Operation() = default;

  enum class OpType : uint8_t {
    kWriteSlice = 0,
    kMkDir = 1,
    kMkNod = 2,
    kUnlink = 3,
  };

  std::string OpName() const { return OpName(GetOpType()); }

  static std::string OpName(OpType op_type) {
    switch (op_type) {
      case OpType::kWriteSlice:
        return "writeslice";
      case OpType::kMkDir:
        return "mkdir";
      case OpType::kMkNod:
        return "mknod";
      case OpType::kUnlink:
        return "unlink";
      default:
        return "unknown";
    }
  }

  virtual OpType GetOpType() const = 0;
  virtual Ino GetIno() const = 0;
  virtual uint64_t GetTime() const { return time_ns_; }

  ContextSPtr GetContext() const { return ctx_; }

  void SetEvent(bthread::CountdownEvent* event) { event_ = event; }
  void NotifyEvent() {
    if (event_) event_->signal();
  }

  void SetStatus(const Status& status) { this->status = status; }
  Status GetStatus() const { return status; }

 private:
  uint64_t time_ns_{0};

  ContextSPtr ctx_{nullptr};

  bthread::CountdownEvent* event_{nullptr};

  Status status;
};

struct WriteSliceOperation : public Operation {
  using DoneClosure =
      std::function<void(const Status& status, CommitTaskSPtr task,
                         const std::vector<mds::ChunkEntry>& chunks)>;

  WriteSliceOperation(ContextSPtr& ctx, Ino ino, CommitTaskSPtr& task,
                      DoneClosure&& done)
      : Operation(ctx), ino(ino), task(task), done(std::move(done)) {}

  Ino ino;
  CommitTaskSPtr task;

  DoneClosure done;

  OpType GetOpType() const override { return OpType::kWriteSlice; }
  Ino GetIno() const override { return ino; }

  void PreHandle(std::vector<mds::DeltaSliceEntry>& delta_slice_entries);

  void Done(const Status& status,
            const std::vector<mds::ChunkEntry>& chunks) const {
    done(status, task, chunks);
  }

  static void BatchRun(MDSClient& mds_client, BatchOperation& batch_operation);
};

using WriteSliceOperationSPtr = std::shared_ptr<WriteSliceOperation>;

struct MkNodOperation : public Operation {
  MkNodOperation(ContextSPtr& ctx, Ino parent, std::string name, uint32_t uid,
                 uint32_t gid, uint32_t mode, uint64_t rdev)
      : Operation(ctx),
        parent(parent),
        name(name),
        uid(uid),
        gid(gid),
        mode(mode),
        rdev(rdev) {}

  struct Result {
    AttrEntry attr_entry;
    AttrEntry parent_attr_entry;
  };

  Ino parent;
  std::string name;
  uint32_t uid;
  uint32_t gid;
  uint32_t mode;
  uint64_t rdev;

  Result result;

  OpType GetOpType() const override { return OpType::kMkNod; }
  Ino GetIno() const override { return parent; }

  void SetResult(AttrEntry&& attr_entry, const AttrEntry& parent_attr_entry) {
    result.attr_entry = std::move(attr_entry);
    result.parent_attr_entry = parent_attr_entry;
  }
  Result& GetResult() { return result; }

  static void BatchRun(MDSClient& mds_client, BatchOperation& batch_operation);
};

using MkNodOperationSPtr = std::shared_ptr<MkNodOperation>;

struct MkDirOperation : public Operation {
  MkDirOperation(ContextSPtr& ctx, Ino parent, std::string name, uint32_t uid,
                 uint32_t gid, uint32_t mode)
      : Operation(ctx),
        parent(parent),
        name(name),
        uid(uid),
        gid(gid),
        mode(mode) {}

  struct Result {
    AttrEntry attr_entry;
    AttrEntry parent_attr_entry;
  };

  Ino parent;
  std::string name;
  uint32_t uid;
  uint32_t gid;
  uint32_t mode;

  Result result;

  OpType GetOpType() const override { return OpType::kMkDir; }
  Ino GetIno() const override { return parent; }

  void SetResult(AttrEntry&& attr_entry, const AttrEntry& parent_attr_entry) {
    result.attr_entry = std::move(attr_entry);
    result.parent_attr_entry = parent_attr_entry;
  }
  Result& GetResult() { return result; }

  static void BatchRun(MDSClient& mds_client, BatchOperation& batch_operation);
};

using MkDirOperationSPtr = std::shared_ptr<MkDirOperation>;

struct UnlinkOperation : public Operation {
  UnlinkOperation(ContextSPtr& ctx, Ino parent, std::string name)
      : Operation(ctx), parent(parent), name(name) {}

  struct Result {
    AttrEntry attr_entry;
    AttrEntry parent_attr_entry;
  };

  Ino parent;
  std::string name;

  Result result;

  OpType GetOpType() const override { return OpType::kUnlink; }
  Ino GetIno() const override { return parent; }

  void SetResult(AttrEntry&& attr_entry, const AttrEntry& parent_attr_entry) {
    result.attr_entry = std::move(attr_entry);
    result.parent_attr_entry = parent_attr_entry;
  }
  Result& GetResult() { return result; }

  static void BatchRun(MDSClient& mds_client, BatchOperation& batch_operation);
};

using UnlinkOperationSPtr = std::shared_ptr<UnlinkOperation>;

struct BatchOperation {
  Ino ino;
  Operation::OpType type;
  std::vector<OperationSPtr> operations;
};

class BatchProcessor {
 public:
  BatchProcessor(MDSClient& mds_client);
  ~BatchProcessor();

  BatchProcessor(const BatchProcessor&) = delete;
  BatchProcessor& operator=(const BatchProcessor&) = delete;
  BatchProcessor(BatchProcessor&&) = delete;
  BatchProcessor& operator=(BatchProcessor&&) = delete;

  bool Init();
  bool Stop();

  bool AsyncRun(OperationSPtr operation);
  bool RunBatched(OperationSPtr operation);

 private:
  void ProcessOperation();

  struct Key {
    Operation::OpType type;
    Ino ino;

    bool operator<(const Key& other) const {
      if (type != other.type) {
        return type < other.type;
      }
      return ino < other.ino;
    }
  };

  static std::map<Key, BatchOperation> Grouping(
      std::vector<OperationSPtr>& operations);
  void LaunchExecuteBatchOperation(BatchOperation&& batch_operation);
  static void ExecuteBatchOperation(MDSClient& mds_client,
                                    BatchOperation& batch_operation);

  // consumer thread
  bthread_t tid_{0};
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  std::atomic<bool> stopped_{false};

  MDSClient& mds_client_;

  butil::MPSCQueue<OperationSPtr> operations_;
};

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_MDS_BATCH_PROCESSOR_H_