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

#ifndef DINGOFS_MDS_FILESYSTEM_STORE_OPERATION_H_
#define DINGOFS_MDS_FILESYSTEM_STORE_OPERATION_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/dentry.h"
#include "mds/filesystem/inode.h"
#include "mds/storage/storage.h"

namespace dingofs {
namespace mds {

using FileSessionSPtr = std::shared_ptr<FileSessionEntry>;

class Operation;
using OperationSPtr = std::shared_ptr<Operation>;

class OperationProcessor;
using OperationProcessorSPtr = std::shared_ptr<OperationProcessor>;

class Operation {
 public:
  Operation(Trace& trace) : trace_(trace) { time_ns_ = Helper::TimestampNs(); }
  virtual ~Operation() = default;

  enum class OpType : uint8_t {
    kCreateFs = 0,
    kGetFs = 1,
    kMountFs = 2,
    kUmountFs = 3,
    kDeleteFs = 4,
    kCleanFs = 5,
    kUpdateFs = 6,
    kUpdateFsPartition = 7,
    kUpdateFsState = 8,
    kUpdateFsRecycleProgress = 9,

    kCreateRoot = 20,
    kMkDir = 21,
    kMkNod = 22,
    kBatchCreateFile = 23,
    kHardLink = 24,
    kSmyLink = 25,
    kUpdateAttr = 26,
    kUpdateXAttr = 27,
    kRemoveXAttr = 28,
    kFallocate = 29,
    kOpenFile = 30,
    kCloseFile = 31,
    kRmDir = 32,
    kUnlink = 33,
    kRename = 34,

    kCompactChunk = 50,
    kUpsertChunk = 51,
    kGetChunk = 52,
    kScanChunk = 53,
    kCleanChunk = 54,

    kSetFsQuota = 60,
    kGetFsQuota = 61,
    kFlushFsUsage = 62,
    kDeleteFsQuota = 63,
    kSetDirQuota = 64,
    kDeleteDirQuota = 65,
    kLoadDirQuotas = 66,
    kFlushDirUsages = 67,
    kGetDirQuota = 68,

    kUpsertMds = 80,
    kDeleteMds = 81,
    kScanMds = 82,
    kUpsertClient = 83,
    kDeleteClient = 84,
    kScanClient = 85,

    kGetFileSession = 100,
    kScanFileSession = 101,
    kDeleteFileSession = 102,

    kCleanDelSlice = 110,
    kGetDelFile = 111,
    kCleanDelFile = 112,

    kScanLock = 120,
    kScanFs = 121,
    kScanPartition = 122,
    kScanDentry = 123,
    kScanDelFile = 124,
    kScanDelSlice = 125,

    kScanMetaTable = 140,
    kScanFsMetaTable = 141,
    kScanFsOpLog = 142,

    kSaveFsStats = 150,
    kScanFsStats = 151,
    kGetAndCompactFsStats = 152,

    kGetInodeAttr = 160,
    kBatchGetInodeAttr = 161,
    KGetDentry = 162,

    kImportKV = 170,

    kUpsertCacheMember = 180,
    kDeleteCacheMember = 181,
    kScanCacheMember = 182,
    KGetCacheMember = 183,
  };

  const char* OpName() const;

  struct Result {
    Status status;
    AttrEntry attr;
  };

  bool IsCreateType() const {
    switch (GetOpType()) {
      case OpType::kMkDir:
      case OpType::kMkNod:
      case OpType::kBatchCreateFile:
      case OpType::kSmyLink:
        return true;

      default:
        return false;
    }
  }

  bool IsSetAttrType() const {
    switch (GetOpType()) {
      case OpType::kUpdateAttr:
      case OpType::kUpdateXAttr:
      case OpType::kRemoveXAttr:
      case OpType::kUpsertChunk:
      case OpType::kOpenFile:
      case OpType::kFallocate:
        return true;

      default:
        return false;
    }
  }

  bool IsBatchRun() const {
    switch (GetOpType()) {
      case OpType::kMkDir:
      case OpType::kMkNod:
      case OpType::kBatchCreateFile:
      case OpType::kSmyLink:
      case OpType::kUpdateAttr:
      case OpType::kUpdateXAttr:
      case OpType::kRemoveXAttr:
      case OpType::kUpsertChunk:
      case OpType::kOpenFile:
      case OpType::kFallocate:
        return true;

      default:
        return false;
    }
  }

  virtual OpType GetOpType() const = 0;

  virtual uint32_t GetFsId() const = 0;
  virtual Ino GetIno() const = 0;
  virtual uint64_t GetTime() const { return time_ns_; }

  void SetIsolationLevel(Txn::IsolationLevel level) { isolation_level_ = level; }
  Txn::IsolationLevel GetIsolationLevel() const { return isolation_level_; }

  virtual std::vector<std::string> PrefetchKey() { return {}; }

  void SetEvent(bthread::CountdownEvent* event) { event_ = event; }
  void NotifyEvent() {
    if (event_) event_->signal();
  }

  virtual Status RunInBatch(TxnUPtr&, AttrEntry&, const std::vector<KeyValue>& prefetch_kvs) {  // NOLINT
    return Status(pb::error::ENOT_SUPPORT, "not support.");
  }
  virtual Status Run(TxnUPtr&) { return Status(pb::error::ENOT_SUPPORT, "not support."); }

  void SetStatus(const Status& status) { result_.status = status; }
  void SetAttr(const AttrEntry& attr) { result_.attr = attr; }

  virtual Result& GetResult() { return result_; }

  Trace& GetTrace() { return trace_; }

 private:
  uint64_t time_ns_{0};

  Result result_;

  bthread::CountdownEvent* event_{nullptr};
  Trace& trace_;

  Txn::IsolationLevel isolation_level_{Txn::kSnapshotIsolation};
};

class CreateFsOperation : public Operation {
 public:
  CreateFsOperation(Trace& trace, const FsInfoEntry& fs_info) : Operation(trace), fs_info_(fs_info) {}
  ~CreateFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kCreateFs; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  FsInfoEntry fs_info_;
};

class GetFsOperation : public Operation {
 public:
  GetFsOperation(Trace& trace, const std::string& fs_name) : Operation(trace), fs_name_(fs_name) {}
  ~GetFsOperation() override = default;

  struct Result : public Operation::Result {
    FsInfoEntry fs_info;
  };

  OpType GetOpType() const override { return OpType::kGetFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  const std::string fs_name_;

  Result result_;
};

class MountFsOperation : public Operation {
 public:
  MountFsOperation(Trace& trace, std::string fs_name, pb::mds::MountPoint mountpoint)
      : Operation(trace), fs_name_(fs_name), mount_point_(mountpoint) {};
  ~MountFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kMountFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string fs_name_;
  pb::mds::MountPoint mount_point_;
};

class UmountFsOperation : public Operation {
 public:
  UmountFsOperation(Trace& trace, std::string fs_name, const std::string& client_id)
      : Operation(trace), fs_name_(fs_name), client_id_(client_id) {};
  ~UmountFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kUmountFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string fs_name_;
  std::string client_id_;
};

class DeleteFsOperation : public Operation {
 public:
  DeleteFsOperation(Trace& trace, std::string fs_name, bool is_force)
      : Operation(trace), fs_name_(fs_name), is_force_(is_force) {};
  ~DeleteFsOperation() override = default;

  struct Result : public Operation::Result {
    FsInfoEntry fs_info;
  };

  OpType GetOpType() const override { return OpType::kDeleteFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  std::string fs_name_;
  bool is_force_{false};

  Result result_;
};

class CleanFsOperation : public Operation {
 public:
  CleanFsOperation(Trace& trace, std::string fs_name, uint32_t fs_id)
      : Operation(trace), fs_name_(fs_name), fs_id_(fs_id) {};
  ~CleanFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kCleanFs; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string fs_name_;
  uint32_t fs_id_{0};
};

class UpdateFsOperation : public Operation {
 public:
  UpdateFsOperation(Trace& trace, const std::string& fs_name, const FsInfoEntry& fs_info)
      : Operation(trace), fs_name_(fs_name), fs_info_(fs_info) {};
  ~UpdateFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateFs; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  const std::string fs_name_;
  FsInfoEntry fs_info_;
};

class UpdateFsPartitionOperation : public Operation {
 public:
  using HandlerType = std::function<Status(PartitionPolicy&, FsOpLog&)>;

  UpdateFsPartitionOperation(Trace& trace, const std::string& fs_name, HandlerType handler)
      : Operation(trace), fs_name_(fs_name), handler_(handler) {};
  ~UpdateFsPartitionOperation() override = default;

  struct Result : public Operation::Result {
    FsInfoEntry fs_info;
  };

  OpType GetOpType() const override { return OpType::kUpdateFsPartition; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  const std::string fs_name_;
  HandlerType handler_;

  Result result_;
};

class UpdateFsStateOperation : public Operation {
 public:
  UpdateFsStateOperation(Trace& trace, const std::string& fs_name, pb::mds::FsStatus status)
      : Operation(trace), fs_name_(fs_name), status_(status) {};
  ~UpdateFsStateOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateFsState; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  const std::string fs_name_;
  pb::mds::FsStatus status_;
};

class UpdateFsRecycleProgressOperation : public Operation {
 public:
  UpdateFsRecycleProgressOperation(Trace& trace, const std::string& fs_name, Ino ino)
      : Operation(trace), fs_name_(fs_name), ino_(ino) {};
  ~UpdateFsRecycleProgressOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateFsRecycleProgress; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  const std::string fs_name_;
  Ino ino_;
};

class CreateRootOperation : public Operation {
 public:
  CreateRootOperation(Trace& trace, const Dentry& dentry, const AttrEntry& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~CreateRootOperation() override = default;

  OpType GetOpType() const override { return OpType::kCreateRoot; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return dentry_.INo(); }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  const Dentry& dentry_;
  AttrEntry attr_;
};

class MkDirOperation : public Operation {
 public:
  MkDirOperation(Trace& trace, const Dentry& dentry, const AttrEntry& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~MkDirOperation() override = default;

  OpType GetOpType() const override { return OpType::kMkDir; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const Dentry dentry_;
  AttrEntry attr_;
};

class MkNodOperation : public Operation {
 public:
  MkNodOperation(Trace& trace, const Dentry& dentry, const AttrEntry& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~MkNodOperation() override = default;

  OpType GetOpType() const override { return OpType::kMkNod; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const Dentry dentry_;
  AttrEntry attr_;
};

class BatchCreateFileOperation : public Operation {
 public:
  BatchCreateFileOperation(Trace& trace, const std::vector<Dentry>& dentries, const std::vector<AttrEntry>& attrs,
                           const std::vector<FileSessionSPtr>& file_sessions)
      : Operation(trace), dentries_(dentries), attrs_(attrs), file_sessions_(file_sessions) {};
  ~BatchCreateFileOperation() override = default;

  OpType GetOpType() const override { return OpType::kBatchCreateFile; }

  uint32_t GetFsId() const override { return dentries_.front().FsId(); }
  Ino GetIno() const override { return dentries_.front().ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const std::vector<Dentry>& dentries_;
  const std::vector<AttrEntry>& attrs_;
  const std::vector<FileSessionSPtr>& file_sessions_;
};

class HardLinkOperation : public Operation {
 public:
  HardLinkOperation(Trace& trace, const Dentry& dentry) : Operation(trace), dentry_(dentry) {};
  ~HardLinkOperation() override = default;

  struct Result : public Operation::Result {
    AttrEntry child_attr;
  };

  OpType GetOpType() const override { return OpType::kHardLink; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

  Status Run(TxnUPtr& txn) override;

 private:
  const Dentry& dentry_;
  Result result_;
};

class SmyLinkOperation : public Operation {
 public:
  SmyLinkOperation(Trace& trace, const Dentry& dentry, const AttrEntry& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~SmyLinkOperation() override = default;

  OpType GetOpType() const override { return OpType::kSmyLink; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const Dentry& dentry_;
  AttrEntry attr_;
};

class UpdateAttrOperation : public Operation {
 public:
  struct ExtraParam {
    uint64_t chunk_size{0};
    uint64_t block_size{0};
  };

  UpdateAttrOperation(Trace& trace, uint64_t ino, uint32_t to_set, const AttrEntry& attr, ExtraParam& extra_param)
      : Operation(trace), ino_(ino), to_set_(to_set), attr_(attr), extra_param_(extra_param) {};
  ~UpdateAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateAttr; }

  uint32_t GetFsId() const override { return attr_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  void ExpandChunk(TxnUPtr& txn, AttrEntry& attr, uint64_t new_length) const;

  uint64_t ino_;
  const uint32_t to_set_;
  const AttrEntry& attr_;

  const ExtraParam extra_param_;
};

class UpdateXAttrOperation : public Operation {
 public:
  UpdateXAttrOperation(Trace& trace, uint32_t fs_id, uint64_t ino, const Inode::XAttrMap& xattrs)
      : Operation(trace), fs_id_(fs_id), ino_(ino), xattrs_(xattrs) {};
  ~UpdateXAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateXAttr; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  const Inode::XAttrMap& xattrs_;
};

class RemoveXAttrOperation : public Operation {
 public:
  RemoveXAttrOperation(Trace& trace, uint32_t fs_id, uint64_t ino, const std::string& name)
      : Operation(trace), fs_id_(fs_id), ino_(ino), name_(name) {};
  ~RemoveXAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kRemoveXAttr; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  std::string name_;
};

class UpsertChunkOperation : public Operation {
 public:
  UpsertChunkOperation(Trace& trace, const FsInfoEntry fs_info, uint64_t ino,
                       const std::vector<DeltaSliceEntry>& delta_slices)
      : Operation(trace), fs_info_(fs_info), ino_(ino), delta_slices_(delta_slices) {};
  ~UpsertChunkOperation() override = default;

  struct Result : public Operation::Result {
    int64_t length_delta{0};
    std::vector<ChunkEntry> effected_chunks;
  };

  OpType GetOpType() const override { return OpType::kUpsertChunk; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  std::vector<std::string> PrefetchKey() override;

  Status RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  const FsInfoEntry fs_info_;
  uint64_t ino_;

  std::vector<DeltaSliceEntry> delta_slices_;

  Result result_;
};

class GetChunkOperation : public Operation {
 public:
  GetChunkOperation(Trace& trace, uint32_t fs_id, uint64_t ino, const std::vector<uint32_t>& chunk_indexes)
      : Operation(trace), fs_id_(fs_id), ino_(ino), chunk_indexes_(chunk_indexes) {};
  ~GetChunkOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<ChunkEntry> chunks;
  };

  OpType GetOpType() const override { return OpType::kGetChunk; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  std::vector<uint32_t> chunk_indexes_;

  Result result_;
};

class ScanChunkOperation : public Operation {
 public:
  ScanChunkOperation(Trace& trace, uint32_t fs_id, uint64_t ino, uint32_t max_slice_num = 0)
      : Operation(trace), fs_id_(fs_id), ino_(ino), max_slice_num_(max_slice_num) {};
  ~ScanChunkOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<ChunkEntry> chunks;
  };

  OpType GetOpType() const override { return OpType::kScanChunk; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  uint32_t max_slice_num_{0};

  Result result_;
};

class CleanChunkOperation : public Operation {
 public:
  CleanChunkOperation(Trace& trace, uint32_t fs_id, uint64_t ino, const std::vector<uint64_t>& chunk_indexs)
      : Operation(trace), fs_id_(fs_id), ino_(ino), chunk_indexs_(chunk_indexs) {};
  ~CleanChunkOperation() override = default;

  OpType GetOpType() const override { return OpType::kCleanChunk; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  std::vector<uint64_t> chunk_indexs_{0};
};

class FallocateOperation : public Operation {
 public:
  struct Param {
    uint32_t fs_id;
    uint64_t ino;
    int32_t mode;
    uint64_t offset;
    uint64_t len;

    uint64_t slice_id{0};
    uint32_t slice_num{0};

    uint64_t chunk_size{0};
    uint64_t block_size{0};
  };

  FallocateOperation(Trace& trace, const Param& param) : Operation(trace), param_(param) {};
  ~FallocateOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<ChunkEntry> effected_chunks;
  };

  OpType GetOpType() const override { return OpType::kFallocate; }

  uint32_t GetFsId() const override { return param_.fs_id; }
  Ino GetIno() const override { return param_.ino; }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Status PreAlloc(TxnUPtr& txn, AttrEntry& attr, uint64_t offset, uint32_t len);
  Status SetZero(TxnUPtr& txn, AttrEntry& attr, uint64_t offset, uint64_t len, bool keep_size);

  Param param_;

  Result result_;
};

class OpenFileOperation : public Operation {
 public:
  OpenFileOperation(Trace& trace, uint32_t flags, const FileSessionEntry& file_session)
      : Operation(trace), flags_(flags), file_session_(file_session) {};
  ~OpenFileOperation() override = default;

  struct Result : public Operation::Result {
    int64_t delta_bytes{0};
  };

  OpType GetOpType() const override { return OpType::kOpenFile; }

  uint32_t GetFsId() const override { return file_session_.fs_id(); }
  Ino GetIno() const override { return file_session_.ino(); }

  Status RunInBatch(TxnUPtr& txn, AttrEntry& attr, const std::vector<KeyValue>& prefetch_kvs) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t flags_;
  FileSessionEntry file_session_;

  Result result_;
};

class CloseFileOperation : public Operation {
 public:
  CloseFileOperation(Trace& trace, uint32_t fs_id, Ino ino, const std::string& session_id)
      : Operation(trace), fs_id_(fs_id), ino_(ino), session_id_(session_id) {};
  ~CloseFileOperation() override = default;

  OpType GetOpType() const override { return OpType::kCloseFile; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  Ino ino_;
  const std::string session_id_;
};

class RmDirOperation : public Operation {
 public:
  RmDirOperation(Trace& trace, Dentry dentry) : Operation(trace), dentry_(dentry) {};
  ~RmDirOperation() override = default;

  OpType GetOpType() const override { return OpType::kRmDir; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status Run(TxnUPtr& txn) override;

 private:
  const Dentry dentry_;
};

class UnlinkOperation : public Operation {
 public:
  UnlinkOperation(Trace& trace, const Dentry& dentry) : Operation(trace), dentry_(dentry) {};
  ~UnlinkOperation() override = default;

  struct Result : public Operation::Result {
    AttrEntry child_attr;
  };

  OpType GetOpType() const override { return OpType::kUnlink; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  const Dentry& dentry_;
  Result result_;
};

class RenameOperation : public Operation {
 public:
  RenameOperation(Trace& trace, uint32_t fs_id, Ino old_parent, const std::string& old_name, Ino new_parent_ino,
                  const std::string& new_name)
      : Operation(trace),
        fs_id_(fs_id),
        old_parent_(old_parent),
        old_name_(old_name),
        new_parent_(new_parent_ino),
        new_name_(new_name) {};
  ~RenameOperation() override = default;

  struct Result : public Operation::Result {
    AttrEntry old_parent_attr;
    AttrEntry new_parent_attr;
    DentryEntry old_dentry;
    DentryEntry prev_new_dentry;
    AttrEntry prev_new_attr;
    DentryEntry new_dentry;
    AttrEntry old_attr;

    bool is_same_parent{false};
    bool is_exist_new_dentry{false};
  };

  OpType GetOpType() const override { return OpType::kRename; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return new_parent_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_{0};

  Ino old_parent_{0};
  std::string old_name_;

  Ino new_parent_{0};
  std::string new_name_;

  Result result_;
};

class CompactChunkOperation;
using CompactChunkOperationSPtr = std::shared_ptr<CompactChunkOperation>;

class CompactChunkOperation : public Operation {
 public:
  CompactChunkOperation(Trace& trace, const FsInfoEntry& fs_info, uint64_t ino, uint64_t chunk_index,
                        uint64_t file_length, bool is_force)
      : Operation(trace),
        fs_info_(fs_info),
        ino_(ino),
        chunk_index_(chunk_index),
        file_length_(file_length),
        is_force_(is_force) {};
  CompactChunkOperation(const FsInfoEntry& fs_info, uint64_t ino, uint64_t chunk_index, uint64_t file_length)
      : Operation(trace_), fs_info_(fs_info), ino_(ino), chunk_index_(chunk_index), file_length_(file_length) {};
  ~CompactChunkOperation() override = default;

  struct Result : public Operation::Result {
    TrashSliceList trash_slice_list;
    ChunkEntry effected_chunk;
  };

  static CompactChunkOperationSPtr New(const FsInfoEntry& fs_info, uint64_t ino, uint64_t chunk_index,
                                       uint64_t file_length) {
    return std::make_shared<CompactChunkOperation>(fs_info, ino, chunk_index, file_length);
  }

  OpType GetOpType() const override { return OpType::kCompactChunk; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

  static bool MaybeCompact(const FsInfoEntry& fs_info, Ino ino, uint64_t file_length, const ChunkEntry& chunk);

  static TrashSliceList TestGenTrashSlices(const FsInfoEntry& fs_info, Ino ino, uint64_t file_length,
                                           const ChunkEntry& chunk) {
    return GenTrashSlices(fs_info, ino, file_length, chunk);
  }

 private:
  static TrashSliceList GenTrashSlices(const FsInfoEntry& fs_info, Ino ino, uint64_t file_length,
                                       const ChunkEntry& chunk);
  TrashSliceList GenTrashSlices(Ino ino, uint64_t file_length, const ChunkEntry& chunk);
  static void UpdateChunk(ChunkEntry& chunk, const TrashSliceList& trash_slices);
  TrashSliceList DoCompactChunk(Ino ino, uint64_t file_length, ChunkEntry& chunk);
  TrashSliceList CompactChunk(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length, ChunkEntry& chunk);
  TrashSliceList CompactChunks(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length, Inode::ChunkMap& chunks);

  FsInfoEntry fs_info_;
  uint64_t ino_;
  uint64_t chunk_index_{0};
  uint64_t file_length_{0};

  bool is_force_{false};

  Trace trace_;  // for async run
  Result result_;
};

class SetFsQuotaOperation : public Operation {
 public:
  SetFsQuotaOperation(Trace& trace, uint32_t fs_id, const QuotaEntry& quota)
      : Operation(trace), fs_id_(fs_id), quota_(quota) {};
  ~SetFsQuotaOperation() override = default;

  struct Result : public Operation::Result {
    QuotaEntry quota;
  };

  OpType GetOpType() const override { return OpType::kSetFsQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  QuotaEntry quota_;
  Result result_;
};

class GetFsQuotaOperation : public Operation {
 public:
  GetFsQuotaOperation(Trace& trace, uint32_t fs_id) : Operation(trace), fs_id_(fs_id) {};
  ~GetFsQuotaOperation() override = default;

  struct Result : public Operation::Result {
    QuotaEntry quota;
  };

  OpType GetOpType() const override { return OpType::kGetFsQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  Result result_;
};

class FlushFsUsageOperation : public Operation {
 public:
  FlushFsUsageOperation(Trace& trace, uint32_t fs_id, const std::vector<UsageEntry>& usages)
      : Operation(trace), fs_id_(fs_id), usages_(usages) {};
  ~FlushFsUsageOperation() override = default;

  struct Result : public Operation::Result {
    QuotaEntry quota;
  };

  OpType GetOpType() const override { return OpType::kFlushFsUsage; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  std::vector<UsageEntry> usages_;
  Result result_;
};

class DeleteFsQuotaOperation : public Operation {
 public:
  DeleteFsQuotaOperation(Trace& trace, uint32_t fs_id) : Operation(trace), fs_id_(fs_id) {};
  ~DeleteFsQuotaOperation() override = default;

  OpType GetOpType() const override { return OpType::kDeleteFsQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
};

class SetDirQuotaOperation : public Operation {
 public:
  SetDirQuotaOperation(Trace& trace, uint32_t fs_id, uint64_t ino, const QuotaEntry& quota)
      : Operation(trace), fs_id_(fs_id), ino_(ino), quota_(quota) {};
  ~SetDirQuotaOperation() override = default;

  struct Result : public Operation::Result {
    QuotaEntry quota;
  };

  OpType GetOpType() const override { return OpType::kSetDirQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  QuotaEntry quota_;

  Result result_;
};

class GetDirQuotaOperation : public Operation {
 public:
  GetDirQuotaOperation(Trace& trace, uint32_t fs_id, uint64_t ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~GetDirQuotaOperation() override = default;

  struct Result : public Operation::Result {
    QuotaEntry quota;
  };

  OpType GetOpType() const override { return OpType::kGetDirQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  Result result_;
};

class DeleteDirQuotaOperation : public Operation {
 public:
  DeleteDirQuotaOperation(Trace& trace, uint32_t fs_id, uint64_t ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~DeleteDirQuotaOperation() override = default;

  struct Result : public Operation::Result {
    QuotaEntry quota;
  };

  OpType GetOpType() const override { return OpType::kDeleteDirQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  Result result_;
};

class LoadDirQuotasOperation : public Operation {
 public:
  LoadDirQuotasOperation(Trace& trace, uint32_t fs_id) : Operation(trace), fs_id_(fs_id) {};
  ~LoadDirQuotasOperation() override = default;

  struct Result : public Operation::Result {
    std::unordered_map<Ino, QuotaEntry> quotas;
  };

  OpType GetOpType() const override { return OpType::kLoadDirQuotas; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  Result result_;
};

class FlushDirUsagesOperation : public Operation {
 public:
  using UsageEntrySet = std::vector<UsageEntry>;
  FlushDirUsagesOperation(Trace& trace, uint32_t fs_id, const std::map<uint64_t, UsageEntrySet>& usage_map)
      : Operation(trace), fs_id_(fs_id), usage_map_(usage_map) {};
  ~FlushDirUsagesOperation() override = default;

  struct Result : public Operation::Result {
    // ino -> Quota
    std::map<uint64_t, QuotaEntry> quotas;
  };

  OpType GetOpType() const override { return OpType::kFlushDirUsages; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  // ino -> UsageEntrySet
  std::map<uint64_t, UsageEntrySet> usage_map_;

  Result result_;
};

class UpsertMdsOperation : public Operation {
 public:
  UpsertMdsOperation(Trace& trace, const MdsEntry& mds_meta) : Operation(trace), mds_meta_(mds_meta) {};
  ~UpsertMdsOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpsertMds; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  MdsEntry mds_meta_;
};

class DeleteMdsOperation : public Operation {
 public:
  DeleteMdsOperation(Trace& trace, uint64_t mds_id) : Operation(trace), mds_id_(mds_id) {};
  ~DeleteMdsOperation() override = default;

  OpType GetOpType() const override { return OpType::kDeleteMds; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint64_t mds_id_;
};

class ScanMdsOperation : public Operation {
 public:
  ScanMdsOperation(Trace& trace) : Operation(trace) {};
  ~ScanMdsOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<MdsEntry> mds_entries;
  };

  OpType GetOpType() const override { return OpType::kScanMds; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Result result_;
};

class UpsertClientOperation : public Operation {
 public:
  UpsertClientOperation(Trace& trace, const ClientEntry& client) : Operation(trace), client_(client) {};
  ~UpsertClientOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpsertClient; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  ClientEntry client_;
};

class DeleteClientOperation : public Operation {
 public:
  DeleteClientOperation(Trace& trace, const std::string& client_id) : Operation(trace), client_id_(client_id) {};
  ~DeleteClientOperation() override = default;

  OpType GetOpType() const override { return OpType::kDeleteClient; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string client_id_;
};

class ScanClientOperation : public Operation {
 public:
  ScanClientOperation(Trace& trace) : Operation(trace) {};
  ~ScanClientOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<ClientEntry> client_entries;
  };

  OpType GetOpType() const override { return OpType::kScanClient; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Result result_;
};

class GetFileSessionOperation : public Operation {
 public:
  GetFileSessionOperation(Trace& trace, uint32_t fs_id, Ino ino, const std::string& session_id)
      : Operation(trace), fs_id_(fs_id), ino_(ino), session_id_(session_id) {};
  ~GetFileSessionOperation() override = default;

  struct Result : public Operation::Result {
    FileSessionEntry file_session;
  };

  OpType GetOpType() const override { return OpType::kGetFileSession; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  Ino ino_;
  std::string session_id_;
  Result result_;
};

class ScanFileSessionOperation : public Operation {
 public:
  using HandlerType = std::function<bool(const FileSessionEntry&)>;

  ScanFileSessionOperation(Trace& trace, uint32_t fs_id, Ino ino, HandlerType handler)
      : Operation(trace), fs_id_(fs_id), ino_(ino), handler_(handler) {};
  ScanFileSessionOperation(Trace& trace, uint32_t fs_id, HandlerType handler)
      : Operation(trace), fs_id_(fs_id), handler_(handler) {};
  ~ScanFileSessionOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<FileSessionEntry> file_sessions;
  };

  OpType GetOpType() const override { return OpType::kScanFileSession; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_{0};
  Ino ino_{0};
  HandlerType handler_;
  Result result_;
};

class DeleteFileSessionOperation : public Operation {
 public:
  DeleteFileSessionOperation(Trace& trace, const std::vector<FileSessionEntry>& file_sessions)
      : Operation(trace), file_sessions_(file_sessions) {};
  ~DeleteFileSessionOperation() override = default;

  OpType GetOpType() const override { return OpType::kDeleteFileSession; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::vector<FileSessionEntry> file_sessions_;
};

class CleanDelSliceOperation : public Operation {
 public:
  CleanDelSliceOperation(Trace& trace, const std::string& key) : Operation(trace), key_(key) {};
  ~CleanDelSliceOperation() override = default;

  OpType GetOpType() const override { return OpType::kCleanDelSlice; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string key_;
};

class GetDelFileOperation : public Operation {
 public:
  GetDelFileOperation(Trace& trace, uint32_t fs_id, Ino ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~GetDelFileOperation() override = default;

  OpType GetOpType() const override { return OpType::kGetDelFile; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  Ino ino_;
};

class CleanDelFileOperation : public Operation {
 public:
  CleanDelFileOperation(Trace& trace, uint32_t fs_id, Ino ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~CleanDelFileOperation() override = default;

  OpType GetOpType() const override { return OpType::kCleanDelFile; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  Ino ino_;
};

class ScanLockOperation : public Operation {
 public:
  ScanLockOperation(Trace& trace) : Operation(trace) {};
  ~ScanLockOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<KeyValue> kvs;
  };

  OpType GetOpType() const override { return OpType::kScanLock; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Result result_;
};

class ScanFsOperation : public Operation {
 public:
  ScanFsOperation(Trace& trace) : Operation(trace) {};
  ~ScanFsOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<FsInfoEntry> fs_infoes;
  };

  OpType GetOpType() const override { return OpType::kScanFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Result result_;
};

class ScanPartitionOperation : public Operation {
 public:
  using HandlerType = std::function<bool(KeyValue&)>;

  ScanPartitionOperation(Trace& trace, uint32_t fs_id, Ino ino, HandlerType handler)
      : Operation(trace), fs_id_(fs_id), ino_(ino), handler_(handler) {};
  ~ScanPartitionOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanPartition; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
  Ino ino_{0};
  HandlerType handler_;
};

class ScanDentryOperation : public Operation {
 public:
  using HandlerType = std::function<bool(const DentryEntry&)>;

  ScanDentryOperation(Trace& trace, uint32_t fs_id, Ino ino, const std::string& last_name, HandlerType handler)
      : Operation(trace), fs_id_(fs_id), ino_(ino), last_name_(last_name), handler_(handler) {};
  ScanDentryOperation(Trace& trace, uint32_t fs_id, Ino ino, HandlerType handler)
      : Operation(trace), fs_id_(fs_id), ino_(ino), last_name_(""), handler_(handler) {};
  ~ScanDentryOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanDentry; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
  Ino ino_{0};
  const std::string last_name_;
  HandlerType handler_;
};

class ScanDelSliceOperation : public Operation {
 public:
  ScanDelSliceOperation(Trace& trace, uint32_t fs_id, Ino ino, uint64_t chunk_index, Txn::ScanHandlerType handler)
      : Operation(trace), fs_id_(fs_id), ino_(ino), chunk_index_(chunk_index), handler_(handler) {};
  ScanDelSliceOperation(Trace& trace, uint32_t fs_id, Txn::ScanHandlerType handler)
      : Operation(trace), fs_id_(fs_id), handler_(handler) {};
  ~ScanDelSliceOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanDelSlice; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
  Ino ino_{0};
  uint64_t chunk_index_{0};
  Txn::ScanHandlerType handler_;
};

class ScanDelFileOperation : public Operation {
 public:
  ScanDelFileOperation(Trace& trace, uint32_t fs_id, Txn::ScanHandlerType scan_handler)
      : Operation(trace), fs_id_(fs_id), scan_handler_(scan_handler) {};
  ~ScanDelFileOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanDelFile; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
  Txn::ScanHandlerType scan_handler_;
};

class ScanMetaTableOperation : public Operation {
 public:
  ScanMetaTableOperation(Trace& trace, Txn::ScanHandlerType scan_handler)
      : Operation(trace), scan_handler_(scan_handler) {};
  ~ScanMetaTableOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanMetaTable; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  Txn::ScanHandlerType scan_handler_;
};

class ScanFsMetaTableOperation : public Operation {
 public:
  ScanFsMetaTableOperation(Trace& trace, uint32_t fs_id, Txn::ScanHandlerType scan_handler)
      : Operation(trace), fs_id_(fs_id), scan_handler_(scan_handler) {};
  ScanFsMetaTableOperation(Trace& trace, uint32_t fs_id, const std::string& start_key,
                           Txn::ScanHandlerType scan_handler)
      : Operation(trace), fs_id_(fs_id), start_key_(start_key), scan_handler_(scan_handler) {};
  ~ScanFsMetaTableOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanFsMetaTable; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
  std::string start_key_;
  Txn::ScanHandlerType scan_handler_;
};

class ScanFsOpLogOperation : public Operation {
 public:
  using HandlerType = std::function<bool(const FsOpLog&)>;
  ScanFsOpLogOperation(Trace& trace, uint32_t fs_id, HandlerType handler)
      : Operation(trace), fs_id_(fs_id), handler_(handler) {};
  ~ScanFsOpLogOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanFsOpLog; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
  HandlerType handler_;
};

class SaveFsStatsOperation : public Operation {
 public:
  SaveFsStatsOperation(Trace& trace, uint32_t fs_id, const FsStatsDataEntry& fs_stats)
      : Operation(trace), fs_id_(fs_id), fs_stats_(fs_stats) {};
  ~SaveFsStatsOperation() override = default;

  OpType GetOpType() const override { return OpType::kSaveFsStats; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  const uint32_t fs_id_{0};
  FsStatsDataEntry fs_stats_;
};

class ScanFsStatsOperation : public Operation {
 public:
  ScanFsStatsOperation(Trace& trace, uint32_t fs_id, uint64_t start_time_ns, Txn::ScanHandlerType handler)
      : Operation(trace), fs_id_(fs_id), start_time_ns_(start_time_ns), handler_(handler) {};
  ~ScanFsStatsOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanFsStats; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  const uint32_t fs_id_;
  const uint64_t start_time_ns_{0};
  Txn::ScanHandlerType handler_;
};

class GetAndCompactFsStatsOperation : public Operation {
 public:
  GetAndCompactFsStatsOperation(Trace& trace, uint32_t fs_id, uint64_t mark_time_ns)
      : Operation(trace), fs_id_(fs_id), mark_time_ns_(mark_time_ns) {};
  ~GetAndCompactFsStatsOperation() override = default;

  struct Result : public Operation::Result {
    FsStatsDataEntry fs_stats;
  };

  OpType GetOpType() const override { return OpType::kGetAndCompactFsStats; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  const uint32_t fs_id_;
  const uint64_t mark_time_ns_{0};
  Result result_;
};

class GetInodeAttrOperation : public Operation {
 public:
  GetInodeAttrOperation(Trace& trace, uint32_t fs_id, uint64_t ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~GetInodeAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kGetInodeAttr; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
};

class BatchGetInodeAttrOperation : public Operation {
 public:
  BatchGetInodeAttrOperation(Trace& trace, uint32_t fs_id, const std::vector<Ino>& inoes)
      : Operation(trace), fs_id_(fs_id), inoes_(inoes) {};
  ~BatchGetInodeAttrOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<AttrEntry> attrs;
  };

  OpType GetOpType() const override { return OpType::kBatchGetInodeAttr; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  std::vector<Ino> inoes_;

  Result result_;
};

class GetDentryOperation : public Operation {
 public:
  GetDentryOperation(Trace& trace, uint32_t fs_id, Ino parent, const std::string& name)
      : Operation(trace), fs_id_(fs_id), parent_(parent), name_(name) {};
  ~GetDentryOperation() override = default;

  struct Result : public Operation::Result {
    DentryEntry dentry;
  };

  OpType GetOpType() const override { return OpType::KGetDentry; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  uint32_t fs_id_;
  Ino parent_;
  std::string name_;

  Result result_;
};

class ImportKVOperation : public Operation {
 public:
  ImportKVOperation(Trace& trace, std::vector<KeyValue> kvs) : Operation(trace), kvs_(std::move(kvs)) {};
  ~ImportKVOperation() override = default;

  OpType GetOpType() const override { return OpType::kImportKV; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::vector<KeyValue> kvs_;
};

class UpsertCacheMemberOperation : public Operation {
 public:
  using HandlerType = std::function<Status(CacheMemberEntry&, const Status&)>;
  UpsertCacheMemberOperation(Trace& trace, const std::string& cache_member_id, HandlerType handler)
      : Operation(trace), cache_member_id_(cache_member_id), handler_(handler) {};
  ~UpsertCacheMemberOperation() override = default;

  struct Result : public Operation::Result {
    CacheMemberEntry cache_member;
  };

  OpType GetOpType() const override { return OpType::kUpsertCacheMember; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  std::string cache_member_id_;
  HandlerType handler_;
  Result result_;
};

class DeleteCacheMemberOperation : public Operation {
 public:
  DeleteCacheMemberOperation(Trace& trace, const std::string& cache_member_id)
      : Operation(trace), cache_member_id_(cache_member_id) {};
  ~DeleteCacheMemberOperation() override = default;

  OpType GetOpType() const override { return OpType::kDeleteCacheMember; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string cache_member_id_;
};

class ScanCacheMemberOperation : public Operation {
 public:
  ScanCacheMemberOperation(Trace& trace) : Operation(trace) {};
  ~ScanCacheMemberOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<CacheMemberEntry> cache_member_entries;
  };

  OpType GetOpType() const override { return OpType::kScanCacheMember; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Result result_;
};

class GetCacheMemberOperation : public Operation {
 public:
  GetCacheMemberOperation(Trace& trace, const std::string& cache_member_id)
      : Operation(trace), cache_member_id_(cache_member_id) {};
  ~GetCacheMemberOperation() override = default;

  struct Result : public Operation::Result {
    CacheMemberEntry cache_member;
  };

  OpType GetOpType() const override { return OpType::KGetCacheMember; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  std::string cache_member_id_;
  Result result_;
};

struct BatchOperation {
  uint32_t fs_id{0};
  uint64_t ino{0};

  // set attr/xattr/chunk
  std::vector<Operation*> setattr_operations;
  // mkdir/mknod/symlink/hardlink
  std::vector<Operation*> create_operations;
};

class OperationTask : public TaskRunnable {
 public:
  using PostHandler = std::function<void(OperationSPtr operation)>;

  OperationTask(OperationSPtr operation, OperationProcessorSPtr processor, PostHandler post_handler)
      : operation_(operation), processor_(processor), post_handler_(post_handler) {}
  ~OperationTask() override = default;

  static TaskRunnablePtr New(OperationSPtr operation, OperationProcessorSPtr processor, PostHandler post_handler) {
    return std::make_shared<OperationTask>(operation, processor, post_handler);
  }

  std::string Type() override { return "STORE_OPERATION"; }

  void Run() override;

 private:
  OperationSPtr operation_;
  OperationProcessorSPtr processor_;

  PostHandler post_handler_{nullptr};
};

class OperationProcessor : public std::enable_shared_from_this<OperationProcessor> {
 public:
  OperationProcessor(KVStorageSPtr kv_storage);
  ~OperationProcessor();

  OperationProcessor(const OperationProcessor&) = delete;
  OperationProcessor& operator=(const OperationProcessor&) = delete;
  OperationProcessor(OperationProcessor&&) = delete;
  OperationProcessor& operator=(OperationProcessor&&) = delete;

  static OperationProcessorSPtr New(KVStorageSPtr kv_storage) {
    return std::make_shared<OperationProcessor>(kv_storage);
  }

  struct Key {
    uint32_t fs_id{0};
    uint64_t ino{0};

    bool operator<(const Key& other) const {
      if (fs_id != other.fs_id) {
        return fs_id < other.fs_id;
      }

      return ino < other.ino;
    }
  };

  OperationProcessorSPtr GetSelfPtr() { return shared_from_this(); }

  bool Init();
  bool Destroy();

  bool RunBatched(Operation* operation);
  Status RunAlone(Operation* operation);
  bool AsyncRun(OperationSPtr operation, OperationTask::PostHandler post_handler);

  Status CheckTable(const Range& range);
  Status CreateTable(const std::string& table_name, const Range& range, int64_t& table_id);

 private:
  static std::map<OperationProcessor::Key, BatchOperation> Grouping(std::vector<Operation*>& operations);
  void ProcessOperation();
  void LaunchExecuteBatchOperation(const BatchOperation& batch_operation);
  void ExecuteBatchOperation(BatchOperation& batch_operation);

  bthread_t tid_{0};
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  std::atomic<bool> is_stop_{false};

  butil::MPSCQueue<Operation*> operations_;

  WorkerSPtr async_worker_;

  // persistence store
  KVStorageSPtr kv_storage_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_STORE_OPERATION_H_
