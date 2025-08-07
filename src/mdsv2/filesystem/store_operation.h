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

#ifndef DINGOFS_MDV2_FILESYSTEM_STORE_OPERATION_H_
#define DINGOFS_MDV2_FILESYSTEM_STORE_OPERATION_H_

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
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

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
    kUpdateFs = 5,
    kUpdateFsPartition = 6,
    kCreateRoot = 7,
    kMkDir = 11,
    kMkNod = 12,
    kHardLink = 13,
    kSmyLink = 14,
    kUpdateAttr = 15,
    kUpdateXAttr = 16,
    kRemoveXAttr = 17,
    kFallocate = 18,
    kOpenFile = 19,
    kCloseFile = 20,
    kRmDir = 21,
    kUnlink = 22,
    kRename = 23,
    kCompactChunk = 24,

    kUpsertChunk = 25,
    kGetChunk = 26,
    kScanChunk = 27,
    kCleanChunk = 28,

    kSetFsQuota = 30,
    kGetFsQuota = 31,
    kFlushFsUsage = 32,
    kDeleteFsQuota = 33,

    kSetDirQuota = 35,
    kDeleteDirQuota = 36,
    kLoadDirQuotas = 37,
    kFlushDirUsages = 38,

    kUpsertMds = 40,
    kDeleteMds = 41,
    kScanMds = 42,
    kUpsertClient = 43,
    kDeleteClient = 44,
    kScanClient = 45,

    kGetFileSession = 50,
    kScanFileSession = 51,
    kDeleteFileSession = 52,

    kCleanDelSlice = 60,
    kGetDelFile = 61,
    kCleanDelFile = 62,

    kScanLock = 70,
    kScanFs = 71,
    kScanDentry = 72,
    kScanDelFile = 73,
    kScanDelSlice = 74,

    kScanMetaTable = 75,
    kScanFsMetaTable = 76,
    kScanFsOpLog = 77,

    kSaveFsStats = 80,
    kScanFsStats = 81,
    kGetAndCompactFsStats = 82,

    kGetInodeAttr = 90,
    kBatchGetInodeAttr = 91,

    kImportKV = 100,
  };

  const char* OpName() const;

  struct Result {
    Status status;
    AttrType attr;
  };

  bool IsCreateType() const {
    switch (GetOpType()) {
      case OpType::kMkDir:
      case OpType::kMkNod:
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

  virtual std::string PrefetchKey() { return ""; }

  void SetEvent(bthread::CountdownEvent* event) { event_ = event; }
  void NotifyEvent() {
    if (event_) event_->signal();
  }

  virtual Status RunInBatch(TxnUPtr&, AttrType&, const std::vector<KeyValue>& prefetch_kvs) {  // NOLINT
    return Status(pb::error::ENOT_SUPPORT, "not support.");
  }
  virtual Status Run(TxnUPtr&) { return Status(pb::error::ENOT_SUPPORT, "not support."); }

  void SetStatus(const Status& status) { result_.status = status; }
  void SetAttr(const AttrType& attr) { result_.attr = attr; }

  virtual Result& GetResult() { return result_; }

  Trace& GetTrace() { return trace_; }

 private:
  uint64_t time_ns_{0};

  Result result_;

  bthread::CountdownEvent* event_{nullptr};
  Trace& trace_;
};

class CreateFsOperation : public Operation {
 public:
  CreateFsOperation(Trace& trace, const FsInfoType& fs_info) : Operation(trace), fs_info_(fs_info) {}
  ~CreateFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kCreateFs; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  FsInfoType fs_info_;
};

class GetFsOperation : public Operation {
 public:
  GetFsOperation(Trace& trace, const std::string& fs_name) : Operation(trace), fs_name_(fs_name) {}
  ~GetFsOperation() override = default;

  struct Result : public Operation::Result {
    FsInfoType fs_info;
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
  MountFsOperation(Trace& trace, std::string fs_name, pb::mdsv2::MountPoint mountpoint)
      : Operation(trace), fs_name_(fs_name), mount_point_(mountpoint) {};
  ~MountFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kMountFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string fs_name_;
  pb::mdsv2::MountPoint mount_point_;
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
    FsInfoType fs_info;
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

class UpdateFsOperation : public Operation {
 public:
  UpdateFsOperation(Trace& trace, const std::string& fs_name, const FsInfoType& fs_info)
      : Operation(trace), fs_name_(fs_name), fs_info_(fs_info) {};
  ~UpdateFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateFs; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  const std::string fs_name_;
  FsInfoType fs_info_;
};

class UpdateFsPartitionOperation : public Operation {
 public:
  using HandlerType = std::function<Status(PartitionPolicy&, FsOpLog&)>;

  UpdateFsPartitionOperation(Trace& trace, const std::string& fs_name, HandlerType handler)
      : Operation(trace), fs_name_(fs_name), handler_(handler) {};
  ~UpdateFsPartitionOperation() override = default;

  struct Result : public Operation::Result {
    FsInfoType fs_info;
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

class CreateRootOperation : public Operation {
 public:
  CreateRootOperation(Trace& trace, const Dentry& dentry, const AttrType& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~CreateRootOperation() override = default;

  OpType GetOpType() const override { return OpType::kCreateRoot; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return dentry_.INo(); }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  const Dentry& dentry_;
  AttrType attr_;
};

class MkDirOperation : public Operation {
 public:
  MkDirOperation(Trace& trace, const Dentry& dentry, const AttrType& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~MkDirOperation() override = default;

  OpType GetOpType() const override { return OpType::kMkDir; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrType& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const Dentry dentry_;
  AttrType attr_;
};

class MkNodOperation : public Operation {
 public:
  MkNodOperation(Trace& trace, const Dentry& dentry, const AttrType& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~MkNodOperation() override = default;

  OpType GetOpType() const override { return OpType::kMkNod; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrType& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const Dentry dentry_;
  AttrType attr_;
};

class HardLinkOperation : public Operation {
 public:
  HardLinkOperation(Trace& trace, const Dentry& dentry) : Operation(trace), dentry_(dentry) {};
  ~HardLinkOperation() override = default;

  struct Result : public Operation::Result {
    AttrType child_attr;
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
  SmyLinkOperation(Trace& trace, const Dentry& dentry, const AttrType& attr)
      : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~SmyLinkOperation() override = default;

  OpType GetOpType() const override { return OpType::kSmyLink; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrType& parent_attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  const Dentry& dentry_;
  AttrType attr_;
};

class UpdateAttrOperation : public Operation {
 public:
  struct ExtraParam {
    uint64_t slice_id{0};
    uint32_t slice_num{0};

    uint64_t chunk_size{0};
    uint64_t block_size{0};
  };

  UpdateAttrOperation(Trace& trace, uint64_t ino, uint32_t to_set, const AttrType& attr, const ExtraParam& extra_param)
      : Operation(trace), ino_(ino), to_set_(to_set), attr_(attr), extra_param_(extra_param) {};
  ~UpdateAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateAttr; }

  uint32_t GetFsId() const override { return attr_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  // todo: optimization, prefetch max chunk key
  Status RunInBatch(TxnUPtr& txn, AttrType& attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  Status ExpandChunk(TxnUPtr& txn, AttrType& attr, ChunkType& max_chunk, uint64_t new_length) const;
  Status Truncate(TxnUPtr& txn, AttrType& attr);

  uint64_t ino_;
  const uint32_t to_set_;
  const AttrType& attr_;

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

  Status RunInBatch(TxnUPtr& txn, AttrType& attr, const std::vector<KeyValue>& prefetch_kvs) override;

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

  Status RunInBatch(TxnUPtr& txn, AttrType& attr, const std::vector<KeyValue>& prefetch_kvs) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  std::string name_;
};

class UpsertChunkOperation : public Operation {
 public:
  UpsertChunkOperation(Trace& trace, const FsInfoType fs_info, uint64_t ino, uint64_t index,
                       const std::vector<pb::mdsv2::Slice>& slices)
      : Operation(trace), fs_info_(fs_info), ino_(ino), chunk_index_(index), slices_(slices) {};
  ~UpsertChunkOperation() override = default;

  struct Result : public Operation::Result {
    int64_t length_delta{0};
    ChunkType chunk;
  };

  OpType GetOpType() const override { return OpType::kUpsertChunk; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  std::string PrefetchKey() override;

  Status RunInBatch(TxnUPtr& txn, AttrType& attr, const std::vector<KeyValue>& prefetch_kvs) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  const FsInfoType fs_info_;
  uint64_t ino_;
  uint64_t chunk_index_{0};

  std::vector<pb::mdsv2::Slice> slices_;

  Result result_;
};

class GetChunkOperation : public Operation {
 public:
  GetChunkOperation(Trace& trace, uint32_t fs_id, uint64_t ino, uint64_t index)
      : Operation(trace), fs_id_(fs_id), ino_(ino), chunk_index_(index) {};
  ~GetChunkOperation() override = default;

  struct Result : public Operation::Result {
    ChunkType chunk;
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
  uint64_t chunk_index_{0};

  Result result_;
};

class ScanChunkOperation : public Operation {
 public:
  ScanChunkOperation(Trace& trace, uint32_t fs_id, uint64_t ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~ScanChunkOperation() override = default;

  struct Result : public Operation::Result {
    std::vector<ChunkType> chunks;
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
    std::vector<ChunkType> effected_chunks;
  };

  OpType GetOpType() const override { return OpType::kFallocate; }

  uint32_t GetFsId() const override { return param_.fs_id; }
  Ino GetIno() const override { return param_.ino; }

  Status RunInBatch(TxnUPtr& txn, AttrType& attr, const std::vector<KeyValue>& prefetch_kvs) override;

  template <int size = 0>
  Result& GetResult() {
    auto& result = Operation::GetResult();
    result_.status = result.status;
    result_.attr = std::move(result.attr);

    return result_;
  }

 private:
  Status PreAlloc(TxnUPtr& txn, AttrType& attr, uint64_t offset, uint32_t len);
  Status SetZero(TxnUPtr& txn, AttrType& attr, uint64_t offset, uint64_t len, bool keep_size);

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

  Status RunInBatch(TxnUPtr& txn, AttrType& attr, const std::vector<KeyValue>& prefetch_kvs) override;

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
    AttrType child_attr;
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
    AttrType old_parent_attr;
    AttrType new_parent_attr;
    DentryType old_dentry;
    DentryType prev_new_dentry;
    AttrType prev_new_attr;
    DentryType new_dentry;
    AttrType old_attr;

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
  CompactChunkOperation(Trace& trace, const FsInfoType& fs_info, uint64_t ino, uint64_t chunk_index,
                        uint64_t file_length, bool is_force)
      : Operation(trace),
        fs_info_(fs_info),
        ino_(ino),
        chunk_index_(chunk_index),
        file_length_(file_length),
        is_force_(is_force) {};
  CompactChunkOperation(const FsInfoType& fs_info, uint64_t ino, uint64_t chunk_index, uint64_t file_length)
      : Operation(trace_), fs_info_(fs_info), ino_(ino), chunk_index_(chunk_index), file_length_(file_length) {};
  ~CompactChunkOperation() override = default;

  struct Result : public Operation::Result {
    TrashSliceList trash_slice_list;
    ChunkType effected_chunk;
  };

  static CompactChunkOperationSPtr New(const FsInfoType& fs_info, uint64_t ino, uint64_t chunk_index,
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

  static bool MaybeCompact(const FsInfoType& fs_info, Ino ino, uint64_t file_length, const ChunkType& chunk);

 private:
  static TrashSliceList GenTrashSlices(const FsInfoType& fs_info, Ino ino, uint64_t file_length,
                                       const ChunkType& chunk);
  TrashSliceList GenTrashSlices(Ino ino, uint64_t file_length, const ChunkType& chunk);
  static void UpdateChunk(ChunkType& chunk, const TrashSliceList& trash_slices);
  TrashSliceList DoCompactChunk(Ino ino, uint64_t file_length, ChunkType& chunk);
  TrashSliceList CompactChunk(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length, ChunkType& chunk);
  TrashSliceList CompactChunks(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length, Inode::ChunkMap& chunks);

  FsInfoType fs_info_;
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

  OpType GetOpType() const override { return OpType::kSetFsQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  QuotaEntry quota_;
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
  FlushFsUsageOperation(Trace& trace, uint32_t fs_id, const UsageEntry& usage)
      : Operation(trace), fs_id_(fs_id), usage_(usage) {};
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
  UsageEntry usage_;
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

  OpType GetOpType() const override { return OpType::kSetDirQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  QuotaEntry quota_;
};

class GetDirQuotaOperation : public Operation {
 public:
  GetDirQuotaOperation(Trace& trace, uint32_t fs_id, uint64_t ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~GetDirQuotaOperation() override = default;

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
  Result result_;
};

class DeleteDirQuotaOperation : public Operation {
 public:
  DeleteDirQuotaOperation(Trace& trace, uint32_t fs_id, uint64_t ino) : Operation(trace), fs_id_(fs_id), ino_(ino) {};
  ~DeleteDirQuotaOperation() override = default;

  OpType GetOpType() const override { return OpType::kDeleteDirQuota; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
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
  FlushDirUsagesOperation(Trace& trace, uint32_t fs_id, const std::map<uint64_t, UsageEntry>& usages)
      : Operation(trace), fs_id_(fs_id), usages_(usages) {};
  ~FlushDirUsagesOperation() override = default;

  struct Result : public Operation::Result {
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
  std::map<uint64_t, UsageEntry> usages_;

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
    std::vector<FsInfoType> fs_infoes;
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

class ScanDentryOperation : public Operation {
 public:
  using HandlerType = std::function<bool(const DentryType&)>;

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
  ~ScanFsMetaTableOperation() override = default;

  OpType GetOpType() const override { return OpType::kScanFsMetaTable; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  uint32_t fs_id_{0};
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
    std::vector<AttrType> attrs;
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
  OperationTask(OperationSPtr operation, OperationProcessorSPtr processor)
      : operation_(operation), processor_(processor) {}
  ~OperationTask() override = default;

  static TaskRunnablePtr New(OperationSPtr operation, OperationProcessorSPtr processor) {
    return std::make_shared<OperationTask>(operation, processor);
  }

  std::string Type() override { return "STORE_OPERATION"; }

  void Run() override;

 private:
  OperationSPtr operation_;
  OperationProcessorSPtr processor_;
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
  bool AsyncRun(OperationSPtr operation);

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

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_STORE_OPERATION_H_
