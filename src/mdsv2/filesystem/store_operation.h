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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/countdown_event.h"
#include "butil/containers/mpsc_queue.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

class Operation {
 public:
  Operation(Trace& trace) : trace_(trace) { time_ns_ = Helper::TimestampNs(); }
  virtual ~Operation() = default;

  enum class OpType {
    kMountFs = 0,
    kUmountFs = 1,
    kDeleteFs = 2,
    kCreateRoot = 10,
    kMkDir = 11,
    kMkNod = 12,
    kHardLink = 13,
    kSmyLink = 14,
    kUpdateAttr = 15,
    kUpdateXAttr = 16,
    kUpdateChunk = 17,
    kRmDir = 20,
    kUnlink = 21,
    kRename = 22,
    kCompactChunk = 23,

    kSetFsQuota = 30,
    kGetFsQuota = 31,
    kFlushFsUsage = 32,
    kDeleteFsQuota = 33,

    kSetDirQuota = 35,
    kDeleteDirQuota = 36,
    kLoadDirQuotas = 37,
    kFlushDirUsages = 38,
  };

  const char* OpName() const {
    switch (GetOpType()) {
      case OpType::kMountFs:
        return "MountFs";

      case OpType::kUmountFs:
        return "UmountFs";

      case OpType::kDeleteFs:
        return "DeleteFs";

      case OpType::kCreateRoot:
        return "CreateRoot";

      case OpType::kMkDir:
        return "MkDir";

      case OpType::kMkNod:
        return "MkNod";

      case OpType::kHardLink:
        return "HardLink";

      case OpType::kSmyLink:
        return "SmyLink";

      case OpType::kUpdateAttr:
        return "UpdateAttr";

      case OpType::kUpdateXAttr:
        return "UpdateXAttr";

      case OpType::kUpdateChunk:
        return "UpdateChunk";

      case OpType::kRmDir:
        return "RmDir";

      case OpType::kUnlink:
        return "Unlink";

      case OpType::kRename:
        return "Rename";

      case OpType::kCompactChunk:
        return "CompactChunk";

      case OpType::kSetFsQuota:
        return "SetFsQuota";

      case OpType::kGetFsQuota:
        return "GetFsQuota";

      case OpType::kFlushFsUsage:
        return "FlushFsUsage";

      case OpType::kDeleteFsQuota:
        return "DeleteFsQuota";

      case OpType::kSetDirQuota:
        return "SetDirQuota";

      case OpType::kDeleteDirQuota:
        return "DeleteDirQuota";

      case OpType::kLoadDirQuotas:
        return "LoadDirQuotas";

      case OpType::kFlushDirUsages:
        return "FlushDirUsages";

      default:
        return "UnknownOperation";
    }

    return nullptr;
  }

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
      case OpType::kUpdateChunk:
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
      case OpType::kUpdateChunk:
        return true;

      default:
        return false;
    }
  }

  virtual OpType GetOpType() const = 0;

  virtual uint32_t GetFsId() const = 0;
  virtual Ino GetIno() const = 0;
  virtual uint64_t GetTime() const { return time_ns_; }

  void SetEvent(bthread::CountdownEvent* event) { event_ = event; }
  void NotifyEvent() {
    if (event_) {
      event_->signal();
    }
  }

  virtual Status RunInBatch(TxnUPtr&, AttrType&) { return Status(pb::error::ENOT_SUPPORT, "not support."); }
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

class MountFsOperation : public Operation {
 public:
  MountFsOperation(Trace& trace, std::string fs_name, pb::mdsv2::MountPoint mount_point)
      : Operation(trace), fs_name_(fs_name), mount_point_(mount_point) {};
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
  UmountFsOperation(Trace& trace, std::string fs_name, pb::mdsv2::MountPoint mount_point)
      : Operation(trace), fs_name_(fs_name), mount_point_(mount_point) {};
  ~UmountFsOperation() override = default;

  OpType GetOpType() const override { return OpType::kUmountFs; }

  uint32_t GetFsId() const override { return 0; }
  Ino GetIno() const override { return 0; }

  Status Run(TxnUPtr& txn) override;

 private:
  std::string fs_name_;
  pb::mdsv2::MountPoint mount_point_;
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
  MkDirOperation(Trace& trace, Dentry dentry, const AttrType& attr) : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~MkDirOperation() override = default;

  OpType GetOpType() const override { return OpType::kMkDir; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrType& parent_attr) override;

 private:
  const Dentry dentry_;
  AttrType attr_;
};

class MkNodOperation : public Operation {
 public:
  MkNodOperation(Trace& trace, Dentry dentry, const AttrType& attr) : Operation(trace), dentry_(dentry), attr_(attr) {};
  ~MkNodOperation() override = default;

  OpType GetOpType() const override { return OpType::kMkNod; }

  uint32_t GetFsId() const override { return dentry_.FsId(); }
  Ino GetIno() const override { return dentry_.ParentIno(); }

  Status RunInBatch(TxnUPtr& txn, AttrType& parent_attr) override;

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

  Status RunInBatch(TxnUPtr& txn, AttrType& parent_attr) override;

 private:
  const Dentry& dentry_;
  AttrType attr_;
};

class UpdateAttrOperation : public Operation {
 public:
  UpdateAttrOperation(Trace& trace, uint64_t ino, uint32_t to_set, const AttrType& attr)
      : Operation(trace), ino_(ino), to_set_(to_set), attr_(attr) {};
  ~UpdateAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateAttr; }

  uint32_t GetFsId() const override { return attr_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  Status RunInBatch(TxnUPtr& txn, AttrType& attr) override;

 private:
  uint64_t ino_;
  const uint32_t to_set_;
  const AttrType& attr_;
};

class UpdateXAttrOperation : public Operation {
 public:
  UpdateXAttrOperation(Trace& trace, uint32_t fs_id, uint64_t ino, const Inode::XAttrMap& xattrs)
      : Operation(trace), fs_id_(fs_id), ino_(ino), xattrs_(xattrs) {};
  ~UpdateXAttrOperation() override = default;

  OpType GetOpType() const override { return OpType::kUpdateXAttr; }

  uint32_t GetFsId() const override { return fs_id_; }
  Ino GetIno() const override { return ino_; }

  Status RunInBatch(TxnUPtr& txn, AttrType& attr) override;

 private:
  uint32_t fs_id_;
  uint64_t ino_;
  const Inode::XAttrMap& xattrs_;
};

class UpdateChunkOperation : public Operation {
 public:
  UpdateChunkOperation(Trace& trace, const FsInfoType fs_info, uint64_t ino, uint64_t index,
                       std::vector<pb::mdsv2::Slice> slices)
      : Operation(trace), fs_info_(fs_info), ino_(ino), chunk_index_(index), slices_(slices) {};
  ~UpdateChunkOperation() override = default;

  struct Result : public Operation::Result {
    int64_t length_delta{0};
  };

  OpType GetOpType() const override { return OpType::kUpdateChunk; }

  uint32_t GetFsId() const override { return fs_info_.fs_id(); }
  Ino GetIno() const override { return ino_; }

  Status RunInBatch(TxnUPtr& txn, AttrType& inode) override;

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

class CompactChunkOperation : public Operation {
 public:
  CompactChunkOperation(Trace& trace, const FsInfoType& fs_info, uint64_t ino, uint64_t chunk_index)
      : Operation(trace), fs_info_(fs_info), ino_(ino), chunk_index_(chunk_index) {};
  ~CompactChunkOperation() override = default;

  struct Result : public Operation::Result {
    TrashSliceList trash_slice_list;
  };

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

 private:
  TrashSliceList GenTrashSlices(Ino ino, uint64_t file_length, const ChunkType& chunk);
  static void UpdateChunk(ChunkType& chunk, const TrashSliceList& trash_slices);
  TrashSliceList DoCompactChunk(Ino ino, uint64_t file_length, ChunkType& chunk);
  TrashSliceList CompactChunk(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length, ChunkType& chunk);
  TrashSliceList CompactChunks(TxnUPtr& txn, uint32_t fs_id, Ino ino, uint64_t file_length, Inode::ChunkMap& chunks);

  FsInfoType fs_info_;
  uint64_t ino_;
  uint64_t chunk_index_{0};
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

struct BatchOperation {
  uint32_t fs_id{0};
  uint64_t ino{0};

  // set attr/xattr/chunk
  std::vector<Operation*> setattr_operations;
  // mkdir/mknod/symlink/hardlink
  std::vector<Operation*> create_operations;
};

class OperationProcessor;
using OperationProcessorSPtr = std::shared_ptr<OperationProcessor>;

class OperationProcessor {
 public:
  OperationProcessor(KVStorageSPtr kv_storage);
  ~OperationProcessor();

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

  bool Init();
  bool Destroy();

  bool RunBatched(Operation* operation);
  Status RunAlone(Operation* operation);

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

  // persistence store
  KVStorageSPtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_STORE_OPERATION_H_
