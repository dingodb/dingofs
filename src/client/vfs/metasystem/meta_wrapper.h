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

#ifndef DINGODB_CLIENT_VFS_META_WRAPPER_H
#define DINGODB_CLIENT_VFS_META_WRAPPER_H

#include <glog/logging.h>

#include <cstdint>
#include <string>
#include <vector>

#include "client/vfs/common/client_id.h"
#include "client/vfs/compaction/compactor.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs.h"
#include "common/metrics/client/vfs/slice_metric.h"
#include "common/trace/context.h"
#include "json/value.h"

namespace dingofs {
namespace client {
namespace vfs {

class MetaWrapper {
 public:
  MetaWrapper(const VFSConfig& vfs_conf, ClientId& client_id,
              TraceManager& trace_manager, Compactor& compactor);
  ~MetaWrapper() = default;

  Status Init(bool upgrade) {
    CHECK(target_ != nullptr) << "meta system is null";
    return target_->Init(upgrade);
  }

  void Stop(bool upgrade) { target_->Stop(upgrade); }

  bool Dump(ContextSPtr ctx, Json::Value& value) {
    return target_->Dump(ctx, value);
  }

  bool Dump(const DumpOption& options, Json::Value& value) {
    return target_->Dump(options, value);
  }

  bool Load(ContextSPtr ctx, const Json::Value& value) {
    return target_->Load(ctx, value);
  }

  Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                Attr* attr) {
    return target_->Lookup(ctx, parent, name, attr);
  }

  Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
               Attr* attr) {
    return target_->MkNod(ctx, parent, name, uid, gid, mode, rdev, attr);
  }

  Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) {
    return target_->Open(ctx, ino, flags, fh);
  }

  Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                Attr* attr, uint64_t fh) {
    return target_->Create(ctx, parent, name, uid, gid, mode, flags, attr, fh);
  }

  Status Flush(ContextSPtr ctx, Ino ino, uint64_t fh);

  Status Close(ContextSPtr ctx, Ino ino, uint64_t fh) {
    return target_->Close(ctx, ino, fh);
  }

  Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) {
    return target_->MkDir(ctx, parent, name, uid, gid, mode, attr);
  }

  Status RmDir(ContextSPtr ctx, Ino parent, const std::string& name) {
    return target_->RmDir(ctx, parent, name);
  }

  Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh, bool& need_cache) {
    return target_->OpenDir(ctx, ino, fh, need_cache);
  }

  Status ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                 bool with_attr, ReadDirHandler handler) {
    return target_->ReadDir(ctx, ino, fh, offset, with_attr, handler);
  }

  Status ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
    return target_->ReleaseDir(ctx, ino, fh);
  }

  Status Link(ContextSPtr ctx, Ino ino, Ino new_parent,
              const std::string& new_name, Attr* attr) {
    return target_->Link(ctx, ino, new_parent, new_name, attr);
  }

  Status Unlink(ContextSPtr ctx, Ino parent, const std::string& name) {
    return target_->Unlink(ctx, parent, name);
  }

  Status Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                 uint32_t uid, uint32_t gid, const std::string& link,
                 Attr* attr) {
    return target_->Symlink(ctx, parent, name, uid, gid, link, attr);
  }

  Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
    return target_->ReadLink(ctx, ino, link);
  }

  Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) {
    return target_->GetAttr(ctx, ino, attr);
  }

  Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) {
    return target_->SetAttr(ctx, ino, set, in_attr, out_attr);
  }

  Status GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  std::string* value) {
    return target_->GetXattr(ctx, ino, name, value);
  }

  Status SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  const std::string& value, int flags) {
    return target_->SetXattr(ctx, ino, name, value, flags);
  }

  Status RemoveXattr(ContextSPtr ctx, Ino ino, const std::string& name) {
    return target_->RemoveXattr(ctx, ino, name);
  }

  Status ListXattr(ContextSPtr ctx, Ino ino, std::vector<std::string>* xattrs) {
    return target_->ListXattr(ctx, ino, xattrs);
  }

  Status Rename(ContextSPtr ctx, Ino old_parent, const std::string& old_name,
                Ino new_parent, const std::string& new_name) {
    return target_->Rename(ctx, old_parent, old_name, new_parent, new_name);
  }

  Status ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                   std::vector<Slice>* slices, uint64_t& version);

  Status NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id);

  Status WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                    const std::vector<Slice>& slices);

  Status AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                         const std::vector<Slice>& slices, DoneClosure done);

  Status Write(ContextSPtr ctx, Ino ino, uint64_t offset, uint64_t size,
               uint64_t fh) {
    return target_->Write(ctx, ino, offset, size, fh);
  }

  Status ManualCompact(ContextSPtr ctx, Ino ino, uint32_t chunk_index) {
    return target_->ManualCompact(ctx, ino, chunk_index);
  }

  Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) {
    return target_->StatFs(ctx, ino, fs_stat);
  }

  Status GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) {
    return target_->GetFsInfo(ctx, fs_info);
  }

  bool GetDescription(Json::Value& value) {
    return target_->GetDescription(value);
  }

 private:
  MetaSystemUPtr target_;
  std::unique_ptr<metrics::client::SliceMetric> slice_metric_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_META_WRAPPER_H