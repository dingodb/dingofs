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

#ifndef DINGOFS_CLIENT_VFS_IMPL_H_
#define DINGOFS_CLIENT_VFS_IMPL_H_

#include <brpc/server.h>

#include <cstdint>
#include <memory>

#include "client/vfs/common/client_id.h"
#include "client/vfs/data_buffer.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/service/compact_service.h"
#include "client/vfs/service/fuse_stat_service.h"
#include "client/vfs/service/inode_blocks_service.h"
#include "client/vfs/vfs.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSImpl : public VFS {
 public:
  VFSImpl(const ClientId& client_id) : client_id_(client_id) {};

  ~VFSImpl() override = default;

  Status Start(const VFSConfig& vfs_conf, bool upgrade) override;

  Status Stop(bool upgrade) override;

  bool Dump(ContextSPtr ctx, Json::Value& value) override;

  bool Load(ContextSPtr ctx, const Json::Value& value) override;

  double GetAttrTimeout(const FileType& type) override;

  double GetEntryTimeout(const FileType& type) override;

  Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                Attr* attr) override;

  Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) override;

  Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;

  Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) override;

  Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t dev,
               Attr* attr) override;

  Status Unlink(ContextSPtr ctx, Ino parent, const std::string& name) override;

  Status Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                 uint32_t uid, uint32_t gid, const std::string& link,
                 Attr* attr) override;

  Status Rename(ContextSPtr ctx, Ino old_parent, const std::string& old_name,
                Ino new_parent, const std::string& new_name) override;

  Status Link(ContextSPtr ctx, Ino ino, Ino new_parent,
              const std::string& new_name, Attr* attr) override;

  Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t* fh) override;

  Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                uint64_t* fh, Attr* attr) override;

  Status Read(ContextSPtr ctx, Ino ino, DataBuffer* data_buffer, uint64_t size,
              uint64_t offset, uint64_t fh, uint64_t* out_rsize) override;

  Status Write(ContextSPtr ctx, Ino ino, const char* buf, uint64_t size,
               uint64_t offset, uint64_t fh, uint64_t* out_wsize) override;

  Status Flush(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status Release(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status Fsync(ContextSPtr ctx, Ino ino, int datasync, uint64_t fh) override;

  Status SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  const std::string& value, int flags) override;

  Status GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  std::string* value) override;

  Status RemoveXattr(ContextSPtr ctx, Ino ino,
                     const std::string& name) override;

  Status ListXattr(ContextSPtr ctx, Ino ino,
                   std::vector<std::string>* xattrs) override;

  Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) override;

  Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t* fh) override;

  Status ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                 bool with_attr, ReadDirHandler handler) override;

  Status ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status RmDir(ContextSPtr ctx, Ino parent, const std::string& name) override;

  Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) override;

  Status Ioctl(ContextSPtr ctx, Ino ino, uint32_t uid, unsigned int cmd,
               unsigned flags, const void* in_buf, size_t in_bufsz,
               char* out_buf, size_t out_bufsz) override;

  uint64_t GetFsId() override;

  uint64_t GetMaxNameLength() override;

  TraceManager& GetTraceManager() override {
    return vfs_hub_->GetTraceManager();
  }
  blockaccess::BlockAccessOptions GetBlockAccesserOptions() override {
    return vfs_hub_->GetBlockAccesserOptions();
  }

 private:
  Status StartBrpcServer();

  Handle* NewHandle(uint64_t fh, Ino ino, int flags, IFileUPtr file);

  const ClientId client_id_;

  std::unique_ptr<VFSHub> vfs_hub_;
  MetaSystem* meta_system_;
  HandleManager* handle_manager_;

  brpc::Server brpc_server_;
  InodeBlocksServiceImpl inode_blocks_service_;
  CompactServiceImpl compact_service_;
  FuseStatServiceImpl fuse_stat_service_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_IMPL_H_
