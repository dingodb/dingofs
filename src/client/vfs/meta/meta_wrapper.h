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

#include <json/value.h>

#include <cstdint>
#include <string>
#include <vector>

#include "client/vfs/meta/meta_system.h"
#include "trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

class MetaWrapper : public MetaSystem {
 public:
  MetaWrapper(MetaSystemUPtr meta_system) : target_(std::move(meta_system)){};

  ~MetaWrapper() override = default;

  Status Init() override;

  void UnInit() override;

  bool Dump(ContextSPtr ctx, Json::Value& value) override;

  bool Dump(const DumpOption& options, Json::Value& value) override;

  bool Load(ContextSPtr ctx, const Json::Value& value) override;

  Status Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                Attr* attr) override;

  Status MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
               Attr* attr) override;

  Status Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) override;

  Status Create(ContextSPtr ctx, Ino parent, const std::string& name,
                uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                Attr* attr, uint64_t fh) override;

  Status Close(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) override;

  Status RmDir(ContextSPtr ctx, Ino parent, const std::string& name) override;

  Status OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                 bool with_attr, ReadDirHandler handler) override;

  Status ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) override;

  Status Link(ContextSPtr ctx, Ino ino, Ino new_parent,
              const std::string& new_name, Attr* attr) override;

  Status Unlink(ContextSPtr ctx, Ino parent, const std::string& name) override;

  Status Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                 uint32_t uid, uint32_t gid, const std::string& link,
                 Attr* att) override;

  Status ReadLink(ContextSPtr ctx, Ino ino, std::string* link) override;

  Status GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) override;

  Status SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                 Attr* out_attr) override;

  Status GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  std::string* value) override;

  Status SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                  const std::string& value, int flags) override;

  Status RemoveXattr(ContextSPtr ctx, Ino ino,
                     const std::string& name) override;

  Status ListXattr(ContextSPtr ctx, Ino ino,
                   std::vector<std::string>* xattrs) override;

  Status Rename(ContextSPtr ctx, Ino old_parent, const std::string& old_name,
                Ino new_parent, const std::string& new_name) override;

  Status ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                   std::vector<Slice>* slices) override;

  Status NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) override;

  Status WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                    const std::vector<Slice>& slices) override;

  Status AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
                         const std::vector<Slice>& slices,
                         DoneClosure done) override;

  Status Write(ContextSPtr ctx, Ino ino, uint64_t offset, uint64_t size,
               uint64_t fh) override;

  Status StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) override;

  Status GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) override;

  bool GetDescription(Json::Value& value) override;

 private:
  MetaSystemUPtr target_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_META_WRAPPER_H