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

#include "client/vfs/metasystem/meta_wrapper.h"

#include <absl/strings/str_format.h>

#include <cstdint>

#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/meta_log.h"
#include "client/vfs/vfs_meta.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

Status MetaWrapper::Init() {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("init %s", s.ToString()); });

  s = target_->Init();
  return s;
}

void MetaWrapper::UnInit() {
  MetaLogGuard log_guard([&]() { return "uninit"; });
  target_->UnInit();
}

bool MetaWrapper::Dump(ContextSPtr ctx, Json::Value& value) {
  MetaLogGuard log_guard([&]() { return "dump"; });
  return target_->Dump(ctx, value);
}

bool MetaWrapper::Dump(const DumpOption& options, Json::Value& value) {
  MetaLogGuard log_guard([&]() { return "dump"; });
  return target_->Dump(options, value);
}

bool MetaWrapper::Load(ContextSPtr ctx, const Json::Value& value) {
  MetaLogGuard log_guard([&]() { return "load"; });
  return target_->Load(ctx, value);
}

Status MetaWrapper::Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                           Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("lookup (%d/%s): %s %s", parent, name, s.ToString(),
                           StrAttr(attr));
  });

  s = target_->Lookup(ctx, parent, name, attr);
  return s;
}

Status MetaWrapper::MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
                          uint32_t uid, uint32_t gid, uint32_t mode,
                          uint64_t rdev, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mknod (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });
  s = target_->MkNod(ctx, parent, name, uid, gid, mode, rdev, attr);
  return s;
}

Status MetaWrapper::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("open (%d) %o: %s [fh:%d]", ino, flags, s.ToString(),
                           fh);
  });

  s = target_->Open(ctx, ino, flags, fh);
  return s;
}

Status MetaWrapper::Create(ContextSPtr ctx, Ino parent, const std::string& name,
                           uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                           Attr* attr, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("create (%d,%s,%s:0%04o): (%d,%d) %s %s [fh:%d]",
                           parent, name, StrMode(mode), mode, uid, gid,
                           s.ToString(), StrAttr(attr), fh);
  });
  s = target_->Create(ctx, parent, name, uid, gid, mode, flags, attr, fh);
  return s;
}

Status MetaWrapper::Close(ContextSPtr ctx, Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("close (%d): %s [fh:%d]", ino, s.ToString(), fh);
  });
  s = target_->Close(ctx, ino, fh);
  return s;
}

Status MetaWrapper::Unlink(ContextSPtr ctx, Ino parent,
                           const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("unlink (%d,%s): %s", parent, name, s.ToString());
  });

  s = target_->Unlink(ctx, parent, name);
  return s;
}

Status MetaWrapper::Rename(ContextSPtr ctx, Ino old_parent,
                           const std::string& old_name, Ino new_parent,
                           const std::string& new_name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rename (%d,%s,%d,%s): %s", old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  s = target_->Rename(ctx, old_parent, old_name, new_parent, new_name);
  return s;
}

Status MetaWrapper::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                         const std::string& new_name, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("link (%d,%d,%s): %s %s", ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  s = target_->Link(ctx, ino, new_parent, new_name, attr);
  return s;
}

Status MetaWrapper::Symlink(ContextSPtr ctx, Ino parent,
                            const std::string& name, uint32_t uid, uint32_t gid,
                            const std::string& link, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("symlink (%d,%s,%s): (%d,%d) %s %s", parent, name,
                           link, uid, gid, s.ToString(), StrAttr(attr));
  });

  s = target_->Symlink(ctx, parent, name, uid, gid, link, attr);
  return s;
}

Status MetaWrapper::ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_link (%d): %s %s", ino, s.ToString(), *link);
  });

  s = target_->ReadLink(ctx, ino, link);
  return s;
}

Status MetaWrapper::GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getattr (%d): %s %d %s", ino, s.ToString(),
                           ctx->is_amend, StrAttr(attr));
  });

  s = target_->GetAttr(ctx, ino, attr);
  return s;
}

Status MetaWrapper::SetAttr(ContextSPtr ctx, Ino ino, int set,
                            const Attr& in_attr, Attr* out_attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  s = target_->SetAttr(ctx, ino, set, in_attr, out_attr);
  return s;
}

Status MetaWrapper::SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                             const std::string& value, int flags) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  s = target_->SetXattr(ctx, ino, name, value, flags);
  return s;
}

Status MetaWrapper::GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                             std::string* value) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getxattr (%d,%s): %s %d %s", ino, name,
                           s.ToString(), ctx->hit_cache, *value);
  });

  s = target_->GetXattr(ctx, ino, name, value);

  return s;
}

Status MetaWrapper::RemoveXattr(ContextSPtr ctx, Ino ino,
                                const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("remotexattr (%d,%s): %s", ino, name, s.ToString());
  });

  s = target_->RemoveXattr(ctx, ino, name);

  return s;
}

Status MetaWrapper::ListXattr(ContextSPtr ctx, Ino ino,
                              std::vector<std::string>* xattrs) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("listxattr (%d): %s %d", ino, s.ToString(),
                           xattrs->size());
  });

  s = target_->ListXattr(ctx, ino, xattrs);
  return s;
}

Status MetaWrapper::MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
                          uint32_t uid, uint32_t gid, uint32_t mode,
                          Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mkdir (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  s = target_->MkDir(ctx, parent, name, uid, gid, mode, attr);

  return s;
}

Status MetaWrapper::RmDir(ContextSPtr ctx, Ino parent,
                          const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rmdir (%d,%s): %s", parent, name, s.ToString());
  });

  s = target_->RmDir(ctx, parent, name);
  return s;
}

Status MetaWrapper::OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("opendir (%d): %d %s", ino, fh, s.ToString());
  });

  s = target_->OpenDir(ctx, ino, fh);
  return s;
}

Status MetaWrapper::ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh,
                            uint64_t offset, bool with_attr,
                            ReadDirHandler handler) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("readdir (%d): %d %d %d %s", ino, fh, offset,
                           with_attr, s.ToString());
  });

  s = target_->ReadDir(ctx, ino, fh, offset, with_attr, handler);
  return s;
}

Status MetaWrapper::ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("releasedir (%d): %d %s", ino, fh, s.ToString());
  });

  s = target_->ReleaseDir(ctx, ino, fh);
  return s;
}

Status MetaWrapper::NewSliceId(ContextSPtr ctx, Ino ino, uint64_t* id) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("new_slice_id: (%d) %d, %s", ino, *id, s.ToString());
  });

  s = target_->NewSliceId(ctx, ino, id);
  return s;
}

Status MetaWrapper::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                              uint64_t fh, std::vector<Slice>* slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_slice (%d,%d): %s [fh:%d] %d %d", ino, index,
                           s.ToString(), fh, ctx->hit_cache, slices->size());
  });

  s = target_->ReadSlice(ctx, ino, index, fh, slices);
  return s;
}

Status MetaWrapper::WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                               uint64_t fh, const std::vector<Slice>& slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("write_slice (%d,%d,%d): %s %d %d", ino, index, fh,
                           s.ToString(), ctx->hit_cache, slices.size());
  });

  s = target_->WriteSlice(ctx, ino, index, fh, slices);
  return s;
}

Status MetaWrapper::AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                    uint64_t fh,
                                    const std::vector<Slice>& slices,
                                    DoneClosure done) {
  uint64_t start_us = butil::cpuwide_time_us();
  uint64_t slice_count = slices.size();

  auto wrapped_done = [start_us, slice_count, ctx = std::move(ctx), ino, index,
                       fh, done = std::move(done)](const Status& status) {
    Status s;
    MetaLogGuard log_guard(start_us, [&]() {
      return absl::StrFormat("async_write_slice (%d,%d,%d): %s %d %d", ino,
                             index, fh, s.ToString(), ctx->hit_cache,
                             slice_count);
    });

    done(status);
  };

  return target_->AsyncWriteSlice(ctx, ino, index, fh, slices,
                                  std::move(wrapped_done));
}

Status MetaWrapper::Write(ContextSPtr ctx, Ino ino, uint64_t offset,
                          uint64_t size, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("write (%d,%d,%d): [fh:%d] %s %d", ino, offset, size,
                           fh, s.ToString(), ctx->hit_cache);
  });

  s = target_->Write(ctx, ino, offset, size, fh);

  return s;
}

Status MetaWrapper::StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  s = target_->StatFs(ctx, ino, fs_stat);
  return s;
}

Status MetaWrapper::GetFsInfo(ContextSPtr ctx, FsInfo* fs_info) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("get_fsinfo %s", s.ToString()); });

  s = target_->GetFsInfo(ctx, fs_info);
  return s;
}

bool MetaWrapper::GetDescription(Json::Value& value) {
  MetaLogGuard log_guard([&]() { return "get client id description"; });
  return target_->GetDescription(value);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
