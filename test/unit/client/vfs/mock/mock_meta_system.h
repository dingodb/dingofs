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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_META_SYSTEM_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_META_SYSTEM_H_

#include <gmock/gmock.h>

#include "client/vfs/metasystem/meta_system.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

class MockMetaSystem : public MetaSystem {
 public:
  MOCK_METHOD(Status, Init, (bool skip_mount), (override));
  MOCK_METHOD(void, Stop, (bool skip_unmount), (override));
  MOCK_METHOD(bool, Dump, (ContextSPtr ctx, Json::Value& value), (override));
  MOCK_METHOD(bool, Dump,
              (const DumpOption& options, Json::Value& value), (override));
  MOCK_METHOD(bool, Load,
              (ContextSPtr ctx, const Json::Value& value), (override));
  MOCK_METHOD(Status, Lookup,
              (ContextSPtr ctx, Ino parent, const std::string& name,
               Attr* attr),
              (override));
  MOCK_METHOD(Status, MkNod,
              (ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, uint64_t rdev,
               Attr* attr),
              (override));
  MOCK_METHOD(Status, Open, (ContextSPtr ctx, Ino ino, int flags, uint64_t fh),
              (override));
  MOCK_METHOD(Status, Create,
              (ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, int flags, Attr* attr,
               uint64_t fh),
              (override));
  MOCK_METHOD(Status, Flush, (ContextSPtr ctx, Ino ino, uint64_t fh),
              (override));
  MOCK_METHOD(Status, Close, (ContextSPtr ctx, Ino ino, uint64_t fh),
              (override));
  MOCK_METHOD(Status, ReadSlice,
              (ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
               std::vector<Slice>* slices, uint64_t& version),
              (override));
  MOCK_METHOD(Status, NewSliceId,
              (ContextSPtr ctx, Ino ino, uint64_t* id), (override));
  MOCK_METHOD(Status, WriteSlice,
              (ContextSPtr ctx, Ino ino, uint64_t index, uint64_t fh,
               const std::vector<Slice>& slices),
              (override));
  MOCK_METHOD(Status, Write,
              (ContextSPtr ctx, Ino ino, const char* buf, uint64_t offset,
               uint64_t size, uint64_t fh),
              (override));
  MOCK_METHOD(Status, Link,
              (ContextSPtr ctx, Ino ino, Ino new_parent,
               const std::string& new_name, Attr* attr),
              (override));
  MOCK_METHOD(Status, Unlink,
              (ContextSPtr ctx, Ino parent, const std::string& name),
              (override));
  MOCK_METHOD(Status, Symlink,
              (ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, const std::string& link, Attr* attr),
              (override));
  MOCK_METHOD(Status, ReadLink, (ContextSPtr ctx, Ino ino, std::string* link),
              (override));
  MOCK_METHOD(Status, GetAttr, (ContextSPtr ctx, Ino ino, Attr* attr),
              (override));
  MOCK_METHOD(Status, SetAttr,
              (ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
               Attr* out_attr),
              (override));
  MOCK_METHOD(Status, SetXattr,
              (ContextSPtr ctx, Ino ino, const std::string& name,
               const std::string& value, int flags),
              (override));
  MOCK_METHOD(Status, GetXattr,
              (ContextSPtr ctx, Ino ino, const std::string& name,
               std::string* value),
              (override));
  MOCK_METHOD(Status, RemoveXattr,
              (ContextSPtr ctx, Ino ino, const std::string& name), (override));
  MOCK_METHOD(Status, ListXattr,
              (ContextSPtr ctx, Ino ino, std::vector<std::string>* xattrs),
              (override));
  MOCK_METHOD(Status, MkDir,
              (ContextSPtr ctx, Ino parent, const std::string& name,
               uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr),
              (override));
  MOCK_METHOD(Status, RmDir,
              (ContextSPtr ctx, Ino parent, const std::string& name),
              (override));
  MOCK_METHOD(Status, OpenDir,
              (ContextSPtr ctx, Ino ino, uint64_t fh, bool& need_cache),
              (override));
  MOCK_METHOD(Status, ReadDir,
              (ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
               bool with_attr, ReadDirHandler handler),
              (override));
  MOCK_METHOD(Status, ReleaseDir, (ContextSPtr ctx, Ino ino, uint64_t fh),
              (override));
  MOCK_METHOD(Status, StatFs, (ContextSPtr ctx, Ino ino, FsStat* fs_stat),
              (override));
  MOCK_METHOD(Status, Rename,
              (ContextSPtr ctx, Ino old_parent, const std::string& old_name,
               Ino new_parent, const std::string& new_name),
              (override));
  MOCK_METHOD(Status, GetFsInfo, (ContextSPtr ctx, FsInfo* fs_info),
              (override));
  MOCK_METHOD(bool, GetDescription, (Json::Value& value), (override));
  MOCK_METHOD(bool, GetSummary, (Json::Value& value), (override));
};

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_META_SYSTEM_H_
