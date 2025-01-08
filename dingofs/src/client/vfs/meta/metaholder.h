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

#include <cstdint>
#include <vector>

#include "client/filesystem/error.h"  // Todo : refact error

namespace dingofs {
namespace client {
namespace vfs {

using Ino = uint64_t;

enum FileType : uint8_t {
  kDirectory = 1,
  kSymlink = 2,
  kFile = 3,  // NOTE: match to pb TYPE_S3
};

struct Attr {
  Ino ino;
  uint32_t mode;
  uint32_t nlink;
  uint32_t uid;
  uint32_t gid;
  uint64_t length;
  uint64_t rdev;
  uint64_t atime;
  uint64_t mtime;
  uint64_t ctime;
  uint32_t mtime_ns;
  uint32_t atime_ns;
  uint32_t ctime_ns;
  FileType type;
  std::vector<Ino>
      parents;  // TODO: refact, maybe use separate key for hardlink
};

struct DirEntry {
  Ino ino;
  std::string name;
  Attr attr;
};

struct FsStat {
  uint64_t max_bytes;
  uint64_t used_bytes;
  uint64_t max_inodes;
  uint64_t used_inodes;
};

// map pb chunkinfo
struct Slice {
  uint64_t id;          // slice id map to old pb chunkid
  uint64_t offset;      // offset in the file
  uint64_t length;      // length of the slice
  uint64_t compaction;  // compaction version
  bool is_zero;         // is zero slice
  uint64_t size;        // now same as length, maybe use for future or remove
};

class MetaHolder {
 public:
  MetaHolder() = default;

  virtual ~MetaHolder() = default;

  virtual ErrNo Lookup(Ino parent, const std::string& name, Attr* attr) = 0;

  // create a regular file in parent directory
  virtual ErrNo Mknod(Ino parent, const std::string& name, uint32_t mode,
                      uint64_t rdev, Attr* attr) = 0;

  // create and open a regular file
  virtual ErrNo Create(Ino parent, const std::string& name, uint32_t mode,
                       Attr* attr) = 0;

  virtual ErrNo Open(Ino ino, int flags, Attr* attr) = 0;

  virtual ErrNo Close(Ino ino) = 0;

  /**
   * Read the slices of a file meta
   * @param ino the file to be read
   * @param index the chunk index
   * @param slices output
   */
  virtual ErrNo ReadSlice(Ino ino, uint64_t index,
                          std::vector<Slice>* slices) = 0;

  virtual ErrNo NewSliceId(uint64_t* id) = 0;

  /**
   * Write the slices of a file meta
   * @param ino the file to be written
   * @param index the chunk index
   * @param slices the slices to be written
   */
  virtual ErrNo WriteSlice(Ino ino, uint64_t index,
                           const std::vector<Slice>& slices) = 0;

  virtual ErrNo Unlink(Ino parent, const std::string& name) = 0;

  virtual ErrNo Rename(Ino old_parent, const std::string& old_name,
                       Ino new_parent, const std::string& new_name) = 0;

  /**
   * Hard link a file to a new parent directory
   * @param ino the file to be linked
   * @param new_parent the new parent directory
   * @param new_name the new name of the file
   * @param attr output
   */
  virtual ErrNo Link(Ino ino, Ino new_parent, const std::string& new_name,
                     Attr* attr) = 0;

  /**
   * Create a symlink in parent directory
   * @param parent
   * @param name to be created
   * @param link the content of the symlink
   * @param attr output
   */
  virtual ErrNo Symlink(Ino parent, const std::string& name,
                        const std::string& link, Attr* attr) = 0;

  virtual ErrNo ReadLink(Ino ino, std::string* link) = 0;

  virtual ErrNo GetAttr(Ino ino, Attr* attr) = 0;

  // attr is input and output
  virtual ErrNo SetAttr(Ino ino, int set, Attr* attr) = 0;

  virtual ErrNo SetXattr(Ino ino, const std::string& name,
                         const std::string& value, int flags) = 0;

  virtual ErrNo GetXattr(Ino ino, const std::string& name,
                         std::string* value) = 0;

  virtual ErrNo ListXattr(Ino ino,
                          std::map<std::string, std::string>* xattrs) = 0;

  // create a directory in parent directory
  virtual ErrNo Mkdir(Ino parent, const std::string& name, uint32_t mode) = 0;

  virtual ErrNo Rmdir(Ino parent, const std::string& name) = 0;

  // used for v1 meta to manage cache
  virtual ErrNo Opendir(Ino ino) = 0;

  virtual ErrNo Readdir(Ino ino, bool with_attr,
                        std::vector<DirEntry>* entries) = 0;

  virtual ErrNo StatFs(Ino ino, FsStat* fs_stat) = 0;
};

}  // namespace v2
}  // namespace client
}  // namespace dingofs