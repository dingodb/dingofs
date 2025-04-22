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

#ifndef DINGOFS_CLIENT_VFS_WRAPPER_H_
#define DINGOFS_CLIENT_VFS_WRAPPER_H_

#include <cstdint>
#include <memory>

#include "client/vfs/vfs.h"
#include "options/client/app.h"
#include "stub/metric/metric.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSWrapper {
 public:
  VFSWrapper() = default;

  ~VFSWrapper() = default;

  Status Start(const char* argv0, const VFSConfig& vfs_conf);

  Status Stop();

  // for fuse op init, may remove in future
  void Init();

  // for fuse op destroy, may remove in future
  void Destory();

  double GetAttrTimeout(const FileType& type);

  double GetEntryTimeout(const FileType& type);

  Status Lookup(Ino parent, const std::string& name, Attr* attr);

  Status GetAttr(Ino ino, Attr* attr);

  Status SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr);

  Status ReadLink(Ino ino, std::string* link);

  Status MkNod(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, uint64_t dev, Attr* attr);

  Status Unlink(Ino parent, const std::string& name);

  /**
   * Create a symlink in parent directory
   * @param parent
   * @param name to be created
   * @param link the content of the symlink
   * @param attr output
   */
  Status Symlink(Ino parent, const std::string& name, uint32_t uid,
                 uint32_t gid, const std::string& link, Attr* attr);

  Status Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                const std::string& new_name);

  Status Link(Ino ino, Ino new_parent, const std::string& new_name, Attr* attr);

  Status Open(Ino ino, int flags, uint64_t* fh);

  Status Create(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
                uint32_t mode, int flags, uint64_t* fh, Attr* attr);

  Status Read(Ino ino, char* buf, uint64_t size, uint64_t offset, uint64_t fh,
              uint64_t* out_rsize);

  Status Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
               uint64_t fh, uint64_t* out_wsize);

  Status Flush(Ino ino, uint64_t fh);

  Status Release(Ino ino, uint64_t fh);

  Status Fsync(Ino ino, int datasync, uint64_t fh);

  Status SetXattr(Ino ino, const std::string& name, const std::string& value,
                  int flags);

  Status GetXattr(Ino ino, const std::string& name, std::string* value);

  Status ListXattr(Ino ino, std::vector<std::string>* xattrs);

  Status MkDir(Ino parent, const std::string& name, uint32_t uid, uint32_t gid,
               uint32_t mode, Attr* attr);

  Status OpenDir(Ino ino, uint64_t* fh);

  Status ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                 ReadDirHandler handler);

  Status ReleaseDir(Ino ino, uint64_t fh);

  Status RmDir(Ino parent, const std::string& name);

  Status StatFs(Ino ino, FsStat* fs_stat);

  uint64_t GetFsId();

  uint64_t GetMaxNameLength();

 private:
  std::unique_ptr<VFS> vfs_;
  std::unique_ptr<stub::metric::ClientOpMetric> client_op_metric_;
};

struct ClientOpMetricGuard {
  explicit ClientOpMetricGuard(
      std::list<dingofs::stub::metric::OpMetric*> p_metric_list)
      : metric_list(p_metric_list), start(butil::cpuwide_time_us()) {
    for (auto& metric : metric_list) {
      metric->inflightOpNum << 1;
    }
  }

  ~ClientOpMetricGuard() {
    for (auto& metric : metric_list) {
      metric->inflightOpNum << -1;
      if (op_ok) {
        metric->qpsTotal << 1;
        auto duration = butil::cpuwide_time_us() - start;
        metric->latency << duration;
        metric->latTotal << duration;
      } else {
        metric->ecount << 1;
      }
    }
  }

  void FailOp() { op_ok = false; }

  bool op_ok{true};
  std::list<dingofs::stub::metric::OpMetric*> metric_list;
  uint64_t start;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_WRAPPER_H_
