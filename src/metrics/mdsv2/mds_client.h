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

#ifndef DINGOFS_SRC_METRICS_MDSV2_MDS_CIENT_H_
#define DINGOFS_SRC_METRICS_MDSV2_MDS_CIENT_H_

#include <bvar/bvar.h>

#include <string>

#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace mdsv2 {

struct MDSClientMetric {
  inline static const std::string prefix = "dingofs_mdsv2_client";

  // fs
  InterfaceMetric GetFsInfo;
  InterfaceMetric MountFs;
  InterfaceMetric UmountFs;
  InterfaceMetric StatFs;

  // basic
  InterfaceMetric Lookup;
  InterfaceMetric MkNod;
  InterfaceMetric MkDir;
  InterfaceMetric RmDir;
  InterfaceMetric OpenDir;
  InterfaceMetric ReadDir;
  InterfaceMetric ReleaseDir;
  InterfaceMetric Open;
  InterfaceMetric Close;
  InterfaceMetric Create;
  InterfaceMetric Release;
  InterfaceMetric Link;
  InterfaceMetric UnLink;
  InterfaceMetric Symlink;
  InterfaceMetric ReadLink;
  InterfaceMetric Rename;
  InterfaceMetric Fallocate;
  InterfaceMetric Write;

  // attr
  InterfaceMetric GetAttr;
  InterfaceMetric SetAttr;
  InterfaceMetric GetXAttr;
  InterfaceMetric SetXAttr;
  InterfaceMetric RemoveXAttr;
  InterfaceMetric ListXAttr;

  // slice
  InterfaceMetric NewSliceId;
  InterfaceMetric ReadSlice;
  InterfaceMetric WriteSlice;

  // quota
  InterfaceMetric GetFsQuota;

  // all
  InterfaceMetric allOperation;

  MDSClientMetric()
      : GetFsInfo(prefix, "getfsinfo"),
        MountFs(prefix, "mountfs"),
        UmountFs(prefix, "umountfs"),
        StatFs(prefix, "statfs"),
        Lookup(prefix, "lookup"),
        MkNod(prefix, "mknod"),
        MkDir(prefix, "mkdir"),
        RmDir(prefix, "rmdir"),
        OpenDir(prefix, "opendir"),
        ReadDir(prefix, "readdir"),
        ReleaseDir(prefix, "releasedir"),
        Open(prefix, "open"),
        Close(prefix, "close"),
        Create(prefix, "create"),
        Release(prefix, "release"),
        Link(prefix, "link"),
        UnLink(prefix, "unlink"),
        Symlink(prefix, "symlink"),
        ReadLink(prefix, "readlink"),
        Rename(prefix, "rename"),
        Fallocate(prefix, "fallocate"),
        Write(prefix, "write"),
        GetAttr(prefix, "getattr"),
        SetAttr(prefix, "setattr"),
        GetXAttr(prefix, "getxattr"),
        SetXAttr(prefix, "setxattr"),
        RemoveXAttr(prefix, "removeattr"),
        ListXAttr(prefix, "listxattr"),
        NewSliceId(prefix, "newsliceid"),
        ReadSlice(prefix, "readslice"),
        WriteSlice(prefix, "writeslice"),
        GetFsQuota(prefix, "getfsquota"),
        allOperation(prefix, "all_operation") {}

 public:
  MDSClientMetric(const MDSClientMetric&) = delete;

  MDSClientMetric& operator=(const MDSClientMetric&) = delete;

  static MDSClientMetric& GetInstance() {
    static MDSClientMetric instance;
    return instance;
  }
};

}  // namespace mdsv2
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_MDSV2_MDS_CIENT_H_
