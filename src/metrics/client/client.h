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

#ifndef DINGOFS_SRC_METRICS_CLIENT_CLIENT_H_
#define DINGOFS_SRC_METRICS_CLIENT_CLIENT_H_

#include <bvar/bvar.h>

#include <string>

#include "metrics/metric.h"

namespace dingofs {
namespace metrics {
namespace client {

struct ClientOpMetric {
  inline static const std::string prefix = "dingofs_fuse";

  OpMetric opLookup;
  OpMetric opOpen;
  OpMetric opCreate;
  OpMetric opMkNod;
  OpMetric opMkDir;
  OpMetric opLink;
  OpMetric opUnlink;
  OpMetric opRmDir;
  OpMetric opOpenDir;
  OpMetric opReleaseDir;
  OpMetric opReadDir;
  OpMetric opRename;
  OpMetric opGetAttr;
  OpMetric opSetAttr;
  OpMetric opGetXattr;
  OpMetric opListXattr;
  OpMetric opSymlink;
  OpMetric opReadLink;
  OpMetric opRelease;
  OpMetric opFsync;
  OpMetric opFlush;
  OpMetric opRead;
  OpMetric opWrite;
  OpMetric opAll;

  ClientOpMetric()
      : opLookup(prefix, "opLookup"),
        opOpen(prefix, "opOpen"),
        opCreate(prefix, "opCreate"),
        opMkNod(prefix, "opMknod"),
        opMkDir(prefix, "opMkdir"),
        opLink(prefix, "opLink"),
        opUnlink(prefix, "opUnlink"),
        opRmDir(prefix, "opRmdir"),
        opOpenDir(prefix, "opOpendir"),
        opReleaseDir(prefix, "opReleasedir"),
        opReadDir(prefix, "opReaddir"),
        opRename(prefix, "opRename"),
        opGetAttr(prefix, "opGetattr"),
        opSetAttr(prefix, "opSetattr"),
        opGetXattr(prefix, "opGetxattr"),
        opListXattr(prefix, "opListxattr"),
        opSymlink(prefix, "opSymlink"),
        opReadLink(prefix, "opReadlink"),
        opRelease(prefix, "opRelease"),
        opFsync(prefix, "opFsync"),
        opFlush(prefix, "opFlush"),
        opRead(prefix, "opRead"),
        opWrite(prefix, "opWrite"),
        opAll(prefix, "opAll") {}
};

struct FSMetric {
  inline static const std::string prefix = "dingofs_filesystem";

  InterfaceMetric user_write;
  InterfaceMetric user_read;
  explicit FSMetric()
      : user_write(prefix, "_user_write"), user_read(prefix, "_user_read") {}

 public:
  FSMetric(const FSMetric&) = delete;

  FSMetric& operator=(const FSMetric&) = delete;

  static FSMetric& GetInstance() {
    static FSMetric instance;
    return instance;
  }
};

}  // namespace client
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_METRICS_CLIENT_CLIENT_H_
