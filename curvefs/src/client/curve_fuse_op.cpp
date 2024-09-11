/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/curve_fuse_op.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/access_log.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/xattr.h"
#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/warmup/warmup_manager.h"
#include "curvefs/src/common/dynamic_vlog.h"
#include "curvefs/src/common/metric_utils.h"
#include "src/common/configuration.h"
#include "src/common/gflags_helper.h"

using ::curve::common::Configuration;
using ::curvefs::client::CURVEFS_ERROR;
using ::curvefs::client::FuseClient;
using ::curvefs::client::FuseS3Client;
using ::curvefs::client::FuseVolumeClient;
using ::curvefs::client::blockcache::InitBlockCacheLog;
using ::curvefs::client::common::FuseClientOption;
using ::curvefs::client::filesystem::AccessLogGuard;
using ::curvefs::client::filesystem::AttrOut;
using ::curvefs::client::filesystem::EntryOut;
using ::curvefs::client::filesystem::FileOut;
using ::curvefs::client::filesystem::InitAccessLog;
using ::curvefs::client::filesystem::IsWarmupXAttr;
using ::curvefs::client::filesystem::StrAttr;
using ::curvefs::client::filesystem::StrEntry;
using ::curvefs::client::filesystem::StrFormat;
using ::curvefs::client::filesystem::StrMode;
using ::curvefs::client::metric::ClientOpMetric;
using ::curvefs::client::rpcclient::MDSBaseClient;
using ::curvefs::client::rpcclient::MdsClientImpl;
using ::curvefs::common::InflightGuard;
using ::curvefs::common::LatencyListUpdater;

using ::curvefs::common::FLAGS_vlog_level;

static FuseClient* g_client_instance = nullptr;
static FuseClientOption* g_fuse_client_option = nullptr;
static ClientOpMetric* g_clientOpMetric = nullptr;

namespace {

void EnableSplice(struct fuse_conn_info* conn) {
  if (!g_fuse_client_option->enableFuseSplice) {
    LOG(INFO) << "Fuse splice is disabled";
    return;
  }

  if (conn->capable & FUSE_CAP_SPLICE_MOVE) {
    conn->want |= FUSE_CAP_SPLICE_MOVE;
    LOG(INFO) << "FUSE_CAP_SPLICE_MOVE enabled";
  }
  if (conn->capable & FUSE_CAP_SPLICE_READ) {
    conn->want |= FUSE_CAP_SPLICE_READ;
    LOG(INFO) << "FUSE_CAP_SPLICE_READ enabled";
  }
  if (conn->capable & FUSE_CAP_SPLICE_WRITE) {
    conn->want |= FUSE_CAP_SPLICE_WRITE;
    LOG(INFO) << "FUSE_CAP_SPLICE_WRITE enabled";
  }
}

int GetFsInfo(const char* fs_name, FsInfo* fs_info) {
  MdsClientImpl mds_client;
  MDSBaseClient mds_base;
  mds_client.Init(g_fuse_client_option->mdsOpt, &mds_base);

  std::string fn = (fs_name == nullptr) ? "" : fs_name;
  FSStatusCode ret = mds_client.GetFsInfo(fn, fs_info);
  if (ret != FSStatusCode::OK) {
    if (FSStatusCode::NOT_FOUND == ret) {
      LOG(ERROR) << "The fsName not exist, fsName = " << fs_name;
      return -1;
    } else {
      LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                 << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                 << ", fsName = " << fs_name;
      return -1;
    }
  }
  return 0;
}

}  // namespace

int InitLog(const char* conf_path, const char* argv0) {
  Configuration conf;
  conf.SetConfigPath(conf_path);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "LoadConfig failed, confPath = " << conf_path;
    return -1;
  }

  // set log dir
  if (FLAGS_log_dir.empty()) {
    if (!conf.GetStringValue("client.common.logDir", &FLAGS_log_dir)) {
      LOG(WARNING) << "no client.common.logDir in " << conf_path
                   << ", will log to /tmp";
    }
  }

  curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
  dummy.Load(&conf, "v", "client.loglevel", &FLAGS_v);
  FLAGS_vlog_level = FLAGS_v;

  // initialize logging module
  google::InitGoogleLogging(argv0);

  bool succ = InitAccessLog(FLAGS_log_dir) && InitBlockCacheLog(FLAGS_log_dir);
  if (!succ) {
    return -1;
  }
  return 0;
}

int InitFuseClient(const struct MountOption* mount_option) {
  g_clientOpMetric = new ClientOpMetric();

  Configuration conf;
  conf.SetConfigPath(mount_option->conf);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "LoadConfig failed, confPath = " << mount_option->conf;
    return -1;
  }
  if (mount_option->mdsAddr)
    conf.SetStringValue("mdsOpt.rpcRetryOpt.addrs", mount_option->mdsAddr);

  conf.PrintConfig();

  g_fuse_client_option = new FuseClientOption();
  curvefs::client::common::InitFuseClientOption(&conf, g_fuse_client_option);

  auto fs_info = std::make_shared<FsInfo>();
  if (GetFsInfo(mount_option->fsName, fs_info.get()) != 0) {
    return -1;
  }

  std::string fs_type_str =
      (mount_option->fsType == nullptr) ? "" : mount_option->fsType;
  std::string fs_type_mds;
  if (fs_info->fstype() == FSType::TYPE_S3) {
    fs_type_mds = "s3";
  } else if (fs_info->fstype() == FSType::TYPE_VOLUME) {
    fs_type_mds = "volume";
  }

  if (fs_type_mds != fs_type_str) {
    LOG(ERROR) << "The parameter fstype is inconsistent with mds!";
    return -1;
  } else if (fs_type_str == "s3") {
    g_client_instance = new FuseS3Client();
  } else if (fs_type_str == "volume") {
    g_client_instance = new FuseVolumeClient();
  } else {
    LOG(ERROR) << "unknown fstype! fstype is " << fs_type_str;
    return -1;
  }

  g_client_instance->SetFsInfo(fs_info);
  CURVEFS_ERROR ret = g_client_instance->Init(*g_fuse_client_option);
  if (ret != CURVEFS_ERROR::OK) {
    return -1;
  }
  ret = g_client_instance->Run();
  if (ret != CURVEFS_ERROR::OK) {
    return -1;
  }

  ret = g_client_instance->SetMountStatus(mount_option);
  if (ret != CURVEFS_ERROR::OK) {
    return -1;
  }

  return 0;
}

void UnInitFuseClient() {
  if (g_client_instance) {
    g_client_instance->Fini();
    g_client_instance->UnInit();
  }
  delete g_client_instance;
  delete g_fuse_client_option;
  delete g_clientOpMetric;
}

int AddWarmupTask(curvefs::client::common::WarmupType type, fuse_ino_t key,
                  const std::string& path,
                  curvefs::client::common::WarmupStorageType storage_type) {
  int ret = 0;
  bool result = true;
  switch (type) {
    case curvefs::client::common::WarmupType::kWarmupTypeList:
      result = g_client_instance->PutWarmFilelistTask(key, storage_type);
      break;
    case curvefs::client::common::WarmupType::kWarmupTypeSingle:
      result = g_client_instance->PutWarmFileTask(key, path, storage_type);
      break;
    default:
      // not support add warmup type (warmup single file/dir or filelist)
      LOG(ERROR) << "not support warmup type, only support single/list";
      ret = EOPNOTSUPP;
  }
  if (!result) {
    ret = ERANGE;
  }
  return ret;
}

void QueryWarmupTask(fuse_ino_t key, std::string* data) {
  curvefs::client::warmup::WarmupProgress progress;
  bool ret = g_client_instance->GetWarmupProgress(key, &progress);
  if (!ret) {
    *data = "finished";
  } else {
    *data = std::to_string(progress.GetFinished()) + "/" +
            std::to_string(progress.GetTotal());
  }
  VLOG(9) << "Warmup [" << key << "]" << *data;
}

int Warmup(fuse_ino_t key, const std::string& name, const std::string& value) {
  // warmup
  if (g_client_instance->GetFsInfo()->fstype() != FSType::TYPE_S3) {
    LOG(ERROR) << "warmup only support s3";
    return EOPNOTSUPP;
  }

  std::vector<std::string> op_type_path;
  curve::common::SplitString(value, "\n", &op_type_path);
  if (op_type_path.size() != curvefs::client::common::kWarmupOpNum) {
    LOG(ERROR) << name << " has invalid xattr value " << value;
    return ERANGE;
  }
  auto storage_type =
      curvefs::client::common::GetWarmupStorageType(op_type_path[3]);
  if (storage_type ==
      curvefs::client::common::WarmupStorageType::kWarmupStorageTypeUnknown) {
    LOG(ERROR) << name << " not support storage type: " << value;
    return ERANGE;
  }
  int ret = 0;
  switch (curvefs::client::common::GetWarmupOpType(op_type_path[0])) {
    case curvefs::client::common::WarmupOpType::kWarmupOpAdd:
      ret =
          AddWarmupTask(curvefs::client::common::GetWarmupType(op_type_path[1]),
                        key, op_type_path[2], storage_type);
      if (ret != 0) {
        LOG(ERROR) << name << " has invalid xattr value " << value;
      }
      break;
    default:
      LOG(ERROR) << name << " has invalid xattr value " << value;
      ret = ERANGE;
  }
  return ret;
}

namespace {

struct CodeGuard {
  explicit CodeGuard(CURVEFS_ERROR* rc, bvar::Adder<uint64_t>* ecount)
      : rc(rc), ecount(ecount) {}

  ~CodeGuard() {
    if (*rc != CURVEFS_ERROR::OK) {
      (*ecount) << 1;
    }
  }

  CURVEFS_ERROR* rc;
  bvar::Adder<uint64_t>* ecount;
};

FuseClient* Client() { return g_client_instance; }

void TriggerWarmup(fuse_req_t req, fuse_ino_t ino, const char* name,
                   const char* value, size_t size) {
  auto fs = Client()->GetFileSystem();

  std::string xattr(value, size);
  int code = Warmup(ino, name, xattr);
  fuse_reply_err(req, code);
}

void QueryWarmup(fuse_req_t req, fuse_ino_t ino, size_t size) {
  auto fs = Client()->GetFileSystem();

  std::string data;
  QueryWarmupTask(ino, &data);
  if (size == 0) {
    return fs->ReplyXattr(req, data.length());
  }
  return fs->ReplyBuffer(req, data.data(), data.length());
}

void ReadThrottleAdd(size_t size) { Client()->Add(true, size); }
void WriteThrottleAdd(size_t size) { Client()->Add(false, size); }

#define METRIC_GUARD(REQUEST)                                             \
  InflightGuard iGuard(&g_clientOpMetric->op##REQUEST.inflightOpNum);     \
  CodeGuard cGuard(&rc, &g_clientOpMetric->op##REQUEST.ecount);           \
  LatencyListUpdater listupdater({&g_clientOpMetric->op##REQUEST.latency, \
                                  &g_clientOpMetric->opAll.latency});
}  // namespace

void FuseOpInit(void* userdata, struct fuse_conn_info* conn) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  AccessLogGuard log([&]() { return StrFormat("init : %s", StrErr(rc)); });

  rc = client->FuseOpInit(userdata, conn);
  if (rc != CURVEFS_ERROR::OK) {
    LOG(FATAL) << "FuseOpInit() failed, retCode = " << rc;
  } else {
    EnableSplice(conn);
    LOG(INFO) << "FuseOpInit() success, retCode = " << rc;
  }
}

void FuseOpDestroy(void* userdata) {
  auto* client = Client();
  AccessLogGuard log([&]() { return StrFormat("destory : OK"); });
  client->FuseOpDestroy(userdata);
}

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  CURVEFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Lookup);
  AccessLogGuard log([&]() {
    return StrFormat("lookup (%d,%s): %s%s", parent, name, StrErr(rc),
                     StrEntry(entry_out));
  });

  rc = client->FuseOpLookup(req, parent, name, &entry_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  LOG(WARNING) << "GetAttr is called, ino = " << ino << ", rc: " << StrErr(rc);
  AttrOut attr_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(GetAttr);
  AccessLogGuard log([&]() {
    return StrFormat("getattr (%d): %s%s", ino, StrErr(rc), StrAttr(attr_out));
  });

  rc = client->FuseOpGetAttr(req, ino, fi, &attr_out);
  LOG(WARNING) << "GetAttr is called, ino = " << ino
               << ", after req rc: " << StrErr(rc);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyAttr(req, &attr_out);
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                   int to_set, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  AttrOut attr_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(SetAttr);
  AccessLogGuard log([&]() {
    return StrFormat("setattr (%d,0x%X): %s%s", ino, to_set, StrErr(rc),
                     StrAttr(attr_out));
  });

  rc = client->FuseOpSetAttr(req, ino, attr, to_set, fi, &attr_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyAttr(req, &attr_out);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
  CURVEFS_ERROR rc;
  std::string link;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(ReadLink);
  AccessLogGuard log([&]() {
    return StrFormat("readlink (%d): %s %s", ino, StrErr(rc), link.c_str());
  });

  rc = client->FuseOpReadLink(req, ino, &link);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyReadlink(req, link);
}

void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode, dev_t rdev) {
  CURVEFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(MkNod);
  AccessLogGuard log([&]() {
    return StrFormat("mknod (%d,%s,%s:0%04o): %s%s", parent, name,
                     StrMode(mode), mode, StrErr(rc), StrEntry(entry_out));
  });

  rc = client->FuseOpMkNod(req, parent, name, mode, rdev, &entry_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode) {
  CURVEFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(MkDir);
  AccessLogGuard log([&]() {
    return StrFormat("mkdir (%d,%s,%s:0%04o): %s%s", parent, name,
                     StrMode(mode), mode, StrErr(rc), StrEntry(entry_out));
  });

  rc = client->FuseOpMkDir(req, parent, name, mode, &entry_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Unlink);
  AccessLogGuard log([&]() {
    return StrFormat("unlink (%d,%s): %s", parent, name, StrErr(rc));
  });

  rc = client->FuseOpUnlink(req, parent, name);
  return fs->ReplyError(req, rc);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(RmDir);
  AccessLogGuard log([&]() {
    return StrFormat("rmdir (%d,%s): %s", parent, name, StrErr(rc));
  });

  rc = client->FuseOpRmDir(req, parent, name);
  return fs->ReplyError(req, rc);
}

void FuseOpSymlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                   const char* name) {
  CURVEFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Symlink);
  AccessLogGuard log([&]() {
    return StrFormat("symlink (%d,%s,%s): %s%s", parent, name, link, StrErr(rc),
                     StrEntry(entry_out));
  });

  rc = client->FuseOpSymlink(req, link, parent, name, &entry_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char* name,
                  fuse_ino_t newparent, const char* newname,
                  unsigned int flags) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Rename);
  AccessLogGuard log([&]() {
    return StrFormat("rename (%d,%s,%d,%s,%d): %s", parent, name, newparent,
                     newname, flags, StrErr(rc));
  });

  rc = client->FuseOpRename(req, parent, name, newparent, newname, flags);
  return fs->ReplyError(req, rc);
}

void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                const char* newname) {
  CURVEFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Link);
  AccessLogGuard log([&]() {
    return StrFormat("link (%d,%d,%s): %s%s", ino, newparent, newname,
                     StrErr(rc), StrEntry(entry_out));
  });

  rc = client->FuseOpLink(req, ino, newparent, newname, &entry_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  FileOut file_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Open);
  AccessLogGuard log([&]() {
    return StrFormat("open (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
  });

  rc = client->FuseOpOpen(req, ino, fi, &file_out);
  if (rc != CURVEFS_ERROR::OK) {
    fs->ReplyError(req, rc);
    return;
  }
  return fs->ReplyOpen(req, &file_out);
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  size_t r_size = 0;
  std::unique_ptr<char[]> buffer(new char[size]);
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Read);
  AccessLogGuard log([&]() {
    return StrFormat("read (%d,%d,%d,%d): %s (%d)", ino, size, off, fi->fh,
                     StrErr(rc), r_size);
  });

  ReadThrottleAdd(size);
  rc = client->FuseOpRead(req, ino, size, off, fi, buffer.get(), &r_size);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(r_size);
  bufvec.buf[0].mem = buffer.get();
  return fs->ReplyData(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
                 off_t off, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  FileOut file_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Read);
  AccessLogGuard log([&]() {
    return StrFormat("write (%d,%d,%d,%d): %s (%d), content:(%s)", ino, size,
                     off, fi->fh, StrErr(rc), file_out.nwritten,
                     std::string(buf, size));
  });

  WriteThrottleAdd(size);
  rc = client->FuseOpWrite(req, ino, buf, size, off, fi, &file_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyWrite(req, &file_out);
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Flush);
  AccessLogGuard log([&]() {
    return StrFormat("flush (%d,%d): %s", ino, fi->fh, StrErr(rc));
  });

  rc = client->FuseOpFlush(req, ino, fi);
  return fs->ReplyError(req, rc);
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Release);
  AccessLogGuard log([&]() {
    return StrFormat("release (%d,%d): %s", ino, fi->fh, StrErr(rc));
  });

  rc = client->FuseOpRelease(req, ino, fi);
  return fs->ReplyError(req, rc);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                 struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Fsync);
  AccessLogGuard log([&]() {
    return StrFormat("fsync (%d,%d): %s", ino, datasync, StrErr(rc));
  });

  rc = client->FuseOpFsync(req, ino, datasync, fi);
  return fs->ReplyError(req, rc);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(OpenDir);
  AccessLogGuard log([&]() {
    return StrFormat("opendir (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
  });

  rc = client->FuseOpOpenDir(req, ino, fi);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyOpen(req, fi);
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  char* buffer;
  size_t r_size;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(ReadDir);
  AccessLogGuard log([&]() {
    return StrFormat("readdir (%d,%d,%d): %s (%d)", ino, size, off, StrErr(rc),
                     r_size);
  });

  rc = client->FuseOpReadDir(req, ino, size, off, fi, &buffer, &r_size, false);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyBuffer(req, buffer, r_size);
}

void FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  char* buffer;
  size_t r_size;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(ReadDir);
  AccessLogGuard log([&]() {
    return StrFormat("readdirplus (%d,%d,%d): %s (%d)", ino, size, off,
                     StrErr(rc), r_size);
  });

  rc = client->FuseOpReadDir(req, ino, size, off, fi, &buffer, &r_size, true);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }

  return fs->ReplyBuffer(req, buffer, r_size);
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(ReleaseDir);
  AccessLogGuard log([&]() {
    return StrFormat("releasedir (%d,%d): %s", ino, fi->fh, StrErr(rc));
  });

  rc = client->FuseOpReleaseDir(req, ino, fi);
  return fs->ReplyError(req, rc);
}

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
  CURVEFS_ERROR rc;
  struct statvfs stbuf;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  AccessLogGuard log(
      [&]() { return StrFormat("statfs (%d): %s", ino, StrErr(rc)); });

  rc = client->FuseOpStatFs(req, ino, &stbuf);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyStatfs(req, &stbuf);
}

void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    const char* value, size_t size, int flags) {
  CURVEFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  AccessLogGuard log([&]() {
    return StrFormat("setxattr (%d,%s,%d,%d): %s", ino, name, size, flags,
                     StrErr(rc));
  });

  // FIXME(Wine93): please handle it in FuseClient.
  if (IsWarmupXAttr(name)) {
    return TriggerWarmup(req, ino, name, value, size);
  }
  rc = client->FuseOpSetXattr(req, ino, name, value, size, flags);
  return fs->ReplyError(req, rc);
}

void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    size_t size) {
  CURVEFS_ERROR rc;
  std::string value;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(GetXattr);
  AccessLogGuard log([&]() {
    return StrFormat("getxattr (%d,%s,%d): %s (%d)", ino, name, size,
                     StrErr(rc), value.size());
  });

  // FIXME(Wine93): please handle it in FuseClient.
  if (IsWarmupXAttr(name)) {
    return QueryWarmup(req, ino, size);
  }

  rc = Client()->FuseOpGetXattr(req, ino, name, &value, size);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  } else if (size == 0) {
    return fs->ReplyXattr(req, value.length());
  }
  return fs->ReplyBuffer(req, value.data(), value.length());
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  CURVEFS_ERROR rc;
  size_t xattr_size = 0;
  std::unique_ptr<char[]> buf(new char[size]);
  std::memset(buf.get(), 0, size);
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(ListXattr);
  AccessLogGuard log([&]() {
    return StrFormat("listxattr (%d,%d): %s (%d)", ino, size, StrErr(rc),
                     xattr_size);
  });

  rc = Client()->FuseOpListXattr(req, ino, buf.get(), size, &xattr_size);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  } else if (size == 0) {
    return fs->ReplyXattr(req, xattr_size);
  }
  return fs->ReplyBuffer(req, buf.get(), xattr_size);
}

void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char* name,
                  mode_t mode, struct fuse_file_info* fi) {
  CURVEFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Create);
  AccessLogGuard log([&]() {
    return StrFormat("create (%d,%s): %s%s [fh:%d]", parent, name, StrErr(rc),
                     StrEntry(entry_out), fi->fh);
  });

  rc = client->FuseOpCreate(req, parent, name, mode, fi, &entry_out);
  if (rc != CURVEFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyCreate(req, &entry_out, fi);
}

void FuseOpBmap(fuse_req_t req, fuse_ino_t /*ino*/, size_t /*blocksize*/,
                uint64_t /*idx*/) {
  // TODO(wuhanqing): implement for volume storage
  auto* client = Client();
  auto fs = client->GetFileSystem();

  return fs->ReplyError(req, CURVEFS_ERROR::NOTSUPPORT);
}
