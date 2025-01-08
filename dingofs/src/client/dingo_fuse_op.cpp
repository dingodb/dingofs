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
 * Project: dingo
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "client/dingo_fuse_op.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "client/blockcache/log.h"
#include "client/common/common.h"
#include "client/common/config.h"
#include "client/filesystem/access_log.h"
#include "client/filesystem/error.h"
#include "client/filesystem/meta.h"
#include "client/fuse_client.h"
#include "client/fuse_s3_client.h"
#include "client/warmup/warmup_manager.h"
#include "common/dynamic_vlog.h"
#include "stub/filesystem/xattr.h"
#include "stub/metric/metric.h"
#include "stub/rpcclient/base_client.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/configuration.h"
#include "utils/gflags_helper.h"

using dingofs::client::DINGOFS_ERROR;
using dingofs::client::FuseClient;
using dingofs::client::FuseS3Client;
using dingofs::client::blockcache::InitBlockCacheLog;
using dingofs::client::common::FuseClientOption;
using dingofs::client::filesystem::AccessLogGuard;
using dingofs::client::filesystem::AttrOut;
using dingofs::client::filesystem::EntryOut;
using dingofs::client::filesystem::FileOut;
using dingofs::client::filesystem::InitAccessLog;
using dingofs::client::filesystem::StrAttr;
using dingofs::client::filesystem::StrEntry;
using dingofs::client::filesystem::StrFormat;
using dingofs::client::filesystem::StrMode;
using dingofs::stub::filesystem::IsWarmupXAttr;
using dingofs::stub::metric::ClientOpMetric;
using dingofs::stub::metric::OpMetric;
using dingofs::stub::rpcclient::MDSBaseClient;
using dingofs::stub::rpcclient::MdsClientImpl;
using dingofs::utils::Configuration;

using dingofs::common::FLAGS_vlog_level;
using dingofs::pb::common::FSType;
using dingofs::pb::mds::FsInfo;
using dingofs::pb::mds::FSStatusCode;
using dingofs::pb::mds::FSStatusCode_Name;

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

  dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
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
  dingofs::client::common::InitFuseClientOption(&conf, g_fuse_client_option);

  auto fs_info = std::make_shared<FsInfo>();
  if (GetFsInfo(mount_option->fsName, fs_info.get()) != 0) {
    return -1;
  }

  std::string fs_type_str =
      (mount_option->fsType == nullptr) ? "" : mount_option->fsType;
  std::string fs_type_mds;
  if (fs_info->fstype() == FSType::TYPE_S3) {
    fs_type_mds = "s3";
  }

  if (fs_type_mds != fs_type_str) {
    LOG(ERROR) << "The parameter fstype is inconsistent with mds!";
    return -1;
  } else if (fs_type_str == "s3") {
    g_client_instance = new FuseS3Client();
  } else {
    LOG(ERROR) << "unknown fstype! fstype is " << fs_type_str;
    return -1;
  }

  g_client_instance->SetFsInfo(fs_info);
  DINGOFS_ERROR ret = g_client_instance->Init(*g_fuse_client_option);
  if (ret != DINGOFS_ERROR::OK) {
    return -1;
  }
  ret = g_client_instance->Run();
  if (ret != DINGOFS_ERROR::OK) {
    return -1;
  }

  ret = g_client_instance->SetMountStatus(mount_option);
  if (ret != DINGOFS_ERROR::OK) {
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

int AddWarmupTask(dingofs::client::common::WarmupType type, fuse_ino_t key,
                  const std::string& path,
                  dingofs::client::common::WarmupStorageType storage_type) {
  int ret = 0;
  bool result = true;
  switch (type) {
    case dingofs::client::common::WarmupType::kWarmupTypeList:
      result = g_client_instance->PutWarmFilelistTask(key, storage_type);
      break;
    case dingofs::client::common::WarmupType::kWarmupTypeSingle:
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
  dingofs::client::warmup::WarmupProgress progress;
  bool ret = g_client_instance->GetWarmupProgress(key, &progress);
  if (!ret) {
    *data = "finished";
  } else {
    *data = std::to_string(progress.GetFinished()) + "/" +
            std::to_string(progress.GetTotal()) + "/" +
            std::to_string(progress.GetErrors());
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
  dingofs::utils::SplitString(value, "\n", &op_type_path);
  if (op_type_path.size() != dingofs::client::common::kWarmupOpNum) {
    LOG(ERROR) << name << " has invalid xattr value " << value;
    return ERANGE;
  }
  auto storage_type =
      dingofs::client::common::GetWarmupStorageType(op_type_path[3]);
  if (storage_type ==
      dingofs::client::common::WarmupStorageType::kWarmupStorageTypeUnknown) {
    LOG(ERROR) << name << " not support storage type: " << value;
    return ERANGE;
  }
  int ret = 0;
  switch (dingofs::client::common::GetWarmupOpType(op_type_path[0])) {
    case dingofs::client::common::WarmupOpType::kWarmupOpAdd:
      ret =
          AddWarmupTask(dingofs::client::common::GetWarmupType(op_type_path[1]),
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

// client metric guard for one metric collection, fuse operation
struct ClientOpMetricGuard {
  explicit ClientOpMetricGuard(DINGOFS_ERROR* rc,
                               std::list<OpMetric*> metricList)
      : rc_(rc), metricList_(metricList) {
    start_ = butil::cpuwide_time_us();
    for (auto& metric_ : metricList_) {
      metric_->inflightOpNum << 1;
    }
  }
  ~ClientOpMetricGuard() {
    for (auto& metric_ : metricList_) {
      metric_->inflightOpNum << -1;
      if (*rc_ == DINGOFS_ERROR::OK) {
        metric_->qpsTotal << 1;
        auto duration = butil::cpuwide_time_us() - start_;
        metric_->latency << duration;
        metric_->latTotal << duration;
      } else {
        metric_->ecount << 1;
      }
    }
  }

  DINGOFS_ERROR* rc_;
  std::list<OpMetric*> metricList_;
  uint64_t start_;
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

#define METRIC_GUARD(REQUEST)              \
  ClientOpMetricGuard clientOpMetricGuard( \
      &rc, {&g_clientOpMetric->op##REQUEST, &g_clientOpMetric->opAll});
}  // namespace

void FuseOpInit(void* userdata, struct fuse_conn_info* conn) {
  DINGOFS_ERROR rc;
  auto* client = Client();
  AccessLogGuard log([&]() { return StrFormat("init : %s", StrErr(rc)); });

  rc = client->FuseOpInit(userdata, conn);
  if (rc != DINGOFS_ERROR::OK) {
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
  DINGOFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Lookup);
  AccessLogGuard log([&]() {
    return StrFormat("lookup (%d,%s): %s%s", parent, name, StrErr(rc),
                     StrEntry(entry_out));
  });

  rc = client->FuseOpLookup(req, parent, name, &entry_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
  AttrOut attr_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(GetAttr);
  AccessLogGuard log([&]() {
    return StrFormat("getattr (%d): %s%s", ino, StrErr(rc), StrAttr(attr_out));
  });

  rc = client->FuseOpGetAttr(req, ino, fi, &attr_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyAttr(req, &attr_out);
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                   int to_set, struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
  AttrOut attr_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(SetAttr);
  AccessLogGuard log([&]() {
    return StrFormat("setattr (%d,0x%X): %s%s", ino, to_set, StrErr(rc),
                     StrAttr(attr_out));
  });

  rc = client->FuseOpSetAttr(req, ino, attr, to_set, fi, &attr_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyAttr(req, &attr_out);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
  DINGOFS_ERROR rc;
  std::string link;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(ReadLink);
  AccessLogGuard log([&]() {
    return StrFormat("readlink (%d): %s %s", ino, StrErr(rc), link.c_str());
  });

  rc = client->FuseOpReadLink(req, ino, &link);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyReadlink(req, link);
}

void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode, dev_t rdev) {
  DINGOFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(MkNod);
  AccessLogGuard log([&]() {
    return StrFormat("mknod (%d,%s,%s:0%04o): %s%s", parent, name,
                     StrMode(mode), mode, StrErr(rc), StrEntry(entry_out));
  });

  rc = client->FuseOpMkNod(req, parent, name, mode, rdev, &entry_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode) {
  DINGOFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(MkDir);
  AccessLogGuard log([&]() {
    return StrFormat("mkdir (%d,%s,%s:0%04o): %s%s", parent, name,
                     StrMode(mode), mode, StrErr(rc), StrEntry(entry_out));
  });

  rc = client->FuseOpMkDir(req, parent, name, mode, &entry_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Symlink);
  AccessLogGuard log([&]() {
    return StrFormat("symlink (%d,%s,%s): %s%s", parent, name, link, StrErr(rc),
                     StrEntry(entry_out));
  });

  rc = client->FuseOpSymlink(req, link, parent, name, &entry_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char* name,
                  fuse_ino_t newparent, const char* newname,
                  unsigned int flags) {
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Link);
  AccessLogGuard log([&]() {
    return StrFormat("link (%d,%d,%s): %s%s", ino, newparent, newname,
                     StrErr(rc), StrEntry(entry_out));
  });

  rc = client->FuseOpLink(req, ino, newparent, newname, &entry_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyEntry(req, &entry_out);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
  FileOut file_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Open);
  AccessLogGuard log([&]() {
    return StrFormat("open (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
  });

  rc = client->FuseOpOpen(req, ino, fi, &file_out);
  if (rc != DINGOFS_ERROR::OK) {
    fs->ReplyError(req, rc);
    return;
  }
  return fs->ReplyOpen(req, &file_out);
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
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
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(r_size);
  bufvec.buf[0].mem = buffer.get();
  return fs->ReplyData(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
                 off_t off, struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
  FileOut file_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Write);
  AccessLogGuard log([&]() {
    return StrFormat("write (%d,%d,%d,%d): %s (%d)", ino, size, off, fi->fh,
                     StrErr(rc), file_out.nwritten);
  });

  WriteThrottleAdd(size);
  rc = client->FuseOpWrite(req, ino, buf, size, off, fi, &file_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyWrite(req, &file_out);
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(OpenDir);
  AccessLogGuard log([&]() {
    return StrFormat("opendir (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
  });

  rc = client->FuseOpOpenDir(req, ino, fi);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyOpen(req, fi);
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
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
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyBuffer(req, buffer, r_size);
}

void FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
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
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }

  return fs->ReplyBuffer(req, buffer, r_size);
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
  struct statvfs stbuf;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  AccessLogGuard log(
      [&]() { return StrFormat("statfs (%d): %s", ino, StrErr(rc)); });

  rc = client->FuseOpStatFs(req, ino, &stbuf);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyStatfs(req, &stbuf);
}

void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    const char* value, size_t size, int flags) {
  DINGOFS_ERROR rc;
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
  DINGOFS_ERROR rc;
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
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  } else if (size == 0) {
    return fs->ReplyXattr(req, value.length());
  }
  return fs->ReplyBuffer(req, value.data(), value.length());
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  DINGOFS_ERROR rc;
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
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  } else if (size == 0) {
    return fs->ReplyXattr(req, xattr_size);
  }
  return fs->ReplyBuffer(req, buf.get(), xattr_size);
}

void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char* name,
                  mode_t mode, struct fuse_file_info* fi) {
  DINGOFS_ERROR rc;
  EntryOut entry_out;
  auto* client = Client();
  auto fs = client->GetFileSystem();
  METRIC_GUARD(Create);
  AccessLogGuard log([&]() {
    return StrFormat("create (%d,%s): %s%s [fh:%d]", parent, name, StrErr(rc),
                     StrEntry(entry_out), fi->fh);
  });

  rc = client->FuseOpCreate(req, parent, name, mode, fi, &entry_out);
  if (rc != DINGOFS_ERROR::OK) {
    return fs->ReplyError(req, rc);
  }
  return fs->ReplyCreate(req, &entry_out, fi);
}

void FuseOpBmap(fuse_req_t req, fuse_ino_t /*ino*/, size_t /*blocksize*/,
                uint64_t /*idx*/) {
  // TODO(wuhanqing): implement for volume storage
  auto* client = Client();
  auto fs = client->GetFileSystem();

  return fs->ReplyError(req, DINGOFS_ERROR::NOTSUPPORT);
}
