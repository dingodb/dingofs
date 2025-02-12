// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>

#include "client/filesystemv2/rpc.h"
#include "client/fuse_common.h"
#define USE_DINGOFS_V2_FILESYSTEM 1
#ifdef USE_DINGOFS_V2_FILESYSTEM

#include <fmt/format.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "client/common/common.h"
#include "client/dingo_fuse_op.h"
#include "client/filesystem/error.h"
#include "client/filesystem/filesystem.h"
#include "client/filesystem/meta.h"
#include "client/filesystemv2/filesystem.h"
#include "client/filesystemv2/parent_cache.h"
#include "common/dynamic_vlog.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/coordinator/dingo_coordinator_client.h"
#include "mdsv2/mds/mds_meta.h"
#include "utils/configuration.h"

using EntryOut = dingofs::client::filesystem::EntryOut;
using AttrOut = dingofs::client::filesystem::AttrOut;
using ErrNo = dingofs::client::filesystem::DINGOFS_ERROR;

static const int kMaxXAttrNameSize = 4096;
static const int kMaxXAttrValueSize = 4096;

static void ToTimeSpec(uint64_t timestamp_ns, struct timespec* ts) {
  ts->tv_sec = timestamp_ns / 1000000000;
  ts->tv_nsec = timestamp_ns % 1000000000;
}

static uint64_t ToTimestamp(const struct timespec& ts) {
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

static void Inode2Stat(const dingofs::pb::mdsv2::Inode& attr,
                       struct stat& stat) {
  std::memset(&stat, 0, sizeof(struct stat));

  stat.st_ino = attr.ino();      // inode number
  stat.st_mode = attr.mode();    // permission mode
  stat.st_nlink = attr.nlink();  // number of links
  stat.st_uid = attr.uid();      // user ID of owner
  stat.st_gid = attr.gid();      // group ID of owner
  stat.st_size = attr.length();  // total size, in bytes
  stat.st_rdev = attr.rdev();    // device ID (if special file)

  ToTimeSpec(attr.atime(), &stat.st_atim);
  ToTimeSpec(attr.mtime(), &stat.st_mtim);
  ToTimeSpec(attr.ctime(), &stat.st_ctim);

  stat.st_blksize = 4096;
  stat.st_blocks = (attr.length() + 511) / 512;
}

static dingofs::pb::mdsv2::Inode GenDummyInode(uint32_t fs_id, uint64_t ino) {
  dingofs::pb::mdsv2::Inode inode;
  inode.set_ino(ino);
  inode.set_fs_id(fs_id);

  inode.set_length(0);

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  inode.set_ctime(ToTimestamp(now));
  inode.set_mtime(ToTimestamp(now));
  inode.set_atime(ToTimestamp(now));

  inode.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP |
                 S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  inode.set_uid(1008);
  inode.set_gid(1008);
  inode.set_nlink(1);
  inode.set_rdev(0);

  return inode;
}

void InitLog(const std::string& log_dir) {
  if (!dingofs::mdsv2::Helper::IsExistPath(log_dir)) {
    dingofs::mdsv2::Helper::CreateDirectories(log_dir);
  }

  FLAGS_logbufsecs = 0;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  std::string program_name = "dingo_fuse";

  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(
      google::GLOG_INFO,
      fmt::format("{}/{}.info.log.", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      fmt::format("{}/{}.warn.log.", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      fmt::format("{}/{}.error.log.", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      fmt::format("{}/{}.fatal.log.", log_dir, program_name).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

int InitLog(const char* conf_path, const char* argv0) {
  dingofs::utils::Configuration conf;
  conf.SetConfigPath(conf_path);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << fmt::format("load config fail, path({}).", conf_path);
    return -1;
  }

  // set log dir
  if (FLAGS_log_dir.empty() &&
      !conf.GetStringValue("client.common.logDir", &FLAGS_log_dir)) {
    LOG(WARNING) << fmt::format(
        "not found client.common.logDir in {}, will log to /tmp", conf_path);
  }

  std::cout << "log_dir: " << FLAGS_log_dir << std::endl;

  // dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
  // dummy.Load(&conf, "v", "client.loglevel", &FLAGS_v);
  // dingofs::common::FLAGS_vlog_level = FLAGS_v;

  InitLog(FLAGS_log_dir);

  return 0;
}

class FuseOperator {
 public:
  static FuseOperator& GetInstance() {
    static FuseOperator instance;
    return instance;
  }

  bool Init(const std::string& fs_name, const std::string& coor_addr,
            const std::string& mountpoint);
  bool Destory();

  void StatFs(fuse_req_t req, fuse_ino_t ino);

  void Lookup(fuse_req_t req, fuse_ino_t parent, const char* name);
  void Forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);

  void GetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
  void SetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
               struct fuse_file_info* fi);
  void SetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                const char* value, size_t size, int flags);
  void GetXattr(fuse_req_t req, fuse_ino_t ino, const char* name, size_t size);
  void ListXattr(fuse_req_t req, fuse_ino_t ino, size_t size);

  void MkNod(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
             dev_t rdev);
  void Create(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
              struct fuse_file_info* fi);

  void MkDir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode);
  void RmDir(fuse_req_t req, fuse_ino_t parent, const char* name);
  void OpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
  void ReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
               struct fuse_file_info* fi);
  void ReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi);
  void ReleaseDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  void Rename(fuse_req_t req, fuse_ino_t parent, const char* name,
              fuse_ino_t newparent, const char* newname, unsigned int flags);

  void Link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
            const char* newname);
  void Unlink(fuse_req_t req, fuse_ino_t parent, const char* name);
  void Symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
               const char* name);
  void ReadLink(fuse_req_t req, fuse_ino_t ino);

  void Open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);
  void Read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
            struct fuse_file_info* fi);
  void Write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
             off_t off, struct fuse_file_info* fi);
  void Flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  void Release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  void Fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
             struct fuse_file_info* fi);

  void Bmap(fuse_req_t req, fuse_ino_t /*ino*/, size_t /*blocksize*/,
            uint64_t /*idx*/);

 private:
  static void ReplyStatfs(fuse_req_t req, const struct statvfs* stbuf) {
    fuse_reply_statfs(req, stbuf);
  }

  static void ReplyError(fuse_req_t req, ErrNo code) {
    fuse_reply_err(req, SysErr(code));
  }

  static void ReplyEntry(fuse_req_t req, EntryOut& entry_out) {
    fuse_entry_param fuse_entry;
    std::memset(&fuse_entry, 0, sizeof(fuse_entry_param));

    fuse_entry.ino = entry_out.inode.ino();
    fuse_entry.generation = 0;

    fuse_entry.entry_timeout = 1;
    fuse_entry.attr_timeout = 1;

    Inode2Stat(entry_out.inode, fuse_entry.attr);

    LOG(INFO) << fmt::format(
        "ReplyEntry ino({}) generation({}) entry_timeout({}) attr_timeout({}) "
        "st_ino({}) st_mode({}) st_nlink({}) st_uid({}) st_gid({}) st_size({}) "
        "st_rdev({}) st_blksize({}) st_blocks({}) atime({} {}) mtime({} {}) "
        "ctime({} {}).",
        fuse_entry.ino, fuse_entry.generation, fuse_entry.entry_timeout,
        fuse_entry.attr_timeout, fuse_entry.attr.st_ino,
        fuse_entry.attr.st_mode, fuse_entry.attr.st_nlink,
        fuse_entry.attr.st_uid, fuse_entry.attr.st_gid, fuse_entry.attr.st_size,
        fuse_entry.attr.st_rdev, fuse_entry.attr.st_blksize,
        fuse_entry.attr.st_blocks, fuse_entry.attr.st_atim.tv_sec,
        fuse_entry.attr.st_atim.tv_nsec, fuse_entry.attr.st_mtim.tv_sec,
        fuse_entry.attr.st_mtim.tv_nsec, fuse_entry.attr.st_ctim.tv_sec,
        fuse_entry.attr.st_ctim.tv_nsec);

    fuse_reply_entry(req, &fuse_entry);
  }

  static void ReplyAttr(fuse_req_t req, AttrOut& attr_out) {
    struct stat stat;

    Inode2Stat(attr_out.inode, stat);

    LOG(INFO) << fmt::format(
        "ReplyAttr st_ino({}) st_mode({}) st_nlink({}) st_uid({}) st_gid({}) "
        "st_size({}) "
        "st_rdev({}) st_blksize({}) st_blocks({}) atime({} {}) mtime({} {}) "
        "ctime({} {}).",
        stat.st_ino, stat.st_mode, stat.st_nlink, stat.st_uid, stat.st_gid,
        stat.st_size, stat.st_rdev, stat.st_blksize, stat.st_blocks,
        stat.st_atim.tv_sec, stat.st_atim.tv_nsec, stat.st_mtim.tv_sec,
        stat.st_mtim.tv_nsec, stat.st_ctim.tv_sec, stat.st_ctim.tv_nsec);

    fuse_reply_attr(req, &stat, 1);
  }

  static void ReplyCreate(fuse_req_t req, EntryOut& entry_out,
                          fuse_file_info* fi) {
    fuse_entry_param fuse_entry;
    std::memset(&fuse_entry, 0, sizeof(fuse_entry_param));

    fuse_entry.ino = entry_out.inode.ino();
    fuse_entry.generation = 0;

    fuse_entry.entry_timeout = 1;
    fuse_entry.attr_timeout = 1;

    Inode2Stat(entry_out.inode, fuse_entry.attr);

    LOG(INFO) << fmt::format(
        "ReplyCreate ino({}) generation({}) entry_timeout({}) attr_timeout({}) "
        "st_ino({}) st_mode({}) st_nlink({}) st_uid({}) st_gid({}) st_size({}) "
        "st_rdev({}) st_blksize({}) st_blocks({}) atime({} {}) mtime({} {}) "
        "ctime({} {}).",
        fuse_entry.ino, fuse_entry.generation, fuse_entry.entry_timeout,
        fuse_entry.attr_timeout, fuse_entry.attr.st_ino,
        fuse_entry.attr.st_mode, fuse_entry.attr.st_nlink,
        fuse_entry.attr.st_uid, fuse_entry.attr.st_gid, fuse_entry.attr.st_size,
        fuse_entry.attr.st_rdev, fuse_entry.attr.st_blksize,
        fuse_entry.attr.st_blocks, fuse_entry.attr.st_atim.tv_sec,
        fuse_entry.attr.st_atim.tv_nsec, fuse_entry.attr.st_mtim.tv_sec,
        fuse_entry.attr.st_mtim.tv_nsec, fuse_entry.attr.st_ctim.tv_sec,
        fuse_entry.attr.st_ctim.tv_nsec);

    fuse_reply_create(req, &fuse_entry, fi);
  }

  static void ReplyBuffer(fuse_req_t req, const std::string& buf) {
    fuse_reply_buf(req, buf.data(), buf.size());
  }

  static void ReplyXattr(fuse_req_t req, size_t size) {
    fuse_reply_xattr(req, size);
  }

  static void ReplyReadlink(fuse_req_t req, const std::string& link) {
    fuse_reply_readlink(req, link.c_str());
  }

  static void ReplyOpen(fuse_req_t req, fuse_file_info* fi) {
    fuse_reply_open(req, fi);
  }

  static void ReplyData(fuse_req_t req, char* buffer, size_t size) {
    struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(size);
    bufvec.buf[0].mem = buffer;
    fuse_reply_data(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
  }

  static void ReplyWrite(fuse_req_t req, size_t w_size) {
    fuse_reply_write(req, w_size);
  }

  std::shared_ptr<dingofs::client::filesystem::MDSV2FileSystem> fs_;
};

bool FuseOperator::Init(const std::string& fs_name,
                        const std::string& coor_addr,
                        const std::string& mountpoint) {
  LOG(INFO) << "FuseOperator init.";

  using dingofs::client::filesystem::EndPoint;
  using dingofs::client::filesystem::MDSClient;
  using dingofs::client::filesystem::MDSClientPtr;
  using dingofs::client::filesystem::MDSDiscovery;
  using dingofs::client::filesystem::MDSDiscoveryPtr;
  using dingofs::client::filesystem::MDSV2FileSystem;
  using dingofs::client::filesystem::RPC;
  using dingofs::client::filesystem::RPCPtr;

  auto coordinator_client = dingofs::mdsv2::DingoCoordinatorClient::New();
  if (!coordinator_client->Init(coor_addr)) {
    LOG(ERROR) << "CoordinatorClient init fail.";
    return false;
  }

  auto mds_discovery = MDSDiscovery::New(coordinator_client);
  if (!mds_discovery->Init()) {
    LOG(ERROR) << "MDSDiscovery init fail.";
    return false;
  }

  // use first mds as default, get fs info
  dingofs::mdsv2::MDSMeta mds_meta;
  mds_discovery->PickFirstMDS(mds_meta);

  auto rpc = RPC::New(EndPoint(mds_meta.Host(), mds_meta.Port()));
  if (!rpc->Init()) {
    LOG(ERROR) << "RPC init fail.";
    return false;
  }

  dingofs::pb::mdsv2::FsInfo fs_info;
  auto status = MDSClient::GetFsInfo(rpc, fs_name, fs_info);
  if (!status.ok()) {
    LOG(ERROR) << "Get fs info fail.";
    return false;
  }

  // parent cache
  auto parent_cache = dingofs::client::filesystem::ParentCache::New();

  // mds router
  dingofs::client::filesystem::MDSRouterPtr mds_router;
  if (fs_info.partition_policy().type() ==
      dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    int64_t mds_id = fs_info.partition_policy().mono().mds_id();

    dingofs::mdsv2::MDSMeta mds_meta;
    if (!mds_discovery->GetMDS(mds_id, mds_meta)) {
      LOG(ERROR) << fmt::format("Get mds({}) meta fail.", mds_id);
      return false;
    }
    mds_router = dingofs::client::filesystem::MonoMDSRouter::New(mds_meta);

  } else if (fs_info.partition_policy().type() ==
             dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    mds_router = dingofs::client::filesystem::ParentHashMDSRouter::New(
        fs_info.partition_policy().parent_hash(), mds_discovery, parent_cache);

  } else {
    LOG(ERROR) << fmt::format("Not support partition policy type({}).",
                              dingofs::pb::mdsv2::PartitionType_Name(
                                  fs_info.partition_policy().type()));
    return false;
  }

  if (!mds_router->Init()) {
    LOG(ERROR) << "MDSRouter init fail.";
    return false;
  }

  // create mds client
  auto mds_client =
      MDSClient::New(fs_info.fs_id(), parent_cache, mds_router, rpc);
  if (!mds_client->Init()) {
    LOG(INFO) << "MDSClient init fail.";
    return false;
  }

  // create filesystem
  fs_ = MDSV2FileSystem::New(fs_info, mountpoint, mds_discovery, mds_client);
  if (!fs_->Init()) {
    LOG(INFO) << "MDSV2FileSystem init fail.";
    return false;
  }

  return true;
}

bool FuseOperator::Destory() {
  LOG(INFO) << "FuseOperator Destory.";
  fs_->UnInit();
  return true;
}

void FuseOperator::StatFs(fuse_req_t req, fuse_ino_t ino) {
  LOG(INFO) << fmt::format("StatFs ino({}).", ino);

  auto fs_info = fs_->GetFsInfo();

  struct statvfs stbuf;
  stbuf.f_frsize = stbuf.f_bsize = fs_info.block_size();
  stbuf.f_blocks = 100;
  stbuf.f_bfree = stbuf.f_bavail = 50;
  stbuf.f_files = UINT64_MAX;
  stbuf.f_ffree = stbuf.f_favail = 1000000;
  stbuf.f_fsid = fs_info.fs_id();

  stbuf.f_flag = 0;
  stbuf.f_namemax = 1024;

  LOG(INFO) << fmt::format(
      "StatFs stbuf[fsid({}) frsize({}) bsize({}) blocks({}) bfree({}) "
      "bavail({}) files({}) ffree({}) favail({}) namemax({})].",
      stbuf.f_fsid, stbuf.f_frsize, stbuf.f_bsize, stbuf.f_blocks,
      stbuf.f_bfree, stbuf.f_bavail, stbuf.f_files, stbuf.f_ffree,
      stbuf.f_favail, stbuf.f_namemax);

  ReplyStatfs(req, &stbuf);
}

void FuseOperator::Lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  if (strncmp(name, ".git", 4) == 0 || strncmp(name, "HEAD", 4) == 0) {
    return FuseOperator::ReplyError(req, ErrNo::NOTEXIST);
  }

  LOG(INFO) << fmt::format("Lookup parent({}), name({}).", parent, name);

  EntryOut entry_out;
  auto status = fs_->Lookup(parent, std::string(name), entry_out);
  if (!status.ok()) {
    if (status.error_code() == dingofs::pb::error::ENOT_FOUND) {
      LOG(INFO) << fmt::format("Lookup not found, name({}).", name);
      return FuseOperator::ReplyError(req, ErrNo::NOTEXIST);
    }
    LOG(ERROR) << fmt::format("Lookup fail, name({}).", name);
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyEntry(req, entry_out);
}

void FuseOperator::Forget(fuse_req_t req, fuse_ino_t, uint64_t) {
  FuseOperator::ReplyError(req, ErrNo::NOTSUPPORT);
}

void FuseOperator::GetAttr(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info*) {
  LOG(INFO) << fmt::format("GetAttr ino({}).", ino);

  AttrOut attr_out;
  auto status = fs_->GetAttr(ino, attr_out);
  if (!status.ok()) {
    LOG(ERROR) << "GetAttr fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyAttr(req, attr_out);
}

std::string ToString(struct stat* attr) {
  return fmt::format(
      "st_ino({}), st_mode({}), st_nlink({}), st_uid({}), "
      "st_gid({}), st_size({}), st_rdev({}), st_atim({}), "
      "st_mtim({}), st_ctim({}), st_blksize({}), st_blocks({})",
      attr->st_ino, attr->st_mode, attr->st_nlink, attr->st_uid, attr->st_gid,
      attr->st_size, attr->st_rdev, attr->st_atim.tv_sec, attr->st_mtim.tv_sec,
      attr->st_ctim.tv_sec, attr->st_blksize, attr->st_blocks);
}

void FuseOperator::SetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                           int to_set, struct fuse_file_info*) {
  LOG(INFO) << fmt::format("SetAttr ino({}) to_set({}) attr[{}].", ino, to_set,
                           ToString(attr));

  AttrOut attr_out;
  auto status = fs_->SetAttr(ino, attr, to_set, attr_out);
  if (!status.ok()) {
    LOG(ERROR) << "SetAttr fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyAttr(req, attr_out);
}

void FuseOperator::SetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                            const char* value, size_t size, int flags) {
  LOG(INFO) << fmt::format(
      "SetXattr ino({}) name({}) value({}) size({}) flags({}).", ino, name,
      value, size, flags);

  if (strlen(name) > kMaxXAttrNameSize || size > kMaxXAttrValueSize) {
    return FuseOperator::ReplyError(req, ErrNo::OUT_OF_RANGE);
  }

  auto status = fs_->SetXAttr(ino, std::string(name), std::string(value));
  if (!status.ok()) {
    LOG(ERROR) << "SetXattr fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::GetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                            size_t size) {
  // LOG(INFO) << fmt::format("GetXattr ino({}) name({}) size({}).", ino, name,
  // size);

  std::string value;
  auto status = fs_->GetXAttr(ino, std::string(name), value);
  if (!status.ok()) {
    LOG(ERROR) << "GetXattr fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  if (value.empty()) {
    // LOG(INFO) << "GetXattr no data.";
    return FuseOperator::ReplyError(req, ErrNo::NODATA);
  }

  if (value.size() > kMaxXAttrValueSize || value.size() > size) {
    LOG(ERROR) << "GetXattr fail, out of range.";
    return FuseOperator::ReplyError(req, ErrNo::OUT_OF_RANGE);
  }

  if (size == 0) {
    return FuseOperator::ReplyXattr(req, value.size());
  }

  FuseOperator::ReplyBuffer(req, value);
}

void FuseOperator::ListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  LOG(INFO) << fmt::format("ListXattr ino({}) size({}).", ino, size);

  std::string out_names;
  auto status = fs_->ListXAttr(ino, size, out_names);
  if (!status.ok()) {
    LOG(ERROR) << "ListXattr fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  if (out_names.size() > size) {
    LOG(ERROR) << "ListXattr fail, out of range.";
    return FuseOperator::ReplyError(req, ErrNo::OUT_OF_RANGE);
  }

  if (size == 0) {
    return FuseOperator::ReplyXattr(req, out_names.size());
  }

  FuseOperator::ReplyBuffer(req, out_names);
}

void FuseOperator::MkNod(fuse_req_t req, fuse_ino_t parent, const char* name,
                         mode_t mode, dev_t rdev) {
  LOG(INFO) << fmt::format("MkNod parent({}) size({}) mode({}) rdev({}).",
                           parent, name, mode, rdev);

  const struct fuse_ctx* ctx = fuse_req_ctx(req);

  EntryOut entry_out;
  auto status = fs_->MkNod(parent, std::string(name), ctx->uid, ctx->gid, mode,
                           rdev, entry_out);
  if (!status.ok()) {
    LOG(ERROR) << "MkNod fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyEntry(req, entry_out);
}

void FuseOperator::Create(fuse_req_t req, fuse_ino_t parent, const char* name,
                          mode_t mode, struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Create parent({}) name({}) mode({}).", parent, name,
                           mode);

  const struct fuse_ctx* ctx = fuse_req_ctx(req);

  EntryOut entry_out;
  auto status = fs_->MkNod(parent, std::string(name), ctx->uid, ctx->gid, mode,
                           0, entry_out);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Create file({}) fail, error: {} {}.", name,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  status = fs_->Open(entry_out.inode.ino());
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Create file({}) open fail, error: {} {}.", name,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyCreate(req, entry_out, fi);
}

void FuseOperator::MkDir(fuse_req_t req, fuse_ino_t parent, const char* name,
                         mode_t mode) {
  LOG(INFO) << fmt::format("MkDir parent({}) name({}) mode({}).", parent, name,
                           mode);

  const struct fuse_ctx* ctx = fuse_req_ctx(req);

  EntryOut entry_out;
  auto status = fs_->MkDir(parent, std::string(name), ctx->uid, ctx->gid, mode,
                           0, entry_out);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("MkDir {}/{} fail, error: {} {}.", parent, name,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyEntry(req, entry_out);
}

void FuseOperator::RmDir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  LOG(INFO) << fmt::format("RmDir parent({}) name({}).", parent, name);

  auto status = fs_->RmDir(parent, std::string(name));
  if (!status.ok()) {
    if (status.error_code() == dingofs::pb::error::ENOT_EMPTY) {
      return FuseOperator::ReplyError(req, ErrNo::NOTEMPTY);
    }
    LOG(ERROR) << fmt::format("RmDir {}/{} fail, error:  {} {}.", parent, name,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::OpenDir(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("OpenDir ino({}).", ino);
  uint64_t fh = 0;
  auto status = fs_->OpenDir(ino, fh);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("OpenDir {} fail, error: {} {}.", ino,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  fi->fh = fh;

  FuseOperator::ReplyOpen(req, fi);
}

std::string GenReadDirBuffer(fuse_req_t req) {
  std::string buffer;
  buffer.resize(1024 * 1024);

  size_t writed_size = 0;
  for (int i = 0; i < 10; i++) {
    uint64_t ino = 10000 + i;
    std::string name = fmt::format("file_{}", i);

    struct stat stat;
    std::memset(&stat, 0, sizeof(stat));
    stat.st_ino = ino;

    size_t need_size =
        fuse_add_direntry(req, nullptr, 0, name.c_str(), nullptr, 0);

    size_t need_size2 =
        fuse_add_direntry(req, buffer.data() + writed_size,
                          buffer.size() - writed_size, name.c_str(), &stat, 1);
    LOG(INFO) << fmt::format("need_size({}) need_size2({}).", need_size,
                             need_size2);
    writed_size += need_size;
  }

  return buffer;
}

// size is the maximum size of the buffer
// off is the offset to start reading from, maybe not buffer offset
void FuseOperator::ReadDir(fuse_req_t req, fuse_ino_t ino, size_t size,
                           off_t off, struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("ReadDir ino({}) fh({}) size({}) off({}).", ino,
                           fi->fh, size, off);
  auto fs_info = fs_->GetFsInfo();

  size_t writed_size = 0;
  std::string buffer(size, '\0');
  fs_->ReadDir(fi->fh, ino, [&](const std::string& name, uint64_t ino) -> bool {
    LOG(INFO) << fmt::format("ReadDir name({}) ino({}).", name, ino);

    struct stat stat;
    std::memset(&stat, 0, sizeof(stat));
    stat.st_ino = ino;

    size_t rest_size = buffer.size() - writed_size;

    size_t need_size = fuse_add_direntry(req, buffer.data() + writed_size,
                                         rest_size, name.c_str(), &stat, ++off);
    LOG(INFO) << fmt::format("ReadDir off({}) need_size({}) rest_size({}).",
                             off, need_size, rest_size);
    if (need_size > rest_size) {
      return false;
    }

    writed_size += need_size;

    return true;
  });

  LOG(INFO) << fmt::format("ReadDir buffer size({}) writed_size({})",
                           buffer.size(), writed_size);
  buffer.resize(writed_size);

  FuseOperator::ReplyBuffer(req, buffer);
}

std::string GenReadDirPlusBuffer(uint32_t fs_id, fuse_req_t req) {
  std::string buffer;
  buffer.resize(1024 * 1024);

  int off = 0;
  size_t writed_size = 0;
  for (int i = 0; i < 10; ++i) {
    uint64_t ino = 100000 + i;
    std::string name = fmt::format("file_{}", i);
    dingofs::pb::mdsv2::Inode inode = GenDummyInode(fs_id, ino);

    struct fuse_entry_param e;
    std::memset(&e, 0, sizeof(fuse_entry_param));

    e.ino = inode.ino();
    e.generation = 0;

    e.entry_timeout = 1;
    e.attr_timeout = 1;

    Inode2Stat(inode, e.attr);

    size_t need_size =
        fuse_add_direntry_plus(req, nullptr, 0, name.c_str(), nullptr, 0);

    size_t need_size2 = fuse_add_direntry_plus(req, buffer.data() + writed_size,
                                               buffer.size() - writed_size,
                                               name.c_str(), &e, ++off);
    LOG(INFO) << fmt::format("need_size({}) need_size2({}).", need_size,
                             need_size2);
    writed_size += need_size;
  }

  LOG(INFO) << fmt::format("buffer size({}) writed_size({}).", buffer.size(),
                           writed_size);

  buffer.resize(writed_size);
  return buffer;
}

void FuseOperator::ReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size,
                               off_t off, struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("ReadDirPlus ino({}) fh({}) size({}) off({}).", ino,
                           fi->fh, size, off);

  auto fs_info = fs_->GetFsInfo();

  size_t writed_size = 0;
  std::string buffer(size, '\0');
  fs_->ReadDirPlus(fi->fh, ino,
                   [&](const std::string& name,
                       const dingofs::pb::mdsv2::Inode& inode) -> bool {
                     LOG(INFO) << fmt::format("ReadDirPlus name({}) ino({}).",
                                              name, inode.ino());
                     struct fuse_entry_param entry;
                     std::memset(&entry, 0, sizeof(fuse_entry_param));

                     entry.ino = inode.ino();
                     entry.generation = 0;
                     entry.entry_timeout = 1;
                     entry.attr_timeout = 1;

                     Inode2Stat(inode, entry.attr);

                     size_t rest_size = buffer.size() - writed_size;

                     size_t need_size = fuse_add_direntry_plus(
                         req, buffer.data() + writed_size, rest_size,
                         name.c_str(), &entry, ++off);
                     LOG(INFO) << fmt::format(
                         "ReadDirPlus off({}) need_size({}) rest_size({}).",
                         off, need_size, rest_size);
                     if (need_size > rest_size) {
                       return false;
                     }

                     writed_size += need_size;

                     return true;
                   });

  LOG(INFO) << fmt::format("ReadDirPlus buffer size({}) writed_size({})",
                           buffer.size(), writed_size);
  buffer.resize(writed_size);

  FuseOperator::ReplyBuffer(req, buffer);
}

void FuseOperator::ReleaseDir(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("ReleaseDir ino({}) fh({}).", ino, fi->fh);
  auto status = fs_->ReleaseDir(ino, fi->fh);
  if (!status.ok()) {
    LOG(ERROR) << "ReleaseDir fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::Rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                          fuse_ino_t newparent, const char* newname,
                          unsigned int flags) {
  LOG(INFO) << fmt::format(
      "Rename parent({}) name({}) newparent({}) newname({}).", parent, name,
      newparent, newname);

  auto status =
      fs_->Rename(parent, std::string(name), newparent, std::string(newname));
  if (!status.ok()) {
    LOG(ERROR) << "Rename fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::Link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                        const char* newname) {
  LOG(INFO) << fmt::format("Link ino({}) newparent({}) newname({}).", ino,
                           newparent, newname);

  EntryOut entry_out;
  auto status = fs_->Link(ino, newparent, std::string(newname), entry_out);
  if (!status.ok()) {
    LOG(ERROR) << "Link fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyEntry(req, entry_out);
}

void FuseOperator::Unlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  LOG(INFO) << fmt::format("Unlink parent({}) name({}).", parent, name);

  auto status = fs_->UnLink(parent, std::string(name));
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Unlink file({}/{}) fail, error: {} {}.", parent,
                              name, status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::Symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                           const char* name) {
  LOG(INFO) << fmt::format("Symlink parent({}) name({}) link({}).", parent,
                           name, link);

  const struct fuse_ctx* ctx = fuse_req_ctx(req);

  EntryOut entry_out;
  auto status = fs_->Symlink(parent, std::string(name), ctx->uid, ctx->gid,
                             std::string(link), entry_out);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Symlink fail, error: {} {}.",
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyEntry(req, entry_out);
}

void FuseOperator::ReadLink(fuse_req_t req, fuse_ino_t ino) {
  LOG(INFO) << fmt::format("ReadLink ino({}).", ino);

  std::string symlink;
  auto status = fs_->ReadLink(ino, symlink);
  if (!status.ok()) {
    LOG(ERROR) << "ReadLink fail.";
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyReadlink(req, symlink);
}

void FuseOperator::Open(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Open ino({}).", ino);

  auto status = fs_->Open(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Open file({}) fail.", ino);
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyOpen(req, fi);
}

void FuseOperator::Read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                        struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Read ino({}) size({}) off({}).", ino, size, off);

  size_t r_size = 0;
  std::unique_ptr<char[]> buffer(new char[size]);
  auto status = fs_->Read(ino, off, size, buffer.get(), r_size);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Read file({}) fail, error: {} {}.", ino,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyData(req, buffer.get(), r_size);
}

void FuseOperator::Write(fuse_req_t req, fuse_ino_t ino, const char* buf,
                         size_t size, off_t off, struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Write ino({}) size({}) off({}).", ino, size, off);

  size_t w_size = 0;
  auto status = fs_->Write(ino, off, buf, size, w_size);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Write file({}) fail, error: {} {}.", ino,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyWrite(req, w_size);
}

void FuseOperator::Flush(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Flush ino({}).", ino);

  auto status = fs_->Flush(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Flush file({}) fail, error: {} {}.", ino,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::Release(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Release ino({}).", ino);

  auto status = fs_->Release(ino);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Release file({}) fail, error: {} {}.", ino,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::Fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                         struct fuse_file_info* fi) {
  LOG(INFO) << fmt::format("Fsync ino({}) datasync({}).", ino, datasync);

  auto status = fs_->Fsync(ino, datasync);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("Fsync file({}) fail, error: {} {}.", ino,
                              status.error_code(), status.error_str());
    return FuseOperator::ReplyError(req, ErrNo::INTERNAL);
  }

  FuseOperator::ReplyError(req, ErrNo::OK);
}

void FuseOperator::Bmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize,
                        uint64_t idx) {
  LOG(INFO) << fmt::format("Bmap ino({}) blocksize({}) idx({}).", ino,
                           blocksize, idx);
  FuseOperator::ReplyError(req, ErrNo::NOTSUPPORT);
}

// =============================== I'm split line ===========================

int InitFuseClient(const struct MountOption* mount_option) {
  LOG(INFO) << "init fuse client.";

  if (mount_option->mountPoint != nullptr) {
    LOG(INFO) << fmt::format("mount_option mount point: {}.",
                             mount_option->mountPoint);
  }

  if (mount_option->fsName != nullptr) {
    LOG(INFO) << fmt::format("mount_option fs name: {}.", mount_option->fsName);
  }

  if (mount_option->fsType != nullptr) {
    LOG(INFO) << fmt::format("mount_option fs type: {}.", mount_option->fsType);
  }

  if (mount_option->mdsAddr != nullptr) {
    LOG(INFO) << fmt::format("mount_option mds: {}.", mount_option->mdsAddr);
  }

  if (mount_option->conf != nullptr) {
    LOG(INFO) << fmt::format("mount_option conf: {}.", mount_option->conf);
  }

  dingofs::utils::Configuration conf;
  conf.SetConfigPath(mount_option->conf);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "load config fail, confPath = " << mount_option->conf;
    return -1;
  }
  if (mount_option->mdsAddr != nullptr) {
    conf.SetStringValue("mdsOpt.rpcRetryOpt.addrs", mount_option->mdsAddr);
  }

  std::string coor_addr;
  conf.GetValueFatalIfFail("coordinator.addr", &coor_addr);
  LOG(INFO) << fmt::format("coordinator addr: {}.", coor_addr);

  conf.PrintConfig();

  auto& fuse_operator = FuseOperator::GetInstance();
  if (!fuse_operator.Init(std::string(mount_option->fsName), coor_addr,
                          std::string(mount_option->mountPoint))) {
    LOG(ERROR) << "init FuseOperator fail.";
    return -1;
  }

  return 0;
}

void UnInitFuseClient() {
  LOG(INFO) << "uninit fuse client.";

  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Destory();
}

void FuseOpInit(void* userdata /* NOLINT */, struct fuse_conn_info* conn) {
  LOG(INFO) << "FuseOpInit.";
}

void FuseOpDestroy(void* userdata /* NOLINT */) { LOG(INFO) << "FuseOpInit."; }

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Lookup(req, parent, name);
}

void FuseOpForget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Forget(req, ino, nlookup);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.GetAttr(req, ino, fi);
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                   int to_set, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.SetAttr(req, ino, attr, to_set, fi);
}

void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode, dev_t rdev) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.MkNod(req, parent, name, mode, rdev);
}

void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.MkDir(req, parent, name, mode);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.RmDir(req, parent, name);
}

void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                const char* newname) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Link(req, ino, newparent, newname);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Unlink(req, parent, name);
}

void FuseOpSymlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                   const char* name) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Symlink(req, link, parent, name);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.ReadLink(req, ino);
}

void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char* name,
                  fuse_ino_t newparent, const char* newname,
                  unsigned int flags) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Rename(req, parent, name, newparent, newname, flags);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Open(req, ino, fi);
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Read(req, ino, size, off, fi);
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
                 off_t off, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Write(req, ino, buf, size, off, fi);
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Flush(req, ino, fi);
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Release(req, ino, fi);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                 struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Fsync(req, ino, datasync, fi);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.OpenDir(req, ino, fi);
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.ReadDir(req, ino, size, off, fi);
}

void FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.ReadDirPlus(req, ino, size, off, fi);
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.ReleaseDir(req, ino, fi);
}

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.StatFs(req, ino);
}

void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    const char* value, size_t size, int flags) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.SetXattr(req, ino, name, value, size, flags);
}

void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    size_t size) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.GetXattr(req, ino, name, size);
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.ListXattr(req, ino, size);
}

void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char* name,
                  mode_t mode, struct fuse_file_info* fi) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Create(req, parent, name, mode, fi);
}

void FuseOpBmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize,
                uint64_t idx) {
  auto& fuse_operator = FuseOperator::GetInstance();
  fuse_operator.Bmap(req, ino, blocksize, idx);
}

#endif  // USE_DINGOFS_V2_FILESYSTEM