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

#include "client/fuse/fuse_op.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "absl/strings/str_format.h"
#include "client/common/utils.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/vfs_meta.h"
#include "client/vfs/vfs_wrapper.h"
#include "common/define.h"
#include "common/status.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "options/client/option.h"
#include "utils/configuration.h"

static dingofs::client::vfs::VFSWrapper* g_vfs = nullptr;

USING_FLAG(client_fuse_file_info_direct_io)
USING_FLAG(client_fuse_file_info_keep_cache)

using dingofs::Status;
using dingofs::client::vfs::Attr;
using dingofs::client::vfs::FsStat;

namespace {

void InitFuseConnInfo(struct fuse_conn_info* conn) {
  auto fuse_option = g_vfs->GetFuseOption();
  const auto& option = fuse_option.conn_info;

  if (option.want_splice_move) {
    LOG_IF(INFO, fuse_set_feature_flag(conn, FUSE_CAP_SPLICE_MOVE))
        << "[enabled] FUSE_CAP_SPLICE_MOVE";
  }
  if (option.want_splice_read) {
    LOG_IF(INFO, fuse_set_feature_flag(conn, FUSE_CAP_SPLICE_READ))
        << "[enabled] FUSE_CAP_SPLICE_READ";
  }
  if (option.want_splice_write) {
    LOG_IF(INFO, fuse_set_feature_flag(conn, FUSE_CAP_SPLICE_WRITE))
        << "[enabled] FUSE_CAP_SPLICE_WRITE";
  }
  if (fuse_get_feature_flag(conn, FUSE_CAP_AUTO_INVAL_DATA) &&
      !option.want_auto_inval_data) {
    fuse_unset_feature_flag(conn, FUSE_CAP_AUTO_INVAL_DATA);
    LOG(INFO) << "[disabled] FUSE_CAP_AUTO_INVAL_DATA";
  }
}

void Attr2Stat(const Attr& attr, struct stat* stat) {
  stat->st_ino = attr.ino;      //  inode number
  stat->st_mode = attr.mode;    // permission mode
  stat->st_nlink = attr.nlink;  // number of links
  stat->st_uid = attr.uid;      // user ID of owner
  stat->st_gid = attr.gid;      // group ID of owner
  stat->st_size = attr.length;  // total size, in bytes
  stat->st_rdev = attr.rdev;    // device ID (if special file)

  dingofs::client::vfs::ToTimeSpec(attr.atime, &stat->st_atim);
  dingofs::client::vfs::ToTimeSpec(attr.mtime, &stat->st_mtim);
  dingofs::client::vfs::ToTimeSpec(attr.ctime, &stat->st_ctim);

  stat->st_blksize = 0x10000u;  // blocksize for file system I/O
  stat->st_blocks =
      (attr.length + 511) / 512;  // number of 512B blocks allocated
}

void Attr2FuseEntry(const Attr& attr, struct fuse_entry_param* e) {
  e->ino = attr.ino;
  e->generation = 0;
  Attr2Stat(attr, &e->attr);
  e->attr_timeout = g_vfs->GetAttrTimeout(attr.type);
  e->entry_timeout = g_vfs->GetEntryTimeout(attr.type);
}

Attr Stat2Attr(struct stat* stat) {
  Attr attr;
  attr.ino = stat->st_ino;
  attr.mode = stat->st_mode;
  attr.nlink = stat->st_nlink;
  attr.uid = stat->st_uid;
  attr.gid = stat->st_gid;
  attr.length = stat->st_size;
  attr.rdev = stat->st_rdev;
  attr.atime = dingofs::client::vfs::ToTimestamp(stat->st_atim);
  attr.mtime = dingofs::client::vfs::ToTimestamp(stat->st_mtim);
  attr.ctime = dingofs::client::vfs::ToTimestamp(stat->st_ctim);

  return attr;
}

}  // namespace

static void ReplyError(fuse_req_t req, const Status& s) {
  fuse_reply_err(req, s.ToSysErrNo());
}

static void ReplyEntry(fuse_req_t req, const Attr& attr) {
  fuse_entry_param e;
  memset(&e, 0, sizeof(e));
  Attr2FuseEntry(attr, &e);
  fuse_reply_entry(req, &e);
}

static void ReplyAttr(fuse_req_t req, const Attr& attr) {
  struct stat stat;
  memset(&stat, 0, sizeof(stat));
  Attr2Stat(attr, &stat);
  fuse_reply_attr(req, &stat, g_vfs->GetAttrTimeout(attr.type));
}

static void ReplyReadlink(fuse_req_t req, const std::string& link) {
  fuse_reply_readlink(req, link.c_str());
}

static void ReplyOpen(fuse_req_t req, struct fuse_file_info* fi) {
  fuse_reply_open(req, fi);
}

static void ReplyCreate(fuse_req_t req, struct fuse_file_info* fi,
                        const Attr& attr) {
  fuse_entry_param e;
  memset(&e, 0, sizeof(fuse_entry_param));
  Attr2FuseEntry(attr, &e);
  fuse_reply_create(req, &e, fi);
}

static void ReplyData(fuse_req_t req, char* buffer, size_t size) {
  struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(size);
  bufvec.buf[0].mem = buffer;
  fuse_reply_data(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

static void ReplyWrite(fuse_req_t req, size_t size) {
  fuse_reply_write(req, size);
}

static void ReplyBuf(fuse_req_t req, char* buffer, size_t size) {
  fuse_reply_buf(req, buffer, size);
}

//  Reply with needed buffer size
static void ReplyXattr(fuse_req_t req, size_t size) {
  fuse_reply_xattr(req, size);
}

static void ReplyIoctl(fuse_req_t req, const char* out_buf, size_t out_bufsz) {
  fuse_reply_ioctl(req, 0, out_buf, out_bufsz);
}

static void ReplyStatfs(fuse_req_t req, const FsStat& stat) {
  uint64_t block_size = 4096;

  uint64_t total_bytes = stat.max_bytes;
  uint64_t total_blocks =
      ((total_bytes % block_size == 0) ? total_bytes / block_size
                                       : (total_bytes / block_size) + 1);

  uint64_t used_bytes = stat.used_bytes;

  uint64_t free_blocks = 0;
  if (total_bytes - used_bytes <= 0) {
    free_blocks = 0;
  } else {
    if (used_bytes > 0) {
      uint64_t used_blocks = (used_bytes % block_size == 0)
                                 ? used_bytes / block_size
                                 : (used_bytes / block_size) + 1;
      free_blocks = total_blocks - used_blocks;
    } else {
      free_blocks = total_blocks;
    }
  }

  uint64_t used_inodes = stat.used_inodes;

  uint64_t total_inodes = stat.max_inodes;
  uint64_t free_inodes = 0;
  if (total_inodes - used_inodes <= 0) {
    free_inodes = 0;
  } else {
    if (used_inodes > 0) {
      free_inodes = total_inodes - used_inodes;
    } else {
      free_inodes = total_inodes;
    }
  }

  struct statvfs stbuf;
  stbuf.f_frsize = stbuf.f_bsize = block_size;
  stbuf.f_blocks = total_blocks;
  stbuf.f_bfree = stbuf.f_bavail = free_blocks;
  stbuf.f_files = total_inodes;
  stbuf.f_ffree = stbuf.f_favail = free_inodes;
  stbuf.f_fsid = g_vfs->GetFsId();
  stbuf.f_flag = 0;
  stbuf.f_namemax = g_vfs->GetMaxNameLength();

  fuse_reply_statfs(req, &stbuf);
}

int InitFuseClient(const char* argv0, const struct MountOption* mount_option) {
  dingofs::client::vfs::VFSConfig config = {
      .mount_point = mount_option->mount_point,
      .fs_name = mount_option->fs_name,
      .config_path = mount_option->conf,
      .fs_type = mount_option->fs_type,
  };

  g_vfs = new dingofs::client::vfs::VFSWrapper();

  Status s = g_vfs->Start(argv0, config);
  if (!s.ok()) {
    LOG(ERROR) << "Start VFS failed, status: " << s.ToString();
  }

  return s.ToSysErrNo();
}

void UnInitFuseClient() { delete g_vfs; }

void FuseOpInit(void* userdata, struct fuse_conn_info* conn) {
  VLOG(1) << "FuseOpInit userdata: " << userdata;
  (void)userdata;
  g_vfs->Init();
  InitFuseConnInfo(conn);
  LOG(INFO) << "FuseOpInit() success";
}

void FuseOpDestroy(void* userdata) {
  VLOG(1) << "FuseOpDestroy userdata: " << userdata;
  if (g_vfs) {
    g_vfs->Stop();
  }
}

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  VLOG(1) << "FuseOpLookup parent: " << parent << ", name: " << name;
  Attr attr;
  Status s = g_vfs->Lookup(parent, name, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyEntry(req, attr);
  }
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpGetAttr ino: " << ino;
  Attr attr;
  Status s = g_vfs->GetAttr(ino, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    VLOG(1) << "FuseOpGetAttr ino: " << ino << " attr=" << Attr2Str(attr);

    ReplyAttr(req, attr);
  }
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                   int to_set, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpSetAttr ino: " << ino;
  Attr in_attr = Stat2Attr(attr);
  Attr out_attr;
  Status s = g_vfs->SetAttr(ino, to_set, in_attr, &out_attr);

  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyAttr(req, out_attr);
  }
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
  VLOG(1) << "FuseOpReadLink ino: " << ino;
  std::string link;
  Status s = g_vfs->ReadLink(ino, &link);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyReadlink(req, link);
  }
}

void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode, dev_t rdev) {
  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  uint32_t uid = ctx->uid;
  uint32_t gid = ctx->gid;

  VLOG(1) << "FuseOpMkNod parent: " << parent << " name: " << name
          << " uid: " << uid << " gid: " << gid << " mode: " << mode
          << " rdev: " << rdev;

  Attr attr;
  Status s = g_vfs->MkNod(parent, name, uid, gid, mode, rdev, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyEntry(req, attr);
  }
}

void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char* name,
                 mode_t mode) {
  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  uint32_t uid = ctx->uid;
  uint32_t gid = ctx->gid;

  VLOG(1) << "FuseOpMkDir parent: " << parent << ", name: " << name
          << " uid: " << uid << ", gid: " << gid << ", mode: " << mode;

  Attr attr;
  Status s = g_vfs->MkDir(parent, name, uid, gid, mode, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyEntry(req, attr);
  }
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  VLOG(1) << "FuseOpUnlink parent: " << parent << ", name: " << name;
  Status s = g_vfs->Unlink(parent, name);
  ReplyError(req, s);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  VLOG(1) << "FuseOpRmDir parent: " << parent << ", name: " << name;
  Status s = g_vfs->RmDir(parent, name);
  ReplyError(req, s);
}

void FuseOpSymlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                   const char* name) {
  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  uint32_t uid = ctx->uid;
  uint32_t gid = ctx->gid;

  VLOG(1) << "FuseOpSymlink link: " << link << ", parent: " << parent
          << ", name: " << name << " uid: " << uid << ", gid: " << gid;

  Attr attr;
  Status s = g_vfs->Symlink(parent, name, uid, gid, link, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyEntry(req, attr);
  }
}

void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char* name,
                  fuse_ino_t newparent, const char* newname,
                  unsigned int flags) {
  VLOG(1) << "FuseOpRename parent: " << parent << ", name: " << name
          << ", newparent: " << newparent << ", newname: " << newname
          << ", flags: " << flags;

  Status s = g_vfs->Rename(parent, name, newparent, newname);
  ReplyError(req, s);
}

void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                const char* newname) {
  VLOG(1) << "FuseOpLink ino: " << ino << ", newparent: " << newparent
          << ", newname: " << newname;
  Attr attr;
  Status s = g_vfs->Link(ino, newparent, newname, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyEntry(req, attr);
  }
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpOpen ino: " << ino;
  uint64_t fh = 0;
  Attr attr;
  Status s = g_vfs->Open(ino, fi->flags, &fh);

  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    fi->fh = fh;

    fi->direct_io =
        (dingofs::IsInternalNode(ino) || FLAGS_client_fuse_file_info_direct_io)
            ? 1
            : 0;
    fi->keep_cache = FLAGS_client_fuse_file_info_keep_cache ? 1 : 0;

    ReplyOpen(req, fi);
  }
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpRead ino: " << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh;
  std::unique_ptr<char[]> buffer(new char[size]);
  memset(buffer.get(), 0, size);

  uint64_t rsize = 0;
  Status s = g_vfs->Read(ino, buffer.get(), size, off, fi->fh, &rsize);
  VLOG(1) << "FuseOpRead ino: " << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh
          << ", rsize: " << rsize
          << ", buf: " << dingofs::client::Char2Addr(buffer.get());
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyData(req, buffer.get(), rsize);
  }
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
                 off_t off, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpWrite ino: " << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh;
  uint64_t wsize = 0;
  Status s = g_vfs->Write(ino, buf, size, off, fi->fh, &wsize);
  VLOG(1) << "FuseOpWrite ino: " << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh
          << ", wsize: " << wsize;
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyWrite(req, wsize);
  }
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpFlush ino: " << ino << ", fi->fh: " << fi->fh;
  Status s = g_vfs->Flush(ino, fi->fh);
  ReplyError(req, s);
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpRelease ino: " << ino << ", fi->fh: " << fi->fh;
  Status s = g_vfs->Release(ino, fi->fh);
  ReplyError(req, s);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                 struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpFsync ino: " << ino << ", datasync: " << datasync
          << ", fi->fh: " << fi->fh;

  Status s = g_vfs->Fsync(ino, datasync, fi->fh);
  ReplyError(req, s);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpOpenDir ino: " << ino;

  uint64_t fh = 0;
  Attr attr;
  Status s = g_vfs->OpenDir(ino, &fh);

  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    fi->fh = fh;

    fi->cache_readdir = FLAGS_client_fuse_file_info_keep_cache ? 1 : 0;
    fi->keep_cache = FLAGS_client_fuse_file_info_keep_cache ? 1 : 0;

    ReplyOpen(req, fi);
  }
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi) {
  VLOG(1) << fmt::format("read dir, ino({}) fh({}) off({}) size({})", ino,
                         fi->fh, off, size);

  CHECK_GE(off, 0) << "offset is illegal, offset: " << off;

  size_t writed_size = 0;
  std::string buffer(size, '\0');
  Status s = g_vfs->ReadDir(
      ino, fi->fh, off, false,
      [&](const dingofs::client::vfs::DirEntry& dir_entry,
          uint64_t off) -> bool {
        (void)off;
        VLOG(1) << fmt::format("read dir entry({}/{})", dir_entry.name,
                               dir_entry.ino);

        struct stat stat;
        std::memset(&stat, 0, sizeof(stat));
        stat.st_ino = dir_entry.ino;

        size_t rest_size = buffer.size() - writed_size;

        size_t entsize =
            fuse_add_direntry(req, buffer.data() + writed_size, rest_size,
                              dir_entry.name.c_str(), &stat, ++off);
        if (entsize > rest_size) {
          VLOG(1) << fmt::format(
              "read dir entry is full, ino({}) fh({}) off({}) size({}/{}) "
              "entry_size({})",
              ino, fi->fh, off, buffer.size(), size, entsize);
          return false;
        }

        writed_size += entsize;

        return true;
      });

  if (!s.ok()) {
    LOG(ERROR) << fmt::format(
        "read dir fail, ino({}) fh({}) off({}) size({}) error({})", ino, fi->fh,
        off, size, s.ToString());
    ReplyError(req, s);
  } else {
    buffer.resize(writed_size);

    VLOG(1) << fmt::format("read dir success, ino({}) fh({}) off({}) size({}) ",
                           ino, fi->fh, off, buffer.size());
    ReplyBuf(req, buffer.data(), buffer.size());
  }
}

void FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info* fi) {
  VLOG(1) << fmt::format("read dir, ino({}) fh({}) off({}) size({})", ino,
                         fi->fh, off, size);

  CHECK_GE(off, 0) << "offset is illegal, offset: " << off;

  size_t writed_size = 0;
  std::string buffer(size, '\0');
  Status s = g_vfs->ReadDir(
      ino, fi->fh, off, true,
      [&](const dingofs::client::vfs::DirEntry& dir_entry, int32_t) -> bool {
        (void)off;
        VLOG(1) << fmt::format("read dir entry({}/{}) attr({})", dir_entry.name,
                               dir_entry.ino, Attr2Str(dir_entry.attr));

        fuse_entry_param fuse_entry;
        memset(&fuse_entry, 0, sizeof(fuse_entry_param));
        Attr2FuseEntry(dir_entry.attr, &fuse_entry);

        size_t rest_size = buffer.size() - writed_size;

        size_t entsize =
            fuse_add_direntry_plus(req, buffer.data() + writed_size, rest_size,
                                   dir_entry.name.c_str(), &fuse_entry, ++off);
        if (entsize > rest_size) {
          VLOG(1) << fmt::format(
              "read dir entry is full, ino({}) fh({}) off({}) size({}/{}) "
              "entry_size({})",
              ino, fi->fh, off, buffer.size(), size, entsize);
          return false;
        }
        writed_size += entsize;

        return true;
      });

  if (!s.ok()) {
    LOG(ERROR) << fmt::format(
        "read dir fail, ino({}) fh({}) off({}) size({}) error({})", ino, fi->fh,
        off, size, s.ToString());
    ReplyError(req, s);
  } else {
    buffer.resize(writed_size);
    VLOG(1) << fmt::format("read dir success, ino({}) fh({}) off({}) size({}) ",
                           ino, fi->fh, off, buffer.size());

    ReplyBuf(req, buffer.data(), buffer.size());
  }
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpReleaseDir ino: " << ino << ", fi->fh: " << fi->fh;
  Status s = g_vfs->ReleaseDir(ino, fi->fh);
  ReplyError(req, s);
}

void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    const char* value, size_t size, int flags) {
  VLOG(1) << "FuseOpSetXattr ino: " << ino << ", name: " << name
          << ", value: " << value << ", size: " << size << ", flags: " << flags;

  std::string strname(name);
  std::string strvalue(value, size);
  Status s = g_vfs->SetXattr(ino, strname, strvalue, flags);
  ReplyError(req, s);
}

void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    size_t size) {
  VLOG(1) << "FuseOpGetXattr ino: " << ino << ", name: " << name
          << ", size: " << size;

  std::string value;
  Status s = g_vfs->GetXattr(ino, name, &value);

  if (!s.ok()) {
    ReplyError(req, s);
    return;
  }

  if (size == 0) {
    // If size is 0, we just reply the size of the xattr
    ReplyXattr(req, value.size());
    return;
  }

  if (size < value.size()) {
    // If size is less than the length of the xattr, we return ERANGE
    ReplyError(req, Status::OutOfRange(absl::StrFormat(
                        "xattr size %zu is less than required %zu", size,
                        value.size())));
    return;
  }

  ReplyBuf(req, value.data(), value.size());
}

void FuseOpRemoveXattr(fuse_req_t req, fuse_ino_t ino, const char* name) {
  VLOG(1) << "FuseOpRemoveXattr ino: " << ino << ", name: " << name;
  std::string strname(name);
  Status s = g_vfs->RemoveXattr(ino, strname);
  ReplyError(req, s);
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  VLOG(1) << "FuseOpListXattr ino: " << ino << ", size: " << size;
  CHECK_GE(size, 0) << "size is illegal, size: " << size;

  std::vector<std::string> names;
  Status s = g_vfs->ListXattr(ino, &names);

  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    int buf_size = 0;
    for (auto& name : names) {
      // +1 because, the format is key\0key\0
      buf_size += name.size() + 1;
    }

    if (size == 0) {
      ReplyXattr(req, buf_size);
    } else {
      if (size < buf_size) {
        // ERANGE
        ReplyError(req, Status::OutOfRange(""));
        return;
      } else {
        std::unique_ptr<char[]> buf(new char[size]);
        char* p = buf.get();

        uint64_t ret_size = 0;
        for (auto& name : names) {
          // +1 for '\0'
          size_t attr_size = name.length() + 1;
          memcpy(p, name.c_str(), attr_size);
          p += attr_size;
          ret_size += attr_size;
        }

        VLOG(1) << "FuseOpListXattr return size: " << ret_size
                << ", ino: " << ino << ", param size: " << size
                << ", buf_size: " << buf_size
                << ", names.size: " << names.size();
        ReplyBuf(req, buf.get(), ret_size);
      }
    }
  }
}

void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char* name,
                  mode_t mode, struct fuse_file_info* fi) {
  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  uint32_t uid = ctx->uid;
  uint32_t gid = ctx->gid;

  VLOG(1) << "FuseOpCreate parent: " << parent << ", name: " << name
          << " uid: " << uid << ", gid: " << gid << ", mode: " << mode;

  uint64_t fh = 0;
  Attr attr;

  Status s = g_vfs->Create(parent, name, uid, gid, mode, fi->flags, &fh, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    fi->fh = fh;

    fi->direct_io = (dingofs::IsInternalNode(attr.ino) ||
                     FLAGS_client_fuse_file_info_direct_io)
                        ? 1
                        : 0;
    fi->keep_cache = FLAGS_client_fuse_file_info_keep_cache ? 1 : 0;

    ReplyCreate(req, fi, attr);
  }
}

#if FUSE_USE_VERSION < 35
void FuseOpIoctl(fuse_req_t req, fuse_ino_t ino, int cmd, void* arg,
                 struct fuse_file_info* fi, unsigned flags, const void* in_buf,
                 size_t in_bufsz, size_t out_bufsz) {
  Status s = Status::NotSupported(
      "FuseOpIoctl is not supported with fuse version lower than 3.5");

  ReplyError(req, s);
}
#else
void FuseOpIoctl(fuse_req_t req, fuse_ino_t ino, unsigned int cmd, void* arg,
                 struct fuse_file_info* fi, unsigned flags, const void* in_buf,
                 size_t in_bufsz, size_t out_bufsz) {
  (void)fi;
  (void)arg;
  VLOG(1) << "FuseOpIoctl ino: " << ino << ", cmd: " << cmd
          << ", flags: " << flags << ", in_bufsz: " << in_bufsz
          << ", out_bufsz: " << out_bufsz;

  const struct fuse_ctx* ctx = fuse_req_ctx(req);
  uint32_t uid = ctx->uid;

  std::string out_buf(out_bufsz, '\0');

  Status s = g_vfs->Ioctl(ino, uid, cmd, flags, in_buf, in_bufsz,
                          out_buf.data(), out_bufsz);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyIoctl(req, out_buf.data(), out_bufsz);
  }
}
#endif

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
  VLOG(1) << "FuseOpStatFs ino: " << ino;
  struct statvfs statfs;

  dingofs::client::vfs::FsStat vfs_stat;
  Status s = g_vfs->StatFs(ino, &vfs_stat);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyStatfs(req, vfs_stat);
  }
}
