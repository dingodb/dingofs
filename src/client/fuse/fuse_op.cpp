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

#include <absl/strings/str_format.h>
#include <glog/logging.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "client/common/status.h"
#include "client/vfs/vfs_meta.h"
#include "client/vfs_wrapper/vfs_wrapper.h"
#include "utils/configuration.h"

static dingofs::client::vfs::VFSWrapper* g_vfs = nullptr;

using dingofs::client::Status;
using dingofs::client::vfs::Attr;
using dingofs::client::vfs::FsStat;

namespace {

void EnableSplice(struct fuse_conn_info* conn) {
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

void Attr2Stat(const Attr& attr, struct stat* stat) {
  stat->st_ino = attr.ino;            //  inode number
  stat->st_mode = attr.mode;          // permission mode
  stat->st_nlink = attr.nlink;        // number of links
  stat->st_uid = attr.uid;            // user ID of owner
  stat->st_gid = attr.gid;            // group ID of owner
  stat->st_size = attr.length;        // total size, in bytes
  stat->st_rdev = attr.rdev;          // device ID (if special file)
  stat->st_atim.tv_sec = attr.atime;  // time of last access
  stat->st_atim.tv_nsec = attr.atime_ns;
  stat->st_mtim.tv_sec = attr.mtime;  // time of last modification
  stat->st_mtim.tv_nsec = attr.mtime_ns;
  stat->st_ctim.tv_sec = attr.ctime;  // time of last status change
  stat->st_ctim.tv_nsec = attr.ctime_ns;
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
  attr.atime = stat->st_atim.tv_sec;
  attr.atime_ns = stat->st_atim.tv_nsec;
  attr.mtime = stat->st_mtim.tv_sec;
  attr.mtime_ns = stat->st_mtim.tv_nsec;
  attr.ctime = stat->st_ctim.tv_sec;
  attr.ctime_ns = stat->st_ctim.tv_nsec;
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

void ReplyCreate(fuse_req_t req, struct fuse_file_info* fi, const Attr& attr) {
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

static void ReplyStatfs(fuse_req_t req, const FsStat& stat) {
  uint64_t block_size = 4096;

  uint64_t total_bytes = stat.max_bytes;
  uint64_t total_blocks =
      ((total_bytes % block_size == 0) ? total_bytes / block_size
                                       : total_bytes / block_size + 1);

  uint64_t used_bytes = stat.used_bytes;

  uint64_t free_blocks = 0;
  if (total_bytes - used_bytes <= 0) {
    free_blocks = 0;
  } else {
    if (used_bytes > 0) {
      uint64_t used_blocks = (used_bytes % block_size == 0)
                                 ? used_bytes / block_size
                                 : used_bytes / block_size + 1;
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
      .mount_point = mount_option->mountPoint,
      .fs_name = mount_option->fsName,
      .config_path = mount_option->conf,
      .fs_type = mount_option->fsType,
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

  if (g_vfs->EnableSplice()) {
    VLOG(1) << "Enable splice";
    EnableSplice(conn);
  }

  g_vfs->Init();
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
  VLOG(1) << "FuseOpGetAttr inodeId=" << ino;
  Attr attr;
  Status s = g_vfs->GetAttr(ino, &attr);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyAttr(req, attr);
  }
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                   int to_set, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpSetAttr inodeId=" << ino;
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
  VLOG(1) << "FuseOpReadLink inodeId=" << ino;
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
  VLOG(1) << "FuseOpLink inodeId=" << ino << ", newparent: " << newparent
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
  VLOG(1) << "FuseOpOpen inodeId=" << ino;
  uint64_t fh = 0;
  Attr attr;
  Status s = g_vfs->Open(ino, fi->flags, &fh);

  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    fi->fh = fh;
    ReplyOpen(req, fi);
  }
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpRead inodeId=" << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh;
  std::unique_ptr<char[]> buffer(new char[size]);
  memset(buffer.get(), 0, size);

  uint64_t rsize = 0;
  Status s = g_vfs->Read(ino, buffer.get(), size, off, fi->fh, &rsize);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyData(req, buffer.get(), rsize);
  }
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
                 off_t off, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpWrite inodeId=" << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh;
  uint64_t wsize = 0;
  Status s = g_vfs->Write(ino, buf, size, off, fi->fh, &wsize);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyWrite(req, wsize);
  }
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpFlush inodeId=" << ino << ", fi->fh: " << fi->fh;
  Status s = g_vfs->Flush(ino, fi->fh);
  ReplyError(req, s);
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpRelease inodeId=" << ino << ", fi->fh: " << fi->fh;
  Status s = g_vfs->Release(ino, fi->fh);
  ReplyError(req, s);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                 struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpFsync inodeId=" << ino << ", datasync: " << datasync
          << ", fi->fh: " << fi->fh;

  Status s = g_vfs->Fsync(ino, datasync, fi->fh);
  ReplyError(req, s);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpOpenDir inodeId=" << ino;

  uint64_t fh = 0;
  Attr attr;
  Status s = g_vfs->OpenDir(ino, &fh);

  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    fi->fh = fh;
    ReplyOpen(req, fi);
  }
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpReadDir inodeId=" << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh;

  CHECK_GE(off, 0) << "offset is illegal, offset: " << off;

  std::unique_ptr<char[]> buffer(new char[size]);
  char* p = buffer.get();
  auto rem = size;

  int ret_count = 0;

  Status s = g_vfs->ReadDir(
      ino, fi->fh, off, false,
      [&](const dingofs::client::vfs::DirEntry& dir_entry,
          uint64_t next_offset) -> bool {
        struct stat stat;
        std::memset(&stat, 0, sizeof(stat));
        stat.st_ino = dir_entry.ino;

        auto entsize = fuse_add_direntry(req, p, rem, dir_entry.name.c_str(),
                                         &stat, next_offset);
        if (entsize > rem) {
          VLOG(1) << "read buffer is full, inodeId=" << ino << " size: " << size
                  << ", from offset: " << off << " next_offset: " << next_offset
                  << " fi->fh: " << fi->fh << " entsize: " << entsize
                  << " rem: " << rem;
          return false;
        }

        p += entsize;
        rem -= entsize;

        ret_count++;
        return true;
      });

  if (!s.ok()) {
    LOG(WARNING) << "Failed FuseOpReadDir  inodeId=" << ino
                 << ", size: " << size << ", offset: " << off
                 << ", fi->fh: " << fi->fh << " status: " << s.ToString();
    ReplyError(req, s);
  } else {
    VLOG(1) << "FuseOpReadDir return entry count: " << ret_count
            << ", inodeId=" << ino << ", size: " << size
            << ", from offset: " << off << ", rem: " << rem
            << ", fi->fh: " << fi->fh;

    ReplyBuf(req, buffer.get(), size - rem);
  }
}

void FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpReadDirPlus inodeId=" << ino << ", size: " << size
          << ", offset: " << off << ", fi->fh: " << fi->fh;

  CHECK_GE(off, 0) << "offset is illegal, offset: " << off;

  std::unique_ptr<char[]> buffer(new char[size]);
  char* p = buffer.get();
  auto rem = size;

  int ret_count = 0;

  Status s = g_vfs->ReadDir(
      ino, fi->fh, off, true,
      [&](const dingofs::client::vfs::DirEntry& dir_entry,
          uint64_t next_offset) -> bool {
        struct fuse_entry_param fuse_entry;
        memset(&fuse_entry, 0, sizeof(fuse_entry));
        Attr2FuseEntry(dir_entry.attr, &fuse_entry);

        auto entsize = fuse_add_direntry_plus(
            req, p, rem, dir_entry.name.c_str(), &fuse_entry, next_offset);
        if (entsize > rem) {
          VLOG(1) << "read buffer is full, inodeId=" << ino << " size: " << size
                  << ", from offset: " << off << " next_offset: " << next_offset
                  << " fi->fh: " << fi->fh << " entsize: " << entsize
                  << " rem: " << rem;
          return false;
        }

        p += entsize;
        rem -= entsize;

        ret_count++;
        return true;
      });

  if (!s.ok()) {
    LOG(WARNING) << "Failed FuseOpReadDirPlus  inodeId=" << ino
                 << ", size: " << size << ", offset: " << off
                 << ", fi->fh: " << fi->fh << " status: " << s.ToString();
    ReplyError(req, s);
  } else {
    VLOG(1) << "FuseOpReadDirPlus return entry count: " << ret_count
            << ", inodeId=" << ino << ", size: " << size
            << ", from offset: " << off << ", rem: " << rem
            << ", fi->fh: " << fi->fh;

    ReplyBuf(req, buffer.get(), size - rem);
  }
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info* fi) {
  VLOG(1) << "FuseOpReleaseDir inodeId=" << ino << ", fi->fh: " << fi->fh;
  Status s = g_vfs->ReleaseDir(ino, fi->fh);
  ReplyError(req, s);
}

void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    const char* value, size_t size, int flags) {
  VLOG(1) << "FuseOpSetXattr inodeId=" << ino << ", name: " << name
          << ", value: " << value << ", size: " << size << ", flags: " << flags;

  std::string strname(name);
  std::string strvalue(value, size);
  Status s = g_vfs->SetXattr(ino, strname, strvalue, flags);
  ReplyError(req, s);
}

void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    size_t size) {
  VLOG(1) << "FuseOpGetXattr inodeId=" << ino << ", name: " << name
          << ", size: " << size;

  std::string value;
  Status s = g_vfs->GetXattr(ino, name, &value);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyBuf(req, value.data(), value.size());
  }
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  VLOG(1) << "FuseOpListXattr inodeId=" << ino << ", size: " << size;
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
                << ", inodeId=" << ino << ", param size: " << size
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
    ReplyCreate(req, fi, attr);
  }
}

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
  VLOG(1) << "FuseOpStatFs inodeId=" << ino;
  struct statvfs statfs;

  dingofs::client::vfs::FsStat vfs_stat;
  Status s = g_vfs->StatFs(ino, &vfs_stat);
  if (!s.ok()) {
    ReplyError(req, s);
  } else {
    ReplyStatfs(req, vfs_stat);
  }
}
