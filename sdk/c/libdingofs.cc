/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "libdingofs.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "binding_client.h"
#include "client/vfs/data_buffer.h"
#include "common/meta.h"
#include "common/status.h"

using dingofs::Attr;
using dingofs::DirEntry;
using dingofs::FsStat;
using dingofs::Ino;
using dingofs::Status;
using dingofs::client::BindingClient;
using dingofs::client::BindingConfig;
using dingofs::client::DataBuffer;
using dingofs::client::BindingClient;
using dingofs::client::IOVec;

static constexpr Ino kRootIno = 1;
static constexpr int kReadDirBatch = 128;  // entries per ReadDir call

// ============================================================================
// Internal per-instance data structures
// ============================================================================

// Per-fd state: inode, low-level file handle, current offset, open flags.
struct FileEntry {
    Ino      ino;
    uint64_t fh;
    int      flags;
    std::mutex mu;    // serialises offset reads/updates on this fd
    int64_t  offset;

    FileEntry(Ino i, uint64_t f, int fl)
        : ino(i), fh(f), flags(fl), offset(0) {}
};

// Per-directory-handle state: inode, low-level dir handle, entry buffer.
struct DirState {
    Ino      ino;
    uint64_t fh;
    uint64_t readdir_offset;   // resume offset for next ReadDir call
    bool     exhausted;
    std::vector<DirEntry> buf;

    DirState(Ino i, uint64_t f) : ino(i), fh(f), readdir_offset(0),
                                   exhausted(false) {}
};

// One mounted DingoFS instance.
struct DingoInstance {
    BindingClient client;
    BindingConfig config;    // accumulated by conf_set before mount

    // C callers typically don't manage glog themselves, so init_glog defaults
    // to true here.  Callers that already run glog can override this via
    // dingofs_conf_set(h, "init.glog", "false") before dingofs_mount().
    DingoInstance() { config.init_glog = true; }

    // --- file descriptor table ---
    std::mutex fd_mu;
    std::unordered_map<int, std::unique_ptr<FileEntry>> fds;
    int next_fd = 3;         // 0/1/2 reserved by POSIX convention

    // --- directory handle table ---
    std::mutex dh_mu;
    std::unordered_map<uint64_t, std::unique_ptr<DirState>> dirs;
    uint64_t next_dh = 1;
};

// ============================================================================
// Internal helpers
// ============================================================================

// Cast opaque handle back to the instance pointer.
static inline DingoInstance* inst(uintptr_t h) {
    return reinterpret_cast<DingoInstance*>(h);
}

// Translate a Status to a negative errno value suitable for return.
static int to_err(const Status& s) {
    if (s.ok()) return 0;
    int e = s.ToSysErrNo();
    return -(e > 0 ? e : EIO);
}

// Copy dingofs::Attr fields into a POSIX struct stat.
static void attr_to_stat(const Attr& a, struct stat* st) {
    memset(st, 0, sizeof(*st));
    st->st_ino     = a.ino;
    st->st_mode    = a.mode;
    st->st_nlink   = a.nlink;
    st->st_uid     = a.uid;
    st->st_gid     = a.gid;
    st->st_size    = static_cast<off_t>(a.length);
    st->st_rdev    = a.rdev;
    st->st_blksize = 4096;
    st->st_blocks  = (st->st_size + 511) / 512;
    // Attr timestamps are seconds since epoch (uint64_t).
    st->st_atim.tv_sec = static_cast<time_t>(a.atime);
    st->st_mtim.tv_sec = static_cast<time_t>(a.mtime);
    st->st_ctim.tv_sec = static_cast<time_t>(a.ctime);
}

// Resolve an absolute path string to an inode number by walking components
// one at a time via BindingClient::Lookup.
//
// Returns 0 on success with *out set, or -errno on failure.
static int lookup_path(BindingClient* c,
                       const std::string& raw_path, Ino* out) {
    // Strip trailing slashes; keep bare "/" intact.
    std::string path = raw_path;
    while (path.size() > 1 && path.back() == '/') path.pop_back();

    if (path == "/") {
        *out = kRootIno;
        return 0;
    }

    Ino cur = kRootIno;
    size_t pos = (path[0] == '/') ? 1 : 0;

    while (pos < path.size()) {
        size_t end = path.find('/', pos);
        if (end == std::string::npos) end = path.size();

        std::string comp = path.substr(pos, end - pos);
        if (!comp.empty() && comp != ".") {
            Attr attr{};
            Status s = c->Lookup(cur, comp, &attr);
            if (!s.ok()) return to_err(s);
            cur = attr.ino;
        }
        pos = end + 1;
    }
    *out = cur;
    return 0;
}

// Split "/a/b/c" → parent="/a/b", name="c".
static void split_path(const std::string& path,
                       std::string* parent, std::string* name) {
    // Strip trailing slash first.
    std::string p = path;
    while (p.size() > 1 && p.back() == '/') p.pop_back();

    size_t slash = p.rfind('/');
    if (slash == std::string::npos) {
        *parent = "/";
        *name   = p;
    } else if (slash == 0) {
        *parent = "/";
        *name   = p.substr(1);
    } else {
        *parent = p.substr(0, slash);
        *name   = p.substr(slash + 1);
    }
}

// Allocate a new fd (caller must hold fd_mu).
static int alloc_fd(DingoInstance* d,
                    Ino ino, uint64_t fh, int flags) {
    int fd = d->next_fd++;
    d->fds.emplace(fd, std::make_unique<FileEntry>(ino, fh, flags));
    return fd;
}

// Allocate a new directory handle (caller must hold dh_mu).
static uint64_t alloc_dh(DingoInstance* d, Ino ino, uint64_t fh) {
    uint64_t dh = d->next_dh++;
    d->dirs.emplace(dh, std::make_unique<DirState>(ino, fh));
    return dh;
}

// Look up a FileEntry; returns nullptr if fd is not open.
static FileEntry* get_file(DingoInstance* d, int fd) {
    auto it = d->fds.find(fd);
    return (it != d->fds.end()) ? it->second.get() : nullptr;
}

// Look up a DirState; returns nullptr if dh is not open.
static DirState* get_dir(DingoInstance* d, uint64_t dh) {
    auto it = d->dirs.find(dh);
    return (it != d->dirs.end()) ? it->second.get() : nullptr;
}

// Scatter-gather copy from a DataBuffer into a flat caller buffer.
// Returns the number of bytes copied.
static size_t gather_copy(const DataBuffer& data_buf,
                          void* dst_raw, size_t dst_cap) {
    std::vector<IOVec> iovecs = data_buf.GatherIOVecs();
    char*  dst    = static_cast<char*>(dst_raw);
    size_t copied = 0;
    for (const IOVec& iov : iovecs) {
        size_t n = std::min(static_cast<size_t>(iov.iov_len),
                            dst_cap - copied);
        if (n == 0) break;
        std::memcpy(dst + copied, iov.iov_base, n);
        copied += n;
    }
    return copied;
}

// ============================================================================
// Lifecycle
// ============================================================================

uintptr_t dingofs_new(void) {
    return reinterpret_cast<uintptr_t>(new DingoInstance());
}

void dingofs_delete(uintptr_t h) {
    delete inst(h);
}

int dingofs_conf_set(uintptr_t h, const char* key, const char* value) {
    if (!h || !key || !value) return -EINVAL;
    DingoInstance* d = inst(h);
    std::string k(key), v(value);

    // Map well-known keys to BindingConfig fields.
    if (k == "conf.file" || k == "conf_file") {
        d->config.conf_file = v;
        return 0;
    }
    if (k == "log.dir" || k == "log_dir") {
        d->config.log_dir = v;
        return 0;
    }
    if (k == "log.level" || k == "log_level") {
        d->config.log_level = v;
        return 0;
    }
    if (k == "log.v" || k == "log_v") {
        try { d->config.log_v = std::stoi(v); } catch (...) { return -EINVAL; }
        return 0;
    }
    // Unknown key: forward to process-wide gflags/tunable store.
    Status s = BindingClient::SetOption(k, v);
    return to_err(s);
}

int dingofs_conf_load(uintptr_t h, const char* path) {
    if (!h || !path || path[0] == '\0') return -EINVAL;
    // Verify the file exists before storing the path, so callers get an
    // immediate error rather than a confusing failure at mount time.
    if (::access(path, R_OK) != 0) return -errno;
    inst(h)->config.conf_file = path;
    return 0;
}

int dingofs_conf_get(uintptr_t h, const char* key,
                     char* value, size_t bufsz) {
    if (!h || !key || !value || bufsz == 0) return -EINVAL;
    DingoInstance* d = inst(h);
    std::string k(key);
    std::string v;

    if (k == "conf.file" || k == "conf_file") {
        v = d->config.conf_file;
    } else if (k == "log.dir" || k == "log_dir") {
        v = d->config.log_dir;
    } else if (k == "log.level" || k == "log_level") {
        v = d->config.log_level;
    } else if (k == "log.v" || k == "log_v") {
        v = std::to_string(d->config.log_v);
    } else {
        Status s = BindingClient::GetOption(k, &v);
        if (!s.ok()) return to_err(s);
    }

    if (v.size() + 1 > bufsz) return -ERANGE;
    std::memcpy(value, v.c_str(), v.size() + 1);
    return 0;
}

// ── conf list / print ─────────────────────────────────────────────────────────

int dingofs_conf_list(uintptr_t /*h*/, dingofs_option_t** opts, int* count) {
    if (!opts || !count) return -EINVAL;

    auto infos = BindingClient::ListOptions();
    int n = static_cast<int>(infos.size());

    // One allocation: array of structs + all string data packed after it.
    size_t strsz = 0;
    for (const auto& o : infos) {
        strsz += o.name.size() + 1;
        strsz += o.type.size() + 1;
        strsz += o.default_value.size() + 1;
        strsz += o.description.size() + 1;
    }

    char* raw = static_cast<char*>(
        std::malloc(static_cast<size_t>(n) * sizeof(dingofs_option_t) + strsz));
    if (!raw) return -ENOMEM;

    auto* arr = reinterpret_cast<dingofs_option_t*>(raw);
    char* sp  = raw + static_cast<size_t>(n) * sizeof(dingofs_option_t);

    auto copy_str = [&](const std::string& s) -> const char* {
        std::memcpy(sp, s.c_str(), s.size() + 1);
        const char* p = sp;
        sp += s.size() + 1;
        return p;
    };

    for (int i = 0; i < n; ++i) {
        arr[i].name          = copy_str(infos[i].name);
        arr[i].type          = copy_str(infos[i].type);
        arr[i].default_value = copy_str(infos[i].default_value);
        arr[i].description   = copy_str(infos[i].description);
    }

    *opts  = arr;
    *count = n;
    return 0;
}

void dingofs_conf_list_free(dingofs_option_t* opts, int /*count*/) {
    std::free(opts);
}

void dingofs_conf_print(uintptr_t /*h*/) {
    BindingClient::PrintOptions();
}

static int do_mount(uintptr_t h, const char* mds_addrs,
                    const char* fsname, const char* mountpoint,
                    bool init_glog) {
    if (!h || !mds_addrs || !fsname || !mountpoint) return -EINVAL;
    DingoInstance* d = inst(h);
    d->config.mds_addrs   = mds_addrs;
    d->config.fs_name     = fsname;
    d->config.mount_point = mountpoint;
    d->config.init_glog   = init_glog;
    Status s = d->client.Start(d->config);
    return to_err(s);
}

int dingofs_mount(uintptr_t h,
                  const char* mds_addrs,
                  const char* fsname,
                  const char* mountpoint) {
    return do_mount(h, mds_addrs, fsname, mountpoint, true);
}

int dingofs_mount_nolog(uintptr_t h,
                        const char* mds_addrs,
                        const char* fsname,
                        const char* mountpoint) {
    return do_mount(h, mds_addrs, fsname, mountpoint, false);
}

int dingofs_umount(uintptr_t h) {
    if (!h) return -EINVAL;
    Status s = inst(h)->client.Stop();
    return to_err(s);
}

// ============================================================================
// File I/O
// ============================================================================

int dingofs_open(uintptr_t h, const char* path, int flags, mode_t mode) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    uint64_t fh;

    if (flags & O_CREAT) {
        // Create or open: resolve parent, then call Create().
        std::string parent, name;
        split_path(path, &parent, &name);
        Ino parent_ino;
        int r = lookup_path(&d->client, parent, &parent_ino);
        if (r < 0) return r;

        uid_t uid = ::getuid();
        gid_t gid = ::getgid();
        Attr attr{};
        Status s = d->client.Create(parent_ino, name,
                                    uid, gid, mode, flags,
                                    &fh, &attr);
        if (!s.ok()) return to_err(s);
        ino = attr.ino;
    } else {
        // Open existing file: resolve path, then Open().
        int r = lookup_path(&d->client, path, &ino);
        if (r < 0) return r;
        Status s = d->client.Open(ino, flags, &fh);
        if (!s.ok()) return to_err(s);
    }

    std::lock_guard<std::mutex> lk(d->fd_mu);
    return alloc_fd(d, ino, fh, flags);
}

ssize_t dingofs_read(uintptr_t h, int fd, void* buf, size_t count) {
    if (!h || !buf) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }

    std::lock_guard<std::mutex> fe_lk(fe->mu);

    DataBuffer data_buf;
    uint64_t rsize = 0;
    Status s = d->client.Read(fe->ino, &data_buf,
                              count, static_cast<uint64_t>(fe->offset),
                              fe->fh, &rsize);
    if (!s.ok()) return to_err(s);

    size_t copied = gather_copy(data_buf, buf, count);
    fe->offset += static_cast<int64_t>(copied);
    return static_cast<ssize_t>(copied);
}

ssize_t dingofs_write(uintptr_t h, int fd,
                      const void* buf, size_t count) {
    if (!h || !buf) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }

    std::lock_guard<std::mutex> fe_lk(fe->mu);

    // Append mode: always write at the current end of file.
    uint64_t write_offset = static_cast<uint64_t>(fe->offset);
    if (fe->flags & O_APPEND) {
        Attr attr{};
        Status gs = d->client.GetAttr(fe->ino, &attr);
        if (!gs.ok()) return to_err(gs);
        write_offset = attr.length;
    }

    uint64_t wsize = 0;
    Status s = d->client.Write(fe->ino,
                               static_cast<const char*>(buf),
                               count, write_offset, fe->fh, &wsize);
    if (!s.ok()) return to_err(s);

    fe->offset = static_cast<int64_t>(write_offset + wsize);
    return static_cast<ssize_t>(wsize);
}

ssize_t dingofs_pread(uintptr_t h, int fd,
                      void* buf, size_t count, off_t offset) {
    if (!h || !buf) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }
    // pread does not modify the file offset — no fe->mu needed for offset.

    DataBuffer data_buf;
    uint64_t rsize = 0;
    Status s = d->client.Read(fe->ino, &data_buf,
                              count, static_cast<uint64_t>(offset),
                              fe->fh, &rsize);
    if (!s.ok()) return to_err(s);

    return static_cast<ssize_t>(gather_copy(data_buf, buf, count));
}

ssize_t dingofs_pwrite(uintptr_t h, int fd,
                       const void* buf, size_t count, off_t offset) {
    if (!h || !buf) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }
    // pwrite does not modify the file offset — no fe->mu needed for offset.

    uint64_t wsize = 0;
    Status s = d->client.Write(fe->ino,
                               static_cast<const char*>(buf),
                               count, static_cast<uint64_t>(offset),
                               fe->fh, &wsize);
    if (!s.ok()) return to_err(s);
    return static_cast<ssize_t>(wsize);
}

off_t dingofs_lseek(uintptr_t h, int fd, off_t offset, int whence) {
    if (!h) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }

    std::lock_guard<std::mutex> fe_lk(fe->mu);

    off_t new_off;
    if (whence == SEEK_SET) {
        new_off = offset;
    } else if (whence == SEEK_CUR) {
        new_off = fe->offset + offset;
    } else if (whence == SEEK_END) {
        Attr attr{};
        Status s = d->client.GetAttr(fe->ino, &attr);
        if (!s.ok()) return to_err(s);
        new_off = static_cast<off_t>(attr.length) + offset;
    } else {
        return -EINVAL;
    }

    if (new_off < 0) return -EINVAL;
    fe->offset = new_off;
    return new_off;
}

int dingofs_flush(uintptr_t h, int fd) {
    if (!h) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }
    return to_err(d->client.Flush(fe->ino, fe->fh));
}

int dingofs_fsync(uintptr_t h, int fd, int datasync) {
    if (!h) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }
    return to_err(d->client.Fsync(fe->ino, datasync, fe->fh));
}

int dingofs_close(uintptr_t h, int fd) {
    if (!h) return -EINVAL;
    DingoInstance* d = inst(h);

    std::unique_ptr<FileEntry> entry;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        auto it = d->fds.find(fd);
        if (it == d->fds.end()) return -EBADF;
        entry = std::move(it->second);
        d->fds.erase(it);
    }

    // Flush then release outside the fd table lock.
    Status fs = d->client.Flush(entry->ino, entry->fh);
    d->client.Release(entry->ino, entry->fh);  // best-effort
    return to_err(fs);
}

int dingofs_ftruncate(uintptr_t h, int fd, off_t length) {
    if (!h || length < 0) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }

    Attr in_attr{};
    in_attr.length = static_cast<uint64_t>(length);
    Attr out_attr{};
    Status s = d->client.SetAttr(fe->ino, dingofs::kSetAttrSize,
                                  in_attr, &out_attr);
    return to_err(s);
}

// ============================================================================
// Metadata
// ============================================================================

int dingofs_stat(uintptr_t h, const char* path, struct stat* st) {
    if (!h || !path || !st) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    Attr attr{};
    Status s = d->client.GetAttr(ino, &attr);
    if (!s.ok()) return to_err(s);

    attr_to_stat(attr, st);
    return 0;
}

// lstat: identical to stat for now — DingoFS Lookup returns the symlink
// inode itself (does not follow), so no extra step is needed.
int dingofs_lstat(uintptr_t h, const char* path, struct stat* st) {
    return dingofs_stat(h, path, st);
}

int dingofs_fstat(uintptr_t h, int fd, struct stat* st) {
    if (!h || !st) return -EINVAL;
    DingoInstance* d = inst(h);

    FileEntry* fe;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        fe = get_file(d, fd);
        if (!fe) return -EBADF;
    }

    Attr attr{};
    Status s = d->client.GetAttr(fe->ino, &attr);
    if (!s.ok()) return to_err(s);

    attr_to_stat(attr, st);
    return 0;
}

int dingofs_chmod(uintptr_t h, const char* path, mode_t mode) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    Attr in_attr{};
    in_attr.mode = mode;
    Attr out_attr{};
    Status s = d->client.SetAttr(ino, dingofs::kSetAttrMode,
                                  in_attr, &out_attr);
    return to_err(s);
}

int dingofs_chown(uintptr_t h, const char* path, uid_t owner, gid_t group) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    Attr in_attr{};
    uint32_t mask = 0;
    if (owner != static_cast<uid_t>(-1)) {
        in_attr.uid = owner;
        mask |= dingofs::kSetAttrUid;
    }
    if (group != static_cast<gid_t>(-1)) {
        in_attr.gid = group;
        mask |= dingofs::kSetAttrGid;
    }
    if (mask == 0) return 0;

    Attr out_attr{};
    Status s = d->client.SetAttr(ino, mask, in_attr, &out_attr);
    return to_err(s);
}

int dingofs_truncate(uintptr_t h, const char* path, off_t length) {
    if (!h || !path || length < 0) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    Attr in_attr{};
    in_attr.length = static_cast<uint64_t>(length);
    Attr out_attr{};
    Status s = d->client.SetAttr(ino, dingofs::kSetAttrSize,
                                  in_attr, &out_attr);
    return to_err(s);
}

int dingofs_utimes(uintptr_t h, const char* path,
                   const struct timeval tv[2]) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    Attr in_attr{};
    uint32_t mask = 0;
    if (tv) {
        in_attr.atime = static_cast<uint64_t>(tv[0].tv_sec);
        in_attr.mtime = static_cast<uint64_t>(tv[1].tv_sec);
        mask = dingofs::kSetAttrAtime | dingofs::kSetAttrMtime;
    } else {
        mask = dingofs::kSetAttrAtimeNow | dingofs::kSetAttrMtimeNow;
    }

    Attr out_attr{};
    Status s = d->client.SetAttr(ino, mask, in_attr, &out_attr);
    return to_err(s);
}

int dingofs_statfs(uintptr_t h, dingofs_statfs_t* st) {
    if (!h || !st) return -EINVAL;
    DingoInstance* d = inst(h);

    FsStat fs{};
    Status s = d->client.StatFs(kRootIno, &fs);
    if (!s.ok()) return to_err(s);

    st->total_bytes  = fs.max_bytes;
    st->used_bytes   = fs.used_bytes;
    st->total_inodes = fs.max_inodes;
    st->used_inodes  = fs.used_inodes;
    return 0;
}

// ============================================================================
// Directory operations
// ============================================================================

int dingofs_mkdir(uintptr_t h, const char* path, mode_t mode) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    std::string parent, name;
    split_path(path, &parent, &name);

    Ino parent_ino;
    int r = lookup_path(&d->client, parent, &parent_ino);
    if (r < 0) return r;

    uid_t uid = ::getuid();
    gid_t gid = ::getgid();
    Attr attr{};
    Status s = d->client.MkDir(parent_ino, name, uid, gid, mode, &attr);
    return to_err(s);
}

int dingofs_mkdirs(uintptr_t h, const char* path, mode_t mode) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    // Walk components left to right, creating each that doesn't exist.
    std::string p = path;
    while (p.size() > 1 && p.back() == '/') p.pop_back();

    uid_t uid = ::getuid();
    gid_t gid = ::getgid();

    Ino cur = kRootIno;
    size_t pos = (p[0] == '/') ? 1 : 0;

    while (pos <= p.size()) {
        size_t end = p.find('/', pos);
        if (end == std::string::npos) end = p.size();
        if (end == pos) { pos = end + 1; continue; }

        std::string comp = p.substr(pos, end - pos);

        // Try Lookup first.
        Attr attr{};
        Status ls = d->client.Lookup(cur, comp, &attr);
        if (ls.ok()) {
            cur = attr.ino;
        } else {
            // Component doesn't exist: create it.
            Status ms = d->client.MkDir(cur, comp, uid, gid, mode, &attr);
            if (!ms.ok()) return to_err(ms);
            cur = attr.ino;
        }
        pos = end + 1;
    }
    return 0;
}

int dingofs_rmdir(uintptr_t h, const char* path) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    std::string parent, name;
    split_path(path, &parent, &name);

    Ino parent_ino;
    int r = lookup_path(&d->client, parent, &parent_ino);
    if (r < 0) return r;

    return to_err(d->client.RmDir(parent_ino, name));
}

int dingofs_opendir(uintptr_t h, const char* path, uint64_t* dh) {
    if (!h || !path || !dh) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    uint64_t fh = 0;
    bool need_cache = false;
    Status s = d->client.OpenDir(ino, &fh, need_cache);
    if (!s.ok()) return to_err(s);

    std::lock_guard<std::mutex> lk(d->dh_mu);
    *dh = alloc_dh(d, ino, fh);
    return 0;
}

int dingofs_readdir(uintptr_t h, uint64_t dh,
                    dingofs_dirent_t* buf, int count) {
    if (!h || !buf || count <= 0) return -EINVAL;
    DingoInstance* d = inst(h);

    DirState* ds;
    {
        std::lock_guard<std::mutex> lk(d->dh_mu);
        ds = get_dir(d, dh);
        if (!ds) return -EBADF;
    }

    // Fill from the internal buffer first; refetch when empty.
    if (ds->buf.empty() && !ds->exhausted) {
        uint64_t last_offset = ds->readdir_offset;

        auto handler = [&](const DirEntry& entry, uint64_t off) -> bool {
            ds->buf.push_back(entry);
            last_offset = off;
            return static_cast<int>(ds->buf.size()) < kReadDirBatch;
        };

        Status s = d->client.ReadDir(ds->ino, ds->fh,
                                      ds->readdir_offset, true, handler);
        if (!s.ok()) return to_err(s);

        if (ds->buf.empty() || last_offset == ds->readdir_offset) {
            ds->exhausted = true;
        } else {
            ds->readdir_offset = last_offset;
            if (static_cast<int>(ds->buf.size()) < kReadDirBatch)
                ds->exhausted = true;
        }
    }

    int n = 0;
    while (n < count && !ds->buf.empty()) {
        const DirEntry& e = ds->buf.front();
        buf[n].d_ino = e.ino;
        // Derive d_type from mode bits stored in the attr.
        buf[n].d_type = IFTODT(e.attr.mode & S_IFMT);
        std::strncpy(buf[n].d_name, e.name.c_str(),
                     sizeof(buf[n].d_name) - 1);
        buf[n].d_name[sizeof(buf[n].d_name) - 1] = '\0';
        ds->buf.erase(ds->buf.begin());
        ++n;
    }
    return n;
}

int dingofs_closedir(uintptr_t h, uint64_t dh) {
    if (!h) return -EINVAL;
    DingoInstance* d = inst(h);

    std::unique_ptr<DirState> ds_ptr;
    {
        std::lock_guard<std::mutex> lk(d->dh_mu);
        auto it = d->dirs.find(dh);
        if (it == d->dirs.end()) return -EBADF;
        ds_ptr = std::move(it->second);
        d->dirs.erase(it);
    }

    return to_err(d->client.ReleaseDir(ds_ptr->ino, ds_ptr->fh));
}

// ============================================================================
// Hard links, symbolic links, rename, unlink
// ============================================================================

int dingofs_rename(uintptr_t h, const char* src, const char* dst) {
    if (!h || !src || !dst) return -EINVAL;
    DingoInstance* d = inst(h);

    std::string src_parent, src_name;
    std::string dst_parent, dst_name;
    split_path(src, &src_parent, &src_name);
    split_path(dst, &dst_parent, &dst_name);

    Ino src_ino, dst_ino;
    int r;
    if ((r = lookup_path(&d->client, src_parent, &src_ino)) < 0) return r;
    if ((r = lookup_path(&d->client, dst_parent, &dst_ino)) < 0) return r;

    return to_err(d->client.Rename(src_ino, src_name, dst_ino, dst_name));
}

int dingofs_unlink(uintptr_t h, const char* path) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    std::string parent, name;
    split_path(path, &parent, &name);

    Ino parent_ino;
    int r = lookup_path(&d->client, parent, &parent_ino);
    if (r < 0) return r;

    return to_err(d->client.Unlink(parent_ino, name));
}

int dingofs_link(uintptr_t h, const char* oldpath, const char* newpath) {
    if (!h || !oldpath || !newpath) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino src_ino;
    int r = lookup_path(&d->client, oldpath, &src_ino);
    if (r < 0) return r;

    std::string new_parent, new_name;
    split_path(newpath, &new_parent, &new_name);
    Ino new_parent_ino;
    if ((r = lookup_path(&d->client, new_parent, &new_parent_ino)) < 0)
        return r;

    Attr attr{};
    return to_err(d->client.Link(src_ino, new_parent_ino, new_name, &attr));
}

int dingofs_symlink(uintptr_t h, const char* target, const char* linkpath) {
    if (!h || !target || !linkpath) return -EINVAL;
    DingoInstance* d = inst(h);

    std::string parent, name;
    split_path(linkpath, &parent, &name);
    Ino parent_ino;
    int r = lookup_path(&d->client, parent, &parent_ino);
    if (r < 0) return r;

    uid_t uid = ::getuid();
    gid_t gid = ::getgid();
    Attr attr{};
    return to_err(d->client.Symlink(parent_ino, name, uid, gid, target,
                                     &attr));
}

ssize_t dingofs_readlink(uintptr_t h, const char* path,
                         char* buf, size_t bufsz) {
    if (!h || !path || !buf || bufsz == 0) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    std::string link;
    Status s = d->client.ReadLink(ino, &link);
    if (!s.ok()) return to_err(s);

    // POSIX readlink(2): does NOT NUL-terminate.
    size_t n = std::min(link.size(), bufsz);
    std::memcpy(buf, link.data(), n);
    return static_cast<ssize_t>(n);
}

// ============================================================================
// Extended attributes
// ============================================================================

int dingofs_setxattr(uintptr_t h, const char* path,
                     const char* name, const void* value, size_t size,
                     int flags) {
    if (!h || !path || !name || !value) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    std::string v(static_cast<const char*>(value), size);
    return to_err(d->client.SetXattr(ino, name, v, flags));
}

ssize_t dingofs_getxattr(uintptr_t h, const char* path,
                         const char* name, void* value, size_t size) {
    if (!h || !path || !name) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    std::string v;
    Status s = d->client.GetXattr(ino, name, &v);
    if (!s.ok()) return to_err(s);

    if (value == nullptr || size == 0) return static_cast<ssize_t>(v.size());
    if (v.size() > size) return -ERANGE;
    std::memcpy(value, v.data(), v.size());
    return static_cast<ssize_t>(v.size());
}

ssize_t dingofs_listxattr(uintptr_t h, const char* path,
                          char* list, size_t size) {
    if (!h || !path) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    std::vector<std::string> names;
    Status s = d->client.ListXattr(ino, &names);
    if (!s.ok()) return to_err(s);

    // Compute total size: each name + its NUL terminator.
    size_t total = 0;
    for (const auto& n : names) total += n.size() + 1;

    if (list == nullptr || size == 0) return static_cast<ssize_t>(total);
    if (total > size) return -ERANGE;

    char* p = list;
    for (const auto& n : names) {
        std::memcpy(p, n.data(), n.size() + 1);
        p += n.size() + 1;
    }
    return static_cast<ssize_t>(total);
}

int dingofs_removexattr(uintptr_t h, const char* path, const char* name) {
    if (!h || !path || !name) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    int r = lookup_path(&d->client, path, &ino);
    if (r < 0) return r;

    return to_err(d->client.RemoveXattr(ino, name));
}

// ── ioctl ─────────────────────────────────────────────────────────────────────

int dingofs_ioctl(uintptr_t h, int fd, unsigned int cmd, void* arg) {
    if (!h || !arg) return -EINVAL;
    DingoInstance* d = inst(h);

    Ino ino;
    {
        std::lock_guard<std::mutex> lk(d->fd_mu);
        auto it = d->fds.find(fd);
        if (it == d->fds.end()) return -EBADF;
        ino = it->second->ino;
    }

    // Decode direction and size from the cmd encoding (_IOC_DIR / _IOC_SIZE).
    //   bits 31:30 — direction: 0=none, 1=write(user→kernel), 2=read(kernel→user)
    //   bits 29:16 — data size in bytes
    unsigned int dir  = (cmd >> 30) & 0x3u;
    size_t       size = (cmd >> 16) & 0x3FFFu;

    const void* in_buf    = (dir & 1u) ? arg  : nullptr;
    size_t      in_bufsz  = (dir & 1u) ? size : 0;
    char*       out_buf   = (dir & 2u) ? static_cast<char*>(arg) : nullptr;
    size_t      out_bufsz = (dir & 2u) ? size : 0;

    uint32_t uid = static_cast<uint32_t>(getuid());
    return to_err(d->client.Ioctl(ino, uid, cmd, 0,
                                  in_buf, in_bufsz, out_buf, out_bufsz));
}
