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

/*
 * libdingofs — C API for the DingoFS distributed filesystem
 *
 * DESIGN NOTES
 * ============
 * Handle (`uintptr_t h`)
 *   An opaque integer that represents one mounted filesystem instance.
 *   Using a plain integer rather than a pointer keeps the ABI stable across
 *   compiler versions and makes the handle trivially usable from any language
 *   FFI (ctypes, cgo, JNI, …) without exposing internal struct layouts.
 *
 * File descriptors (`int fd`)
 *   Per-instance small integers, NOT system file descriptors.  They are valid
 *   only within the same `h` that returned them.
 *
 * Error convention
 *   Every function returns 0 on success or −errno on failure.
 *   Read/write functions return the byte count (≥ 0) on success or −errno.
 *   The global `errno` is NOT modified.
 *
 * Thread safety
 *   All functions are thread-safe.  Different instances are fully independent.
 *   Concurrent reads/writes on the same fd are serialised internally.
 *
 * Logging
 * =======
 * dingofs_mount() initialises glog automatically and shuts it down in
 * dingofs_umount().  The following keys, set via dingofs_conf_set() or a
 * dingofs_conf_load() config file, control log behaviour:
 *
 *   "log.dir"   (path string, default "/tmp")
 *       Directory where glog writes its log files.
 *
 *   "log.level" ("INFO" | "WARNING" | "ERROR" | "FATAL", default "WARNING")
 *       Minimum severity for messages written to the log files.
 *
 *   "log.v"     (integer >= 0, default 0)
 *       VLOG verbosity level.  Higher values produce more detail.
 *
 * If the calling process already manages glog itself
 * (google::InitGoogleLogging() has already been called), use
 * dingofs_mount_nolog() instead.  It has the identical signature but skips
 * glog initialisation entirely; log.dir / log.level / log.v are ignored.
 *
 * Configuration file
 * ==================
 * Instead of (or in addition to) individual dingofs_conf_set() calls, you may
 * supply a configuration file via dingofs_conf_load():
 *
 *   dingofs_conf_load(h, "/etc/myapp/dingofs.conf");
 *
 * The file follows the gflags flags-file format:
 *   - One option per line:  --flag_name=value
 *   - Lines starting with # are comments; blank lines are ignored.
 *   - Only process-wide tunable flags (those visible via dingofs_conf_list())
 *     belong in the file.  log.dir / log.level / log.v may also be placed
 *     in the file.
 *
 * The file is applied at mount time.  Any dingofs_conf_set() calls made
 * *after* dingofs_conf_load() take precedence over the file contents.
 *
 * Example configuration file (/etc/myapp/dingofs.conf):
 *
 *   # DingoFS options
 *   --fuseClient.attrTimeOut=3600
 *   --fuseClient.entryTimeOut=3600
 *
 * Typical patterns:
 *
 *   // Library manages glog, log options set via conf_set
 *   dingofs_conf_set(h, "log.dir",   "/var/log/myapp");
 *   dingofs_conf_set(h, "log.level", "WARNING");
 *   dingofs_mount(h, "10.0.0.1:8801", fsname, mountpoint);
 *
 *   // Load tunable flags (including log.dir/level) from a file
 *   dingofs_conf_load(h, "/etc/myapp/dingofs.conf");
 *   dingofs_mount(h, "10.0.0.1:8801", fsname, mountpoint);
 *
 *   // Calling process already manages glog — skip glog init entirely
 *   dingofs_mount_nolog(h, "10.0.0.1:8801", fsname, mountpoint);
 *
 * Typical usage
 * =============
 *   uintptr_t h = dingofs_new();
 *   dingofs_mount(h, "10.0.0.1:8801", "myfs", "/mnt/myfs");
 *
 *   int fd = dingofs_open(h, "/data/file.dat", O_RDONLY, 0);
 *   char buf[4096];
 *   ssize_t n = dingofs_read(h, fd, buf, sizeof(buf));
 *   dingofs_close(h, fd);
 *
 *   dingofs_umount(h);
 *   dingofs_delete(h);
 */

#ifndef DINGOFS_C_LIBDINGOFS_H_
#define DINGOFS_C_LIBDINGOFS_H_

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>   /* struct stat, mode_t, off_t, … */
#include <sys/types.h>  /* uid_t, gid_t, ssize_t */
#include <sys/time.h>   /* struct timeval */
#include <dirent.h>     /* DT_REG, DT_DIR, DT_LNK, … */

#ifdef __cplusplus
extern "C" {
#endif


/* =========================================================================
 * Types
 * ========================================================================= */

/*
 * dingofs_dirent_t — directory entry returned by dingofs_readdir().
 *
 * d_ino   Inode number.
 * d_type  File type: DT_REG, DT_DIR, DT_LNK, DT_UNKNOWN, …
 * d_name  Null-terminated filename (not the full path).
 */
typedef struct {
    uint64_t d_ino;
    uint8_t  d_type;
    char     d_name[256];
} dingofs_dirent_t;

/*
 * dingofs_statfs_t — filesystem-level usage statistics.
 */
typedef struct {
    int64_t  total_bytes;   /* Total capacity in bytes  */
    int64_t  used_bytes;    /* Used bytes               */
    int64_t  total_inodes;  /* Total inode capacity     */
    int64_t  used_inodes;   /* Used inodes              */
} dingofs_statfs_t;


/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/*
 * dingofs_new() — allocate a new (unmounted) client instance.
 *
 * Returns an opaque handle.  The handle must be freed with dingofs_delete()
 * after dingofs_umount() has been called.
 */
uintptr_t dingofs_new(void);

/*
 * dingofs_delete() — free all memory associated with `h`.
 *
 * Behaviour is undefined if the filesystem is still mounted.
 * Passing 0 is a no-op.
 */
void dingofs_delete(uintptr_t h);

/*
 * dingofs_option_t — descriptor for one tunable option.
 *
 * All string fields are NUL-terminated and owned by the array returned by
 * dingofs_conf_list(); do NOT free them individually.
 */
typedef struct {
  const char* name;          /* flag name, used as the key in conf_set/get */
  const char* type;          /* value type, e.g. "string", "int32", "bool" */
  const char* default_value; /* compiled-in default */
  const char* description;   /* human-readable description */
} dingofs_option_t;

/*
 * dingofs_conf_list() — return all tunable options as an array.
 *
 * On success writes a pointer to a heap-allocated array of `*count` entries
 * to `*opts` and returns 0.  The caller must free the array with
 * dingofs_conf_list_free().  `h` may be 0 (options are process-global).
 *
 * Returns 0 on success, -ENOMEM on allocation failure.
 */
int dingofs_conf_list(uintptr_t h, dingofs_option_t** opts, int* count);

/*
 * dingofs_conf_list_free() — release an array returned by dingofs_conf_list().
 */
void dingofs_conf_list_free(dingofs_option_t* opts, int count);

/*
 * dingofs_conf_print() — print all tunable options to stdout.
 *
 * Useful for quick inspection from a shell or demo program.
 * `h` may be 0.
 */
void dingofs_conf_print(uintptr_t h);

/*
 * dingofs_conf_set() — set a configuration option before mounting.
 *
 * Instance-specific keys (handled directly, NOT forwarded to gflags, and
 * NOT supported in a dingofs_conf_load() config file — must always be set
 * explicitly via this function):
 *
 *   Logging (see "Logging" section in the file header for full details;
 *   these keys are ignored when using dingofs_mount_nolog()):
 *     "log.dir"          — directory for glog log files  (default: "/tmp")
 *     "log.level"        — min severity: INFO/WARNING/ERROR/FATAL
 *                          (default: "WARNING")
 *     "log.v"            — VLOG verbosity level  (default: "0")
 *
 * All other keys are forwarded to the process-wide gflags store and CAN be
 * placed in a dingofs_conf_load() config file.  Use dingofs_conf_print() or
 * dingofs_conf_list() to discover the full list of available keys.
 *
 * Returns 0 on success, -EINVAL if the key is unknown or value is invalid.
 */
int dingofs_conf_set(uintptr_t h, const char* key, const char* value);

/*
 * dingofs_conf_load() — load tunable options from a configuration file.
 *
 * The file must follow the gflags flags-file format: one "--key=value" per
 * line; lines beginning with '#' are treated as comments.  Only flags visible
 * via dingofs_conf_list() may appear in the file.  Instance-specific keys
 * ("mds.listen.addr", "log.dir", "log.level", "log.v", "init.glog") are not
 * stored in the flags-file and must be set with dingofs_conf_set().
 *
 * The file is read at dingofs_mount() time.  Calls to dingofs_conf_set() made
 * *after* dingofs_conf_load() override values in the file.
 *
 * Returns 0 on success, -EINVAL if `path` is NULL or empty,
 * -ENOENT if the file does not exist.
 */
int dingofs_conf_load(uintptr_t h, const char* path);

/*
 * dingofs_conf_get() — query a configuration option.
 *
 * Writes up to `bufsz` bytes (including the NUL terminator) to `value`.
 * Returns 0 on success, -EINVAL if the key is unknown,
 * -ERANGE if `bufsz` is too small.
 */
int dingofs_conf_get(uintptr_t h, const char* key,
                     char* value, size_t bufsz);

/*
 * dingofs_mount() — connect to the MDS and mount the named filesystem.
 *
 * Must be called after all dingofs_conf_set() / dingofs_conf_load() calls
 * and before any I/O.
 *
 *   `mds_addrs`  — (required) comma-separated MDS address list,
 *                  e.g. "10.0.0.1:8801,10.0.0.2:8801"
 *   `fsname`     — (required) name of the filesystem to mount, as created
 *                  on the MDS.
 *   `mountpoint` — (required) logical label recorded in server-side logs
 *                  and metrics; does NOT create an OS-level mount point.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_mount(uintptr_t h,
                  const char* mds_addrs,
                  const char* fsname,
                  const char* mountpoint);

/*
 * dingofs_mount_nolog() — mount without touching glog.
 *
 * Identical to dingofs_mount() except that glog initialisation is skipped.
 * Use this when the calling process has already called
 * google::InitGoogleLogging() and owns the glog lifecycle.
 * dingofs_umount() will NOT call ShutdownGoogleLogging() in this case.
 *
 * log.dir / log.level / log.v set via dingofs_conf_set() are ignored.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_mount_nolog(uintptr_t h,
                        const char* mds_addrs,
                        const char* fsname,
                        const char* mountpoint);

/*
 * dingofs_umount() — flush pending writes and disconnect from the MDS.
 *
 * All open file descriptors are implicitly flushed and closed.
 * Returns 0 on success, -errno on failure.
 */
int dingofs_umount(uintptr_t h);


/* =========================================================================
 * File I/O
 * ========================================================================= */

/*
 * dingofs_open() — open or create a file.
 *
 * `flags`  Standard open(2) flags: O_RDONLY, O_WRONLY, O_RDWR,
 *           O_CREAT, O_TRUNC, O_APPEND, …
 * `mode`   Creation mode bits (used only when O_CREAT is set).
 *
 * Returns a non-negative fd on success, -errno on failure.
 */
int dingofs_open(uintptr_t h, const char* path, int flags, mode_t mode);

/*
 * dingofs_read() — read from the current file position.
 *
 * Returns bytes read (0 = EOF), or -errno on failure.
 */
ssize_t dingofs_read(uintptr_t h, int fd, void* buf, size_t count);

/*
 * dingofs_write() — write at the current file position.
 *
 * Returns bytes written, or -errno on failure.
 */
ssize_t dingofs_write(uintptr_t h, int fd, const void* buf, size_t count);

/*
 * dingofs_pread() — read without changing the file position.
 *
 * Returns bytes read (0 = EOF), or -errno on failure.
 */
ssize_t dingofs_pread(uintptr_t h, int fd,
                      void* buf, size_t count, off_t offset);

/*
 * dingofs_pwrite() — write without changing the file position.
 *
 * Returns bytes written, or -errno on failure.
 */
ssize_t dingofs_pwrite(uintptr_t h, int fd,
                       const void* buf, size_t count, off_t offset);

/*
 * dingofs_lseek() — reposition the file offset.
 *
 * `whence`: SEEK_SET, SEEK_CUR, or SEEK_END.
 *
 * Returns the new absolute offset, or -errno on failure.
 */
off_t dingofs_lseek(uintptr_t h, int fd, off_t offset, int whence);

/*
 * dingofs_flush() — flush client-side write buffers to the server.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_flush(uintptr_t h, int fd);

/*
 * dingofs_fsync() — flush and sync file data to durable storage.
 *
 * If `datasync` is non-zero, only data (not metadata) is synced
 * (equivalent to fdatasync(2)).
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_fsync(uintptr_t h, int fd, int datasync);

/*
 * dingofs_close() — flush and release a file descriptor.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_close(uintptr_t h, int fd);

/*
 * dingofs_ftruncate() — resize an open file.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_ftruncate(uintptr_t h, int fd, off_t length);


/* =========================================================================
 * Metadata
 * ========================================================================= */

/*
 * dingofs_stat() — get file attributes, following symlinks.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_stat(uintptr_t h, const char* path, struct stat* st);

/*
 * dingofs_lstat() — get file attributes, NOT following the final symlink.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_lstat(uintptr_t h, const char* path, struct stat* st);

/*
 * dingofs_fstat() — get attributes of an open file by fd.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_fstat(uintptr_t h, int fd, struct stat* st);

/*
 * dingofs_chmod() — change permission bits.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_chmod(uintptr_t h, const char* path, mode_t mode);

/*
 * dingofs_chown() — change owner and/or group.
 *
 * Pass (uid_t)-1 or (gid_t)-1 to leave the respective field unchanged.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_chown(uintptr_t h, const char* path, uid_t owner, gid_t group);

/*
 * dingofs_truncate() — resize a file by path.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_truncate(uintptr_t h, const char* path, off_t length);

/*
 * dingofs_utimes() — set access and modification times.
 *
 * `tv[0]` = atime, `tv[1]` = mtime.
 * Pass NULL to set both to the current time.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_utimes(uintptr_t h, const char* path,
                   const struct timeval tv[2]);

/*
 * dingofs_statfs() — query filesystem-level usage statistics.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_statfs(uintptr_t h, dingofs_statfs_t* st);


/* =========================================================================
 * Directory operations
 * ========================================================================= */

/*
 * dingofs_mkdir() — create a directory.
 *
 * Returns 0 on success, -errno on failure (-EEXIST if it already exists).
 */
int dingofs_mkdir(uintptr_t h, const char* path, mode_t mode);

/*
 * dingofs_mkdirs() — create a directory and all missing parents (mkdir -p).
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_mkdirs(uintptr_t h, const char* path, mode_t mode);

/*
 * dingofs_rmdir() — remove an empty directory.
 *
 * Returns 0 on success, -errno on failure (-ENOTEMPTY if not empty).
 */
int dingofs_rmdir(uintptr_t h, const char* path);

/*
 * dingofs_opendir() — open a directory for reading.
 *
 * On success, writes a directory handle to `*dh`.
 * Returns 0 on success, -errno on failure.
 */
int dingofs_opendir(uintptr_t h, const char* path, uint64_t* dh);

/*
 * dingofs_readdir() — read up to `count` entries from an open directory.
 *
 * Entries are written to `buf[0..n-1]`.  Dot (".") and dotdot ("..")
 * entries are included.  Successive calls continue from where the last
 * call left off.
 *
 * Returns the number of entries read (0 = end of directory), or -errno.
 */
int dingofs_readdir(uintptr_t h, uint64_t dh,
                    dingofs_dirent_t* buf, int count);

/*
 * dingofs_closedir() — release a directory handle.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_closedir(uintptr_t h, uint64_t dh);


/* =========================================================================
 * Hard links, symbolic links, rename, unlink
 * ========================================================================= */

/*
 * dingofs_rename() — atomically rename/move `src` to `dst`.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_rename(uintptr_t h, const char* src, const char* dst);

/*
 * dingofs_unlink() — delete a file (or remove a hard-link name).
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_unlink(uintptr_t h, const char* path);

/*
 * dingofs_link() — create a hard link `newpath` pointing to `oldpath`.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_link(uintptr_t h, const char* oldpath, const char* newpath);

/*
 * dingofs_symlink() — create a symbolic link `linkpath` pointing to `target`.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_symlink(uintptr_t h, const char* target, const char* linkpath);

/*
 * dingofs_readlink() — read the target of a symbolic link.
 *
 * Writes up to `bufsz` bytes to `buf`.  The result is NOT NUL-terminated
 * (POSIX readlink(2) convention).
 *
 * Returns the number of bytes written to `buf`, or -errno on failure.
 */
ssize_t dingofs_readlink(uintptr_t h, const char* path,
                         char* buf, size_t bufsz);


/* =========================================================================
 * Extended attributes
 * ========================================================================= */

/*
 * dingofs_setxattr() — set extended attribute `name` on `path`.
 *
 * `flags`: 0, XATTR_CREATE (fail if exists), or XATTR_REPLACE (fail if not).
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_setxattr(uintptr_t h, const char* path,
                     const char* name, const void* value, size_t size,
                     int flags);

/*
 * dingofs_getxattr() — get extended attribute value.
 *
 * Writes up to `size` bytes to `value`.
 * Pass value=NULL and size=0 to query the required buffer size.
 *
 * Returns the attribute size in bytes, or -errno on failure.
 */
ssize_t dingofs_getxattr(uintptr_t h, const char* path,
                         const char* name, void* value, size_t size);

/*
 * dingofs_listxattr() — list all extended attribute names.
 *
 * Names are written to `list` as a sequence of NUL-terminated strings.
 * Pass list=NULL and size=0 to query the required buffer size.
 *
 * Returns total bytes written, or -errno on failure.
 */
ssize_t dingofs_listxattr(uintptr_t h, const char* path,
                          char* list, size_t size);

/*
 * dingofs_removexattr() — remove an extended attribute.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_removexattr(uintptr_t h, const char* path, const char* name);


/* =========================================================================
 * ioctl
 * ========================================================================= */

/*
 * dingofs_ioctl() — send a control command to an open file.
 *
 * Supports the following commands (defined in <linux/fs.h>):
 *   FS_IOC_GETFLAGS   / FS_IOC32_GETFLAGS   — read inode flags into *arg
 *   FS_IOC_SETFLAGS   / FS_IOC32_SETFLAGS   — write inode flags from *arg
 *   FS_IOC_FSGETXATTR                        — read extended inode flags
 *                                              into a struct fsxattr at *arg
 *
 * `arg` is used as both input and output buffer according to the direction
 * bits encoded in `cmd` (_IOC_DIR / _IOC_SIZE convention, same as Linux
 * ioctl(2)).  The buffer must be at least _IOC_SIZE(cmd) bytes.
 *
 * Returns 0 on success, -errno on failure.
 */
int dingofs_ioctl(uintptr_t h, int fd, unsigned int cmd, void* arg);


#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* DINGOFS_C_LIBDINGOFS_H_ */
