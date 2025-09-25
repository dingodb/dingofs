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

#ifndef DINGOFS_SRC_CLIENT_FUSE_OPS_FUNC_H_
#define DINGOFS_SRC_CLIENT_FUSE_OPS_FUNC_H_

#include "client/fuse/fuse_op.h"

static const struct fuse_lowlevel_ops kFuseOp = {
    .init = FuseOpInit,
    .destroy = FuseOpDestroy,
    .lookup = FuseOpLookup,
    .forget = nullptr,
    .getattr = FuseOpGetAttr,
    .setattr = FuseOpSetAttr,
    .readlink = FuseOpReadLink,
    .mknod = FuseOpMkNod,
    .mkdir = FuseOpMkDir,
    .unlink = FuseOpUnlink,
    .rmdir = FuseOpRmDir,
    .symlink = FuseOpSymlink,
    .rename = FuseOpRename,
    .link = FuseOpLink,
    .open = FuseOpOpen,
    .read = FuseOpRead,
    .write = FuseOpWrite,
    .flush = FuseOpFlush,
    .release = FuseOpRelease,
    .fsync = FuseOpFsync,
    .opendir = FuseOpOpenDir,
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
    .readdir = nullptr,
#else
    .readdir = FuseOpReadDir,
#endif
    .releasedir = FuseOpReleaseDir,
    .fsyncdir = nullptr,
    .statfs = FuseOpStatFs,
    .setxattr = FuseOpSetXattr,
    .getxattr = FuseOpGetXattr,
    .listxattr = FuseOpListXattr,
    .removexattr = FuseOpRemoveXattr,
    .access = nullptr,
    .create = FuseOpCreate,
    .getlk = nullptr,
    .setlk = nullptr,
    .bmap = nullptr,
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
    .ioctl = nullptr,
    .poll = nullptr,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 9)
    .write_buf = nullptr,
    .retrieve_reply = nullptr,
    .forget_multi = nullptr,
    .flock = nullptr,
    .fallocate = nullptr,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
    .readdirplus = FuseOpReadDirPlus,
#else
    .readdirplus = nullptr,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 4)
    .copy_file_range = nullptr,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 8)
    .lseek = nullptr
#endif

};

#endif  // DINGOFS_SRC_CLIENT_FUSE_OPS_FUNC_H_