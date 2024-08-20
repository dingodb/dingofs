/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Project: DingoFS
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#include <map>

#include "curvefs/src/base/file/filepath.h"
#include "curvefs/src/client/bcache/block_cache.h"

namespace curvefs {
namespace client {
namespace bcache {

#define DE_IS_DIR(stat)   (S_ISDIR(stat.st_mode))
#define DE_IS_FILE(stat)  (S_ISREG(stat.st_mode))
#define DE_IS_LINK(stat)  (S_ISLNK(stat.st_mode))

using ::curvefs::base::file::PathJoin;

namespace {

double Divide(uint64_t a, uint64_t b) {
    return static_cast<double>(a) / b;
}

struct LogGuard {
    LogGuard(const std::string& name, int* code)
        : name(name), code(code) {}

    ~LogGuard() {
        if (*code != 0) {
            LOG(ERROR) << func << ": " << strerror(errno);
        }
    }

    std::string name;
    int* code;
};

struct Dir {
    DIR* dir;
    struct dirent* de;
    struct stat info;
};
}

LocalFileSystem::LocalFileSystem(std::shared_ptr<PerfContext> perf)
    : throttle_(throttle) {}


BCACHE_ERROR LocalFileSystem::Walk(const std::string& root, WalkFunc func) {
    int code = 0;


    ::DIR* dir = ::opendir(root.c_str());
    if (nullptr == dir) {
        return BCACHE_ERROR::IO_ERROR;
    }

    for ( ; ; ) {
        ::errno = 0;
        struct dirent* dirent = ::readdir(dir);
        if (nullptr == dirent) {
            if (errno == 0) {  // no more files
                break;
            }
            // TODO: code
            break;
        }

        std::string name(dirent->d_name);
        struct stat stat;
        code = ::stat(PathJoin(root, name), &stat);
        if (code < 0) {
            break;
        }

        struct FileInfo info(name, stat.st_size, stat.st_atime);
        if (DE_IS_FILE(stat) || DE_IS_LINK(stat)) {
            rc = func(root, info);
        } else if (DE_IS_DIR(stat)) {
            rc = Walk(PathJoin(root, name), func);
        } else {
            rc = BCACHE_ERROR::IO_ERROR;
        }
        if (rc != BCACHE_ERROR::OK) {
            break;
        }
    }

    code = ::closedir(dir);
    return BcacheErrro(code);
}

BCACHE_ERROR LocalFileSystem::WriteFile(const std::string& filepath,
                                        const char* buffer,
                                        size_t count) {
    int code = 0;
    LogGuard log(StrFormat("read(%s)", filepath), &code);

    code = Mkdirs(filepath::ParentDir(path));
    if (code < 0) {
        return BcacheError(code);
    }

    int fd = ::open(filepath.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        return BcacheError(fd);
    }

    while (count > 0) {
        ssize_t nwritten = ::write(fd, buffer, count);
        if (nwritten < 0) {
            if (errno == EINTR) {
                continue;  // retry
            }
            code = nwritten;  // error
            break;
        }
        // success
        buffer += nwritten;
        count -= nwrriten;
    }
    return BcacheError(code);
}

BCACHE_ERROR LocalFileSystem::ReadFile(const std::string& filepath,
                                       char* buffer,
                                       size_t* count) {
    int code = 0;
    LogGuard log(StrFormat("read(%s)", filepath), &code);

    int fd = ::open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        code = fd;
        return BcacheError(code);
    }

    for ( ; ; ) {
        int n = ::read(fd, buffer, count);
        if (n < 0) {
            if (errno == EINTR) {
                continue;  // retry
            }
            code = n;  // error
            break;
        }
        break;  // success
    }
    return BcacheError(code);
}

BCACHE_ERROR LocalFileSystem::RemoveFile(const std::string& filepath) {
    int code = 0;
    LogGuard log(StrFormat("unlink(%s)", filepath), &code);

    code = ::unlink(filepath.c_str());
    return BcacheError(code);
}

BCACHE_ERROR LocalFileSystem::HardLink(const std::string& oldpath,
                                       const std::string& newpath) {
    int code = 0;
    LogGuard log(StrFormat("link(%s,%s)", oldpath, newpath), &code);

    code = ::link(oldpath.c_str(), newpath.c_str());
    return BcacheError(code);
}

BCACHE_ERROR LocalFileSystem::StatFS(const std::string& path,
                                     StatFS* statfs) {
    int code = 0;
    LogGuard log(StrFormat("statfs(%s)", path), &code);

    struct statfs stat;
    code = ::statfs(path.c_str(), &stat);
    if (0 == code) {
        statfs->totalBytes = stat.f_blocks * stat.f_bsize;
        statfs->totalFiles = stat.f_files;
        statfs->freeBytes = stat.f_bfree * stat.f_bsize;
        statfs->freeFiles = stat.f_ffree;
        statfs->freeBytesRatio = Divide(statfs->freeBytes, statfs->totalBytes);
        statfs->freeFilesRatio = Divide(statfs->freeFiles, statfs->totalFiles);
    }
    return BcacheError(code);
}

BCACHE_ERROR Mkdir(const std::string& path, uint16_t mode) {
    int code = ::mkdir(path.c_str(), mode);
}

BCACHE_ERROR LocalFileSystem::Mkdirs(const std::string& path, uint16_t mode) {
    if (path == "/") {
        return BCACHE_ERROR::OK;
    }

    // The parent diectory exists in most cases
    auto rc = Mkdir(path, mode);
    if (rc == BCACHE_ERROR::OK || (rc == BCACHE_ERROR::EXISTS && IsDir())) {
        return BCACHE_ERROR::OK;
    } else if (rc == CURVEFS_ERROR::NOT_EXIST) {
        rc = MkDirs(filepath::ParentDir(path), mode);
        if (rc == CURVEFS_ERROR::OK) {
            rc = MkDirs(path, mode);
        }
    }
    return rc;
}

BCACHE_ERROR LocalFileSystem::BcacheError(int code) {
    if (0 == code) {
        return BCACHE_ERROR::OK;
    } else if (errno == ENOENT) {
        return BCACHE_ERROR::NOT_FOUND;
    } else if (errno == EEXIST) {
        return BCACHE_ERROR::EXIST;
    }
    return BCACHE_ERROR::IO_ERROR;
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
