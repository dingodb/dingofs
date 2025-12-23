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

#ifndef DINGOFS_SRC_CLIENT_FUSE_COMMON_H_
#define DINGOFS_SRC_CLIENT_FUSE_COMMON_H_

#define FUSE_USE_VERSION 317

#include <poll.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>

#include <csignal>
#include <cstddef>
#include <fstream>
#include <unordered_map>

#include "absl/strings/str_format.h"
#include "common/define.h"
#include "common/types.h"
#include "fuse3/fuse_lowlevel.h"
#include "fuse3/fuse_opt.h"
#include "utils/string.h"

using dingofs::utils::TrimChars;
using dingofs::utils::TrimSpace;

const std::string kFdCommPathKey = "fd_comm_path";

struct MountOption {
  std::string mount_point;
  std::string fs_name;
  dingofs::client::MetaSystemType metasystem_type;
  std::string mds_addrs;
  std::string storage_info;
};

inline int FuseAddOpts(struct fuse_args* args, const char* arg_value) {
  if (fuse_opt_add_arg(args, "-o") == -1) return 1;
  if (fuse_opt_add_arg(args, arg_value) == -1) return 1;
  return 0;
}

// Get file inode number
inline int GetFileInode(const std::string& file_name) {
  struct stat file_info;

  if (stat(file_name.c_str(), &file_info) == 0) {
    return file_info.st_ino;
  }
  return -1;
}

/**
 * Read dingo-client runtime information from .stats file
 * At present, the purpose is to obtain the PID of old dingo-client, and in the
 * Subsequent smooth upgrade, send signals old dingo-client
 * Returns key,value map
 *
 * @param filename path of .stats file
 */
inline std::unordered_map<std::string, std::string> LoadDingoRunTimeData(
    const std::string& filename) {
  std::unordered_map<std::string, std::string> result;
  std::ifstream file(filename);

  if (!file.is_open()) {
    return result;
  }

  const std::string delimiter = ":";
  std::string line;
  while (std::getline(file, line)) {
    size_t colon_pos = line.find(delimiter);
    if (colon_pos == std::string::npos) {
      continue;
    }
    std::string key = line.substr(0, colon_pos);
    std::string value = line.substr(colon_pos + delimiter.length());
    key = TrimSpace(key);
    value = TrimSpace(value);
    if (!key.empty()) {
      result[key] = value;
    }
  }
  return result;
}

/**
 * Get dingo-client pid from .stats file.
 * Returns pid, -1 on error.
 *
 * @param filename path of .stats file
 */
inline int GetDingoFusePid(const std::string& filename) {
  auto runtime_data = LoadDingoRunTimeData(filename);
  auto it = runtime_data.find("pid");
  if (it != runtime_data.end()) {
    return std::stoi(it->second);
  }

  return -1;
}

/**
 * Get dingo-client unix domain socket file from .stats file.
 * Returns filename, "" on error.
 *
 * @param filename path of .stats file
 */
inline std::string GetFdCommFileName(const std::string& filename) {
  auto runtime_data = LoadDingoRunTimeData(filename);
  auto it = runtime_data.find(kFdCommPathKey);
  if (it != runtime_data.end()) {
    return TrimChars(it->second, "\"");
  }

  return "";
}

/**
 * Check dingo-client mountpoint can support  smooth upgrade
 * Smooth upgrade requires new dingo-client mount at same mountpoint as old
 * Returns true,false.
 *
 * @param mountpoint dingo-client mountpoint
 */
inline bool CanShutdownGracefully(const std::string& mountpoint) {
  if (GetFileInode(mountpoint) != dingofs::ROOTINODEID) {
    return false;
  }
  return true;
}

/**
 * Umount libfuse filesystem after smooth upgrade
 * Returns true,false.
 *
 * @param mountpoint dingo-client mountpoint
 * @param fd file descriptor for /dev/fuse
 */
inline void DingoSessionUnmount(const std::string& mountpoint, int fd) {
  int res;

  if (fd != -1) {
    struct pollfd pfd;

    pfd.fd = fd;
    pfd.events = 0;
    res = poll(&pfd, 1, 0);

    /* Need to close file descriptor, otherwise synchronous umount
       would recurse into filesystem, and deadlock.

       Caller expects fuse_kern_unmount to close the fd, so close it
       anyway. */
    close(fd);

    /* If file poll returns POLLERR on the device file descriptor,
       then the filesystem is already unmounted or the connection
       was severed via /sys/fs/fuse/connections/NNN/abort */
    if (res == 1 && (pfd.revents & POLLERR)) return;
  }

  res = umount2(mountpoint.c_str(), 2);
  if (res == 0) return;
}

// check process is alive by pid
inline bool CheckProcessAlive(pid_t pid) {
  return (kill(pid, 0) == 0) || (errno != ESRCH);
}

#endif  // DINGOFS_SRC_CLIENT_FUSE_COMMON_H_