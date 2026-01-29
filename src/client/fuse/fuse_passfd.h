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

#ifndef DINGOFS_SRC_CLIENT_FUSE_PASSFD_H
#define DINGOFS_SRC_CLIENT_FUSE_PASSFD_H

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "glog/logging.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace client {
namespace fuse {
/**
 * Send a file descriptor over a Unix domain socket.
 * Returns 0 on success, -1 on error.
 *
 * @param socket unix domain socket
 * @param fd file descriptor to send
 * @param data data to send
 * @param data_size size of data to send
 */
inline int SendFd(int socket, int fd, void* data, size_t data_size) {
  struct msghdr msg = {nullptr};
  struct cmsghdr* cmsg = nullptr;
  char buf[CMSG_SPACE(sizeof(int))];
  struct iovec iov;

  msg.msg_control = buf;
  msg.msg_controllen = sizeof(buf);

  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));

  *((int*)CMSG_DATA(cmsg)) = fd;

  msg.msg_controllen = cmsg->cmsg_len;

  iov.iov_base = data;
  iov.iov_len = data_size;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  return sendmsg(socket, &msg, 0);
}

/**
 * Get a file descriptor from a Unix domain socket.
 * Returns fuse fd, -1 on error.
 *
 * @param socket unix domain socket
 * @param data_buf buffer to store receive data
 * @param data_bufsize buffer size store receive data
 * @param real_size real size of receive data
 */
inline int GetFd(int socket, void* data_buf, size_t data_bufsize,
                 size_t* real_size) {
  struct msghdr msg = {nullptr, 0, nullptr, 0, nullptr, 0, 0};
  struct cmsghdr* cmsg = nullptr;
  char ctl_buf[CMSG_SPACE(sizeof(int))];
  struct iovec iov;

  msg.msg_control = ctl_buf;
  msg.msg_controllen = sizeof(ctl_buf);

  iov.iov_base = data_buf;
  iov.iov_len = data_bufsize;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  ssize_t recv_size = recvmsg(socket, &msg, 0);
  if (recv_size == -1) {
    return -1;
  }
  *real_size = recv_size;

  cmsg = CMSG_FIRSTHDR(&msg);
  if (cmsg == nullptr || cmsg->cmsg_len != CMSG_LEN(sizeof(int))) {
    return -1;
  }
  if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
    return -1;
  }
  int tmpfd = *((int*)CMSG_DATA(cmsg));

  return tmpfd;
}

/**
 * Get /dev/fuse file descriptor from a Unix domain socket.
 * Returns fuse fd, -1 on error.
 *
 * @param fd_comm_path unix domain socket file path
 * @param data_buf buffer to store receive data
 * @param data_bufsize buffer size store receive data
 * @param real_size real size of receive data
 */
inline int GetFuseFd(const char* fd_comm_path, void* data_buf,
                     size_t data_bufsize, size_t* real_size) {
  int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (client_fd == -1) {
    LOG(ERROR) << "create socket failed, path: " << fd_comm_path
               << ", error: " << std::strerror(errno);
    return -1;
  }
  auto defer_close = MakeScopedCleanup([&]() { close(client_fd); });

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, fd_comm_path, sizeof(addr.sun_path) - 1);
  if (connect(client_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
    LOG(ERROR) << "connect uds server failed, path: " << fd_comm_path
               << ", error: " << std::strerror(errno);
    return -1;
  }

  int fuse_fd = GetFd(client_fd, data_buf, data_bufsize, real_size);
  if (fuse_fd == -1) {
    LOG(ERROR) << "get mount fd failed, error: " << std::strerror(errno);
    return -1;
  }

  return fuse_fd;
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_PASSFD_H