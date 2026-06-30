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

#include "cache/tools/bench/common/file_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

namespace dingofs {
namespace cache {
namespace bench {

namespace {

const char* ContentType(const std::string& path) {
  auto ends = [&](const char* ext) {
    size_t n = std::string(ext).size();
    return path.size() >= n && path.compare(path.size() - n, n, ext) == 0;
  };
  if (ends(".svg")) return "image/svg+xml";
  if (ends(".html")) return "text/html; charset=utf-8";
  if (ends(".folded") || ends(".txt") || ends(".log")) {
    return "text/plain; charset=utf-8";
  }
  return "application/octet-stream";
}

void SendAll(int fd, const std::string& data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::send(fd, data.data() + off, data.size() - off, MSG_NOSIGNAL);
    if (n <= 0) return;
    off += static_cast<size_t>(n);
  }
}

void Respond(int fd, const std::string& status, const char* ctype,
             const std::string& body, bool head) {
  std::ostringstream hdr;
  hdr << "HTTP/1.1 " << status << "\r\n"
      << "Content-Type: " << ctype << "\r\n"
      << "Content-Length: " << body.size() << "\r\n"
      << "Connection: close\r\n\r\n";
  SendAll(fd, hdr.str());
  if (!head) SendAll(fd, body);
}

void Handle(int fd, std::string dir) {
  char buf[8192];
  ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
  if (n <= 0) {
    ::close(fd);
    return;
  }
  buf[n] = '\0';

  // Parse the request line: "GET /path HTTP/1.1".
  std::istringstream req(buf);
  std::string method, target;
  req >> method >> target;
  bool head = method == "HEAD";
  if (method != "GET" && !head) {
    Respond(fd, "405 Method Not Allowed", "text/plain", "method not allowed",
            false);
    ::close(fd);
    return;
  }

  std::string path = target.substr(0, target.find('?'));
  if (path.empty() || path == "/") path = "/index.html";
  if (path.find("..") != std::string::npos) {  // no path traversal
    Respond(fd, "400 Bad Request", "text/plain", "bad request", false);
    ::close(fd);
    return;
  }

  std::ifstream f(dir + path, std::ios::binary);
  if (!f) {
    Respond(fd, "404 Not Found", "text/plain", "not found", head);
    ::close(fd);
    return;
  }
  std::ostringstream ss;
  ss << f.rdbuf();
  Respond(fd, "200 OK", ContentType(path), ss.str(), head);
  ::close(fd);
}

}  // namespace

void ServeDirectory(const std::string& dir, int port) {
  std::signal(SIGPIPE, SIG_IGN);  // a client hangup must not kill us

  int srv = ::socket(AF_INET, SOCK_STREAM, 0);
  if (srv < 0) {
    std::cerr << "[flamegraph] http: socket() failed\n";
    return;
  }
  int yes = 1;
  ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      ::listen(srv, 16) != 0) {
    std::cerr << "[flamegraph] http: bind/listen on port " << port
              << " failed\n";
    ::close(srv);
    return;
  }

  for (;;) {
    int cli = ::accept(srv, nullptr, nullptr);
    if (cli < 0) continue;
    std::thread(Handle, cli, dir).detach();
  }
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
