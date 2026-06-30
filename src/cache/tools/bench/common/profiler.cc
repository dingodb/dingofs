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

#include "cache/tools/bench/common/profiler.h"

#include "cache/tools/bench/common/file_server.h"
#include "cache/tools/flamegraph/flamegraph.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

namespace dingofs {
namespace cache {
namespace bench {

namespace {

// Bind to port 0 to let the kernel hand us a free ephemeral port, then close.
int PickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  int port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
      port = ntohs(addr.sin_port);
    }
  }
  ::close(fd);
  return port;
}

// Best-effort primary IP: a UDP "connect" sends nothing but makes the kernel
// pick the outbound interface, so getsockname yields the reachable address.
std::string PrimaryIp() {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return "127.0.0.1";
  sockaddr_in remote{};
  remote.sin_family = AF_INET;
  remote.sin_port = htons(53);
  ::inet_pton(AF_INET, "8.8.8.8", &remote.sin_addr);
  std::string ip = "127.0.0.1";
  if (::connect(fd, reinterpret_cast<sockaddr*>(&remote), sizeof(remote)) == 0) {
    sockaddr_in local{};
    socklen_t len = sizeof(local);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &len) == 0) {
      char buf[INET_ADDRSTRLEN];
      if (::inet_ntop(AF_INET, &local.sin_addr, buf, sizeof(buf)) != nullptr) {
        ip = buf;
      }
    }
  }
  ::close(fd);
  return ip;
}

// Locate an executable in PATH; returns full path or "".
std::string Which(const std::string& name) {
  const char* path = ::getenv("PATH");
  if (path == nullptr) return "";
  std::stringstream ss(path);
  std::string dir;
  while (std::getline(ss, dir, ':')) {
    if (dir.empty()) continue;
    std::string cand = dir + "/" + name;
    if (::access(cand.c_str(), X_OK) == 0) return cand;
  }
  return "";
}

// offcputime ships in different places per distro: in PATH on Debian/Ubuntu
// (offcputime-bpfcc), but /usr/share/bcc/tools/offcputime on RHEL/Fedora (NOT in
// PATH, no -bpfcc suffix). Probe all of them so no PATH setup is needed.
std::string ResolveOffcputime() {
  for (const char* name : {"offcputime-bpfcc", "offcputime"}) {
    std::string r = Which(name);
    if (!r.empty()) return r;
  }
  for (const char* path :
       {"/usr/share/bcc/tools/offcputime", "/usr/sbin/offcputime-bpfcc"}) {
    if (::access(path, X_OK) == 0) return path;
  }
  return "";
}

bool FileExists(const std::string& path) {
  return ::access(path.c_str(), R_OK) == 0;
}

bool FileNonEmpty(const std::string& path) {
  struct stat st{};
  return ::stat(path.c_str(), &st) == 0 && st.st_size > 0;
}

// Run perf script on the recorded data and fold it into native folded stacks.
// Uses /bin/sh only as a pipe to a file (always present, not a runtime dep we
// carry); the perf-script text is transient and removed afterwards.
bool PerfDataToFolded(const std::string& perf_bin, const std::string& data,
                      const std::string& folded) {
  const std::string txt = data + ".script.txt";
  const std::string cmd =
      perf_bin + " script -i '" + data + "' > '" + txt + "' 2>/dev/null";
  std::system(cmd.c_str());  // NOLINT(cert-env33-c)
  std::ifstream in(txt);
  bool ok = in && flamegraph::CollapsePerfScript(in, folded);
  in.close();
  ::unlink(txt.c_str());
  return ok;
}

}  // namespace

void RegisterProfileFlags(FlagSet* fs, ProfileOptions* o) {
  fs->Switch("flamegraph", &o->flamegraph,
             "capture on-cpu/off-cpu flame graphs and serve them over http");
  fs->Str("profile_mode", &o->mode, "flame graph mode: on_cpu | off_cpu | both");
  fs->U32("profile_freq", &o->freq, "on-cpu perf sampling frequency (Hz)");
  fs->Str("profile_dir", &o->dir,
          "flame graph output dir; empty = /tmp/cb-flame-<pid>");
  fs->U32("profile_port", &o->port, "http server port; 0 = auto-pick free port");
}

std::string ValidateProfile(const ProfileOptions& o) {
  if (!o.flamegraph) return "";
  if (o.mode != "on_cpu" && o.mode != "off_cpu" && o.mode != "both") {
    return "profile_mode must be on_cpu|off_cpu|both: " + o.mode;
  }
  if (o.freq == 0 || o.freq > 10000) return "profile_freq must be in [1, 10000]";
  if (o.port > 65535) return "profile_port must be <= 65535";
  return "";
}

pid_t Profiler::Spawn(const std::vector<std::string>& args,
                      const std::string& out_path,
                      const std::string& err_path) {
  // Build argv BEFORE fork: nothing between fork and exec may allocate.
  std::vector<char*> argv;
  argv.reserve(args.size() + 1);
  for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
  argv.push_back(nullptr);

  int out_fd = -1;
  if (!out_path.empty()) {
    out_fd = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) return -1;  // refuse to spawn without the intended redirect
  }
  int err_fd = -1;
  if (!err_path.empty()) {
    err_fd = (err_path == out_path)
                 ? out_fd
                 : ::open(err_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  }

  pid_t pid = ::fork();
  if (pid < 0) {
    if (out_fd >= 0) ::close(out_fd);
    if (err_fd >= 0 && err_fd != out_fd) ::close(err_fd);
    return -1;
  }
  if (pid == 0) {
    // Child: async-signal-safe calls only until exec.
    ::prctl(PR_SET_PDEATHSIG, SIGINT);  // die (and flush) if cb's main thread does
    if (out_fd >= 0) ::dup2(out_fd, STDOUT_FILENO);
    if (err_fd >= 0) ::dup2(err_fd, STDERR_FILENO);
    ::execvp(argv[0], argv.data());
    ::_exit(127);
  }
  if (out_fd >= 0) ::close(out_fd);
  if (err_fd >= 0 && err_fd != out_fd) ::close(err_fd);
  return pid;
}

void Profiler::Start() {
  if (!options_.flamegraph || started_) return;

  want_on_cpu_ = options_.mode == "on_cpu" || options_.mode == "both";
  want_off_cpu_ = options_.mode == "off_cpu" || options_.mode == "both";

  const std::string pid = std::to_string(::getpid());
  out_dir_ = options_.dir.empty() ? "/tmp/cb-flame-" + pid : options_.dir;
  std::error_code ec;
  std::filesystem::create_directories(out_dir_, ec);
  if (ec) {
    std::cerr << "[flamegraph] cannot create " << out_dir_ << ": " << ec.message()
              << "; profiling disabled\n";
    return;
  }

  if (want_on_cpu_) {
    perf_bin_ = Which("perf");
    if (perf_bin_.empty()) {
      std::cerr << "[flamegraph] perf not found in PATH; skipping on-cpu "
                   "(install: dnf install perf)\n";
      want_on_cpu_ = false;
    } else {
      const std::string log = out_dir_ + "/on_cpu.log";
      on_cpu_pid_ = Spawn({perf_bin_, "record", "-F", std::to_string(options_.freq),
                           "--call-graph", "dwarf,32768", "-p", pid, "-o",
                           out_dir_ + "/on_cpu.perf.data"},
                          log, log);
      if (on_cpu_pid_ < 0) {
        std::cerr << "[flamegraph] failed to launch perf; skipping on-cpu\n";
        want_on_cpu_ = false;
      }
    }
  }

  if (want_off_cpu_) {
    offcputime_bin_ = ResolveOffcputime();
    if (offcputime_bin_.empty()) {
      std::cerr << "[flamegraph] offcputime not found; skipping off-cpu "
                   "(install: dnf install bcc-tools)\n";
      want_off_cpu_ = false;
    } else {
      // Folded stacks go to stdout (-> off_cpu.folded); a big duration is cut
      // short by the SIGINT in Stop(), which makes offcputime print and exit.
      off_cpu_pid_ = Spawn({offcputime_bin_, "-d", "-f", "-p", pid, "99999"},
                           out_dir_ + "/off_cpu.folded", out_dir_ + "/off_cpu.log");
      if (off_cpu_pid_ < 0) {
        std::cerr << "[flamegraph] failed to launch offcputime; skipping off-cpu\n";
        want_off_cpu_ = false;
      }
    }
  }

  started_ = on_cpu_pid_ > 0 || off_cpu_pid_ > 0;
  if (started_) {
    std::cout << "[flamegraph] capturing "
              << (want_on_cpu_ ? "on-cpu " : "")
              << (want_off_cpu_ ? "off-cpu " : "") << "-> " << out_dir_ << '\n'
              << std::flush;
  }
}

void Profiler::Stop() {
  if (!started_) return;
  // Signal both first so they wind down together, then reap (no zombies before
  // the exec in RenderAndServe).
  if (on_cpu_pid_ > 0) ::kill(on_cpu_pid_, SIGINT);
  if (off_cpu_pid_ > 0) ::kill(off_cpu_pid_, SIGINT);
  int status = 0;
  if (on_cpu_pid_ > 0) {
    ::waitpid(on_cpu_pid_, &status, 0);
    on_cpu_pid_ = -1;
  }
  if (off_cpu_pid_ > 0) {
    ::waitpid(off_cpu_pid_, &status, 0);
    off_cpu_pid_ = -1;
  }
}

void Profiler::RenderAndServe() {
  if (!started_) return;

  const std::string on_cpu_data = out_dir_ + "/on_cpu.perf.data";
  const std::string on_cpu_folded = out_dir_ + "/on_cpu.folded";
  const std::string off_cpu_folded = out_dir_ + "/off_cpu.folded";
  const std::string on_cpu_svg = out_dir_ + "/on_cpu.svg";
  const std::string off_cpu_svg = out_dir_ + "/off_cpu.svg";

  std::cout << '\n';

  // on-cpu: perf.data -> folded (native) -> svg (native).
  if (want_on_cpu_ && FileExists(on_cpu_data)) {
    PerfDataToFolded(perf_bin_, on_cpu_data, on_cpu_folded);
    flamegraph::FlameOptions o;
    o.title = "cb on-cpu";
    o.palette = flamegraph::Palette::kHot;
    if (FileNonEmpty(on_cpu_folded) &&
        flamegraph::RenderFlameSvg(on_cpu_folded, on_cpu_svg, o)) {
      std::cout << "[flamegraph] on-cpu  : " << on_cpu_svg << '\n';
    }
  }

  // off-cpu: offcputime already wrote folded stacks; just render (native).
  if (want_off_cpu_ && FileNonEmpty(off_cpu_folded)) {
    flamegraph::FlameOptions o;
    o.title = "cb off-cpu";
    o.countname = "us";
    o.palette = flamegraph::Palette::kIo;
    if (flamegraph::RenderFlameSvg(off_cpu_folded, off_cpu_svg, o)) {
      std::cout << "[flamegraph] off-cpu : " << off_cpu_svg << '\n';
    }
  } else if (want_off_cpu_) {
    std::cout << "[flamegraph] off-cpu : no off-cpu samples captured "
                 "(workload was CPU-bound, or offcputime produced no stacks)\n";
  }

  // index.html linking whatever artifacts exist.
  {
    std::ofstream html(out_dir_ + "/index.html");
    html << "<!doctype html><html><head><meta charset=\"utf-8\">"
            "<title>cb flame graphs</title><style>body{font-family:sans-serif;"
            "margin:2rem}a{display:block;margin:.4rem 0}</style></head><body>"
            "<h1>cb flame graphs</h1><ul>";
    if (FileNonEmpty(on_cpu_svg)) {
      html << "<li><a href=\"on_cpu.svg\">on-cpu</a> &mdash; where CPU time goes"
              "</li>";
    }
    if (FileNonEmpty(off_cpu_svg)) {
      html << "<li><a href=\"off_cpu.svg\">off-cpu</a> &mdash; where threads "
              "block / wait (io &amp; locks)</li>";
    }
    html << "</ul><p>raw folded stacks: ";
    if (FileNonEmpty(on_cpu_folded)) html << "<a href=\"on_cpu.folded\">on_cpu</a> ";
    if (FileNonEmpty(off_cpu_folded)) html << "<a href=\"off_cpu.folded\">off_cpu</a>";
    html << "</p></body></html>";
  }

  const int port = options_.port != 0 ? static_cast<int>(options_.port)
                                       : PickFreePort();
  if (port == 0) {
    std::cout << "[flamegraph] no free port; open the files under " << out_dir_
              << " manually\n"
              << std::flush;
    return;
  }
  std::cout << "[flamegraph] serving : http://" << PrimaryIp() << ':' << port
            << "/   (Ctrl-C to stop)\n"
            << std::flush;

  ServeDirectory(out_dir_, port);  // blocks until Ctrl-C
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
