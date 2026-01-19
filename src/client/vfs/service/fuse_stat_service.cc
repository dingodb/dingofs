// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/service/fuse_stat_service.h"

#include <sys/types.h>

#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/metasystem/meta_system.h"
#include "common/version.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "json/value.h"
#include "utils/string.h"

namespace dingofs {
namespace client {
namespace vfs {

static std::string RenderHead(const std::string& title) {
  butil::IOBufBuilder os;

  os << fmt::format(R"(<head>{})", brpc::gridtable_style());
  os << fmt::format(R"(<script src="/js/sorttable"></script>)");
  os << fmt::format(
      R"(<script language="javascript" type="text/javascript" src="/js/jquery_min"></script>)");
  os << brpc::TabsHead();

  os << R"(<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  body {
    font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
  }
  .red-text {
  color: red;
  }
  .blue-text {
  color: blue;
  }
  .green-text {
  color: green;
  }
  .bold-text {
  font-weight: bold;
  }
  a {
    text-decoration: none;
  }
</style>)";

  os << fmt::format(R"(<title>{}</title>)", title);
  os << "</head>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static void RenderGitInfo(butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller">)";
  os << R"(<h3>Git</h3>)";
  os << R"(<div style="font-size:smaller;">)";

  auto infos = dingofs::DingoVersion();
  for (const auto& info : infos) {
    os << fmt::format("{}: {}", info.first, info.second);
    os << "<br>";
  }

  os << R"(</div>)";
  os << R"(</div>)";
}

static void RenderGitVersion(butil::IOBufBuilder& os) {
  os << R"(<div style="margin:2px;font-size:smaller;text-align:center">)";
  os << fmt::format(R"(<p >{} {} {}</p>)", dingofs::GetGitVersion(),
                    dingofs::GetGitCommitHash(), dingofs::GetGitCommitTime());
  os << "</div>";
}

static void RenderJsonPage(const std::string& title, const std::string& header,
                           const std::string& json, butil::IOBufBuilder& os) {
  os << R"(<!DOCTYPE html><html lang="zh-CN">)";

  os << R"(<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  )";
  os << fmt::format(R"(<title>{}</title>)", title);
  os << R"(
  <style>
    body {
      font-family: ui-monospace, SFMono-Regular, SF Mono, Menlo, Consolas, Liberation Mono, monospace;
      margin: 20px;
      background-color: #f5f5f5;
    }

    .container {
      max-width: 800px;
      margin: 0 auto;
      background-color: white;
      border-radius: 8px;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
      padding: 20px;
    }

    h1 {
      text-align: center;
      color: #333;
    }

    pre {
      background-color: #f9f9f9;
      border: 1px solid #ddd;
      border-radius: 4px;
      padding: 15px;
      overflow: auto;
      font-family: monospace;
      white-space: pre-wrap;
      line-height: 1.5;
    }

    .string {
      color: #008000;
    }

    .number {
      color: #0000ff;
    }

    .boolean {
      color: #b22222;
    }

    .null {
      color: #808080;
    }

    .key {
      color: #a52a2a;
    }
  </style>
</head>)";

  os << "<body>";
  os << R"(<div class="container">)";
  os << fmt::format("<h1>{}</h1>", header);
  os << R"(<pre id="json-display"></pre>)";
  os << "</div>";

  os << "<script>";
  if (!json.empty()) {
    os << "const jsonString =`" + json + "`;";
  } else {
    os << "const jsonString = \"{}\";";
  }

  os << R"(
    function syntaxHighlight(json) {
      if (typeof json === 'string') {
        json = JSON.parse(json);
      }

      json = JSON.stringify(json, null, 4);

      json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

      return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        let cls = 'number';

        if (/^"/.test(match)) {
          if (/:$/.test(match)) {
            cls = 'key';
          } else {
            cls = 'string';
          }
        } else if (/true|false/.test(match)) {
          cls = 'boolean';
        } else if (/null/.test(match)) {
          cls = 'null';
        }

        return '<span class="' + cls + '">' + match + '</span>';
      });
    }

    document.addEventListener('DOMContentLoaded', function () {
      try {
        const highlighted = syntaxHighlight(jsonString);
        document.getElementById('json-display').innerHTML = highlighted;
      } catch (e) {
        document.getElementById('json-display').innerHTML = 'Invalid JSON: ' + e.message;
      }
    });)";
  os << "</script>";

  os << "</body>";
  os << "</html>";
}

static void RenderNavigation(butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>Navagation </h3>)";

  auto render_navigation_func = []() -> std::string {
    std::string result;
    result += fmt::format(
        R"(<a href="FuseStatService/handler" target="_blank">handler</a>: show open file handler info at vfs)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/diriterator" target="_blank">dir iterator</a>: show dir iterator info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/filesession" target="_blank">file session</a>: show open file session info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/parentmemo" target="_blank">parent memo</a>: show parent memo info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/modifytimememo" target="_blank">modify time memo</a>: show modify time memo info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/chunkmemo" target="_blank">chunk memo</a>: show chunk memo info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/chunkcache" target="_blank">chunk cache</a>: show chunk cache info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/mdsrouter" target="_blank">mds router</a>: show mds router info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/inodecache" target="_blank">inode cache</a>: show inode cache info at meta)");
    result += "<br>";
    result += fmt::format(
        R"(<a href="FuseStatService/rpc" target="_blank">rpc</a>: show rpc info at meta)");
    return result;
  };

  os << render_navigation_func();
  os << "</div>";
}

static void RenderClientInfo(const Json::Value& json_value,
                             butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>Client </h3>)";

  // get client info
  const Json::Value& client_id = json_value["client_id"];

  if (client_id.empty()) {
    LOG(ERROR) << "no client to load";
    os << "</div>";
    return;
  }
  std::string id = client_id["id"].asString();
  std::string host_name = client_id["host_name"].asString();
  uint32_t port = client_id["port"].asUInt();
  std::string mount_point = client_id["mount_point"].asString();
  std::string mds_addr = client_id["mds_addr"].asString();

  os << fmt::format("id: {}", id);
  os << "<br>";
  os << fmt::format("hostname: {}", host_name);
  os << "<br>";
  os << fmt::format("port: {}", port);
  os << "<br>";
  os << fmt::format("mountpoint: {}", mount_point);
  os << "<br>";
  os << fmt::format("mds_addr: {}", mds_addr);
  os << "<br>";

  os << "</div>";
  os << "<br>";
}

static void RenderFsInfo(const Json::Value& json_value,
                         butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>FS </h3>)";

  // get fs info
  const Json::Value& fs_info = json_value["fs_info"];

  if (fs_info.empty()) {
    LOG(ERROR) << "no fs_info to load";
    os << "</div>";
    return;
  }

  std::string id = fs_info["id"].asString();
  std::string name = fs_info["name"].asString();
  std::string owner = fs_info["owner"].asString();
  uint64_t block_size = fs_info["block_size"].asUInt64();
  uint64_t chunk_size = fs_info["chunk_size"].asUInt64();
  uint64_t capacity = fs_info["capacity"].asUInt64();
  uint64_t create_time_s = fs_info["create_time_s"].asUInt64();
  uint64_t last_update_time_ns = fs_info["last_update_time_ns"].asUInt64();
  uint32_t recycle_time = fs_info["recycle_time"].asUInt();
  std::string s3_endpoint = fs_info["s3_endpoint"].asString();
  std::string s3_bucket = fs_info["s3_bucket"].asString();

  std::string rados_mon_host = fs_info["rados_mon_host"].asString();
  std::string rados_pool_name = fs_info["rados_pool_name"].asString();
  std::string rados_user_name = fs_info["rados_user_name"].asString();
  std::string rados_cluster_name = fs_info["rados_cluster_name"].asString();

  auto render_size_func = [&]() -> std::string {
    std::string result;
    result += fmt::format("chunk size: {}MB", chunk_size / (1024 * 1024));
    result += "<br>";
    result += fmt::format("block size: {}MB", block_size / (1024 * 1024));
    result += "<br>";
    result += fmt::format("capacity: {}MB", capacity / (1024 * 1024));
    result += "<br>";
    return result;
  };

  auto render_time_func = [&]() -> std::string {
    std::string result;
    result +=
        fmt::format("create time: {}", FormatTime(create_time_s / 1000000000));
    result += "<br>";
    result += fmt::format("update time: {}",
                          FormatTime(last_update_time_ns / 1000000000));
    result += "<br>";
    return result;
  };

  auto render_storage_func = [&]() -> std::string {
    std::string result;
    if (!s3_endpoint.empty()) {
      result +=
          fmt::format("s3 endpoint({}) bucket({})", s3_endpoint, s3_bucket);
      result += "<br>";
    } else if (!rados_mon_host.empty()) {
      result += fmt::format("rados mon_host({}) pool({}) user({}) cluster({})",
                            rados_mon_host, rados_pool_name, rados_user_name,
                            rados_cluster_name);
      result += "<br>";
    }

    return result;
  };

  os << fmt::format("id: {}", id);
  os << "<br>";
  os << fmt::format("name: {}", name);
  os << "<br>";
  os << render_size_func();
  os << fmt::format("owner: {}", owner);
  os << "<br>";
  os << render_time_func();
  os << fmt::format("recycle time: {}hour", recycle_time);
  os << "<br>";
  os << fmt::format("storage: {}", render_storage_func());
  os << "</div>";
  os << "<br>";
}

static void RenderMdsInfo(const Json::Value& json_value,
                          butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>MDS </h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Addr</th>";
  os << "<th>State</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Details</th>";
  os << "</tr>";

  // get client info
  const Json::Value& mdses = json_value["mdses"];
  if (!mdses.isArray()) {
    LOG(ERROR) << "mdses is not an array.";
    os << "</table>\n";
    os << "</div>";
    return;
  }
  if (mdses.empty()) {
    LOG(INFO) << "no mdses to load";
    os << "</table>\n";
    os << "</div>";
    return;
  }

  for (const auto& mds : mdses) {
    auto id = mds["id"].asUInt64();
    auto host = mds["host"].asString();
    auto port = mds["port"].asInt();
    auto state = mds["state"].asString();
    auto last_online_time_ms = mds["last_online_time_ms"].asUInt64();
    os << "<td>" << id << "</td>";
    os << fmt::format(
        R"(<td><a href="http://{}:{}/FsStatService" target="_blank">{}:{} </a></td>)",
        host, port, host, port);
    os << "<td>" << state << "</td>";
    os << "<td>" << utils::FormatMsTime(last_online_time_ms) << "</td>";

    os << fmt::format(
        R"(<td><a href="http://{}:{}/FsStatService/server" target="_blank">details</a></td>)",
        host, port);
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderHandlerInfoPage(const Json::Value& json_value,
                                  butil::IOBufBuilder& os,
                                  std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs dir iterator") << "</head>";
  os << "<body>";
  os << fmt::format(R"(<h1 style="text-align:center;">Client({}) Handler</h1>)",
                    client_name);

  // get handlers info
  const Json::Value& handlers = json_value["handlers"];
  if (!handlers.isArray()) {
    LOG(ERROR) << "handlers is not an array.";
    return;
  }
  if (handlers.empty()) {
    LOG(INFO) << "no handlers to load";
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Handler [{}]</h3>)", handlers.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Fh</th>";
  os << "<th>Flags</th>";
  os << "</tr>";

  for (const auto& handler : handlers) {
    Ino ino = handler["ino"].asUInt64();
    uint64_t fh = handler["fh"].asUInt64();
    uint flags = handler["flags"].asUInt();
    char flags_str[14];

    std::snprintf(flags_str, sizeof(flags_str), "0%o",
                  static_cast<uint32_t>(flags));

    os << "<td>" << ino << "</td>";
    os << "<td>" << fh << "</td>";
    os << "<td>" << flags_str << "</td>";
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static std::string RenderDirEntries(const Json::Value& entries) {
  std::string result;
  result += "ino,name,mode,nlink,uid,gid,length,rdev,atime,mtime,ctime,type";
  for (const auto& entry : entries) {
    result += "<br>";
    // get attr info
    uint64_t ino = entry["ino"].asUInt64();
    std::string name = entry["name"].asString();
    const Json::Value& attr = entry["attr"];

    uint32_t mode = attr["mode"].asUInt();
    uint32_t nlink = attr["nlink"].asUInt();
    uint32_t uid = attr["uid"].asUInt();
    uint32_t gid = attr["gid"].asUInt();
    uint64_t length = attr["length"].asUInt64();
    uint64_t rdev = attr["rdev"].asUInt64();
    uint64_t atime = attr["atime"].asUInt64();
    uint64_t mtime = attr["mtime"].asUInt64();
    uint64_t ctime = attr["ctime"].asUInt64();
    int32_t type = attr["type"].asInt();
    result += fmt::format("{},{},{},{},{},{},{},{},{},{},{},{}", ino, name,
                          mode, nlink, uid, gid, length, rdev,
                          utils::FormatMsTime(atime / 1000000),
                          utils::FormatMsTime(mtime / 1000000),
                          utils::FormatMsTime(ctime / 1000000), type);

    result += "<br>";
  }
  return result;
}

static void RenderDirInfoPage(const Json::Value& json_value,
                              butil::IOBufBuilder& os,
                              std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs dir iterator") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Dir Iterator</h1>)",
      client_name);

  // get dir_iterators info
  const Json::Value& dir_iterators = json_value["dir_iterators"];
  if (!dir_iterators.isArray()) {
    LOG(ERROR) << "dir_iterators is not an array.";
    return;
  }
  if (dir_iterators.empty()) {
    LOG(INFO) << "no dir_iterators to load";
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Dir Iterator [{}]</h3>)", dir_iterators.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Fh</th>";
  os << "<th>Ino</th>";
  os << "<th>Last name</th>";
  os << "<th>With attr</th>";
  os << "<th>Offset</th>";
  os << "<th>Entries</th>";
  os << "</tr>";

  for (const auto& dir_iterator : dir_iterators) {
    uint64_t fh = dir_iterator["fh"].asUInt64();
    uint64_t ino = dir_iterator["ino"].asUInt64();
    std::string last_name = dir_iterator["last_name"].asString();
    bool with_attr = dir_iterator["with_attr"].asBool();
    uint32_t offset = dir_iterator["offset"].asUInt();

    os << "<td>" << fh << "</td>";
    os << "<td>" << ino << "</td>";
    os << "<td>" << last_name << "</td>";
    os << "<td>" << with_attr << "</td>";
    os << "<td>" << offset << "</td>";
    os << "<td>";
    const Json::Value& entries = dir_iterator["entries"];
    if (!entries.isArray()) {
      LOG(ERROR) << "entries is not an array.";
      os << "</td>";
      os << "</tr>";
      continue;
    }
    if (entries.empty()) {
      LOG(INFO) << "no entries to load";
      os << "</td>";
      os << "</tr>";
      continue;
    }
    auto result = RenderDirEntries(entries);
    os << result;
    os << "</td>";
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static std::string RenderSessionIdMap(const Json::Value& json_value) {
  const Json::Value& session_id_map = json_value["session_id_map"];
  if (!session_id_map.isArray()) {
    LOG(ERROR) << "session_id_map is not an array.";
    return "";
  }
  if (session_id_map.empty()) {
    LOG(INFO) << "no session_id_map to load";
  }

  std::string result;
  for (const auto& session : session_id_map) {
    uint64_t fh = session["fh"].asUInt64();
    std::string session_id = session["session_id"].asString();
    result += fmt::format("{},{}", fh, session_id);
    result += "<br>";
  }
  return result;
}

static std::string RenderChunkMap(Ino ino, const Json::Value& json_value) {
  if (json_value.isNull()) return "";
  if (!json_value.isArray()) {
    LOG(ERROR) << "chunk is not array.";
    return "";
  }

  std::string result;
  result += "<div>";
  result += "<div>";
  for (const auto& value : json_value) {
    result += "<br>";
    result += fmt::format(
        R"({},{},<span style="color:{}">{}</span>,<span style="color:{}">{}</span>,{},{})",
        value["index"].asUInt64(), value["is_completed"].asBool(),
        value["stage_slices"].size() > 0 ? "red" : "green",
        value["stage_slices"].size(),
        value["commiting_slices"].size() > 0 ? "red" : "green",
        value["commiting_slices"].size(), value["commited_slices"].size(),
        value["commited_version"].asUInt64());
  }
  result += "</div>";
  result += "<div>";
  result += "<br>";
  result += fmt::format(
      R"(<a href="filesession/{}" target="_blank">details</a>)", ino);
  result += "</div>";
  result += "</div>";

  return result;
}

static void RenderFileSessionPage(const Json::Value& json_value,
                                  butil::IOBufBuilder& os,
                                  std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs file session") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) File Session</h1>)",
      client_name);

  // get dir_iterators info
  const Json::Value& file_sessions = json_value["file_sessions"];
  if (!file_sessions.isArray()) {
    LOG(ERROR) << "file_sessions is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>File Session [{}]</h3>)", file_sessions.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Ref Count</th>";
  os << "<th>Session<br>(fh, session_id)</th>";
  os << "<th>Chunks<br>(index,completed,stage,commiting,commited,version)</th>";
  os << "</tr>";

  for (const auto& file_session : file_sessions) {
    uint64_t ino = file_session["ino"].asUInt64();
    uint32_t ref_count = file_session["ref_count"].asUInt();
    const auto& chunk_set_value = file_session["chunk_set"];

    os << "<td>" << ino << "</td>";
    os << "<td>" << ref_count << "</td>";
    os << "<td>" << RenderSessionIdMap(file_session) << "</td>";
    if (!chunk_set_value.isNull()) {
      os << "<td>" << RenderChunkMap(ino, chunk_set_value["chunk_map"])
         << "</td>";
    } else {
      os << "<td></td>";
    }
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderSingleFileSessionPage(Ino ino, const Json::Value& json_value,
                                        butil::IOBufBuilder& os,
                                        std::string& client_name) {
  std::string header =
      fmt::format("Client({}) File Session({})", client_name, ino);

  RenderJsonPage("dingofs file session", header, json_value.toStyledString(),
                 os);
}

static void RenderSingleChunkPage(Ino ino, const Json::Value& json_value,
                                  butil::IOBufBuilder& os,
                                  std::string& client_name) {
  std::string header = fmt::format("Client({}) Chunk({})", client_name, ino);

  RenderJsonPage("dingofs chunk session", header, json_value.toStyledString(),
                 os);
}

static void RenderParentMemoPage(const Json::Value& json_value,
                                 butil::IOBufBuilder& os,
                                 std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs parent memo") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Parent Memo</h1>)",
      client_name);

  const Json::Value& items = json_value["parent_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "parent_memo value is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Parent Memo [{}]</h3>)", items.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Parent</th>";
  os << "<th>Version</th>";
  os << "</tr>";

  for (const auto& item : items) {
    auto ino = item["ino"].asUInt64();
    auto parent = item["parent"].asUInt64();
    auto version = item["version"].asUInt64();

    os << "<td>" << ino << "</td>";
    os << "<td>" << parent << "</td>";
    os << "<td>" << version << "</td>";
    os << "</tr>";
  }
  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderModifyTimeMemoPage(const Json::Value& json_value,
                                     butil::IOBufBuilder& os,
                                     std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs modify time memo") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Modify Time Memo</h1>)",
      client_name);

  const Json::Value& items = json_value["modify_time_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "modify_time_memo value is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Modify Time Memo [{}]</h3>)", items.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Modify Time</th>";
  os << "</tr>";

  for (const auto& item : items) {
    auto ino = item["ino"].asUInt64();
    auto modify_time_ns = item["modify_time_ns"].asUInt64();

    os << "<td>" << ino << "</td>";
    os << "<td>" << utils::FormatNsTime(modify_time_ns) << "</td>";
    os << "</tr>";
  }
  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderChunkMemoPage(const Json::Value& json_value,
                                butil::IOBufBuilder& os,
                                std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs chunk memo") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Chunk Memo</h1>)",
      client_name);

  const Json::Value& items = json_value["chunk_memo"];
  if (!items.isArray()) {
    LOG(ERROR) << "chunk_memo value is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Chunk Memo [{}]</h3>)", items.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>chunk_index</th>";
  os << "<th>Version</th>";
  os << "<th>Time</th>";
  os << "</tr>";

  for (const auto& item : items) {
    auto ino = item["ino"].asUInt64();
    auto chunk_index = item["chunk_index"].asUInt64();
    auto version = item["version"].asUInt64();
    auto time_ns = item["time_ns"].asUInt64();

    os << "<td>" << ino << "</td>";
    os << "<td>" << chunk_index << "</td>";
    os << "<td>" << version << "</td>";
    os << "<td>" << utils::FormatNsTime(time_ns) << "</td>";

    os << "</tr>";
  }
  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderChunkCachePage(const Json::Value& json_value,
                                 butil::IOBufBuilder& os,
                                 std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs chunk cache") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Chunk Cache</h1>)",
      client_name);

  const Json::Value& chunk_cache_value = json_value["chunk_cache"];
  if (!chunk_cache_value.isArray()) {
    LOG(ERROR) << "chunk_cache value is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Chunk Cache [{}]</h3>)", chunk_cache_value.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>WriteMemo<br>last_write_length last_time</th>";
  os << "<th>ChunkCount</th>";
  os << "<th>TaskCount</th>";
  os << "<th>NextTaskId</th>";
  os << "<th>LastActiveTime</th>";
  os << "<th>Details</th>";
  os << "</tr>";

  for (const auto& item : chunk_cache_value) {
    Ino ino = item["ino"].asUInt64();

    os << "<td>" << ino << "</td>";
    os << fmt::format(R"(<td>{} {}</td>)", item["last_write_length"].asUInt64(),
                      utils::FormatNsTime(item["last_time_ns"].asUInt64()));
    os << "<td>" << item["chunk_count"].asUInt64() << "</td>";
    os << "<td>" << item["commit_task_count"].asUInt64() << "</td>";
    os << "<td>" << item["id_generator"].asUInt64() << "</td>";

    os << "<td>" << utils::FormatTime(item["last_active_s"].asUInt64())
       << "</td>";

    // details
    os << fmt::format(
        R"(<td><a href="/FuseStatService/chunkset/{}" target="_blank">details</a></td>)",
        ino);

    os << "</tr>";
  }
  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderChunkSetPage(Ino ino, const Json::Value& json_value,
                               butil::IOBufBuilder& os,
                               std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs chunk set") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Chunk Set</h1>)",
      client_name);

  if (!json_value.isObject()) {
    LOG(ERROR) << "chunk_set value is not an object.";
    return;
  }

  // render write memo table
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << "<h3>Write Memo</h3>";
  os << fmt::format(R"(<div>last_write_length: {} last_time: {}</div>)",
                    json_value["last_write_length"].asUInt64(),
                    utils::FormatNsTime(json_value["last_time_ns"].asUInt64()));

  os << "</div>";

  // render commit task table
  const auto& commit_tasks_value = json_value["commit_tasks"];
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Commit Task [{}]</h3>)", commit_tasks_value.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Id</th>";
  os << "<th>State</th>";
  os << "<th>status</th>";
  os << "<th>retries</th>";
  os << "<th>Slices<br>count length</th>";
  os << "</tr>";

  for (const auto& item : commit_tasks_value) {
    const auto& delta_slices = item["delta_slices"];

    os << "<td>" << item["task_id"].asUInt64() << "</td>";
    os << "<td>" << item["state"].asUInt64() << "</td>";
    os << "<td>" << item["status"].asString() << "</td>";
    os << "<td>" << item["retries"].asUInt64() << "</td>";

    std::string delta_slice_str;
    for (const auto& delta_slice : delta_slices) {
      delta_slice_str +=
          fmt::format("chunk: {}", delta_slice["chunk_index"].asUInt64());
      delta_slice_str += "<br>";
      for (const auto& slice : delta_slice["slices"]) {
        delta_slice_str +=
            fmt::format("[{} {} {} {} {}]", slice["id"].asUInt64(),
                        slice["offset"].asUInt64(), slice["length"].asUInt64(),
                        slice["size"].asUInt64(), slice["is_zero"].asInt());
        delta_slice_str += "<br>";
      }
    }

    os << "<td>" << delta_slice_str << "</td>";

    os << "</tr>";
  }
  os << "</table>\n";
  os << "</div>";

  // render chunk table
  const auto& chunks_value = json_value["chunks"];
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Chunk [{}]</h3>)", chunks_value.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Index</th>";
  os << "<th>Completed</th>";
  os << "<th>Version</th>";
  os << "<th>LastCompactionTime</th>";
  os << "<th>StageSlices<br>id offset length size is_zero</th>";
  os << "<th>CommitingSlices<br>id offset length size is_zero</th>";
  os << "<th>CommitedSlices<br>id offset length size is_zero</th>";
  os << "<th>Details</th>";
  os << "</tr>";

  for (const auto& item : chunks_value) {
    const uint32_t chunk_index = item["index"].asUInt64();

    os << "<td>" << chunk_index << "</td>";
    os << "<td>" << item["is_completed"].asUInt64() << "</td>";
    os << "<td>" << item["commited_version"].asUInt64() << "</td>";
    os << "<td>"
       << utils::FormatMsTime(item["last_compaction_time_ms"].asUInt64())
       << "</td>";

    // stage slices
    std::string stage_slices_str;
    for (const auto& slice : item["stage_slices"]) {
      stage_slices_str +=
          fmt::format("[{} {} {} {} {}]", slice["id"].asUInt64(),
                      slice["offset"].asUInt64(), slice["length"].asUInt64(),
                      slice["size"].asUInt64(), slice["is_zero"].asInt());
      stage_slices_str += "<br>";
    }
    os << "<td>" << stage_slices_str << "</td>";

    // commiting slices
    std::string commiting_slices_str;
    for (const auto& slice : item["commiting_slices"]) {
      commiting_slices_str +=
          fmt::format("[{} {} {} {} {}]", slice["id"].asUInt64(),
                      slice["offset"].asUInt64(), slice["length"].asUInt64(),
                      slice["size"].asUInt64(), slice["is_zero"].asInt());
      commiting_slices_str += "<br>";
    }
    os << "<td>" << commiting_slices_str << "</td>";

    // commited slices
    std::string commited_slices_str;
    for (const auto& slice : item["commited_slices"]) {
      commited_slices_str +=
          fmt::format("[{} {} {} {} {}]", slice["id"].asUInt64(),
                      slice["offset"].asUInt64(), slice["length"].asUInt64(),
                      slice["size"].asUInt64(), slice["is_zero"].asInt());
      commited_slices_str += "<br>";
    }
    os << "<td>" << commited_slices_str << "</td>";

    // details link
    os << fmt::format(
        R"(<td><a href="/FuseStatService/chunk/{}/{}" target="_blank">details</a></td>)",
        ino, chunk_index);

    os << "</tr>";
  }
  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderMdsRouterPage(const Json::Value& json_value,
                                butil::IOBufBuilder& os,
                                std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs mds router") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) MDS Router</h1>)",
      client_name);

  const Json::Value& mds_routers = json_value["mds_routers"];
  if (!mds_routers.isArray()) {
    LOG(ERROR) << "mds_routers is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>MDS Router [{}]</h3>)", mds_routers.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Bucket ID</th>";
  os << "<th>ID</th>";
  os << "<th>Addr</th>";
  os << "<th>State</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Type</th>";
  os << "</tr>";

  for (const auto& mds_router : mds_routers) {
    auto bucket_id = mds_router["bucket_id"].asInt64();
    auto id = mds_router["id"].asUInt64();
    auto host = mds_router["host"].asString();
    auto port = mds_router["port"].asInt();
    auto state = mds_router["state"].asString();
    auto last_online_time_ms = mds_router["last_online_time_ms"].asUInt64();
    auto type = mds_router["type"].asString();
    os << "<td>" << bucket_id << "</td>";
    os << "<td>" << id << "</td>";
    os << fmt::format(
        R"(<td><a href="http://{}:{}/FsStatService" target="_blank">{}:{} </a></td>)",
        host, port, host, port);
    os << "<td>" << state << "</td>";
    os << "<td>" << utils::FormatMsTime(last_online_time_ms) << "</td>";
    os << "<td>" << type << "</td>";
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderInodeCachePage(const Json::Value& json_value,
                                 butil::IOBufBuilder& os,
                                 std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs inode cache") << "</head>";
  os << "<body>";
  os << fmt::format(
      R"(<h1 style="text-align:center;">Client({}) Inode Cache</h1>)",
      client_name);

  const Json::Value& inodes = json_value["inodes"];
  if (!inodes.isArray()) {
    LOG(ERROR) << "inodes is not an array.";
    return;
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Inode Cache [{}]</h3>)", inodes.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Type</th>";
  os << "<th>Length</th>";
  os << "<th>Uid</th>";
  os << "<th>Gid</th>";
  os << "<th>Mode</th>";
  os << "<th>NLink</th>";
  os << "<th>SymLink</th>";
  os << "<th>Rdev</th>";
  os << "<th>Ctime</th>";
  os << "<th>Mtime</th>";
  os << "<th>Atime</th>";
  os << "<th>Parents</th>";
  os << "<th>XAttr</th>";
  os << "<th>Version</th>";
  os << "</tr>";

  for (const auto& inode : inodes) {
    os << "<td>" << inode["ino"].asUInt64() << "</td>";
    os << "<td>" << inode["type"].asString() << "</td>";
    os << "<td>" << inode["length"].asUInt64() << "</td>";
    os << "<td>" << inode["uid"].asUInt() << "</td>";
    os << "<td>" << inode["gid"].asUInt() << "</td>";
    os << "<td>" << inode["mode"].asUInt() << "</td>";
    os << "<td>" << inode["nlink"].asUInt() << "</td>";
    os << "<td>" << inode["symlink"].asString() << "</td>";
    os << "<td>" << inode["rdev"].asUInt64() << "</td>";
    os << "<td>" << utils::FormatMsTime(inode["ctime"].asUInt64() / 1000000)
       << "</td>";
    os << "<td>" << utils::FormatMsTime(inode["mtime"].asUInt64() / 1000000)
       << "</td>";
    os << "<td>" << utils::FormatMsTime(inode["atime"].asUInt64() / 1000000)
       << "</td>";

    // parents
    std::vector<uint64_t> parent_inos;
    for (const auto& parent : inode["parents"]) {
      parent_inos.push_back(parent.asUInt64());
    }
    os << "<td>" << fmt::format("{}", fmt::join(parent_inos, ",")) << "</td>";

    // xattr
    std::string xattr_str;
    for (const auto& xattr : inode["xattrs"]) {
      xattr_str += fmt::format("{}:{},", xattr["key"].asString(),
                               xattr["value"].asString());
    }
    os << "<td>" << xattr_str << "</td>";

    os << "<td>" << inode["version"].asUInt64() << "</td>";

    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderRPCPage(const Json::Value& json_value,
                          butil::IOBufBuilder& os, std::string& client_name) {
  os << "<!DOCTYPE html><html>";

  os << "<head>" << RenderHead("dingofs rpc") << "</head>";
  os << "<body>";
  os << fmt::format(R"(<h1 style="text-align:center;">Client({}) RPC</h1>)",
                    client_name);

  const Json::Value& endpoint = json_value["init_endpoint"];

  if (endpoint.empty()) {
    LOG(ERROR) << "no endpoint to load";
  }
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<div><h3>Init EndPoint: {} </h3></div>)",
                    endpoint.asString());
  os << "</div>";

  const Json::Value& channels = json_value["channels"];
  if (!channels.isArray()) {
    LOG(ERROR) << "channels is not an array.";
    return;
  }
  if (channels.empty()) {
    LOG(INFO) << "no channels to load";
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Channel EndPoint [{}]</h3>)", channels.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Channel EndPoint</th>";
  os << "</tr>";
  std::string channel_endpoint;

  for (const auto& channel : channels) {
    channel_endpoint += "<div>";
    channel_endpoint += channel["endpoint"].asString();
    channel_endpoint += "<br>";
    channel_endpoint += "</div>";
  }

  os << "<tr>";
  os << "<td>" << channel_endpoint << "</td>";
  os << "</tr>";
  os << "</table>\n";
  os << "</div>";

  const Json::Value& fallbacks = json_value["fallbacks"];
  if (!fallbacks.isArray()) {
    LOG(ERROR) << "fallbacks is not an array.";
    return;
  }
  if (fallbacks.empty()) {
    LOG(INFO) << "no fallbacks to load";
  }

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << fmt::format(R"(<h3>Fallback EndPoint [{}]</h3>)", fallbacks.size());
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Fallback EndPoint</th>";
  os << "</tr>";
  std::string fallback_endpoint;

  for (const auto& fallback : fallbacks) {
    fallback_endpoint += "<div>";
    fallback_endpoint += fallback["endpoint"].asString();
    fallback_endpoint += "<br>";
    fallback_endpoint += "</div>";
  }

  os << "<tr>";
  os << "<td>" << fallback_endpoint << "</td>";
  os << "</tr>";

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

void FuseStatServiceImpl::RenderMainPage(const brpc::Server* server,
                                         butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>\n";

  os << "<head>";
  os << RenderHead("dingofs-client dashboard");
  os << "</head>";

  os << "<body>";
  server->PrintTabsBody(os, "fusesstat");
  os << R"(<h1 style="text-align:center;">dingofs-client dashboard</h1>)";

  RenderGitVersion(os);

  Json::Value meta_value;

  if (!vfs_hub_->GetMetaSystem().GetDescription(meta_value)) {
    LOG(ERROR) << fmt::format("GetDescription failed.");
    os << "</body>";
    os << "</html>";
    return;
  }

  // client info
  RenderClientInfo(meta_value, os);
  // fs info
  RenderFsInfo(meta_value, os);
  // mds info
  RenderMdsInfo(meta_value, os);
  // navigation
  RenderNavigation(os);
  // git info
  RenderGitInfo(os);

  os << "</body>";
  os << "</html>";
}

void FuseStatServiceImpl::default_method(
    ::google::protobuf::RpcController* controller,
    const pb::web::FuseStatRequest*, pb::web::FuseStatResponse*,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const bool use_html = brpc::UseHTML(cntl->http_request());
  cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");
  const std::string& path = cntl->http_request().unresolved_path();

  LOG(INFO) << fmt::format("FuseStatService path: {}", path);

  auto client_id = vfs_hub_->GetClientId();
  std::string client_name =
      fmt::format("{}:{}", client_id.Hostname(), client_id.Port());

  std::vector<std::string> params;
  SplitString(path, '/', params);

  LOG(INFO) << fmt::format("FuseStatService params size: {}.", params.size());

  // /FuseStatService
  if (params.empty()) {
    RenderMainPage(server, os);

  } else if (params.size() == 1) {
    const std::string& api_name = params[0];

    Json::Value json_value;
    DumpOption options;

    if (api_name == "diriterator") {
      // /FuseStatService/diriterator
      options.dir_iterator = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderDirInfoPage(json_value, os, client_name);
      }
    } else if (api_name == "filesession") {
      // /FuseStatService/filesession
      options.file_session = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderFileSessionPage(json_value, os, client_name);
      }

    } else if (api_name == "handler") {
      // /FuseStatService/handler
      if (!vfs_hub_->GetHandleManager()->Dump(json_value)) {
        cntl->SetFailed("GetHandleManager failed.");
        return;
      }
      RenderHandlerInfoPage(json_value, os, client_name);

    } else if (api_name == "parentmemo") {
      // /FuseStatService/parentmemo
      options.parent_memo = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderParentMemoPage(json_value, os, client_name);
      }

    } else if (api_name == "modifytimememo") {
      // /FuseStatService/modifytimememo
      options.modify_time_memo = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderModifyTimeMemoPage(json_value, os, client_name);
      }

    } else if (api_name == "chunkmemo") {
      // /FuseStatService/chunkmemo
      options.chunk_memo = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderChunkMemoPage(json_value, os, client_name);
      }

    } else if (api_name == "chunkcache") {
      // /FuseStatService/chunkcache
      options.chunk_cache = true;
      options.is_summary = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderChunkCachePage(json_value, os, client_name);
      }

    } else if (api_name == "mdsrouter") {
      // /FuseStatService/mdsrouter
      options.mds_router = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderMdsRouterPage(json_value, os, client_name);
      }

    } else if (api_name == "inodecache") {
      // /FuseStatService/inodecache
      options.inode_cache = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderInodeCachePage(json_value, os, client_name);
      }

    } else if (api_name == "rpc") {
      // /FuseStatService/rpc
      options.rpc = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderRPCPage(json_value, os, client_name);
      }
    } else {
      return cntl->SetFailed("unknown path: " + path);  // NOLINT
    }

  } else if (params.size() == 2) {
    LOG(INFO) << "Dump chunkset for ino: ";

    const std::string& api_name = params[0];
    const Ino ino = strtoull(params[1].c_str(), nullptr, 10);

    DumpOption options;
    Json::Value json_value;

    if (api_name == "filesession") {
      // /FuseStatService/filesession/{ino}

      options.ino = ino;
      options.file_session = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderSingleFileSessionPage(ino, json_value, os, client_name);
      }
    } else if (api_name == "chunkset") {
      // /FuseStatService/chunkset/{ino}

      LOG(INFO) << "Dump chunkset for ino: " << ino;

      options.ino = ino;
      options.chunk_set = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderChunkSetPage(ino, json_value, os, client_name);
      }
    }

  } else if (params.size() == 3) {
    const std::string& api_name = params[0];
    const Ino ino = strtoull(params[1].c_str(), nullptr, 10);
    const uint32_t chunk_index = strtoull(params[2].c_str(), nullptr, 10);

    DumpOption options;
    Json::Value json_value;
    if (api_name == "chunk") {
      // /FuseStatService/chunk/{ino}/{chunk_index}
      LOG(INFO) << "Dump chunk for ino: " << ino
                << ", chunk_index: " << chunk_index;

      options.ino = ino;
      options.chunk_index = chunk_index;
      options.chunk = true;
      if (vfs_hub_->GetMetaSystem().Dump(options, json_value)) {
        RenderSingleChunkPage(ino, json_value, os, client_name);
      }
    }

  } else {
    return cntl->SetFailed("unknown path: " + path);  // NOLINT
  }

  if (os.buf().empty()) {
    cntl->SetFailed("dump metasystem fail.");

  } else {
    os.move_to(cntl->response_attachment());
    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
  }
}

void FuseStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "dingofs";
  tab->path = "/FuseStatService";
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
