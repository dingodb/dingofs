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

#include <json/json.h>
#include <sys/types.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "client/const.h"
#include "client/vfs/common/helper.h"
//#include "client/vfs/common/version.h"
#include "fmt/format.h"
#include "mdsv2/common/version.h"
#include "utils/string.h"

namespace dingofs {
namespace client {
namespace vfs {

// DECLARE_uint32(mds_heartbeat_mds_offline_period_time_ms);
// DECLARE_uint32(mds_heartbeat_client_offline_period_ms);
// DECLARE_uint32(cache_member_heartbeat_offline_timeout_s);
// DECLARE_uint32(cache_member_heartbeat_miss_timeout_s);

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

  auto infos = dingofs::mdsv2::DingoVersion();
  for (const auto& info : infos) {
    os << fmt::format("{}: {}", info.first, info.second);
    os << "<br>";
  }

  os << R"(</div>)";
  os << R"(</div>)";
}

static void RenderClientInfo(const Json::Value& json_value,
                             butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>Client Info</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Host</th>";
  os << "<th>Port</th>";
  os << "<th>MountPoint</th>";
  os << "<th>MdsAddr</th>";
  os << "</tr>";

  // get client info
  const Json::Value& client_id = json_value["client_id"];

  if (client_id.empty()) {
    LOG(ERROR) << "no client to load";
    os << "</table>\n";
    os << "</div>";
    return;
  }
  std::string id = client_id["id"].asString();
  std::string host_name = client_id["host_name"].asString();
  uint32_t port = client_id["port"].asUInt();
  std::string mount_point = client_id["mount_point"].asString();
  std::string mds_add = client_id["mds_addr"].asString();

  os << "<tr>";
  os << "<td>" << id << "</td>";
  os << "<td>" << host_name << "</td>";
  os << "<td>" << fmt::format("{}", port) << "</td>";
  os << "<td>" << mount_point << "</td>";
  os << "<td>" << mds_add << "</td>";
  os << "</tr>";

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderFsInfo(const Json::Value& json_value,
                         butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>FS Info</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Name</th>";
  os << "<th>Type</th>";
  os << "<th>Size(MB)</th>";
  os << "<th>Owner</th>";
  os << "<th>Time</th>";
  os << "<th>RecycleTime</th>";
  os << "<th>S3</th>";
  os << "</tr>";

  LOG(ERROR) << "enter RenderFsInfo";
  // get fs info
  const Json::Value& fs_info = json_value["fs_info"];

  if (fs_info.empty()) {
    LOG(ERROR) << "no fs_info to load";
    os << "</table>\n";
    os << "</div>";
    return;
  }

  std::string id = fs_info["id"].asString();
  std::string name = fs_info["name"].asString();
  std::string owner = fs_info["owner"].asString();
  std::string type = fs_info["type"].asString();
  uint64_t block_size = fs_info["block_size"].asUInt64();
  uint64_t chunk_size = fs_info["chunk_size"].asUInt64();
  uint64_t capacity = fs_info["capacity"].asUInt64();
  uint64_t create_time_s = fs_info["create_time_s"].asUInt64();
  uint64_t last_update_time_ns = fs_info["last_update_time_ns"].asUInt64();
  uint32_t recycle_time = fs_info["recycle_time"].asUInt();
  std::string s3_endpoint = fs_info["s3_endpoint"].asString();
  std::string s3_bucket = fs_info["s3_bucket"].asString();

  LOG(INFO) << fmt::format("yjddebug RenderFsInfo id:{}, name:{}", id, name);
  auto render_size_func = [&]() -> std::string {
    std::string result;
    result += "<div>";
    result += fmt::format("chunk size: {}", chunk_size / (1024 * 1024));
    result += "<br>";
    result += fmt::format("block size: {}", block_size / (1024 * 1024));
    result += "<br>";
    result += fmt::format("capacity: {}", capacity / (1024 * 1024));
    result += "</div>";
    return result;
  };

  auto render_time_func = [&]() -> std::string {
    std::string result;
    result += "<div>";
    result += "update time:";
    result += "<br>";
    result += fmt::format("<span>{}</span>",
                          FormatTime(last_update_time_ns / 1000000000));
    result += "<br>";
    result += "create time:";
    result += "<br>";
    result += fmt::format("<span>{}</span>", FormatTime(create_time_s));
    result += "</div>";
    return result;
  };

  auto render_s3_func = [&]() -> std::string {
    std::string result;
    if (!s3_endpoint.empty()) {
      result += s3_endpoint + "<br>" + s3_bucket;
    }

    return result;
  };

  os << "<tr>";
  os << "<td>" << id << "</td>";
  os << "<td>" << name << "</td>";
  os << "<td>" << type << "</td>";
  os << "<td>" << render_size_func() << "</td>";
  os << "<td>" << owner << "</td>";
  os << "<td>" << render_time_func() << "</td>";
  os << "<td>" << recycle_time << "</td>";
  os << "<td>" << render_s3_func() << "</td>";
  os << "</tr>";

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderMdsInfo(const Json::Value& json_value,
                          butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>MDS Info</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Addr</th>";
  os << "</tr>";

  // get client info
  const Json::Value& mds_list = json_value["mds_list"];
  if (!mds_list.isArray()) {
    LOG(ERROR) << "mds_list is not an array.";
    os << "</table>\n";
    os << "</div>";
    return;
  }
  if (mds_list.empty()) {
    LOG(INFO) << "no mds_list to load";
    os << "</table>\n";
    os << "</div>";
    return;
  }

  for (const auto& mds : mds_list) {
    auto id = mds["id"].asUInt64();
    auto host = mds["host"].asString();
    auto port = mds["port"].asInt();

    os << "<td>" << id << "</td>";
    os << fmt::format(
        R"(<td><a href="http://{}:{}/FsStatService" target="_blank">{}:{} </a></td>)",
        host, port, host, port);
    os << "</tr>";
  }

  os << "</table>\n";
  os << "</div>";

  os << "<br>";
}

static void RenderHandlerInfo(const Json::Value& json_value,
                              butil::IOBufBuilder& os) {
  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>handler Info</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>Ino</th>";
  os << "<th>Fh</th>";
  os << "<th>Flags</th>";
  os << "</tr>";

  // get client info
  const Json::Value& handlers = json_value["handlers"];
  if (!handlers.isArray()) {
    LOG(ERROR) << "handlers is not an array.";
    os << "</table>\n";
    os << "</div>";
    return;
  }
  if (handlers.empty()) {
    LOG(INFO) << "no handlers to load";
    os << "</table>\n";
    os << "</div>";
    return;
  }

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

void FuseStatServiceImpl::RenderMainPage(const brpc::Server* server,
                                         butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>\n";

  os << "<head>";
  os << RenderHead("dingofs-client dashboard");
  os << "</head>";

  os << "<body>";
  server->PrintTabsBody(os, "fusesstat");
  os << R"(<h1 style="text-align:center;">dingofs-client dashboard</h1>)";

  Json::Value meta_value;

  auto span = vfs_hub_->GetTracer()->StartSpan(kVFSWrapperMoudule, __func__);
  if (!vfs_hub_->GetMetaSystem()->GetDescription(span->GetContext(),
                                                 meta_value)) {
    LOG(INFO) << fmt::format("GetDescription failed.");
    os << "</body>";
    os << "</html>";
    return;
  }

  Json::Value handler_value;
  if (!vfs_hub_->GetHandleManager()->Dump(handler_value)) {
    LOG(INFO) << fmt::format("hadler manager dump failed.");
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
  // handler info
  RenderHandlerInfo(handler_value, os);
  // git info
  RenderGitInfo(os);

  os << "</body>";
  os << "</html>";
}

static void RenderJsonPage(const std::string& header, const std::string& json,
                           butil::IOBufBuilder& os) {
  os << R"(<!DOCTYPE html><html lang="zh-CN">)";

  os << R"(
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>dingofs inode details</title>
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

  std::vector<std::string> params;
  SplitString(path, '/', params);

  // /FuseStatService
  if (params.empty()) {
    RenderMainPage(server, os);

  } else {
    cntl->SetFailed("unknown path: " + path);
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void FuseStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "dingofs";
  tab->path = "/FuseStatService";
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
