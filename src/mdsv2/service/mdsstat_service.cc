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

#include "mdsv2/service/mdsstat_service.h"

#include <gflags/gflags_declare.h>
#include <sys/types.h>

#include <string>
#include <vector>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(mds_offline_period_time_ms);

DEFINE_uint32(client_offline_period_time_ms, 30 * 1000, "client offline period time ms");

static std::string RenderHead() {
  butil::IOBufBuilder os;

  os << fmt::format(R"(<head>{})", brpc::gridtable_style());
  os << fmt::format(R"(<script src="/js/sorttable"></script>)");
  os << fmt::format(R"(<script language="javascript" type="text/javascript" src="/js/jquery_min"></script>)");
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

  os << brpc::TabsHead() << "</head>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static std::string RenderMdsList(const std::vector<MDSMeta>& mds_metas) {
  butil::IOBufBuilder os;

  os << R"(<div style="margin:12px;font-size:smaller;">)";
  os << R"(<h3>MDS</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Addr</th>";
  os << "<th>State</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Online</th>";
  os << "</tr>";

  int64_t now_ms = Helper::TimestampMs();

  for (const auto& mds_meta : mds_metas) {
    os << "<tr>";
    os << "<td>" << mds_meta.ID() << "</td>";
    os << "<td>" << fmt::format("{}:{}", mds_meta.Host(), mds_meta.Port()) << "</td>";
    os << "<td>" << MDSMeta::StateName(mds_meta.GetState()) << "</td>";
    os << "<td>" << Helper::FormatMsTime(mds_meta.LastOnlineTimeMs()) << "</td>";
    if (mds_meta.LastOnlineTimeMs() + FLAGS_mds_offline_period_time_ms < now_ms) {
      os << "<td style=\"color:red\">NO</td>";
    } else {
      os << "<td>YES</td>";
    }

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static std::string RenderClientList(const std::vector<pb::mdsv2::Client>& clients) {
  butil::IOBufBuilder os;

  os << R"(<div style="margin:12px;margin-top:64px;font-size:smaller;">)";
  os << R"(<h3>Client</h3>)";
  os << R"(<table class="gridtable sortable" border=1 style="max-width:100%;white-space:nowrap;">)";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Host</th>";
  os << "<th>MountPoint</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Online</th>";
  os << "</tr>";

  int64_t now_ms = Helper::TimestampMs();

  for (const auto& client : clients) {
    os << "<tr>";
    os << "<td>" << client.id() << "</td>";
    os << "<td>" << fmt::format("{}:{}", client.hostname(), client.port()) << "</td>";
    os << "<td>" << client.mountpoint() << "</td>";
    os << "<td>" << Helper::FormatMsTime(client.last_online_time_ms()) << "</td>";
    if (client.last_online_time_ms() + FLAGS_client_offline_period_time_ms < now_ms) {
      os << R"(<td style="color:red">NO</td>)";
    } else {
      os << "<td>YES</td>";
    }

    os << "</tr>";
  }

  os << "</table>";
  os << "</div>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

void MdsStatServiceImpl::default_method(::google::protobuf::RpcController* controller,
                                        const pb::web::MdsStatRequest* request, pb::web::MdsStatResponse* response,
                                        ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const bool use_html = brpc::UseHTML(cntl->http_request());
  cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");

  auto file_system_set = Server::GetInstance().GetFileSystemSet();

  os << "<!DOCTYPE html><html>";

  os << "<head>";
  os << RenderHead();
  os << "</head>";

  os << "<body>";
  server->PrintTabsBody(os, "mdsstat");

  auto mds_meta_map = Server::GetInstance().GetMDSMetaMap();
  auto mds_metas = mds_meta_map->GetAllMDSMeta();

  // sort by id
  sort(mds_metas.begin(), mds_metas.end(), [](const MDSMeta& a, const MDSMeta& b) { return a.ID() < b.ID(); });

  os << RenderMdsList(mds_metas);

  // Render client list
  auto heartbeat = Server::GetInstance().GetHeartbeat();
  std::vector<pb::mdsv2::Client> clients;
  auto status = heartbeat->GetClientList(clients);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[mdsstat] get client list fail, error({}).", status.error_str());
    os << fmt::format(R"(<div style="color:red;">get client list fail, error({}).</div>)", status.error_str());
  } else {
    // sort by last_online_time
    sort(clients.begin(), clients.end(), [](const pb::mdsv2::Client& a, const pb::mdsv2::Client& b) {
      return a.last_online_time_ms() > b.last_online_time_ms();
    });

    os << RenderClientList(clients);
  }

  os << "</body>";

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void MdsStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "node";
  tab->path = "/MdsStatService";
}

}  // namespace mdsv2
}  // namespace dingofs
