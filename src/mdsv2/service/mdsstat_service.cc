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
#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(mds_offline_period_time_ms);

static std::string RenderHead() {
  butil::IOBufBuilder os;

  os << "<head>\n"
     << brpc::gridtable_style() << "<script src=\"/js/sorttable\"></script>\n"
     << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
     << brpc::TabsHead();

  os << R"(<meta charset="UTF-8">"
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

  os << "<div style=\"margin: 12px;\">";
  os << "<table class=\"gridtable sortable\" border=\"1\">\n";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Addr</th>";
  os << "<th>State</th>";
  os << "<th>Register Time</th>";
  os << "<th>Last Online Time</th>";
  os << "<th>Online</th>";
  os << "</tr>\n";

  int64_t now_ms = Helper::TimestampMs();

  for (const auto& mds_meta : mds_metas) {
    os << "<tr>";
    os << "<td>" << mds_meta.ID() << "</td>";
    os << "<td>" << fmt::format("{}:{}", mds_meta.Host(), mds_meta.Port()) << "</td>";
    os << "<td>" << MDSMeta::StateName(mds_meta.GetState()) << "</td>";
    os << "<td>" << Helper::FormatMsTime(mds_meta.RegisterTimeMs()) << "</td>";
    os << "<td>" << Helper::FormatMsTime(mds_meta.LastOnlineTimeMs()) << "</td>";
    if (mds_meta.LastOnlineTimeMs() + FLAGS_mds_offline_period_time_ms < now_ms) {
      os << "<td style=\"color:red\">NO</td>";
    } else {
      os << "<td>YES</td>";
    }

    os << "</tr>\n";
  }

  os << "</table>\n";
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

  os << "<!DOCTYPE html><html>\n";

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

  os << "</body>";

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void MdsStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "mds";
  tab->path = "/MdsStatService";
}

}  // namespace mdsv2
}  // namespace dingofs
