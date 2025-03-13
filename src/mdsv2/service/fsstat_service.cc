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

#include "mdsv2/service/fsstat_service.h"

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "butil/iobuf.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/filesystem/fs_utils.h"
#include "mdsv2/server.h"

namespace dingofs {
namespace mdsv2 {

static std::string RenderHead() {
  butil::IOBufBuilder os;

  os << "<head>\n"
     << brpc::gridtable_style() << "<script src=\"/js/sorttable\"></script>\n"
     << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
     << brpc::TabsHead();

  os << "<meta charset=\"UTF-8\">\n"
     << "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
     << "<style>\n"
     << "  /* Define styles for different colors */\n"
     << "  .red-text {\n"
     << "    color: red;\n"
     << "  }\n"
     << "  .blue-text {\n"
     << "    color: blue;\n"
     << "  }\n"
     << "  .green-text {\n"
     << "    color: green;\n"
     << "  }\n"
     << "  .bold-text {"
     << "    font-weight: bold;"
     << "  }"
     << "</style>\n";

  os << brpc::TabsHead() << "</head>";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static std::string RenderMountpoint(const pb::mdsv2::FsInfo& fs_info) {
  std::string result;
  for (const auto& mountpoint : fs_info.mount_points()) {
    result += fmt::format("{}:{}", mountpoint.hostname(), mountpoint.port());
    result += "<br>";
    result += mountpoint.path();
    result += "<br>";
  }

  return result;
};

static std::string RenderPartitionPolicy(pb::mdsv2::PartitionPolicy partition_policy) {
  std::string result;

  switch (partition_policy.type()) {
    case pb::mdsv2::PartitionType::MONOLITHIC_PARTITION: {
      const auto& mono = partition_policy.mono();
      result += fmt::format("epoch: {}", mono.epoch());
      result += "<br>";
      result += fmt::format("mds: {}", mono.mds_id());
    } break;

    case pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION: {
      const auto& parent_hash = partition_policy.parent_hash();
      result += fmt::format("epoch: {}", parent_hash.epoch());
      result += "<br>";
      result += fmt::format("bucket_num: {}", parent_hash.bucket_num());
      result += "<br>";
      result += "mds: ";
      for (const auto& [mds_id, _] : parent_hash.distributions()) {
        result += fmt::format("{},", mds_id);
      }
      result.resize(result.size() - 1);
    } break;

    default:
      result = "Unknown";
      break;
  }

  return result;
}

static std::string PartitionTypeName(pb::mdsv2::PartitionType partition_type) {
  switch (partition_type) {
    case pb::mdsv2::PartitionType::MONOLITHIC_PARTITION:
      return "MONOLITHIC";
    case pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION:
      return "PARENT_ID_HASH";
    default:
      return "Unknown";
  }
}

static std::string RenderCapacity(uint64_t capacity) { return fmt::format("{}MB", capacity / (1024 * 1024)); }

static std::string RenderFsInfo(const std::vector<pb::mdsv2::FsInfo>& fs_infoes) {
  butil::IOBufBuilder os;

  os << "<table class=\"gridtable sortable\" border=\"1\">\n";
  os << "<tr>";
  os << "<th>ID</th>";
  os << "<th>Name</th>";
  os << "<th>Type</th>";
  os << "<th>PartitionType</th>";
  os << "<th>PartitionPolicy</th>";
  os << "<th>Capacity</th>";
  os << "<th>Owner</th>";
  os << "<th>RecycleTime</th>";
  os << "<th>MountPoint</th>";
  os << "<th>UpdateTime</th>";
  os << "<th>CreateTime</th>";
  os << "</tr>";

  for (const auto& fs_info : fs_infoes) {
    const auto& partition_policy = fs_info.partition_policy();

    os << "<tr>";

    os << "<td><a href=\"FsStatService/" << fs_info.fs_id() << R"(" target="_blank">)" << fs_info.fs_id()
       << "</a></td>";
    os << "<td>" << fs_info.fs_name() << "</td>";
    os << "<td>" << pb::mdsv2::FsType_Name(fs_info.fs_type()) << "</td>";
    os << "<td>" << PartitionTypeName(partition_policy.type()) << "</td>";
    os << "<td>" << RenderPartitionPolicy(partition_policy) << "</td>";
    os << "<td>" << RenderCapacity(fs_info.capacity()) << "</td>";
    os << "<td>" << fs_info.owner() << "</td>";
    os << "<td>" << fs_info.recycle_time_hour() << "</td>";
    os << "<td style=\"font-size:smaller\">" << RenderMountpoint(fs_info) << "</td>";
    os << "<td style=\"font-size:smaller\">" << Helper::FormatNsTime(fs_info.last_update_time_ns()) << "</td>";
    os << "<td style=\"font-size:smaller\">" << Helper::FormatTime(fs_info.create_time_s()) << "</td>";

    os << "</tr>";
  }

  os << "</table>\n";

  butil::IOBuf buf;
  os.move_to(buf);

  return buf.to_string();
}

static void RenderMainPage(const brpc::Server* server, FileSystemSetPtr file_system_set, butil::IOBufBuilder& os) {
  os << "<!DOCTYPE html><html>\n";

  os << "<head>";
  os << RenderHead();
  os << "</head>";

  os << "<body>";
  server->PrintTabsBody(os, "fsstat");

  std::vector<pb::mdsv2::FsInfo> fs_infoes;
  file_system_set->GetAllFsInfo(fs_infoes);
  // sort by fs_id
  sort(fs_infoes.begin(), fs_infoes.end(),
       [](const pb::mdsv2::FsInfo& a, const pb::mdsv2::FsInfo& b) { return a.fs_id() < b.fs_id(); });

  os << RenderFsInfo(fs_infoes);

  os << "</body>";
}

static void RenderFsTreePage(FsUtils& fs_utils, uint32_t fs_id, butil::IOBufBuilder& os) {
  if (fs_id == 0) {
    os << "Invalid fs_id";
    return;
  }

  os << "<!DOCTYPE html>";
  os << "<html lang=\"zh-CN\">";

  os << "<head>";
  os << R"(
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>File System Directory Tree</title>
<style>
body {
  font-family: Arial, sans-serif;
  margin: 20px;
}

.tree {
  margin-left: 20px;
}

.tree,
.tree ul {
  list-style-type: none;
  padding-left: 20px;
}

.tree li {
  position: relative;
  padding: 5px 0;
}

.tree li::before {
  content: "";
  position: absolute;
  top: 12px;
  left: -15px;
  width: 10px;
  height: 1px;
  background-color: #666;
}

.tree li::after {
  content: "";
  position: absolute;
  top: 0;
  left: -15px;
  width: 1px;
  height: 100%;
  background-color: #666;
}

.tree li:last-child::after {
  height: 12px;
}

.folder {
  cursor: pointer;
  color: #007bff;
  font-weight: bold;
}

.file {
  color: #333;
}

.collapsed>ul {
  display: none;
}

.icon {
  margin-right: 5px;
}
</style>)";
  os << "</head>";

  os << "<body>";
  os << "<h1>File System Directory Tree</h1>";
  os << "<p>format: name [ino,mode,nlink,uid,gid,size,ctime,mtime,atime]</p>";
  os << R"(
<div class="controls">
  <button id="expandAll">Expand</button>
  <button id="collapseAll">Collapse</button>
</div>
<ul id="fileTree" class="tree"></ul>)";

  os << "<script>";

  os << "const fileSystem =" + fs_utils.GenFsTreeJsonString(fs_id) + ";";

  os << R"(
    function generateTree(item, parentElement) {
      const li = document.createElement('li');

      if (item.type === 'directory') {
        const folderSpan = document.createElement('span');
        folderSpan.className = 'folder';
        folderSpan.innerHTML = `<span class="icon">📁</span>${item.name} [${item.ino},${item.description}]`;
        folderSpan.addEventListener('click', function () {
          this.parentElement.classList.toggle('collapsed');
          if (this.parentElement.classList.contains('collapsed')) {
            this.querySelector('.icon').textContent = '📁';
          } else {
            this.querySelector('.icon').textContent = '📂';
          }
        });
        li.appendChild(folderSpan);

        if (item.children && item.children.length > 0) {
          const ul = document.createElement('ul');
          item.children.forEach(child => {
            generateTree(child, ul);
          });
          li.appendChild(ul);
        }
      } else {
        const fileSpan = document.createElement('span');
        fileSpan.className = 'file';
        fileSpan.innerHTML = `<span class="icon">📄</span>${item.name} [${item.ino},${item.description}]`;
        li.appendChild(fileSpan);
      }

      parentElement.appendChild(li);
    }

    const treeRoot = document.getElementById('fileTree');
    generateTree(fileSystem, treeRoot);

    document.getElementById('expandAll').addEventListener('click', function () {
      const collapsedItems = document.querySelectorAll('.collapsed');
      collapsedItems.forEach(item => {
        item.classList.remove('collapsed');
        item.querySelector('.icon').textContent = '📂';
      });
    });

    document.getElementById('collapseAll').addEventListener('click', function () {
      const folders = document.querySelectorAll('.folder');
      folders.forEach(folder => {
        const li = folder.parentElement;
        if (!li.classList.contains('collapsed') && li.querySelector('ul')) {
          li.classList.add('collapsed');
          folder.querySelector('.icon').textContent = '📁';
        }
      });
    });
  )";
  os << "</script>";
  os << "</body>";
  os << "</html>";
}

void FsStatServiceImpl::default_method(::google::protobuf::RpcController* controller,
                                       const pb::web::FsStatRequest* request, pb::web::FsStatResponse* response,
                                       ::google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const bool use_html = brpc::UseHTML(cntl->http_request());
  cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");
  const std::string& path = cntl->http_request().unresolved_path();

  DINGO_LOG(INFO) << fmt::format("FsStatService path: {}", path);

  if (path.empty()) {
    auto file_system_set = Server::GetInstance().GetFileSystemSet();
    RenderMainPage(server, file_system_set, os);

  } else {
    uint32_t fs_id = Helper::StringToInt32(path);
    FsUtils fs_utils(Server::GetInstance().GetKVStorage());
    RenderFsTreePage(fs_utils, fs_id, os);
  }

  os.move_to(cntl->response_attachment());
  cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void FsStatServiceImpl::GetTabInfo(brpc::TabInfoList* tab_list) const {
  brpc::TabInfo* tab = tab_list->add();
  tab->tab_name = "fs";
  tab->path = "/FsStatService";
}

}  // namespace mdsv2
}  // namespace dingofs
