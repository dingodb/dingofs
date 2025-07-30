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

#ifndef DINGOFS_OPTIONS_MDSV2_H_
#define DINGOFS_OPTIONS_MDSV2_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace mdsv2 {

class MetaServiceOption : public BaseOption {
  BIND_uint32(read_worker_num, 128, "read service worker num");
  BIND_uint64(read_worker_max_pending_num, 1024,
              "read service worker max pending num");
  BIND_bool(read_worker_use_pthread, false, "read worker use pthread");

  BIND_uint32(write_worker_num, 128, "write service worker num");
  BIND_uint64(write_worker_max_pending_num, 1024,
              "write service worker max pending num");
  BIND_bool(write_worker_use_pthread, false, "write worker use pthread");
};

class ServiceOption : public BaseOption {
  BIND_suboption(meta, "meta", MetaServiceOption);
};

class ServerOption : public BaseOption {
  BIND_uint32(id, 0, "server id, must be unique in the cluster");
  BIND_string(host, "", "server host");
  BIND_string(listen_host, "", "server listen host");
  BIND_uint32(port, 0, "server port");

  BIND_suboption(service, "service", ServiceOption);
};

class LogOption : public BaseOption {
  BIND_string(level, "INFO", "log level, DEBUG, INFO, WARNING, ERROR, FATAL");
  BIND_string(path, "./log", "log path, if empty, use default log path");
};

class CrontabOption : public BaseOption {
  BIND_uint32(heartbeat_interval_s, 5, "heartbeat interval seconds");
  BIND_uint32(fsinfosync_interval_s, 10, "fs info sync interval seconds");
  BIND_uint32(mdsmonitor_interval_s, 5, "mds monitor interval seconds");
  BIND_uint32(quota_sync_interval_s, 6, "quota sync interval seconds");
  BIND_uint32(gc_interval_s, 60, "gc interval seconds");
};

class AppOption : public BaseOption {
  BIND_suboption(server, "server", ServerOption);
  BIND_suboption(log, "log", LogOption);
  BIND_suboption(crontab, "crontab", CrontabOption);
};

// DECLARE_OPTION(mdsv2, AppOption);

}  // namespace mdsv2
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_OPTIONS_MDSV2_H_