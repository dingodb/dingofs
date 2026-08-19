/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_HEADERS_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_HEADERS_H_

// The one brpc include point: <linux/fs.h>'s BLOCK_SIZE macro breaks butil.
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <butil/endpoint.h>

#endif  // DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_HEADERS_H_
