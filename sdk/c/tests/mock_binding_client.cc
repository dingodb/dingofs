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

/*
 * mock_binding_client.cc — in-memory filesystem stub for BindingClient
 *
 * Provides a self-contained, cluster-free implementation of every
 * BindingClient method used by libdingofs.cc.  Linked in place of
 * libdingofs_shim when building the mock test binary.
 */

#include "binding_client.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <gflags/gflags.h>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/stat.h>

#include "client/vfs/data_buffer.h"
#include "common/meta.h"
#include "common/status.h"
#include "common/io_buffer.h"   // IOBuffer, IOBuf

using dingofs::Attr;
using dingofs::DirEntry;
using dingofs::FsStat;
using dingofs::Ino;
using dingofs::Status;
using dingofs::client::DataBuffer;
using dingofs::client::IOVec;

// ============================================================================
// In-memory filesystem state
// ============================================================================

namespace {

struct INode {
    Attr                                 attr;
    std::string                          data;      // file content
    std::string                          link;      // symlink target
    std::map<std::string, Ino>           children;  // dir entries
    std::map<std::string, std::string>   xattrs;
};

struct MockFS {
    std::mutex                           mu;
    std::unordered_map<Ino, INode>       inodes;
    Ino                                  next_ino = 10;
    uint64_t                             next_fh  = 100;

    MockFS() { reset(); }

    void reset() {
        inodes.clear();
        next_ino = 10;
        next_fh  = 100;

        // Create the root inode.
        INode root;
        root.attr.ino   = 1;
        root.attr.mode  = S_IFDIR | 0755;
        root.attr.nlink = 2;
        root.attr.uid   = 0;
        root.attr.gid   = 0;
        inodes[1]       = root;
    }

    Ino alloc_ino() { return next_ino++; }
    uint64_t alloc_fh() { return next_fh++; }

    Attr make_attr(Ino ino, uint32_t mode,
                   uint32_t uid = 0, uint32_t gid = 0,
                   uint64_t size = 0) {
        Attr a{};
        a.ino    = ino;
        a.mode   = mode;
        a.nlink  = S_ISDIR(mode) ? 2 : 1;
        a.uid    = uid;
        a.gid    = gid;
        a.length = size;
        return a;
    }
};

static MockFS g_fs;

}  // namespace

// ============================================================================
// BindingClient implementation
// ============================================================================

namespace dingofs {
namespace client {

BindingClient::BindingClient()  : vfs_(nullptr) {}
BindingClient::~BindingClient() = default;

Status BindingClient::Start(const BindingConfig&) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    g_fs.reset();
    return Status::OK();
}

Status BindingClient::Stop() {
    return Status::OK();
}

Status BindingClient::StatFs(Ino, FsStat* fs_stat) {
    fs_stat->max_bytes    = 1LL << 40;  // 1 TiB
    fs_stat->used_bytes   = 0;
    fs_stat->max_inodes   = 1000000;
    fs_stat->used_inodes  = static_cast<int64_t>(g_fs.inodes.size());
    return Status::OK();
}

Status BindingClient::Lookup(Ino parent, const std::string& name, Attr* attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");
    auto cit = pit->second.children.find(name);
    if (cit == pit->second.children.end())
        return Status::NotExist("not found");
    auto iit = g_fs.inodes.find(cit->second);
    if (iit == g_fs.inodes.end()) return Status::NotExist("inode missing");
    *attr = iit->second.attr;
    return Status::OK();
}

Status BindingClient::GetAttr(Ino ino, Attr* attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    *attr = it->second.attr;
    attr->length = it->second.data.size();
    return Status::OK();
}

Status BindingClient::SetAttr(Ino ino, int set,
                              const Attr& in_attr, Attr* out_attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    INode& nd = it->second;

    if (set & static_cast<int>(dingofs::kSetAttrMode))  nd.attr.mode   = in_attr.mode;
    if (set & static_cast<int>(dingofs::kSetAttrUid))   nd.attr.uid    = in_attr.uid;
    if (set & static_cast<int>(dingofs::kSetAttrGid))   nd.attr.gid    = in_attr.gid;
    if (set & static_cast<int>(dingofs::kSetAttrSize)) {
        nd.data.resize(static_cast<size_t>(in_attr.length));
        nd.attr.length = in_attr.length;
    }
    if (set & static_cast<int>(dingofs::kSetAttrAtime)) nd.attr.atime  = in_attr.atime;
    if (set & static_cast<int>(dingofs::kSetAttrMtime)) nd.attr.mtime  = in_attr.mtime;

    nd.attr.length = nd.data.size();
    *out_attr = nd.attr;
    return Status::OK();
}

Status BindingClient::MkDir(Ino parent, const std::string& name,
                            uint32_t uid, uint32_t gid, uint32_t mode,
                            Attr* attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");
    if (pit->second.children.count(name))
        return Status::Exist("already exists");

    Ino ino = g_fs.alloc_ino();
    INode nd;
    nd.attr = g_fs.make_attr(ino, S_IFDIR | (mode & 07777), uid, gid);
    g_fs.inodes[ino] = nd;
    pit->second.children[name] = ino;

    *attr = g_fs.inodes[ino].attr;
    return Status::OK();
}

Status BindingClient::RmDir(Ino parent, const std::string& name) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");
    auto cit = pit->second.children.find(name);
    if (cit == pit->second.children.end())
        return Status::NotExist("not found");

    auto iit = g_fs.inodes.find(cit->second);
    if (iit != g_fs.inodes.end() && !iit->second.children.empty())
        return Status::NotEmpty("not empty");

    if (iit != g_fs.inodes.end()) g_fs.inodes.erase(iit);
    pit->second.children.erase(cit);
    return Status::OK();
}

Status BindingClient::OpenDir(Ino ino, uint64_t* fh, bool& need_cache) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    if (!g_fs.inodes.count(ino)) return Status::NotExist("not found");
    *fh        = g_fs.alloc_fh();
    need_cache = false;
    return Status::OK();
}

Status BindingClient::ReadDir(Ino ino, uint64_t /*fh*/, uint64_t offset,
                              bool with_attr, ReadDirHandler handler) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");

    const auto& children = it->second.children;
    uint64_t idx = 0;
    for (const auto& kv : children) {
        if (idx < offset) { ++idx; continue; }
        DirEntry entry;
        entry.name = kv.first;
        entry.ino  = kv.second;
        if (with_attr) {
            auto iit = g_fs.inodes.find(kv.second);
            if (iit != g_fs.inodes.end()) entry.attr = iit->second.attr;
        }
        if (!handler(entry, idx + 1)) break;
        ++idx;
    }
    return Status::OK();
}

Status BindingClient::ReleaseDir(Ino, uint64_t) { return Status::OK(); }

Status BindingClient::Create(Ino parent, const std::string& name,
                             uint32_t uid, uint32_t gid, uint32_t mode,
                             int /*flags*/, uint64_t* fh, Attr* attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");

    Ino ino;
    if (pit->second.children.count(name)) {
        // Overwrite (O_TRUNC semantics).
        ino = pit->second.children[name];
        g_fs.inodes[ino].data.clear();
        g_fs.inodes[ino].attr.length = 0;
    } else {
        ino = g_fs.alloc_ino();
        INode nd;
        nd.attr = g_fs.make_attr(ino, S_IFREG | (mode & 07777), uid, gid);
        g_fs.inodes[ino] = nd;
        pit->second.children[name] = ino;
    }

    *fh   = g_fs.alloc_fh();
    *attr = g_fs.inodes[ino].attr;
    return Status::OK();
}

Status BindingClient::MkNod(Ino parent, const std::string& name,
                            uint32_t uid, uint32_t gid, uint32_t mode,
                            uint64_t /*dev*/, Attr* attr) {
    // Reuse Create logic (minus the fh).
    uint64_t fh;
    return Create(parent, name, uid, gid, mode, 0, &fh, attr);
}

Status BindingClient::Open(Ino ino, int /*flags*/, uint64_t* fh) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    if (!g_fs.inodes.count(ino)) return Status::NotExist("not found");
    *fh = g_fs.alloc_fh();
    return Status::OK();
}

Status BindingClient::Read(Ino ino, DataBuffer* data_buffer,
                           uint64_t size, uint64_t offset,
                           uint64_t /*fh*/, uint64_t* out_rsize) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");

    const std::string& data = it->second.data;
    if (offset >= data.size()) {
        *out_rsize = 0;
        return Status::OK();
    }

    uint64_t to_read = std::min(size, static_cast<uint64_t>(data.size()) - offset);
    // Fill the DataBuffer via the internal IOBuffer.
    dingofs::IOBuffer* io_buf = data_buffer->RawIOBuffer();
    io_buf->IOBuf().append(data.data() + offset, static_cast<size_t>(to_read));
    *out_rsize = to_read;
    return Status::OK();
}

Status BindingClient::Write(Ino ino, const char* buf, uint64_t size,
                            uint64_t offset, uint64_t /*fh*/,
                            uint64_t* out_wsize) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");

    INode& nd = it->second;
    size_t end = static_cast<size_t>(offset + size);
    if (end > nd.data.size()) nd.data.resize(end);
    std::memcpy(&nd.data[static_cast<size_t>(offset)], buf,
                static_cast<size_t>(size));
    nd.attr.length = nd.data.size();
    *out_wsize = size;
    return Status::OK();
}

Status BindingClient::Flush(Ino, uint64_t)           { return Status::OK(); }
Status BindingClient::Fsync(Ino, int, uint64_t)       { return Status::OK(); }
Status BindingClient::Release(Ino, uint64_t)          { return Status::OK(); }

Status BindingClient::Unlink(Ino parent, const std::string& name) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");
    auto cit = pit->second.children.find(name);
    if (cit == pit->second.children.end())
        return Status::NotExist("not found");

    Ino ino = cit->second;
    pit->second.children.erase(cit);
    // Remove the inode if nlink reaches 0 (simplification: always remove).
    g_fs.inodes.erase(ino);
    return Status::OK();
}

Status BindingClient::Link(Ino ino, Ino new_parent,
                           const std::string& new_name, Attr* attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(new_parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");
    if (!g_fs.inodes.count(ino)) return Status::NotExist("src not found");

    pit->second.children[new_name] = ino;
    g_fs.inodes[ino].attr.nlink++;
    *attr = g_fs.inodes[ino].attr;
    return Status::OK();
}

Status BindingClient::Symlink(Ino parent, const std::string& name,
                              uint32_t uid, uint32_t gid,
                              const std::string& link, Attr* attr) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto pit = g_fs.inodes.find(parent);
    if (pit == g_fs.inodes.end()) return Status::NotExist("parent not found");
    if (pit->second.children.count(name))
        return Status::Exist("already exists");

    Ino ino = g_fs.alloc_ino();
    INode nd;
    nd.attr = g_fs.make_attr(ino, S_IFLNK | 0777, uid, gid);
    nd.link = link;
    g_fs.inodes[ino] = nd;
    pit->second.children[name] = ino;

    *attr = g_fs.inodes[ino].attr;
    return Status::OK();
}

Status BindingClient::ReadLink(Ino ino, std::string* link) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    *link = it->second.link;
    return Status::OK();
}

Status BindingClient::Rename(Ino old_parent, const std::string& old_name,
                             Ino new_parent, const std::string& new_name) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto op = g_fs.inodes.find(old_parent);
    auto np = g_fs.inodes.find(new_parent);
    if (op == g_fs.inodes.end() || np == g_fs.inodes.end())
        return Status::NotExist("parent not found");

    auto cit = op->second.children.find(old_name);
    if (cit == op->second.children.end())
        return Status::NotExist("not found");

    Ino ino = cit->second;
    // Remove from old parent and add to new parent.
    op->second.children.erase(cit);
    // If new name already exists, remove it first.
    auto nit = np->second.children.find(new_name);
    if (nit != np->second.children.end()) {
        g_fs.inodes.erase(nit->second);
        np->second.children.erase(nit);
    }
    np->second.children[new_name] = ino;
    return Status::OK();
}

Status BindingClient::SetXattr(Ino ino, const std::string& name,
                               const std::string& value, int flags) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    it->second.xattrs[name] = value;
    (void)flags;
    return Status::OK();
}

Status BindingClient::GetXattr(Ino ino, const std::string& name,
                               std::string* value) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    auto xit = it->second.xattrs.find(name);
    if (xit == it->second.xattrs.end())
        return Status::NotExist("xattr not found");
    *value = xit->second;
    return Status::OK();
}

Status BindingClient::ListXattr(Ino ino,
                                std::vector<std::string>* xattrs) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    for (const auto& kv : it->second.xattrs)
        xattrs->push_back(kv.first);
    return Status::OK();
}

Status BindingClient::RemoveXattr(Ino ino, const std::string& name) {
    std::lock_guard<std::mutex> lk(g_fs.mu);
    auto it = g_fs.inodes.find(ino);
    if (it == g_fs.inodes.end()) return Status::NotExist("not found");
    it->second.xattrs.erase(name);
    return Status::OK();
}

uint64_t BindingClient::GetMaxNameLength() { return 255; }

Status BindingClient::Ioctl(Ino, uint32_t, unsigned int, unsigned,
                            const void*, size_t, char*, size_t) {
    return Status::OK();
}

// static
Status BindingClient::SetOption(const std::string& key,
                                const std::string& value) {
    if (gflags::SetCommandLineOption(key.c_str(), value.c_str()).empty()) {
        return Status::InvalidParam("unknown or invalid option: " + key);
    }
    return Status::OK();
}

// static
Status BindingClient::GetOption(const std::string& key, std::string* value) {
    if (!gflags::GetCommandLineOption(key.c_str(), value)) {
        return Status::InvalidParam("unknown option: " + key);
    }
    return Status::OK();
}

// static
std::vector<OptionInfo> BindingClient::ListOptions() { return {}; }

// static
void BindingClient::PrintOptions() {}

}  // namespace client
}  // namespace dingofs
