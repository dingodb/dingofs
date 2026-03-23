// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client_bindings.h"

#include <cstring>
#include <string>
#include <tuple>
#include <vector>

#include <Python.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/function.h>
#include <nanobind/ndarray.h>

#include "client/vfs/data_buffer.h"
#include "common/meta.h"
#include "common/status.h"
#include "binding_client.h"

void DefineClientBindings(nanobind::module_& m) {
  namespace nb = nanobind;
  using dingofs::Attr;
  using dingofs::DirEntry;
  using dingofs::FsStat;
  using dingofs::Ino;
  using dingofs::ReadDirHandler;
  using dingofs::Status;
  using dingofs::client::DataBuffer;
  using dingofs::client::IOVec;
  using dingofs::client::BindingClient;
  using dingofs::client::BindingConfig;

  // ----------------------------------------------------------------
  // BindingConfig
  // ----------------------------------------------------------------
  nb::class_<BindingConfig>(m, "BindingConfig")
      .def(nb::init<>())
      .def_rw("mds_addrs",   &BindingConfig::mds_addrs)
      .def_rw("fs_name",     &BindingConfig::fs_name)
      .def_rw("mount_point", &BindingConfig::mount_point)
      .def_rw("conf_file",   &BindingConfig::conf_file)
      .def_rw("init_glog",   &BindingConfig::init_glog)
      .def_rw("log_dir",     &BindingConfig::log_dir)
      .def_rw("log_level",   &BindingConfig::log_level)
      .def_rw("log_v",       &BindingConfig::log_v);

  // ----------------------------------------------------------------
  // BindingClient
  // ----------------------------------------------------------------
  nb::class_<BindingClient>(m, "BindingClient")
      .def(nb::init<>())

      // ---- Lifecycle ----

      .def("Start",
           [](BindingClient& self, const BindingConfig& cfg) {
             return self.Start(cfg);
           })

      .def("Stop",
           [](BindingClient& self) {
             return self.Stop();
           })

      // ---- Filesystem stats ----

      .def("StatFs",
           [](BindingClient& self, Ino ino) {
             FsStat fs_stat{};
             Status s = self.StatFs(ino, &fs_stat);
             return std::make_tuple(s, fs_stat);
           })

      // ---- Metadata ----

      .def("Lookup",
           [](BindingClient& self, Ino parent, const std::string& name) {
             Attr attr{};
             Status s = self.Lookup(parent, name, &attr);
             return std::make_tuple(s, attr);
           })

      .def("GetAttr",
           [](BindingClient& self, Ino ino) {
             Attr attr{};
             Status s = self.GetAttr(ino, &attr);
             return std::make_tuple(s, attr);
           })

      .def("SetAttr",
           [](BindingClient& self, Ino ino, int set, const Attr& in_attr) {
             Attr out_attr{};
             Status s = self.SetAttr(ino, set, in_attr, &out_attr);
             return std::make_tuple(s, out_attr);
           })

      // ---- Directory operations ----

      .def("MkDir",
           [](BindingClient& self, Ino parent, const std::string& name,
              uint32_t uid, uint32_t gid, uint32_t mode) {
             Attr attr{};
             Status s = self.MkDir(parent, name, uid, gid, mode, &attr);
             return std::make_tuple(s, attr);
           })

      .def("RmDir",
           [](BindingClient& self, Ino parent, const std::string& name) {
             return self.RmDir(parent, name);
           })

      .def("OpenDir",
           [](BindingClient& self, Ino ino) {
             uint64_t fh = 0;
             bool need_cache = false;
             Status s = self.OpenDir(ino, &fh, need_cache);
             return std::make_tuple(s, fh, need_cache);
           })

      .def("ReadDir",
           [](BindingClient& self, Ino ino, uint64_t fh, uint64_t offset,
              bool with_attr, nb::callable handler) {
             ReadDirHandler cpp_handler =
                 [&handler](const DirEntry& entry, uint64_t off) -> bool {
                   nb::object result = handler(entry, off);
                   return nb::cast<bool>(result);
                 };
             return self.ReadDir(ino, fh, offset, with_attr, cpp_handler);
           })

      .def("ReleaseDir",
           [](BindingClient& self, Ino ino, uint64_t fh) {
             return self.ReleaseDir(ino, fh);
           })

      // ---- File operations ----

      .def("Create",
           [](BindingClient& self, Ino parent, const std::string& name,
              uint32_t uid, uint32_t gid, uint32_t mode, int flags) {
             uint64_t fh = 0;
             Attr attr{};
             Status s = self.Create(parent, name, uid, gid, mode, flags,
                                    &fh, &attr);
             return std::make_tuple(s, fh, attr);
           })

      .def("MkNod",
           [](BindingClient& self, Ino parent, const std::string& name,
              uint32_t uid, uint32_t gid, uint32_t mode, uint64_t dev) {
             Attr attr{};
             Status s = self.MkNod(parent, name, uid, gid, mode, dev, &attr);
             return std::make_tuple(s, attr);
           })

      .def("Open",
           [](BindingClient& self, Ino ino, int flags) {
             uint64_t fh = 0;
             Status s = self.Open(ino, flags, &fh);
             return std::make_tuple(s, fh);
           })

      // read_bytes: allocates DataBuffer internally, returns bytes.
      .def("read_bytes",
           [](BindingClient& self, Ino ino, uint64_t size, uint64_t offset,
              uint64_t fh) {
             DataBuffer buf;
             uint64_t rsize = 0;
             Status s = self.Read(ino, &buf, size, offset, fh, &rsize);
             if (!s.ok()) {
               return std::make_tuple(s, nb::bytes("", 0));
             }
             std::vector<IOVec> iovecs = buf.GatherIOVecs();
             std::string result;
             result.reserve(rsize);
             for (const IOVec& iov : iovecs) {
               result.append(static_cast<const char*>(iov.iov_base),
                              iov.iov_len);
             }
             if (result.size() > rsize) result.resize(rsize);
             return std::make_tuple(s,
                 nb::bytes(result.data(), result.size()));
           })

      // read_into: single-copy path into a caller-provided writable buffer.
      // Accepts any object implementing the writable Buffer Protocol
      // (bytearray, memoryview over writable memory, numpy.ndarray, etc.).
      // Uses PyBUF_WRITABLE to reject read-only buffers (e.g. bytes).
      .def("read_into",
           [](BindingClient& self, Ino ino, nb::object buf,
              uint64_t size, uint64_t offset, uint64_t fh) {
             Py_buffer view;
             if (PyObject_GetBuffer(buf.ptr(), &view,
                                    PyBUF_WRITABLE | PyBUF_SIMPLE) < 0) {
               throw nb::python_error();
             }
             DataBuffer data_buf;
             uint64_t rsize = 0;
             Status s = self.Read(ino, &data_buf, size, offset, fh, &rsize);
             if (!s.ok()) {
               PyBuffer_Release(&view);
               return std::make_tuple(s, static_cast<uint64_t>(0));
             }
             std::vector<IOVec> iovecs = data_buf.GatherIOVecs();
             char* dst = static_cast<char*>(view.buf);
             size_t dst_cap = static_cast<size_t>(view.len);
             size_t copied = 0;
             for (const IOVec& iov : iovecs) {
               size_t n = std::min(iov.iov_len, dst_cap - copied);
               if (n == 0) break;
               std::memcpy(dst + copied, iov.iov_base, n);
               copied += n;
             }
             PyBuffer_Release(&view);
             return std::make_tuple(s, static_cast<uint64_t>(copied));
           })

      // Write accepts any object implementing the Buffer Protocol
      // (bytes, bytearray, memoryview, numpy.ndarray, etc.) without copying.
      // Uses Python C API PyObject_GetBuffer / PyBUF_SIMPLE for zero-copy access.
      .def("Write",
           [](BindingClient& self, Ino ino, nb::object data,
              uint64_t size, uint64_t offset, uint64_t fh) {
             Py_buffer view;
             if (PyObject_GetBuffer(data.ptr(), &view, PyBUF_SIMPLE) < 0) {
               throw nb::python_error();
             }
             const char* buf_ptr = static_cast<const char*>(view.buf);
             size_t buf_len = static_cast<size_t>(view.len);
             uint64_t actual_size =
                 (size == 0) ? static_cast<uint64_t>(buf_len) : size;
             uint64_t wsize = 0;
             Status s = self.Write(ino, buf_ptr, actual_size, offset, fh,
                                   &wsize);
             PyBuffer_Release(&view);
             return std::make_tuple(s, wsize);
           })

      .def("Flush",
           [](BindingClient& self, Ino ino, uint64_t fh) {
             return self.Flush(ino, fh);
           })

      .def("Fsync",
           [](BindingClient& self, Ino ino, int datasync, uint64_t fh) {
             return self.Fsync(ino, datasync, fh);
           })

      .def("Release",
           [](BindingClient& self, Ino ino, uint64_t fh) {
             return self.Release(ino, fh);
           })

      // ---- Hard links, symlinks ----

      .def("Link",
           [](BindingClient& self, Ino ino, Ino new_parent,
              const std::string& new_name) {
             Attr attr{};
             Status s = self.Link(ino, new_parent, new_name, &attr);
             return std::make_tuple(s, attr);
           })

      .def("Unlink",
           [](BindingClient& self, Ino parent, const std::string& name) {
             return self.Unlink(parent, name);
           })

      .def("Symlink",
           [](BindingClient& self, Ino parent, const std::string& name,
              uint32_t uid, uint32_t gid, const std::string& link) {
             Attr attr{};
             Status s = self.Symlink(parent, name, uid, gid, link, &attr);
             return std::make_tuple(s, attr);
           })

      .def("ReadLink",
           [](BindingClient& self, Ino ino) {
             std::string link;
             Status s = self.ReadLink(ino, &link);
             return std::make_tuple(s, link);
           })

      .def("Rename",
           [](BindingClient& self, Ino old_parent, const std::string& old_name,
              Ino new_parent, const std::string& new_name) {
             return self.Rename(old_parent, old_name, new_parent, new_name);
           })

      // ---- Extended attributes ----

      .def("SetXattr",
           [](BindingClient& self, Ino ino, const std::string& name,
              const std::string& value, int flags) {
             return self.SetXattr(ino, name, value, flags);
           })

      .def("GetXattr",
           [](BindingClient& self, Ino ino, const std::string& name) {
             std::string value;
             Status s = self.GetXattr(ino, name, &value);
             return std::make_tuple(s, value);
           })

      .def("ListXattr",
           [](BindingClient& self, Ino ino) {
             std::vector<std::string> xattrs;
             Status s = self.ListXattr(ino, &xattrs);
             return std::make_tuple(s, xattrs);
           })

      .def("RemoveXattr",
           [](BindingClient& self, Ino ino, const std::string& name) {
             return self.RemoveXattr(ino, name);
           })

      // ---- Misc ----

      .def("GetMaxNameLength",
           [](BindingClient& self) {
             return self.GetMaxNameLength();
           })

      // Ioctl: in_buf as bytes, out_buf returned as bytes.
      .def("Ioctl",
           [](BindingClient& self, Ino ino, uint32_t uid, unsigned int cmd,
              unsigned int flags, nb::bytes in_buf, size_t out_bufsz) {
             std::string out(out_bufsz, '\0');
             Status s = self.Ioctl(ino, uid, cmd, flags,
                                   in_buf.data(),
                                   in_buf.size(),
                                   out.data(), out_bufsz);
             return std::make_tuple(s,
                 nb::bytes(out.data(), out.size()));
           });

  // ----------------------------------------------------------------
  // Module-level option helpers (delegate to BindingClient statics)
  // ----------------------------------------------------------------

  m.def("set_option",
        [](const std::string& key, const std::string& value) {
          return dingofs::client::BindingClient::SetOption(key, value);
        },
        nb::arg("key"), nb::arg("value"));

  m.def("get_option",
        [](const std::string& key) {
          std::string value;
          Status s = dingofs::client::BindingClient::GetOption(key, &value);
          return std::make_tuple(s, value);
        },
        nb::arg("key"));

  m.def("list_options",
        []() { return dingofs::client::BindingClient::ListOptions(); });

  m.def("print_options",
        []() { dingofs::client::BindingClient::PrintOptions(); });

  // ----------------------------------------------------------------
  // Module-level constants
  // ----------------------------------------------------------------
  m.attr("ROOT_INO") = static_cast<uint64_t>(dingofs::kRootIno);

  m.attr("SET_ATTR_MODE")      = dingofs::kSetAttrMode;
  m.attr("SET_ATTR_UID")       = dingofs::kSetAttrUid;
  m.attr("SET_ATTR_GID")       = dingofs::kSetAttrGid;
  m.attr("SET_ATTR_SIZE")      = dingofs::kSetAttrSize;
  m.attr("SET_ATTR_ATIME")     = dingofs::kSetAttrAtime;
  m.attr("SET_ATTR_MTIME")     = dingofs::kSetAttrMtime;
  m.attr("SET_ATTR_ATIME_NOW") = dingofs::kSetAttrAtimeNow;
  m.attr("SET_ATTR_MTIME_NOW") = dingofs::kSetAttrMtimeNow;
  m.attr("SET_ATTR_CTIME")     = dingofs::kSetAttrCtime;
  m.attr("SET_ATTR_FLAGS")     = dingofs::kSetAttrFlags;
  m.attr("SET_ATTR_NLINK")     = dingofs::kSetAttrNlink;
}
