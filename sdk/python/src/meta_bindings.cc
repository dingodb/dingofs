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

#include "meta_bindings.h"

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/map.h>

#include "binding_client.h"
#include "common/meta.h"

void DefineMetaBindings(nanobind::module_& m) {
  namespace nb = nanobind;
  using dingofs::Attr;
  using dingofs::DirEntry;
  using dingofs::FileType;
  using dingofs::FsStat;
  using dingofs::client::OptionInfo;

  nb::enum_<FileType>(m, "FileType")
      .value("Directory", FileType::kDirectory)
      .value("Symlink",   FileType::kSymlink)
      .value("File",      FileType::kFile);

  nb::class_<Attr>(m, "Attr")
      .def(nb::init<>())
      .def_rw("ino",     &Attr::ino)
      .def_rw("mode",    &Attr::mode)
      .def_rw("nlink",   &Attr::nlink)
      .def_rw("uid",     &Attr::uid)
      .def_rw("gid",     &Attr::gid)
      .def_rw("length",  &Attr::length)
      .def_rw("rdev",    &Attr::rdev)
      .def_rw("atime",   &Attr::atime)
      .def_rw("mtime",   &Attr::mtime)
      .def_rw("ctime",   &Attr::ctime)
      .def_rw("type",    &Attr::type)
      .def_rw("flags",   &Attr::flags)
      .def_rw("version", &Attr::version)
      .def_rw("parents", &Attr::parents)
      .def_rw("xattrs",  &Attr::xattrs);

  nb::class_<DirEntry>(m, "DirEntry")
      .def(nb::init<>())
      .def_rw("ino",  &DirEntry::ino)
      .def_rw("name", &DirEntry::name)
      .def_rw("attr", &DirEntry::attr);

  nb::class_<FsStat>(m, "FsStat")
      .def(nb::init<>())
      .def_rw("max_bytes",   &FsStat::max_bytes)
      .def_rw("used_bytes",  &FsStat::used_bytes)
      .def_rw("max_inodes",  &FsStat::max_inodes)
      .def_rw("used_inodes", &FsStat::used_inodes);

  nb::class_<OptionInfo>(m, "OptionInfo")
      .def(nb::init<>())
      .def_rw("name",          &OptionInfo::name)
      .def_rw("type",          &OptionInfo::type)
      .def_rw("default_value", &OptionInfo::default_value)
      .def_rw("description",   &OptionInfo::description)
      .def("__repr__", [](const OptionInfo& o) {
          return "<OptionInfo name=" + o.name + " type=" + o.type + ">";
      });
}
