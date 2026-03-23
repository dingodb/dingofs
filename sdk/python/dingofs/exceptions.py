# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DingoFS exception hierarchy."""


class DingofsError(OSError):
    """Base exception for all DingoFS errors.

    Inherits from :class:`OSError` so that ``errno`` / ``strerror`` semantics
    work as expected.  The ``dingofs_errno`` attribute carries the POSIX error
    number translated from the C++ ``Status::ToSysErrNo()`` method.

    Args:
        errno:   POSIX error number (e.g. ``errno.ENOENT``).
        message: Human-readable description of the error.
    """

    def __init__(self, errno: int, message: str) -> None:
        super().__init__(errno, message)
        self.dingofs_errno = errno
