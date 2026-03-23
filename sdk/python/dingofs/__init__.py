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

from .client import Client, ScandirIterator
from .config import Config
from .exceptions import DingofsError
from .file import DingoFile

try:
    from ._dingofs_core import (  # type: ignore[import]
        Attr,
        DirEntry,
        FsStat,
        SET_ATTR_MODE,
        SET_ATTR_UID,
        SET_ATTR_GID,
        SET_ATTR_SIZE,
        SET_ATTR_ATIME,
        SET_ATTR_MTIME,
        SET_ATTR_ATIME_NOW,
        SET_ATTR_MTIME_NOW,
        SET_ATTR_CTIME,
        SET_ATTR_FLAGS,
        OptionInfo,
    )
except ModuleNotFoundError:
    pass
