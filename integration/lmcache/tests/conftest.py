# SPDX-License-Identifier: Apache-2.0
"""pytest configuration for the dingofs LMCache connector test suite."""

from __future__ import annotations

import sys
from pathlib import Path

# Add src/ to sys.path so the tests can `import lmcache_dingofs` and
# `import dingofs_connector` without requiring a wheel install. The native
# extension is still picked up from its installed location (or from the
# CMake build dir if the user copies it in).
_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
