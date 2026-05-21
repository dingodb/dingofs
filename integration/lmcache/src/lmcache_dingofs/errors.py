# SPDX-License-Identifier: Apache-2.0
"""Exception types raised by the dingofs LMCache connector."""

from __future__ import annotations


class DingoFSConnectorError(RuntimeError):
    """Base exception for all errors raised by the dingofs connector."""


class DingoFSURLError(DingoFSConnectorError, ValueError):
    """Raised when `dingofs://` URL parsing fails."""


class DingoFSConfigError(DingoFSConnectorError, ValueError):
    """Raised when L2 adapter config is malformed."""
