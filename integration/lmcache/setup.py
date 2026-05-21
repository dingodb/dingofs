# SPDX-License-Identifier: Apache-2.0
"""setup.py shim — exists only so the wheel gets the correct platform / ABI tag.

The package ships a pre-built CPython extension (`_dingofs_native.*.so`) as
package_data; without this shim setuptools would tag the wheel as
`py3-none-any`, and pip would happily install it onto an incompatible
Python / OS, causing an ImportError at runtime instead of at install time.
"""

from setuptools import Distribution, setup


class BinaryDistribution(Distribution):
    """Force setuptools to treat this package as platform-specific."""

    def has_ext_modules(self) -> bool:  # type: ignore[override]
        return True

    def is_pure(self) -> bool:  # type: ignore[override]
        return False


setup(distclass=BinaryDistribution)
