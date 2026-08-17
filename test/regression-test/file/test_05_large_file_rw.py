#!/usr/bin/env python3
"""Case 05: large file (default 1GB, override DINGOFS_LARGE_SIZE_MB) chunked write/read verify."""
import hashlib
import os
from common import run_case, rand_data


def case(c, d):
    size_mb = int(os.environ.get("DINGOFS_LARGE_SIZE_MB", "1024"))
    chunk = 4 << 20
    p = os.path.join(d, "large")
    h_w = hashlib.md5()
    written = 0
    with open(p, "wb") as f:
        i = 0
        while written < size_mb << 20:
            data = rand_data(chunk, seed=i)
            f.write(data)
            h_w.update(data)
            written += len(data)
            i += 1
    c.check_eq(os.path.getsize(p), written, "large file size")
    h_r = hashlib.md5()
    with open(p, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h_r.update(b)
    c.check_eq(h_r.hexdigest(), h_w.hexdigest(), "large file md5")


if __name__ == "__main__":
    run_case("05_large_file_rw", case)
