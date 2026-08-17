#!/usr/bin/env python3
"""Case 34: two fds on same file - writer's data visible to reader fd."""
import os
from common import run_case, rand_data


def case(c, d):
    p = os.path.join(d, "x")
    open(p, "wb").close()
    wfd = os.open(p, os.O_WRONLY)
    rfd = os.open(p, os.O_RDONLY)
    try:
        data = rand_data(65536, seed=34)
        os.write(wfd, data)
        os.fsync(wfd)
        got = os.pread(rfd, len(data), 0)
        c.check_eq(got, data, "reader fd sees writer fd data after fsync")
        data2 = rand_data(1000, seed=344)
        os.pwrite(wfd, data2, 100)
        os.fsync(wfd)
        c.check_eq(os.pread(rfd, 1000, 100), data2, "overwrite visible via reader fd")
    finally:
        os.close(wfd)
        os.close(rfd)


if __name__ == "__main__":
    run_case("34_multi_fd_visibility", case)
