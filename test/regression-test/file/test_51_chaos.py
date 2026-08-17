#!/usr/bin/env python3
"""Case 51 (slow): chaos test - random create/write/read/truncate/rename/delete,
tracking expected state in memory and comparing at each step and at the end.
Env: DINGOFS_CHAOS_OPS (default 2000), DINGOFS_CHAOS_SEED."""
import os
import random
from common import run_case, rand_data


def case(c, d):
    ops = int(os.environ.get("DINGOFS_CHAOS_OPS", "2000"))
    seed = int(os.environ.get("DINGOFS_CHAOS_SEED", "51"))
    rnd = random.Random(seed)
    state = {}  # name -> bytearray
    mismatches = 0
    for step in range(ops):
        op = rnd.choice(["create", "write", "read", "truncate", "rename", "delete", "stat"])
        if not state and op != "create":
            op = "create"
        if op == "create":
            name = "f%d" % rnd.randrange(10000)
            data = rand_data(rnd.randrange(0, 65536), seed=step)
            with open(os.path.join(d, name), "wb") as f:
                f.write(data)
            state[name] = bytearray(data)
        else:
            name = rnd.choice(list(state))
            p = os.path.join(d, name)
            exp = state[name]
            if op == "write":
                off = rnd.randrange(0, len(exp) + 1) if exp else 0
                buf = rand_data(rnd.randrange(1, 8192), seed=step)
                with open(p, "r+b") as f:
                    f.seek(off)
                    f.write(buf)
                if off + len(buf) > len(exp):
                    exp.extend(b"\x00" * (off + len(buf) - len(exp)))
                exp[off:off + len(buf)] = buf
            elif op == "read":
                with open(p, "rb") as f:
                    if f.read() != bytes(exp):
                        mismatches += 1
            elif op == "truncate":
                n = rnd.randrange(0, len(exp) + 4096)
                os.truncate(p, n)
                if n <= len(exp):
                    del exp[n:]
                else:
                    exp.extend(b"\x00" * (n - len(exp)))
            elif op == "rename":
                new = "f%d" % rnd.randrange(10000)
                if new != name:
                    os.rename(p, os.path.join(d, new))
                    state[new] = state.pop(name)
            elif op == "delete":
                os.unlink(p)
                del state[name]
            elif op == "stat":
                if os.path.getsize(p) != len(exp):
                    mismatches += 1
    c.check_eq(mismatches, 0, "no mid-run mismatches over %d ops" % ops)
    listed = set(os.listdir(d))
    c.check(listed == set(state), "final directory listing matches expected set "
            "(extra=%s missing=%s)" % (sorted(listed - set(state))[:5],
                                       sorted(set(state) - listed)[:5]))
    bad = sum(1 for n, exp in state.items()
              if open(os.path.join(d, n), "rb").read() != bytes(exp))
    c.check_eq(bad, 0, "final content of all %d files correct" % len(state))


if __name__ == "__main__":
    run_case("51_chaos", case)
