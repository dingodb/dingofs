#!/usr/bin/env python3
"""事务 RPC 耗时时序图：横轴时间、纵轴耗时，支持多并发度对比。

数据源是 SDK 的 unary_rpc.h:174 日志行，形如：

  mds.info.log.20260825-021826.4127359:W20260825 02:18:26.624899 4127380 unary_rpc.h:174] \
[OnRpcDone] [sdk][55002085872894208][StoreService.TxnPrewriteRpc] total_time_us(1159) \
total_phase_time_us(35) raft_commit_time_us(750) total_mvcc_version(0) total_internal_skipped(0) \
 check_lock(19 0 0 0 0 0) check_rollback(8 0 0 0 0 0) check_write_conflict(8 0 2 0 0 0)

两段式用法（40GB 日志只扫一次，之后换图是秒级）：

  # 1) 解析成缓存（每个 bench 并发度各跑一次；--concurrency 填 --bench_threads 的值）
  ./txn_latency_timeline.py parse all_txn_rpc.log -o t32.npz --label t32 --concurrency 32

  # 2) 出图
  ./txn_latency_timeline.py plot t32.npz -o t32.png
  ./txn_latency_timeline.py plot t16=t16.npz t32=t32.npz t64=t64.npz -o compare.png

  # 裸路径 = 按需 parse + plot
  ./txn_latency_timeline.py all_txn_rpc.log

缓存里存的是每秒一桶的对数直方图（可加），所以换分桶只是 reshape+sum，不用重扫日志。
注意：本机无 CJK 字体，图内所有文字必须是 ASCII。
"""

import argparse
import json
import os
import sys
import time
import warnings
from multiprocessing import Pool

import numpy as np

# ---------------------------------------------------------------- 直方图分箱

# cell 0        : 精确等于 0（读 RPC 的 raft_commit 全部落这里）
# cell 1..384   : log2 分箱，16 bins/octave，覆盖 [1us, 2^24us=16.78s)
# cell 385      : 溢出（>= 16.78s）
SUB = 16                       # 每个 octave 的箱数
OCT = 24                       # octave 数
NBINS = 1 + SUB * OCT + 1      # 386
BIN_EDGES = np.concatenate([[0.0], 2.0 ** (np.arange(SUB * OCT + 1) / SUB), [np.inf]])
# BIN_EDGES[i] 是 cell i 的下沿：cell1 下沿 1us，cell385 下沿 2^24us

METRICS = ("total", "raft", "apply", "phase")
METRIC_LABEL = {
    "total": "total_time_us",
    "raft": "raft_commit_time_us",
    "apply": "raft_apply_time_us",       # 新版 SDK 才有，旧日志里恒为 0
    "phase": "total_phase_time_us",
}
RPC_ALIAS = {
    "write": ("TxnPrewrite", "TxnCommit"),
    "read": ("TxnBatchGet", "TxnGet", "TxnScan"),
}


def bin_index(v):
    """把 uint 微秒值数组映射到 cell 下标 [0, 385]。"""
    v = np.asarray(v, dtype=np.int64)
    idx = np.zeros(v.shape, dtype=np.int64)
    nz = v > 0
    if nz.any():
        b = (SUB * np.log2(v[nz].astype(np.float64))).astype(np.int64)
        idx[nz] = 1 + np.clip(b, 0, SUB * OCT)  # SUB*OCT -> cell 385 溢出
    return idx


# ---------------------------------------------------------------- 字体

_HAS_CJK = None


def _setup_font():
    """图里要写中文就得有中文字体。本机装的是 fonts-wqy-microhei；
    没有的话自动退回全英文标签，脚本换台机器也能跑。"""
    global _HAS_CJK
    if _HAS_CJK is not None:
        return _HAS_CJK
    import glob
    import matplotlib
    import matplotlib.font_manager as fm
    names = {f.name for f in fm.fontManager.ttflist}
    want = [n for n in names if any(k in n for k in ("WenQuanYi", "Noto Sans CJK", "Source Han"))]
    if not want:
        for c in glob.glob("/usr/share/fonts/**/*.tt[cf]", recursive=True) + \
                 glob.glob("/usr/share/fonts/**/*.otf", recursive=True) + \
                 glob.glob(os.path.expanduser("~/.local/share/fonts/*"), recursive=True):
            if any(k in c.lower() for k in ("wqy", "microhei", "notosanscjk", "sourcehan")):
                try:
                    fm.fontManager.addfont(c)
                    want.append(fm.FontProperties(fname=c).get_name())
                except Exception:
                    pass
    if want:
        matplotlib.rcParams["font.sans-serif"] = [want[0], "DejaVu Sans"]
        matplotlib.rcParams["axes.unicode_minus"] = False
    _HAS_CJK = bool(want)
    return _HAS_CJK


def T(zh, en):
    """有中文字体就用中文，没有就退回英文（否则渲染成豆腐块）。"""
    return zh if _setup_font() else en


# ---------------------------------------------------------------- 解析

_DAY_CACHE = {}


def _day_number(d):
    """b'20260825' -> 距离 1970-01-01 的天数。只有一两个不同取值，缓存住。"""
    n = _DAY_CACHE.get(d)
    if n is None:
        y, m, dd = int(d[0:4]), int(d[4:6]), int(d[6:8])
        # 民用历天数公式，避免每行走 datetime
        a = (14 - m) // 12
        yy = y + 4800 - a
        mm = m + 12 * a - 3
        jdn = dd + (153 * mm + 2) // 5 + 365 * yy + yy // 4 - yy // 100 + yy // 400 - 32045
        n = jdn - 2440588
        _DAY_CACHE[d] = n
    return n


def _line_epoch_sec(line):
    """取出该行的绝对秒（Unix epoch，按日志里写的时区，不做换算）。失败返回 None。"""
    head = line[:200]
    if head[:1] in (b"W", b"I", b"E", b"F") and head[1:9].isdigit():
        p = 0
    else:
        q = head.find(b":W")
        if q < 0:
            return None
        p = q + 1
    d = line[p + 1:p + 9]
    if not d.isdigit():
        return None
    t = line[p + 10:p + 18]  # HH:MM:SS
    try:
        hh = (t[0] - 48) * 10 + (t[1] - 48)
        mm = (t[3] - 48) * 10 + (t[4] - 48)
        ss = (t[6] - 48) * 10 + (t[7] - 48)
    except IndexError:
        return None
    return _day_number(d) * 86400 + hh * 3600 + mm * 60 + ss


class _Acc:
    """单个 RPC 的累加器。桶数固定，metric 三个。"""

    __slots__ = ("cnt", "sum", "mn", "mx", "hist")

    def __init__(self, nb):
        self.cnt = np.zeros(nb, dtype=np.int64)
        self.sum = [np.zeros(nb, dtype=np.float64) for _ in METRICS]
        self.mn = [np.full(nb, np.iinfo(np.int64).max, dtype=np.int64) for _ in METRICS]
        self.mx = [np.zeros(nb, dtype=np.int64) for _ in METRICS]
        self.hist = [np.zeros((nb, NBINS), dtype=np.uint32) for _ in METRICS]


def _flush(acc, nb, base, buf, clamped):
    """把一批 (bucket, rpc, tot, phase, raft) 向量化地灌进累加器。

    桶下标一律先减去 base 转成 worker 局部下标；用 unique/inverse 分组，
    所以即使一批里跨了很大的时间缺口，内存也只跟"出现过的桶数"成正比。
    """
    if not buf[0]:
        return clamped
    n = len(buf[0])
    b = np.fromiter(buf[0], dtype=np.int64, count=n) - base
    lo_bad = b < 0
    hi_bad = b >= nb
    if lo_bad.any() or hi_bad.any():
        clamped += int(lo_bad.sum() + hi_bad.sum())
        np.clip(b, 0, nb - 1, out=b)
    rn = buf[1]
    vals = [np.fromiter(buf[2], dtype=np.int64, count=n),   # total
            np.fromiter(buf[4], dtype=np.int64, count=n),   # raft
            np.fromiter(buf[5], dtype=np.int64, count=n),   # apply
            np.fromiter(buf[3], dtype=np.int64, count=n)]   # phase
    rarr = np.array(rn, dtype=object)
    for name in set(rn):
        m = rarr == name
        a = acc.get(name)
        if a is None:
            a = acc[name] = _Acc(nb)
        loc = b[m]
        order = np.argsort(loc, kind="stable")
        ls = loc[order]
        uq, st = np.unique(ls, return_index=True)
        grp = np.diff(np.append(st, len(ls)))          # 每个桶的样本数
        inv = np.repeat(np.arange(len(uq)), grp)       # 排序后每个样本属于第几个 uq
        a.cnt[uq] += grp
        for mi in range(len(METRICS)):
            vv = vals[mi][m]
            vs = vv[order]
            a.sum[mi][uq] += np.add.reduceat(vs.astype(np.float64), st)
            # 花式索引取出来的是副本，必须整体赋值回去，不能用 out=
            a.mx[mi][uq] = np.maximum(a.mx[mi][uq], np.maximum.reduceat(vs, st))
            a.mn[mi][uq] = np.minimum(a.mn[mi][uq], np.minimum.reduceat(vs, st))
            flat = inv * NBINS + bin_index(vs)
            a.hist[mi][uq] += np.bincount(
                flat, minlength=len(uq) * NBINS).reshape(len(uq), NBINS).astype(np.uint32)
    for lst in buf:
        del lst[:]
    return clamped


def _probe_range(path, lo, hi, t0, atom):
    """探测一个 byte-range 覆盖的桶区间。chunk 是连续字节 => 时间也基本连续。"""
    with open(path, "rb") as f:
        if lo:
            f.seek(lo - 1)
            f.readline()
        t_lo = None
        for _ in range(8):
            ln = f.readline()
            if not ln:
                break
            t_lo = _line_epoch_sec(ln)
            if t_lo is not None:
                break
        f.seek(max(lo, hi - 65536))
        tail = f.read(hi - max(lo, hi - 65536) + 65536).splitlines()
        t_hi = None
        for ln in reversed(tail):
            t_hi = _line_epoch_sec(ln)
            if t_hi is not None:
                break
    if t_lo is None and t_hi is None:
        return None
    t_lo = t_lo if t_lo is not None else t_hi
    t_hi = t_hi if t_hi is not None else t_lo
    b1 = (min(t_lo, t_hi) - t0) // atom
    b2 = (max(t_lo, t_hi) - t0) // atom
    return int(b1), int(b2)


def _parse_chunk(job):
    """一个 byte-range worker。约定：一行归属它首字节所在的 chunk。"""
    path, lo, hi, t0, atom, nb_global = job
    rng = _probe_range(path, lo, hi, t0, atom)
    if rng is None:
        return 0, {}, 0, 0, 0, 0
    MARGIN = 120                       # 线程间时间戳抖动只有 ms 级，120 桶足够宽裕
    base = max(0, rng[0] - MARGIN)
    nb = min(nb_global, rng[1] + MARGIN + 1) - base
    if nb <= 0:
        return 0, {}, 0, 0, 0, 0
    acc = {}
    buf = ([], [], [], [], [], [])   # bucket, rpc, total, phase, raft, apply
    n_lines = n_bad = n_anom = clamped = 0
    BATCH = 200_000
    with open(path, "rb") as f:
        if lo:
            f.seek(lo - 1)
            f.readline()          # 丢掉跨界的半行（它属于上一个 chunk）
        else:
            f.seek(0)
        while True:
            if f.tell() > hi:
                break
            line = f.readline()
            if not line:
                break
            n_lines += 1
            i = line.find(b"StoreService.")
            if i < 0:
                n_bad += 1
                continue
            j = line.find(b"Rpc]", i)
            if j < 0:
                n_bad += 1
                continue
            sec = _line_epoch_sec(line)
            if sec is None:
                n_bad += 1
                continue
            try:
                a = line.index(b"total_time_us(", j)
                tot = int(line[a + 14:line.index(b")", a)])
                a = line.index(b"total_phase_time_us(", a)
                ph = int(line[a + 20:line.index(b")", a)])
                a = line.index(b"raft_commit_time_us(", a)
                rc = int(line[a + 20:line.index(b")", a)])
                ap = 0
                k = line.find(b"raft_apply_time_us(", a)      # 新版 SDK 才有这个字段
                if 0 <= k < line.find(b"total_internal_skipped(", a):
                    ap = int(line[k + 19:line.index(b")", k)])
                a = line.index(b"total_internal_skipped(", a)
                a = line.index(b")", a)
            except ValueError:
                n_bad += 1
                continue
            if not line[a + 1:].strip():
                n_anom += 1          # 无 phase 元组：服务端早退（region epoch 不匹配等）
            buf[0].append((sec - t0) // atom)
            buf[1].append(line[i + 13:j].decode("ascii", "replace"))
            buf[2].append(tot)
            buf[3].append(ph)
            buf[4].append(rc)
            buf[5].append(ap)
            if len(buf[0]) >= BATCH:
                clamped = _flush(acc, nb, base, buf, clamped)
    clamped = _flush(acc, nb, base, buf, clamped)
    packed = {name: (a.cnt, a.sum, a.mn, a.mx, a.hist) for name, a in acc.items()}
    return base, packed, n_lines, n_bad, n_anom, clamped


def _first_last_sec(path):
    """取首行和末行的绝对秒，用来定桶范围。

    原始 glog 文件开头有几行文件头（"Log file created at:" 等），所以要往下多扫几行；
    这样也支持直接喂未经 grep 过滤的 mds.info.log。
    """
    t_first = t_last = None
    with open(path, "rb") as f:
        for _ in range(1000):
            ln = f.readline()
            if not ln:
                break
            t_first = _line_epoch_sec(ln)
            if t_first is not None:
                break
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - (1 << 20)))
        tail = f.read().splitlines()
    for ln in reversed(tail):
        t_last = _line_epoch_sec(ln)
        if t_last is not None:
            break
    return t_first, t_last


def do_parse(args):
    paths = args.logs
    for p in paths:
        if not os.path.isfile(p):
            sys.exit(f"错误：文件不存在 {p}")
    t_first, t_last = _first_last_sec(paths[0])
    tl = _first_last_sec(paths[-1])[1]
    if t_first is None or tl is None:
        sys.exit("错误：文件里找不到可解析的 glog 时间戳，确认这是 MDS 日志（或已过滤出的 unary_rpc.h:174 行）")
    t0 = t_first
    nb = int((tl - t0) // args.bucket) + 2
    print(f"时间范围 {_fmt_clock(t0)} -> {_fmt_clock(tl)}  跨度 {tl - t0 + 1}s  "
          f"分桶 {args.bucket}s -> {nb} 个桶", file=sys.stderr)

    jobs = []
    for p in paths:
        size = os.path.getsize(p)
        n = max(1, args.jobs)
        step = max(1, size // n)
        off = 0
        while off < size:
            end = min(size, off + step)
            jobs.append((p, off, end - 1, t0, args.bucket, nb))
            off = end
    t_start = time.time()
    if len(jobs) == 1:
        results = [_parse_chunk(jobs[0])]
    else:
        with Pool(min(args.jobs, len(jobs))) as pool:
            results = pool.map(_parse_chunk, jobs)
    elapsed = time.time() - t_start

    merged, n_lines, n_bad, n_anom, n_clamp = {}, 0, 0, 0, 0
    for base, packed, nl, nbad, nan, ncl in results:
        n_lines += nl
        n_bad += nbad
        n_anom += nan
        n_clamp += ncl
        for name, (cnt, ssum, smn, smx, shist) in packed.items():
            m = merged.get(name)
            if m is None:
                m = merged[name] = _Acc(nb)
            k = len(cnt)
            sl = slice(base, base + k)
            m.cnt[sl] += cnt
            for mi in range(len(METRICS)):
                m.sum[mi][sl] += ssum[mi]
                np.maximum(m.mx[mi][sl], smx[mi], out=m.mx[mi][sl])
                np.minimum(m.mn[mi][sl], smn[mi], out=m.mn[mi][sl])
                m.hist[mi][sl] += shist[mi]
    if n_clamp:
        print(f"警告：{n_clamp} 行的时间戳落在所属 chunk 的预估桶范围外，已夹到边界", file=sys.stderr)

    total_bytes = sum(os.path.getsize(p) for p in paths)
    n_ok = n_lines - n_bad
    skip = f"，跳过非目标行 {n_bad:,}" if n_bad else ""
    print(f"解析完成：{n_ok:,} 条 txn RPC 记录{skip}，{len(merged)} 种 RPC，早退行 {n_anom:,}；"
          f"耗时 {elapsed:.1f}s（{total_bytes / elapsed / 1e6:.0f} MB/s，"
          f"{n_lines / elapsed / 1e6:.2f} M行/s）", file=sys.stderr)

    out = args.out or (os.path.splitext(paths[0])[0] + ".npz")
    label = args.label or os.path.basename(os.path.splitext(paths[0])[0])
    conc = args.concurrency
    if conc is None:
        import re
        m = re.search(r"(\d+)$", label)
        conc = int(m.group(1)) if m else None
    arrays, names = {}, sorted(merged)
    for name in names:
        a = merged[name]
        arrays[f"cnt/{name}"] = a.cnt.astype(np.uint32)
        for mi, mk in enumerate(METRICS):
            mn = a.mn[mi].copy()
            mn[a.cnt == 0] = 0
            arrays[f"sum/{name}/{mk}"] = a.sum[mi]
            arrays[f"min/{name}/{mk}"] = mn.astype(np.uint32)
            arrays[f"max/{name}/{mk}"] = a.mx[mi].astype(np.uint32)
            arrays[f"hist/{name}/{mk}"] = a.hist[mi]
    meta = dict(schema=1, label=label, concurrency=conc, t0_sec=int(t0),
                bucket_sec=int(args.bucket), n_buckets=int(nb), rpcs=names,
                metrics=list(METRICS), sources=[os.path.abspath(p) for p in paths],
                n_lines=int(n_lines), n_bad=int(n_bad), n_anomaly=int(n_anom),
                parse_seconds=round(elapsed, 1))
    np.savez_compressed(out, meta=json.dumps(meta), bin_edges=BIN_EDGES, **arrays)
    print(f"缓存已写出：{out}  ({os.path.getsize(out) / 1e6:.1f} MB)", file=sys.stderr)
    return out


# ---------------------------------------------------------------- 缓存读取 / 聚合

class Run:
    def __init__(self, path, label=None):
        z = np.load(path, allow_pickle=False)
        self.z = z
        self.meta = json.loads(str(z["meta"]))
        self.edges = z["bin_edges"]
        self.label = label or self.meta["label"]
        self.conc = self.meta.get("concurrency")
        self.t0 = self.meta["t0_sec"]
        self.atom = self.meta["bucket_sec"]
        self.nb = self.meta["n_buckets"]
        self.rpcs = self.meta["rpcs"]

    def select(self, spec):
        if spec in RPC_ALIAS:
            want = RPC_ALIAS[spec]
        elif spec == "all":
            want = tuple(self.rpcs)
        else:
            want = tuple(s.strip() for s in spec.split(","))
        return [r for r in want if r in self.rpcs]

    def series(self, rpcs, metric, factor):
        """把若干 RPC 的原子桶按 factor 合并，返回 (cnt, sum, mn, mx, hist)。"""
        factor = max(1, min(int(factor), self.nb))
        nb = (self.nb // factor) * factor
        cnt = np.zeros(self.nb, dtype=np.int64)
        s = np.zeros(self.nb)
        mn = np.full(self.nb, np.iinfo(np.int64).max, dtype=np.int64)
        mx = np.zeros(self.nb, dtype=np.int64)
        h = np.zeros((self.nb, NBINS), dtype=np.int64)
        for r in rpcs:
            c = self.z[f"cnt/{r}"].astype(np.int64)
            cnt += c
            s += self.z[f"sum/{r}/{metric}"]
            rmn = self.z[f"min/{r}/{metric}"].astype(np.int64)
            rmn[c == 0] = np.iinfo(np.int64).max
            np.minimum(mn, rmn, out=mn)
            np.maximum(mx, self.z[f"max/{r}/{metric}"].astype(np.int64), out=mx)
            h += self.z[f"hist/{r}/{metric}"].astype(np.int64)
        if factor > 1:
            k = nb // factor
            cnt = cnt[:nb].reshape(k, factor).sum(1)
            s = s[:nb].reshape(k, factor).sum(1)
            mn = mn[:nb].reshape(k, factor).min(1)
            mx = mx[:nb].reshape(k, factor).max(1)
            h = h[:nb].reshape(k, factor, NBINS).sum(1)
        mn = np.where(cnt > 0, mn, 0)
        return cnt, s, mn, mx, h


def _roll_med(y, w):
    """居中滚动中位数，忽略 NaN。周期性尖峰下比滚动均值稳得多。"""
    y = np.asarray(y, dtype=float)
    if w <= 1 or len(y) < w:
        return y
    pad = w // 2
    padded = np.concatenate([np.full(pad, np.nan), y, np.full(w - 1 - pad, np.nan)])
    win = np.lib.stride_tricks.sliding_window_view(padded, w)
    with warnings.catch_warnings():          # 全 NaN 的窗口是正常的（空桶/低样本区）
        warnings.simplefilter("ignore", RuntimeWarning)
        out = np.nanmedian(win, axis=1)
    return np.where(np.isnan(y), np.nan, out)


def _min_n(q, floor):
    """算 q 分位所需的最小样本数：尾部越极端要求越高（10/(1-q)）。"""
    if q >= 100:
        return floor
    return max(floor, int(10.0 / (1.0 - q / 100.0)))


def hist_pct(h, cnt, mn, mx, q, edges=BIN_EDGES):
    """从直方图取百分位：左连续逆 CDF + 桶内几何插值，用精确 min/max 夹紧。"""
    out = np.full(h.shape[0], np.nan)
    ok = cnt > 0
    if not ok.any():
        return out
    cum = np.cumsum(h[ok], axis=1)
    n = cnt[ok]
    tgt = q / 100.0 * n
    idx = np.array([np.searchsorted(cum[i], tgt[i], side="left") for i in range(cum.shape[0])])
    idx = np.clip(idx, 0, NBINS - 1)
    rows = np.arange(cum.shape[0])
    c0 = np.where(idx > 0, cum[rows, np.maximum(idx - 1, 0)], 0)
    ch = h[ok][rows, idx]
    frac = np.clip(np.divide(tgt - c0, np.maximum(ch, 1)), 0.0, 1.0)
    lo = edges[idx]
    hi = edges[np.minimum(idx + 1, NBINS)]
    est = np.where(idx == 0, 0.0,
                   np.where(np.isfinite(hi), lo * (hi / np.maximum(lo, 1e-9)) ** frac, mx[ok]))
    out[ok] = np.clip(est, mn[ok], mx[ok])
    return out


# ---------------------------------------------------------------- 劣化窗口检测

def detect_windows(sig, cnt, bucket_sec, min_cnt=1000,
                   enter_factor=2.0, abs_floor=500.0, min_enter=3,
                   exit_factor=1.3, min_exit=5, merge_gap=20):
    """Schmitt 触发 + 20 分位基线。返回 [(i_start, i_end, baseline, peak), ...]。

    基线取全程 20 分位而不是开头窗口：如果一开始就慢，用头部窗口会把劣化本身当基线。
    """
    qual = (cnt >= min_cnt) & np.isfinite(sig)
    if qual.sum() < 20:
        return [], np.nan
    base = float(np.percentile(sig[qual], 20))
    enter = max(base * enter_factor, base + abs_floor)
    exit_ = base * exit_factor
    wins, run_in, run_out, start = [], 0, 0, None
    for i in range(len(sig)):
        if not qual[i]:
            continue
        if start is None:
            run_in = run_in + 1 if sig[i] > enter else 0
            if run_in >= min_enter:
                start = i - min_enter + 1
                run_out = 0
        else:
            run_out = run_out + 1 if sig[i] < exit_ else 0
            if run_out >= min_exit:
                wins.append((start, i - min_exit + 1))
                start, run_in = None, 0
    if start is not None:
        wins.append((start, len(sig) - 1))
    # 合并靠得很近的窗口：持续劣化叠周期尖峰时，信号会在阈值附近来回，
    # 不合并的话一段连续劣化会被打碎成十几个窗口，反而看不出"持续了多久"
    merged = []
    for a, b in wins:
        if merged and a - merged[-1][1] <= merge_gap:
            merged[-1][1] = b
        else:
            merged.append([a, b])
    return [(a, b, base, float(np.nanmax(sig[a:b + 1]))) for a, b in merged], base


# ---------------------------------------------------------------- 绘图

def _fmt_clock(sec):
    s = int(sec) % 86400
    return "%02d:%02d:%02d" % (s // 3600, s % 3600 // 60, s % 60)


def _fmt_dur(sec):
    sec = int(sec)
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m{sec % 60:02d}s"
    return f"{sec // 3600}h{sec % 3600 // 60:02d}m"


def _lat_fmt(v, _=None):
    if v <= 0:
        return "0"
    if v < 1000:
        return f"{v:.0f}us"
    if v < 1e6:
        return f"{v / 1000:.0f}ms" if v >= 1e4 else f"{v / 1000:.1f}ms"
    return f"{v / 1e6:.1f}s"


def _pick_factor(run, span_sec, want_points):
    f = max(1, int(round(span_sec / run.atom / want_points)))
    for c in (1, 2, 5, 10, 15, 30, 60, 120, 300, 600):
        if c * run.atom >= f * run.atom:
            return max(1, int(c * run.atom // run.atom))
    return f


def plot_simple(run, args):
    """简版图：只回答"什么时候开始慢、慢了多久、是不是变忙导致的"三个问题。

    刻意只画三条线，纵轴标成"毫秒"，图上直接把结论写成一句话。
    """
    import matplotlib
    matplotlib.use("Agg")
    _setup_font()
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    rpcs = run.select(args.rpc)
    if not rpcs:
        sys.exit(f"错误：{run.label} 里没有匹配 --rpc {args.rpc} 的 RPC，可选：{run.rpcs}")
    span = run.nb * run.atom
    factor = args.bucket_factor or _pick_factor(run, span, 700)
    bs = factor * run.atom
    cnt, ssum, mn, mx, h = run.series(rpcs, args.metric, factor)
    all_cnt = run.series(run.rpcs, "total", factor)[0]
    x = np.arange(len(cnt)) * bs / 60.0

    LINES = [
        (50.0,   "#1a9850", T("一半的请求比这条线快",              "half the requests are faster")),
        (99.0,   "#fdae61", T("每 100 个请求有 1 个比这条线慢",     "1 in 100 is slower than this")),
        (99.9,   "#d73027", T("每 1000 个请求有 1 个比这条线慢",    "1 in 1000 is slower than this")),
    ]
    vals = {}
    for q, _, _ in LINES:
        v = hist_pct(h, cnt, mn, mx, q, run.edges)
        vals[q] = _roll_med(np.where(cnt < _min_n(q, args.min_count), np.nan, v), args.smooth)

    sig = vals[99.9]
    wins, base = detect_windows(sig, cnt, bs, min_cnt=_min_n(99.9, args.min_count),
                                enter_factor=args.enter_factor,
                                merge_gap=max(1, int(args.merge_gap / bs)))

    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(15, 9), sharex=True,
                                  gridspec_kw={"height_ratios": [3.2, 1]})
    for (q, color, lab), _ in zip(LINES, LINES):
        ax.plot(x, vals[q] / 1000.0, color=color, lw=2.4, label=lab, zorder=3)
    for a, b, bl, peak in wins:
        ax.axvspan(x[a], x[min(b, len(x) - 1)], color="#d73027", alpha=0.10, zorder=0)
        ax2.axvspan(x[a], x[min(b, len(x) - 1)], color="#d73027", alpha=0.10, zorder=0)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: (f"{v:g} " + T("毫秒", "ms")) if v >= 1
                                               else f"{v:g} " + T("毫秒", "ms")))
    ax.set_ylabel(T("一次操作的耗时", "latency per request"), fontsize=12)
    ax.legend(fontsize=12, loc="upper left", framealpha=0.9)
    ax.grid(alpha=0.3, which="both")

    ax2.plot(x, all_cnt / bs, color="#4575b4", lw=1.8)
    ax2.set_ylabel(T("压测负载\n(每秒请求数)", "load\n(requests/s)"), fontsize=11)
    ax2.set_xlabel(T(f"压测开始后的时间（分钟）  —  0 分 = {_fmt_clock(run.t0)}",
                     f"minutes since start  --  0 = {_fmt_clock(run.t0)}"), fontsize=12)
    ax2.set_ylim(bottom=0)
    ax2.grid(alpha=0.3)

    # 把结论直接写在图上，不让人自己去推
    busy = sum(b - a + 1 for a, b, _, _ in wins) * bs
    if wins:
        # 取最长的那个窗口当主结论：靠前的短窗口往往只是压力爬坡期的抖动
        main = max(wins, key=lambda w: w[1] - w[0])
        a0, b0 = main[0], main[1]
        load_from = _load_start(run, factor)     # 负载稳定之后才有可比性
        qi = all_cnt[a0:b0 + 1].sum() / max((b0 - a0 + 1) * bs, 1)
        pre = slice(load_from, a0)
        npre = max(a0 - load_from, 0)
        qo = all_cnt[pre].sum() / (npre * bs) if npre else qi
        same = abs(qi - qo) / max(qo, 1) < 0.15
        verdict = T("基本没变 —— 所以不是压力变大导致的",
                    "essentially unchanged -- so this is not a load effect") if same else \
                  T("有明显变化 —— 需要先排除是负载本身变了",
                    "changed materially -- rule out a load change first")
        msg = T(
            f"结论：{_fmt_clock(run.t0 + a0 * bs)} 开始变慢"
            f"（压测开始后第 {a0 * bs / 60:.0f} 分钟），这一段持续了 {_fmt_dur((b0 - a0 + 1) * bs)}；"
            f"全程累计 {_fmt_dur(busy)}，占 {100 * busy / span:.0f}%。\n"
            f"变慢前后负载 {qo:.0f} → {qi:.0f} 请求/秒（只比负载稳定后的时段），{verdict}。",
            f"Onset at {_fmt_clock(run.t0 + a0 * bs)} (t+{a0 * bs / 60:.0f}min), "
            f"lasting {_fmt_dur((b0 - a0 + 1) * bs)}; {_fmt_dur(busy)} total "
            f"({100 * busy / span:.0f}% of the run). Load {qo:.0f} -> {qi:.0f} rps, {verdict}.")
    else:
        msg = T("结论：全程没有检测到明显的变慢时段。", "No degradation window detected.")
    fig.suptitle(f"{run.label}  |  {'+'.join(rpcs)}  |  {METRIC_LABEL[args.metric]}  |  "
                 f"{int(cnt.sum()):,} " + T("次请求", "requests"), fontsize=13)
    fig.text(0.012, 0.012, msg, fontsize=12, va="bottom",
             bbox=dict(boxstyle="round,pad=0.5", fc="#fff8e1", ec="#d73027", alpha=0.95))
    fig.tight_layout(rect=[0, 0.10, 1, 0.97])
    fig.savefig(args.out, dpi=110)
    print(f"图已写出：{args.out}", file=sys.stderr)
    _print_windows(run, wins, bs, base, sig, cnt, all_cnt)


def plot_single(run, args):
    import matplotlib
    matplotlib.use("Agg")
    _setup_font()
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    from matplotlib.colors import LogNorm

    rpcs = run.select(args.rpc)
    if not rpcs:
        sys.exit(f"错误：{run.label} 里没有匹配 --rpc {args.rpc} 的 RPC，可选：{run.rpcs}")
    span = run.nb * run.atom
    factor = args.bucket_factor or _pick_factor(run, span, args.points)
    bs = factor * run.atom
    cnt, s, mn, mx, h = run.series(rpcs, args.metric, factor)
    all_cnt = run.series(run.rpcs, "total", factor)[0]
    x = np.arange(len(cnt)) * bs / 60.0

    pcts = [float(p) for p in args.pct.split(",")]
    series = {q: hist_pct(h, cnt, mn, mx, q, run.edges) for q in pcts}
    for q in pcts:
        # 样本数不足时百分位没有意义：p99 至少要 1000 个样本，p99.9 要 10000
        series[q] = np.where(cnt < _min_n(q, args.min_count), np.nan, series[q])
    mean = np.where(cnt > 0, s / np.maximum(cnt, 1), np.nan)
    mxf = np.where(cnt > 0, mx.astype(float), np.nan)

    dq = args.detect_pct
    sig = series.get(dq) if dq in series else hist_pct(h, cnt, mn, mx, dq, run.edges)
    sig = np.where(cnt < _min_n(dq, args.min_count), np.nan, sig)
    sig = _roll_med(sig, args.smooth)      # 在平滑后的信号上检测，避免周期尖峰把窗口打碎
    wins, base = detect_windows(sig, cnt, bs, min_cnt=_min_n(dq, args.min_count),
                                enter_factor=args.enter_factor,
                                merge_gap=max(1, int(args.merge_gap / bs)))

    fig, axes = plt.subplots(4, 1, figsize=(17, 14), sharex=True,
                             gridspec_kw={"height_ratios": [3.4, 2.2, 1.3, 1.3]})
    ramp = plt.get_cmap("PuBu")(np.linspace(0.45, 0.95, len(pcts)))

    # Panel 1: 百分位扇形
    ax = axes[0]
    ax.plot(x, mxf, color="#cccccc", lw=0.6, alpha=0.5, label="max", zorder=1)
    for c, q in zip(ramp, sorted(pcts)):
        ax.plot(x, series[q], color=c, lw=0.6, alpha=0.28, zorder=2)      # 原始，看抖动幅度
        ax.plot(x, _roll_med(series[q], args.smooth), color=c,            # 平滑，看趋势
                lw=2.2 if q >= 99 else 1.5, ls="--" if q > 99 else "-",
                label=f"p{q:g}", zorder=4)
    if 50.0 in series and 90.0 in series:
        ax.fill_between(x, _roll_med(series[50.0], args.smooth),
                        _roll_med(series[90.0], args.smooth), color=ramp[0], alpha=0.16, zorder=2)
    ax.plot(x, _roll_med(mean, args.smooth), color="#ff7f0e", lw=1.2, alpha=0.9,
            label="mean (exact)", zorder=4)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(_lat_fmt))
    ax.set_ylabel(f"{METRIC_LABEL[args.metric]} (log)")
    ax.set_title(f"{run.label}  |  {METRIC_LABEL[args.metric]}  |  rpc={'+'.join(rpcs)}  |  "
                 f"bucket={bs}s  |  {int(cnt.sum()):,} requests  |  "
                 f"{_fmt_clock(run.t0)}-{_fmt_clock(run.t0 + span)}")
    ax.legend(ncol=len(pcts) + 2, fontsize=8, loc="upper left")
    ax.grid(alpha=0.3, which="both")

    # Panel 2: 密度热力图（按列归一化，看分布形状随时间的演化）
    ax = axes[1]
    lo_bin, hi_bin = 1, NBINS - 1
    occupied = np.where(h[:, lo_bin:hi_bin].sum(0) > 0)[0]
    if len(occupied):
        lo_bin += max(0, occupied[0] - 2)
        hi_bin = 1 + min(NBINS - 1, occupied[-1] + 3)
    sub = h[:, lo_bin:hi_bin].astype(float)
    col = sub.sum(1, keepdims=True)
    dens = np.divide(sub, col, out=np.zeros_like(sub), where=col > 0)
    dens = np.ma.masked_where(sub <= 0, dens)      # 空格子留白，别让 LogNorm 涂成深色
    mesh = ax.pcolormesh(np.append(x, x[-1] + bs / 60.0), run.edges[lo_bin:hi_bin + 1],
                         dens.T, cmap="viridis", norm=LogNorm(vmin=1e-5, vmax=1.0))
    cb = fig.colorbar(mesh, ax=ax, pad=0.005, fraction=0.02)
    cb.set_label("share of bucket", fontsize=8)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(_lat_fmt))
    ax.set_ylabel("latency density\n(column-normalized)")

    # Panel 3: 负载
    ax = axes[2]
    ax.plot(x, all_cnt / bs, color="#2ca02c", lw=1.1, label="all RPC qps")
    ax.plot(x, cnt / bs, color="#1f77b4", lw=1.1, label=f"{args.rpc} qps")
    ax.set_ylabel("requests/s")
    ax.legend(fontsize=8, loc="lower right")
    ax.grid(alpha=0.3)

    # Panel 4: 均值分解
    ax = axes[3]
    c_t, s_t, _, _, _ = run.series(rpcs, "total", factor)
    c_r, s_r, _, _, _ = run.series(rpcs, "raft", factor)
    c_p, s_p, _, _, _ = run.series(rpcs, "phase", factor)
    s_a = run.series(rpcs, "apply", factor)[1] if "apply" in run.meta["metrics"] else np.zeros_like(s_r)
    d = np.maximum(c_t, 1)
    m_r, m_a, m_p = s_r / d, s_a / d, s_p / d
    m_o = np.maximum(s_t / d - m_r - m_a - m_p, 0)
    ax.stackplot(x, m_r, m_a, m_p, m_o,
                 colors=["#d62728", "#9467bd", "#1f77b4", "#bbbbbb"],
                 labels=["raft_commit", "raft_apply", "phase (rocksdb read)", "remainder (queue)"])
    ax.set_ylabel("mean us")
    ax.set_xlabel(f"elapsed time (minutes since {_fmt_clock(run.t0)})")
    ax.legend(ncol=3, fontsize=8, loc="upper left")
    ax.grid(alpha=0.3)

    _draw_windows(axes, wins, x, bs, base, sig, cnt, run, args)
    fig.tight_layout()
    fig.savefig(args.out, dpi=110)
    print(f"图已写出：{args.out}", file=sys.stderr)
    _print_windows(run, wins, bs, base, sig, cnt, all_cnt)


def _draw_windows(axes, wins, x, bs, base, sig, cnt, run, args):
    for k, (a, b, bl, peak) in enumerate(wins):
        for ax in axes:
            ax.axvspan(x[a], x[min(b, len(x) - 1)], color="#d62728", alpha=0.10, zorder=0)
        axes[0].axvline(x[a], color="#d62728", lw=1.4, zorder=4)
        axes[0].axvline(x[min(b, len(x) - 1)], color="#d62728", lw=1.0, ls="--", zorder=4)
        lift = 1.6 * (2.6 ** (k % 3))          # 相邻窗口的标注错开三档高度
        axes[0].annotate(
            f"W{k + 1} {_fmt_clock(run.t0 + a * bs)}  +{_fmt_dur((b - a + 1) * bs)}\n"
            f"p{args.detect_pct:g} {_lat_fmt(bl)} -> {_lat_fmt(peak)} ({peak / max(bl, 1e-9):.1f}x)",
            xy=(x[a], peak), xytext=(x[a] + (x[-1] - x[0]) * 0.01, peak * lift),
            fontsize=7.5, zorder=6,
            arrowprops=dict(arrowstyle="-", color="#d62728", lw=0.6, alpha=0.7),
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="#d62728", alpha=0.9))


def _print_windows(run, wins, bs, base, sig, cnt, all_cnt):
    print(f"\n# degradation windows for {run.label} (baseline p20 = {base:.0f} us, bucket = {bs}s)")
    print("idx\tstart\tend\tduration\tpeak_us\tratio\trequests\tqps_in\tqps_out")
    if not wins:
        print("# (none detected)")
        return
    for k, (a, b, bl, peak) in enumerate(wins):
        inside = np.zeros(len(cnt), bool)
        inside[a:b + 1] = True
        q_in = all_cnt[inside].sum() / max(inside.sum() * bs, 1)
        q_out = all_cnt[~inside].sum() / max((~inside).sum() * bs, 1)
        print(f"W{k + 1}\t{_fmt_clock(run.t0 + a * bs)}\t{_fmt_clock(run.t0 + (b + 1) * bs)}\t"
              f"{_fmt_dur((b - a + 1) * bs)}\t{peak:.0f}\t{peak / max(bl, 1e-9):.1f}x\t"
              f"{int(cnt[a:b + 1].sum())}\t{q_in:.0f}\t{q_out:.0f}")


def _load_start(run, factor):
    """负载起点：QPS 达到 p95 的一半且连续若干桶维持。用于多 run 对齐。"""
    c = run.series(run.rpcs, "total", factor)[0]
    nz = c[c > 0]
    if not len(nz):
        return 0
    thr = 0.5 * np.percentile(nz, 95)
    need = 5
    ok = c >= thr
    for i in range(len(ok) - need):
        if ok[i:i + need].all():
            return i
    return 0


def plot_compare(runs, args):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    from matplotlib.lines import Line2D

    runs = sorted(runs, key=lambda r: (r.conc is None, r.conc or 0, r.label))
    cmap = plt.get_cmap("viridis")(np.linspace(0.05, 0.85, len(runs)))
    pcts = sorted(float(p) for p in args.pct.split(","))
    styles = {p: st for p, st in zip(pcts, ["-", "--", "-.", ":"] * 4)}

    fig, axes = plt.subplots(3, 1, figsize=(17, 12),
                             gridspec_kw={"height_ratios": [3, 1.4, 2.2]})
    axes[1].sharex(axes[0])
    run_handles, pct_handles = [], []
    for color, run in zip(cmap, runs):
        rpcs = run.select(args.rpc)
        if not rpcs:
            print(f"警告：{run.label} 无匹配 RPC，跳过", file=sys.stderr)
            continue
        factor = args.bucket_factor or _pick_factor(run, run.nb * run.atom, args.points)
        bs = factor * run.atom
        cnt, s, mn, mx, h = run.series(rpcs, args.metric, factor)
        all_cnt = run.series(run.rpcs, "total", factor)[0]
        t0i = _load_start(run, factor) if args.align != "first" else 0
        x = (np.arange(len(cnt)) - t0i) * bs / 60.0
        keep = x >= 0
        tag = f"{run.label}" + (f" (c={run.conc})" if run.conc else "")
        dur = int(cnt[keep].sum())
        for q in pcts:
            v = hist_pct(h, cnt, mn, mx, q, run.edges)
            v = np.where(cnt < _min_n(q, args.min_count), np.nan, v)
            axes[0].plot(x[keep], v[keep], color=color, ls=styles[q], alpha=0.25, lw=0.6)
            axes[0].plot(x[keep], _roll_med(v, args.smooth)[keep], color=color,
                         ls=styles[q], lw=2.0 if q >= 99 else 1.3)
        axes[1].plot(x[keep], (all_cnt / bs)[keep], color=color, lw=1.3)
        run_handles.append(Line2D([], [], color=color, lw=2.4,
                                  label=f"{tag}  n={dur:,}  {_fmt_dur(int(keep.sum() * bs))}"))
        qps = (cnt / bs)[keep]
        p99 = hist_pct(h, cnt, mn, mx, 99, run.edges)[keep]
        good = (cnt[keep] >= _min_n(99, args.min_count)) & np.isfinite(p99)
        axes[2].scatter(qps[good], p99[good], s=5, alpha=0.35, color=color, label=tag)

    axes[0].set_yscale("log")
    axes[0].yaxis.set_major_formatter(FuncFormatter(_lat_fmt))
    axes[0].set_ylabel(f"{METRIC_LABEL[args.metric]} (log)")
    axes[0].set_title(f"Concurrency comparison  |  {METRIC_LABEL[args.metric]}  |  "
                      f"rpc={args.rpc}  |  aligned on load start")
    pct_handles = [Line2D([], [], color="#444444", ls=styles[q], lw=1.6, label=f"p{q:g}")
                   for q in pcts]
    lg1 = axes[0].legend(handles=run_handles, fontsize=8, loc="upper left", title="run")
    axes[0].add_artist(lg1)
    axes[0].legend(handles=pct_handles, fontsize=8, loc="upper right", title="percentile", ncol=len(pcts))
    axes[0].grid(alpha=0.3, which="both")
    axes[1].set_ylabel("requests/s")
    axes[1].legend(handles=run_handles, fontsize=7, loc="lower right")
    axes[1].grid(alpha=0.3)
    axes[1].set_xlabel("elapsed since load start (minutes)")
    axes[2].set_xlabel("achieved throughput (requests/s)")
    axes[2].set_ylabel("p99 latency (us)")
    axes[2].set_yscale("log")
    axes[2].yaxis.set_major_formatter(FuncFormatter(_lat_fmt))
    axes[2].set_title("Latency vs achieved throughput -- separates queueing from real degradation",
                      fontsize=10)
    axes[2].legend(fontsize=7, loc="upper left")
    axes[2].grid(alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(args.out, dpi=110)
    print(f"对比图已写出：{args.out}", file=sys.stderr)


# ---------------------------------------------------------------- CLI

def main():
    ap = argparse.ArgumentParser(
        description="事务 RPC 耗时时序图（横轴时间，纵轴耗时，支持多并发度对比）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("两段式用法")[1] if "两段式用法" in __doc__ else None)
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("parse", help="扫描日志生成缓存（40GB 只需扫一次）")
    p.add_argument("logs", nargs="+", help="同一次运行的日志文件（多个按顺序拼接）")
    p.add_argument("-o", "--out", help="缓存输出路径，默认 <首个日志>.npz")
    p.add_argument("--label", help="这次运行的名字，默认取文件名")
    p.add_argument("--concurrency", type=int,
                   help="本次压测的 bench 并发度（即 mdtest_bench 的 --bench_threads）。"
                        "只作为元数据记进缓存，决定对比图的图例文字、配色深浅和排序；"
                        "不给则从 label 结尾的数字推断。与 --jobs 无关")
    p.add_argument("--bucket", type=int, default=1, help="原子分桶秒数（默认 1，后续可再合并）")
    p.add_argument("--jobs", type=int, default=min(16, os.cpu_count() or 4),
                   help="本脚本解析日志用的进程数（纯粹是分析工具的并行度，"
                        "跟被测系统的并发度没有关系）")

    q = sub.add_parser("plot", help="从缓存出图")
    q.add_argument("srcs", nargs="+", help="[LABEL=]缓存.npz 或 [LABEL=]日志（自动 parse）")
    q.add_argument("-o", "--out", default="txn_latency.png")
    q.add_argument("--metric", choices=METRICS, default="raft",
                   help="画哪个耗时：raft=落盘 apply=状态机应用 phase=rocksdb读 total=总耗时")
    q.add_argument("--rpc", default="write", help="write|read|all|逗号分隔的 RPC 名")
    q.add_argument("--pct", default="50,90,99,99.9", help="百分位列表")
    q.add_argument("--bucket-factor", type=int, help="显示分桶 = 原子桶 × 该值，默认按像素自动")
    q.add_argument("--points", type=int, default=1200, help="自动分桶的目标点数")
    q.add_argument("--min-count", type=int, default=30, help="样本数低于此值的桶不画尾部百分位")
    q.add_argument("--align", choices=["load", "first"], default="load",
                   help="多 run 对齐方式（默认按各自负载起点对齐）")
    q.add_argument("--simple", action="store_true",
                   help="简版图：只画三条线 + 负载，图上直接写结论。第一次看建议用这个")
    q.add_argument("--smooth", type=int, default=9,
                   help="滚动中位数窗口（桶数）；周期性尖峰下靠它读趋势，设 1 关闭")
    q.add_argument("--detect-pct", type=float, default=99.9,
                   help="劣化检测用哪个分位做信号（默认 99.9）")
    q.add_argument("--enter-factor", type=float, default=2.0,
                   help="进入劣化窗口的倍数阈值（相对 20 分位基线）")
    q.add_argument("--merge-gap", type=float, default=300,
                   help="间隔小于该秒数的劣化窗口合并成一个，默认 300s")

    argv = sys.argv[1:]
    if not argv:
        ap.print_help()
        sys.exit(1)
    if argv[0] not in ("parse", "plot"):
        argv = ["plot"] + argv          # 裸路径 = 隐含 plot（缓存或日志都行）
    args = ap.parse_args(argv)

    if args.cmd == "parse":
        do_parse(args)
        return

    runs = []
    for src in args.srcs:
        label, _, path = src.rpartition("=")
        if not path:
            path = src
            label = None
        if path.endswith(".npz"):
            cache = path
        else:
            cache = os.path.splitext(path)[0] + ".npz"
            if not os.path.isfile(cache):
                pa = argparse.Namespace(logs=[path], out=cache, label=label, concurrency=None,
                                        bucket=1, jobs=min(16, os.cpu_count() or 4))
                do_parse(pa)
        if not os.path.isfile(cache):
            sys.exit(f"错误：缓存不存在 {cache}")
        runs.append(Run(cache, label or None))

    if len(runs) == 1:
        (plot_simple if args.simple else plot_single)(runs[0], args)
    else:
        plot_compare(runs, args)


if __name__ == "__main__":
    main()
