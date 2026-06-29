/*
 * 01-read-after-write: reading a block you JUST O_DIRECT-wrote is much slower
 * than reading the same data once it has been flushed to NAND.
 *
 * Every read below is O_DIRECT (no page cache). The ONLY variable is how many
 * other writes happened between a block's write and its read-back (the "lag").
 *   lag=0  -> read the very block we just wrote   (still in the SSD write buffer)
 *   lag=k  -> read a block written k iters ago    (device equally busy writing)
 * If the slowdown were just "device busy", every lag would be slow. It isn't.
 */
#include "../common/nvme_bench.h"

static double run_lag(const char* dir, int n, long bs, int lag) {
  void* buf = nv_alloc((size_t)bs);
  if (!buf) { perror("alloc"); exit(1); }
  nv_samples r;
  nv_samp_init(&r, n);
  for (int i = 0; i < n; i++) {
    char p[512];
    snprintf(p, sizeof p, "%s/f%d.blk", dir, i);
    int fd = nv_open_w(p);
    if (fd < 0) { perror("open(w)"); exit(1); }
    if (pwrite(fd, buf, (size_t)bs, 0) != bs) { perror("pwrite"); exit(1); }
    close(fd);

    int j = i - lag;
    if (j < 0) continue;
    char q[512];
    snprintf(q, sizeof q, "%s/f%d.blk", dir, j);
    int rfd = nv_open_r(q);
    if (rfd < 0) { perror("open(r)"); exit(1); }
    uint64_t t0 = nv_now_ns();
    if (pread(rfd, buf, (size_t)bs, 0) != bs) { perror("pread"); exit(1); }
    nv_samp_add(&r, nv_now_ns() - t0);
    close(rfd);
  }
  nv_samp_sort(&r);
  double mean = nv_samp_mean(&r);
  printf("  lag=%-2d  read mean=%-9s p99=%-9s (n=%d)\n", lag, nv_us(mean),
         nv_us((double)nv_samp_pct(&r, 0.99)), r.n);
  free(r.v);
  free(buf);
  return mean;
}

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  int n = (argc > 2) ? atoi(argv[2]) : 200;
  long bs = (argc > 3) ? atol(argv[3]) : 4L * 1024 * 1024;

  printf("read-after-write: write f[i] then read f[i-lag], O_DIRECT, bs=%ldKiB n=%d\n",
         bs / 1024, n);
  double a = run_lag(dir, n, bs, 0);   /* read the just-written block */
  run_lag(dir, n, bs, 1);              /* one write later */
  double b = run_lag(dir, n, bs, 4);   /* a few writes later: flushed */
  nv_verdict("read-just-written vs read-already-flushed", b, a, 1.5);
  return 0;
}
