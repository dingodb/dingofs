/*
 * 08-tail-latency-qos: even a pure-read workload sees occasional latency
 * spikes (background GC, internal housekeeping). The mean hides this; the tail
 * (p99.9 / max) is what hurts SLAs. We do many QD1 O_DIRECT reads of a cold
 * file and print the full latency distribution.
 */
#include "../common/nvme_bench.h"

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  int iters = (argc > 2) ? atoi(argv[2]) : 5000;
  long bs = (argc > 3) ? atol(argv[3]) : 128L * 1024; /* 128 KiB reads */
  long total = 8L * 1024 * 1024 * 1024;               /* 8 GiB spread */
  char path[512];
  snprintf(path, sizeof path, "%s/tail.dat", dir);

  void* buf = nv_alloc((size_t)bs);
  int fd = nv_open_w(path);
  long big = 4L * 1024 * 1024;
  void* bigbuf = nv_alloc((size_t)big);
  for (long off = 0; off < total; off += big)
    if (pwrite(fd, bigbuf, (size_t)big, off) != big) { perror("prep"); return 1; }
  close(fd);
  free(bigbuf);

  nv_drop_caches();
  int rfd = nv_open_r(path);
  if (rfd < 0) { perror("open"); return 1; }
  long nblocks = total / bs;
  nv_samples s;
  nv_samp_init(&s, iters);
  for (int i = 0; i < iters; i++) {
    off_t off = (off_t)((i * 2654435761u) % (unsigned)nblocks) * bs;
    uint64_t t0 = nv_now_ns();
    if (pread(rfd, buf, (size_t)bs, off) != bs) { perror("pread"); break; }
    nv_samp_add(&s, nv_now_ns() - t0);
  }
  close(rfd);
  nv_samp_sort(&s);

  double p50 = (double)nv_samp_pct(&s, 0.50);
  printf("tail-latency: %d QD1 O_DIRECT reads of %ldKiB\n", iters, bs / 1024);
  printf("  mean   = %s\n", nv_us(nv_samp_mean(&s)));
  printf("  p50    = %s\n", nv_us(p50));
  printf("  p99    = %s\n", nv_us((double)nv_samp_pct(&s, 0.99)));
  printf("  p99.9  = %s\n", nv_us((double)nv_samp_pct(&s, 0.999)));
  printf("  max    = %s\n", nv_us((double)nv_samp_max(&s)));
  nv_verdict("max vs p50 (tail spread)", p50, (double)nv_samp_max(&s), 3.0);
  free(buf);
  free(s.v);
  unlink(path);
  return 0;
}
