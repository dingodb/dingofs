/*
 * 06-fsync-plp: how expensive fdatasync() is depends on whether the drive has
 * power-loss protection (PLP). On a PLP drive (e.g. Intel/Solidigm D7 series),
 * data is durable once it reaches the controller's protected DRAM, so
 * fdatasync barely costs anything. On a consumer drive without PLP, fdatasync
 * issues a real FLUSH to NAND and is much slower.
 *
 * A = O_DIRECT write, no sync.   B = O_DIRECT write + fdatasync after each.
 * Tiny B/A ratio  => PLP drive (durability is nearly free here).
 * Large B/A ratio => no PLP (every fsync waits on NAND).
 */
#include "../common/nvme_bench.h"

static double run(const char* dir, int n, long bs, int do_sync) {
  void* buf = nv_alloc((size_t)bs);
  nv_samples s;
  nv_samp_init(&s, n);
  char p[512];
  for (int i = 0; i < n; i++) {
    snprintf(p, sizeof p, "%s/s%d.dat", dir, i % 64);
    int fd = nv_open_w(p);
    if (fd < 0) { perror("open"); exit(1); }
    uint64_t t0 = nv_now_ns();
    if (pwrite(fd, buf, (size_t)bs, 0) != bs) { perror("pwrite"); exit(1); }
    if (do_sync) fdatasync(fd);
    nv_samp_add(&s, nv_now_ns() - t0);
    close(fd);
  }
  nv_samp_sort(&s);
  double mean = nv_samp_mean(&s);
  printf("    %-22s write+sync mean=%-9s p99=%-9s\n",
         do_sync ? "B) O_DIRECT+fdatasync" : "A) O_DIRECT no sync",
         nv_us(mean), nv_us((double)nv_samp_pct(&s, 0.99)));
  free(buf);
  free(s.v);
  return mean;
}

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  int n = (argc > 2) ? atoi(argv[2]) : 300;
  long bs = (argc > 3) ? atol(argv[3]) : 64L * 1024; /* 64 KiB: sync cost visible */

  printf("fsync-plp: per-op latency, O_DIRECT bs=%ldKiB n=%d\n", bs / 1024, n);
  double a = run(dir, n, bs, 0);
  double b = run(dir, n, bs, 1);
  printf("  fdatasync adds ~%.0f us/op (%.2fx).\n", (b - a) / 1e3, a > 0 ? b / a : 0);
  printf("  interpret with the vwc/PLP line above: vwc=0 (PLP) -> this is a fixed\n");
  printf("  flush-cmd + FS-metadata-commit cost, NOT a NAND-programming stall. On a\n");
  printf("  consumer drive with vwc=1 and no PLP, fdatasync forces NAND and costs far\n");
  printf("  more, especially under sustained write load.\n");
  return 0;
}
