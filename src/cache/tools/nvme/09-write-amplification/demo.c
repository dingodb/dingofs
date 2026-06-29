/*
 * 09-write-amplification: small random writes are the SSD's worst case. For the
 * SAME number of host bytes, scattered small writes make the controller
 * relocate far more data during GC than large sequential writes -> higher write
 * amplification (WAF = NAND-bytes-written / host-bytes-written), which both
 * wears the drive and slows sustained writes.
 *
 * This binary just generates a fixed host-byte volume in one of two patterns
 * and reports achieved bandwidth. run.sh brackets it with nvme-cli SMART deltas
 * to compute host vs NAND writes (WAF) when the drive exposes a NAND counter.
 */
#include "../common/nvme_bench.h"

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  const char* pat = (argc > 2) ? argv[2] : "rand4k"; /* rand4k | seq1m */
  long total = (argc > 3) ? atol(argv[3]) : 2L * 1024 * 1024 * 1024; /* host bytes */
  long fsize = 4L * 1024 * 1024 * 1024;                              /* 4 GiB area */
  char path[512];
  snprintf(path, sizeof path, "%s/waf.dat", dir);

  int seq = (strcmp(pat, "seq1m") == 0);
  long bs = seq ? (1L * 1024 * 1024) : 4096;

  /* ensure the file exists at full size so random writes hit allocated blocks */
  if (access(path, F_OK) != 0) {
    long big = 4L * 1024 * 1024;
    void* bb = nv_alloc((size_t)big);
    int f = nv_open_w(path);
    for (long off = 0; off < fsize; off += big)
      if (pwrite(f, bb, (size_t)big, off) != big) { perror("prep"); return 1; }
    close(f);
    free(bb);
  }

  int fd = open(path, O_WRONLY | O_DIRECT);
  void* buf = nv_alloc((size_t)bs);
  if (fd < 0 || !buf) { perror("open"); return 1; }
  long pages = fsize / bs;
  long done = 0;
  unsigned r = 12345;
  uint64_t t0 = nv_now_ns();
  while (done < total) {
    off_t off;
    if (seq)
      off = (off_t)((done / bs) % pages) * bs;
    else {
      r = r * 1103515245u + 12345u;
      off = (off_t)(r % (unsigned)pages) * bs;
    }
    if (pwrite(fd, buf, (size_t)bs, off) != bs) { perror("pwrite"); break; }
    done += bs;
  }
  fdatasync(fd);
  uint64_t dt = nv_now_ns() - t0;
  printf("  pattern=%-7s bs=%-7ld host=%ldMiB  %5.2f GB/s  (%.0f ms)\n", pat, bs,
         total / 1024 / 1024, nv_gbps((double)done, (double)dt), dt / 1e6);
  close(fd);
  free(buf);
  return 0;
}
