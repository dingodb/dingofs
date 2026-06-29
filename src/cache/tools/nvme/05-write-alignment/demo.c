/*
 * 05-write-alignment: writing a PARTIAL device block forces a read-modify-write
 * (RMW) -- the kernel/drive must read the surrounding block, splice in your
 * bytes, then write the whole block back. Full-block aligned writes skip the
 * read. We also show the hard rule: O_DIRECT rejects unaligned IO outright.
 *
 * A = 4KiB full-page random overwrite (no RMW).
 * B = 512B partial random write into cold pages (RMW: page must be read first).
 * Both buffered + O_SYNC so each lands on the device.
 */
#include "../common/nvme_bench.h"

static double run(const char* path, long fsize, int n, long iosize) {
  nv_drop_caches(); /* cold pages so a partial write must read first */
  int fd = open(path, O_WRONLY | O_SYNC);
  void* buf = nv_alloc(4096);
  if (fd < 0 || !buf) { perror("open"); exit(1); }
  long pages = fsize / 4096;
  nv_samples s;
  nv_samp_init(&s, n);
  for (int i = 0; i < n; i++) {
    off_t page = (off_t)((i * 2654435761u) % (unsigned)pages);
    off_t off = page * 4096; /* page-aligned page; iosize<4096 => partial */
    uint64_t t0 = nv_now_ns();
    if (pwrite(fd, buf, (size_t)iosize, off) != iosize) { perror("pwrite"); break; }
    nv_samp_add(&s, nv_now_ns() - t0);
  }
  nv_samp_sort(&s);
  double mean = nv_samp_mean(&s);
  printf("    iosize=%-5ld  write mean=%-9s p99=%-9s\n", iosize, nv_us(mean),
         nv_us((double)nv_samp_pct(&s, 0.99)));
  close(fd);
  free(buf);
  free(s.v);
  return mean;
}

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  int n = (argc > 2) ? atoi(argv[2]) : 1000;
  long fsize = 1L * 1024 * 1024 * 1024; /* 1 GiB working file */
  char path[512];
  snprintf(path, sizeof path, "%s/align.dat", dir);

  /* prep: lay down a real 1 GiB file so partial writes hit existing blocks */
  long big = 4L * 1024 * 1024;
  void* bb = nv_alloc((size_t)big);
  int fd = nv_open_w(path);
  for (long off = 0; off < fsize; off += big)
    if (pwrite(fd, bb, (size_t)big, off) != big) { perror("prep"); return 1; }
  close(fd);
  free(bb);

  /* the hard rule: unaligned O_DIRECT is rejected */
  int dfd = nv_open_r(path);
  void* ub = nv_alloc(8192);
  errno = 0;
  ssize_t rc = pread(dfd, (char*)ub + 13, 4096, 512); /* misaligned buf+offset */
  printf("write-alignment: O_DIRECT unaligned pread -> rc=%zd errno=%d (%s)\n", rc,
         errno, rc < 0 ? strerror(errno) : "ok");
  close(dfd);
  free(ub);

  printf("  A) full 4KiB page write (aligned, no RMW):\n");
  double a = run(path, fsize, n, 4096);
  printf("  B) 512B partial write into cold pages (RMW):\n");
  double b = run(path, fsize, n, 512);
  nv_verdict("partial-block (RMW) vs full-block write", a, b, 1.2);
  unlink(path);
  return 0;
}
