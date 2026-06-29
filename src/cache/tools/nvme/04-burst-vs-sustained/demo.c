/*
 * 04-burst-vs-sustained: a short write burst is absorbed by the SSD's volatile
 * write buffer (and, on consumer drives, the SLC cache) and runs fast; once you
 * keep writing past what the buffer can hide, bandwidth drops to the sustained
 * rate as the controller must fold/GC in the background.
 *
 * We write a large file O_DIRECT and report bandwidth per window. Enterprise
 * drives (steady TLC, big PLP buffer) show a small cliff; consumer drives a big
 * one. Either way you SEE the early-fast / later-slower shape.
 */
#include "../common/nvme_bench.h"

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  long total = (argc > 2) ? atol(argv[2]) : 16L * 1024 * 1024 * 1024; /* 16 GiB */
  long bs = 4L * 1024 * 1024;
  long window = 1L * 1024 * 1024 * 1024; /* report every 1 GiB */
  char path[512];
  snprintf(path, sizeof path, "%s/burst.dat", dir);

  void* buf = nv_alloc((size_t)bs);
  int fd = nv_open_w(path);
  if (fd < 0 || !buf) { perror("open"); return 1; }

  printf("burst-vs-sustained: write %ld GiB O_DIRECT bs=4MiB, bandwidth per 1 GiB window\n",
         total / 1024 / 1024 / 1024);
  double first = 0, last = 0;
  uint64_t wstart = nv_now_ns();
  long wbytes = 0;
  int win = 0;
  for (long off = 0; off < total; off += bs) {
    if (pwrite(fd, buf, (size_t)bs, off) != bs) { perror("pwrite"); break; }
    wbytes += bs;
    if (wbytes >= window) {
      uint64_t dt = nv_now_ns() - wstart;
      double gbps = nv_gbps((double)wbytes, (double)dt);
      printf("  window %2d  [%3ld..%3ld GiB]  %5.2f GB/s\n", win,
             (off + bs) / window - 1, (off + bs) / window, gbps);
      if (win == 0) first = gbps;
      last = gbps;
      win++;
      wbytes = 0;
      wstart = nv_now_ns();
    }
  }
  close(fd);
  free(buf);
  unlink(path);
  nv_verdict("first-window vs last-window bandwidth (burst absorption)", last, first,
             1.15);
  return 0;
}
