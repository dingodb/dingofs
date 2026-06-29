/*
 * 03-mixed-read-write: a read's latency inflates when the device is busy
 * absorbing writes (write-back / program / erase competing with the read).
 * Phase A measures QD1 O_DIRECT read latency with NO writer.
 * Phase B measures the SAME reads while writer threads hammer the device.
 */
#include "../common/nvme_bench.h"

#include <pthread.h>

static volatile int g_stop = 0;

static void* writer(void* a) {
  const char* dir = (const char*)a;
  long bs = 4L * 1024 * 1024;
  void* buf = nv_alloc((size_t)bs);
  char p[512];
  int k = 0;
  while (!g_stop) {
    snprintf(p, sizeof p, "%s/w%d.dat", dir, k++ % 64);
    int fd = nv_open_w(p);
    if (fd < 0) break;
    if (pwrite(fd, buf, (size_t)bs, 0) != bs) { close(fd); break; }
    close(fd);
  }
  free(buf);
  return NULL;
}

static double read_lat(const char* path, long total, long bs, int iters) {
  nv_drop_caches();
  int fd = nv_open_r(path);
  void* buf = nv_alloc((size_t)bs);
  if (fd < 0 || !buf) { perror("read open"); exit(1); }
  long nblocks = total / bs;
  nv_samples s;
  nv_samp_init(&s, iters);
  for (int i = 0; i < iters; i++) {
    off_t off = (off_t)((i * 2654435761u) % (unsigned)nblocks) * bs; /* scattered */
    uint64_t t0 = nv_now_ns();
    if (pread(fd, buf, (size_t)bs, off) != bs) { perror("pread"); break; }
    nv_samp_add(&s, nv_now_ns() - t0);
  }
  nv_samp_sort(&s);
  double mean = nv_samp_mean(&s);
  printf("    read mean=%-9s p99=%-9s\n", nv_us(mean),
         nv_us((double)nv_samp_pct(&s, 0.99)));
  close(fd);
  free(buf);
  free(s.v);
  return mean;
}

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  long total = 2L * 1024 * 1024 * 1024; /* 2 GiB read file */
  long bs = 4L * 1024 * 1024;
  int iters = (argc > 2) ? atoi(argv[2]) : 200;
  char path[512];
  snprintf(path, sizeof path, "%s/read.dat", dir);

  void* buf = nv_alloc((size_t)bs);
  int fd = nv_open_w(path);
  for (long off = 0; off < total; off += bs)
    if (pwrite(fd, buf, (size_t)bs, off) != bs) { perror("prep"); return 1; }
  close(fd);
  free(buf);

  printf("mixed-read-write: QD1 O_DIRECT read latency, idle vs under write load\n");
  printf("  A) reads with NO concurrent writer:\n");
  double a = read_lat(path, total, bs, iters);

  printf("  B) reads while 2 writer threads hammer the device:\n");
  g_stop = 0;
  pthread_t w1, w2;
  pthread_create(&w1, NULL, writer, (void*)dir);
  pthread_create(&w2, NULL, writer, (void*)dir);
  double b = read_lat(path, total, bs, iters);
  g_stop = 1;
  pthread_join(w1, NULL);
  pthread_join(w2, NULL);

  nv_verdict("read latency under write load vs idle", a, b, 1.3);
  return 0;
}
