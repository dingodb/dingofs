/*
 * 02-queue-depth: a single synchronous IO at a time (QD1) cannot fill an NVMe.
 * NAND is internally parallel (many dies/channels); you only reach peak
 * bandwidth with many IOs in flight. Here "in flight" = number of reader
 * threads, each doing its own stream of O_DIRECT reads over a disjoint range.
 */
#include "../common/nvme_bench.h"

#include <pthread.h>

typedef struct {
  const char* path;
  long bs;
  off_t start, end;
} job_t;

static void* worker(void* a) {
  job_t* j = (job_t*)a;
  int fd = nv_open_r(j->path);
  void* buf = nv_alloc((size_t)j->bs);
  if (fd < 0 || !buf) { perror("worker open/alloc"); pthread_exit(NULL); }
  for (off_t off = j->start; off + j->bs <= j->end; off += j->bs)
    if (pread(fd, buf, (size_t)j->bs, off) != j->bs) { perror("pread"); break; }
  close(fd);
  free(buf);
  return NULL;
}

static double run(const char* path, long total, long bs, int nthreads) {
  nv_drop_caches();
  pthread_t th[256];
  job_t jb[256];
  off_t per = (total / nthreads) & ~(off_t)(bs - 1); /* bs-aligned per-thread */
  if (per < bs) per = bs;
  uint64_t t0 = nv_now_ns();
  for (int t = 0; t < nthreads; t++) {
    jb[t] = (job_t){path, bs, (off_t)t * per, (off_t)(t + 1) * per};
    pthread_create(&th[t], NULL, worker, &jb[t]);
  }
  for (int t = 0; t < nthreads; t++) pthread_join(th[t], NULL);
  uint64_t dt = nv_now_ns() - t0;
  double gbps = nv_gbps((double)(per * nthreads), (double)dt);
  printf("  threads=%-3d (in-flight IOs~%-3d)  aggregate %5.2f GB/s   (%.0f ms)\n",
         nthreads, nthreads, gbps, dt / 1e6);
  return gbps;
}

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  long total = (argc > 2) ? atol(argv[2]) : 4L * 1024 * 1024 * 1024; /* 4 GiB */
  long bs = 4L * 1024 * 1024;
  char path[512];
  snprintf(path, sizeof path, "%s/qd.dat", dir);

  void* buf = nv_alloc((size_t)bs);
  int fd = nv_open_w(path);
  if (fd < 0 || !buf) { perror("prep"); return 1; }
  for (long off = 0; off < total; off += bs)
    if (pwrite(fd, buf, (size_t)bs, off) != bs) { perror("prep write"); return 1; }
  close(fd);
  free(buf);

  printf("queue-depth: read a %ld MiB file O_DIRECT bs=4MiB; threads = in-flight depth\n",
         total / 1024 / 1024);
  double a = run(path, total, bs, 1);
  run(path, total, bs, 4);
  double b = run(path, total, bs, 16);
  nv_verdict("QD16 vs QD1 aggregate bandwidth", a, b, 1.4);
  unlink(path);
  return 0;
}
