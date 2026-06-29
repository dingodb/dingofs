/*
 * 07-numa-locality: an NVMe hangs off one NUMA node's PCIe root. Driving its
 * IO (and placing the DMA buffers) from a CPU on the OTHER socket adds
 * cross-socket (UPI/QPI) hops -> lower bandwidth / higher latency.
 *
 * This binary just runs a fixed read benchmark and prints one machine-readable
 * line. run.sh runs it twice under numactl: pinned to the disk's LOCAL node vs
 * a REMOTE node, and compares.
 */
#include "../common/nvme_bench.h"

#include <pthread.h>

typedef struct {
  const char* path;
  long bs;
  off_t start, end;
} job_t;

static volatile uint64_t g_sink = 0;

static void* worker(void* a) {
  job_t* j = (job_t*)a;
  int fd = nv_open_r(j->path);
  void* buf = nv_alloc((size_t)j->bs);
  if (fd < 0 || !buf) pthread_exit(NULL);
  uint64_t sum = 0;
  for (off_t off = j->start; off + j->bs <= j->end; off += j->bs) {
    if (pread(fd, buf, (size_t)j->bs, off) != j->bs) break;
    /* actually TOUCH the data: pulls the DMA'd buffer across UPI when the
       buffer lives on a remote NUMA node -> exposes the real locality cost */
    const volatile char* p = (const volatile char*)buf;
    for (long k = 0; k < j->bs; k += 64) sum += (uint64_t)p[k];
  }
  g_sink += sum;
  close(fd);
  free(buf);
  return NULL;
}

int main(int argc, char** argv) {
  const char* dir = (argc > 1) ? argv[1] : "/tmp/nvme-demo";
  const char* label = (argc > 2) ? argv[2] : "run";
  int nthreads = (argc > 3) ? atoi(argv[3]) : 8;
  long total = 4L * 1024 * 1024 * 1024;
  long bs = 4L * 1024 * 1024;
  char path[512];
  snprintf(path, sizeof path, "%s/numa.dat", dir);

  /* prep once (label "prep") then exit; subsequent runs reuse the file */
  if (access(path, F_OK) != 0) {
    void* buf = nv_alloc((size_t)bs);
    int fd = nv_open_w(path);
    for (long off = 0; off < total; off += bs)
      if (pwrite(fd, buf, (size_t)bs, off) != bs) { perror("prep"); return 1; }
    close(fd);
    free(buf);
  }

  nv_drop_caches();
  pthread_t th[256];
  job_t jb[256];
  off_t per = (total / nthreads) & ~(off_t)(bs - 1);
  uint64_t t0 = nv_now_ns();
  for (int t = 0; t < nthreads; t++) {
    jb[t] = (job_t){path, bs, (off_t)t * per, (off_t)(t + 1) * per};
    pthread_create(&th[t], NULL, worker, &jb[t]);
  }
  for (int t = 0; t < nthreads; t++) pthread_join(th[t], NULL);
  uint64_t dt = nv_now_ns() - t0;
  double gbps = nv_gbps((double)(per * nthreads), (double)dt);
  printf("  %-8s threads=%d  aggregate %5.2f GB/s\n", label, nthreads, gbps);
  printf("RESULT %s %.4f\n", label, gbps); /* machine-readable for run.sh */
  return 0;
}
