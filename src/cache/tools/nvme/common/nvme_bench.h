/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Shared header-only harness for the tools/nvme demos.
 * Pure libc; each demo is a single .c that does `#include "../common/nvme_bench.h"`.
 * Goal: keep every demo tiny so the A/B comparison logic is all that's left.
 */
#ifndef NVME_BENCH_H_
#define NVME_BENCH_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ---- timing ---- */
static inline uint64_t nv_now_ns(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

/* ---- aligned buffer (O_DIRECT needs 4K alignment) ---- */
static inline void* nv_alloc(size_t sz) {
  void* p = NULL;
  if (posix_memalign(&p, 4096, sz) != 0 || p == NULL) return NULL;
  memset(p, 1, sz);  /* non-zero so the FS can't cheat with a hole */
  return p;
}

/* ---- sample set: collect, then percentile ---- */
typedef struct {
  uint64_t* v;
  int n, cap;
} nv_samples;

static inline void nv_samp_init(nv_samples* s, int cap) {
  s->v = (uint64_t*)malloc(sizeof(uint64_t) * (size_t)cap);
  s->n = 0;
  s->cap = cap;
}
static inline void nv_samp_add(nv_samples* s, uint64_t x) {
  if (s->n < s->cap) s->v[s->n++] = x;
}
static int nv__cmp(const void* a, const void* b) {
  uint64_t x = *(const uint64_t*)a, y = *(const uint64_t*)b;
  return (x > y) - (x < y);
}
static inline void nv_samp_sort(nv_samples* s) {
  qsort(s->v, (size_t)s->n, sizeof(uint64_t), nv__cmp);
}
static inline uint64_t nv_samp_pct(nv_samples* s, double p) { /* call sort first */
  if (s->n == 0) return 0;
  int i = (int)(p * (s->n - 1) + 0.5);
  if (i < 0) i = 0;
  if (i >= s->n) i = s->n - 1;
  return s->v[i];
}
static inline double nv_samp_mean(nv_samples* s) {
  if (s->n == 0) return 0;
  double sum = 0;
  for (int i = 0; i < s->n; i++) sum += (double)s->v[i];
  return sum / s->n;
}
static inline uint64_t nv_samp_max(nv_samples* s) {
  uint64_t m = 0;
  for (int i = 0; i < s->n; i++)
    if (s->v[i] > m) m = s->v[i];
  return m;
}

/* ---- formatting: ring of buffers so several nv_us() in one printf are safe -- */
static inline const char* nv_us(double ns) {
  static char ring[8][32];
  static int idx = 0;
  char* b = ring[idx];
  idx = (idx + 1) & 7;
  snprintf(b, 32, "%.1f us", ns / 1e3);
  return b;
}
static inline double nv_gbps(double bytes, double ns) { /* GB/s (1e9) */
  if (ns <= 0) return 0;
  return bytes / ns; /* bytes/ns == GB/s */
}

/* ---- O_DIRECT open helpers ---- */
static inline int nv_open_w(const char* p) {
  return open(p, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT, 0644);
}
static inline int nv_open_r(const char* p) {
  return open(p, O_RDONLY | O_DIRECT);
}

/* ---- drop the kernel page cache (needs root; no-op+warn otherwise) ---- */
static inline void nv_drop_caches(void) {
  sync();
  int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
  if (fd < 0) {
    fprintf(stderr, "[warn] cannot drop page cache (run as root); "
                    "results may be polluted by cache\n");
    return;
  }
  if (write(fd, "3\n", 2) < 0) { /* ignore */
  }
  close(fd);
}

/* ---- print a verdict line: ratio of B/A with a CONFIRMED/weak tag ---- */
static inline void nv_verdict(const char* what, double a, double b,
                             double threshold) {
  double ratio = (a > 0) ? b / a : 0;
  printf("  VERDICT: %s  ->  %.2fx  [%s]\n", what, ratio,
         ratio >= threshold ? "EFFECT CONFIRMED" : "effect weak/none");
}

#endif  /* NVME_BENCH_H_ */
