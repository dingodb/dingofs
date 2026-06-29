#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
source ../common/lib.sh
nv_need_root "$@"
nv_section "09 write amplification (small-random vs large-sequential)"
nv_prep_dir
nv_device_info
nv_build demo.c demo

src=$(findmnt -no SOURCE --target "$NVME_DIR" 2>/dev/null)
kname=$(lsblk -no KNAME "$src" 2>/dev/null | head -1)
phys=$kname; [ -d /sys/block/$kname/slaves ] && phys=$(ls /sys/block/$kname/slaves | head -1)
ctrl=/dev/${phys%n*}
ctr() { nvme intel smart-log-add "$ctrl" 2>/dev/null | awk -v k="$1" '$0 ~ k {print $NF; exit}'; }
HAVE_WAF=$([ -n "$(ctr nand_bytes_written)" ] && echo 1 || echo 0)

BYTES=${1:-8589934592}   # 8 GiB host writes per pattern
./demo "$NVME_DIR" seq1m 1048576 >/dev/null   # create working file (not measured)

# ---- primary, always-robust signal: achieved bandwidth ----
echo "  [A] achieved write bandwidth (the cost you feel directly):"
BW_RND=$(./demo "$NVME_DIR" rand4k "$BYTES" | tee /dev/stderr | grep -oE '[0-9.]+ GB/s' | grep -oE '[0-9.]+')
BW_SEQ=$(./demo "$NVME_DIR" seq1m  "$BYTES" | tee /dev/stderr | grep -oE '[0-9.]+ GB/s' | grep -oE '[0-9.]+')
awk -v s="$BW_SEQ" -v r="$BW_RND" 'BEGIN{ if(r>0) printf "  VERDICT: sequential vs random-4k bandwidth -> %.1fx faster  [%s]\n", s/r, (s/r>=1.3)?"EFFECT CONFIRMED":"weak" }'

# ---- secondary: the drive's LIFETIME write amplification from vendor SMART ----
# (per-workload Δ is unreliable on a shared/busy drive: the nand counter lags
#  ~10s AND includes background GC. The lifetime ratio is clean and always there.)
if [ "$HAVE_WAF" = 1 ]; then
  H=$(ctr host_bytes_written); N=$(ctr nand_bytes_written)
  awk -v h="$H" -v n="$N" 'BEGIN{ printf "  [B] drive LIFETIME write amplification: nand/host = %d/%d = WAF %.2f\n", n, h, (h>0?n/h:0) }'
  echo "      (>1 means the SSD has physically written more than the host asked -- GC/relocation overhead.)"
  echo "      per-workload WAF needs an idle drive + large volume + ~60s settle; see README."
else
  echo "  [B] this drive does not expose NAND-written; WAF not measurable (see README for OCP/vendor alternatives)."
fi
nv_cleanup
