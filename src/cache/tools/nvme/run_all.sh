#!/usr/bin/env bash
# Run every demo's run.sh (A/B program) or observe.sh (read-only) in order.
# Serial on purpose: concurrent benchmarks on one NVMe corrupt each other's numbers.
cd "$(dirname "$0")" || exit 1
[ "$(id -u)" -ne 0 ] && exec sudo -E "$0" "$@"
for d in [0-9][0-9]-*/; do
  if   [ -f "$d/run.sh" ];     then bash "$d/run.sh";
  elif [ -f "$d/observe.sh" ]; then bash "$d/observe.sh";
  fi
done
echo
echo "[done] all nvme demos. See each subdir's README.md for what the numbers mean."
