#!/usr/bin/env bash
# run_rq4.sh — RQ4 shard scaling (k = 1, 2, 4, 8) with the rebuilt batched
# RingORAM client + listeners.  Plain EC2 loopback: NO network emulation.
#   bash run_rq4.sh                      (from anywhere)
#   REPO=~/DePart/DePart DIST=skewed bash run_rq4.sh
# Every argument has the RQ4_ShardScaling default; override with env vars.
set -euo pipefail
REPO=${REPO:-$HOME/DePart/DePart}
ROWS=${ROWS:-20000}; REGIONS=${REGIONS:-64}; EPS=${EPS:-1.0}; REPEATS=${REPEATS:-5}
BASE=${BASE:-8881}; SHARDS=${SHARDS:-1,2,4,8}; OFFERED=${OFFERED:-0}; WARMUP=${WARMUP:-1}
DIST=${DIST:-uniform}; POOL=${POOL:-200000}
BIN=$REPO/build/bin; OUT=${OUT:-$REPO/results_rq4}; mkdir -p "$OUT"
[ -x "$BIN/RQ4_ShardScaling" ] || { echo "no $BIN/RQ4_ShardScaling — build it first"; exit 1; }
MAXK=$(echo "$SHARDS" | tr ',' '\n' | sort -n | tail -1)

unset DEPART_NET_RTT_MS DEPART_NET_BW_MBPS            # no simulated network
if tc qdisc show dev lo | grep -q netem; then echo "netem is active on lo — run: sudo TPC-C/net_emulate.sh off"; exit 1; fi

pkill -f Servers_MultiRingORAM 2>/dev/null || true; sleep 1
for ((i = 0; i < MAXK; i++)); do
    "$BIN/Servers_MultiRingORAM" $((BASE + i)) > "$OUT/listener_$((BASE + i)).log" 2>&1 &
done
trap 'pkill -f Servers_MultiRingORAM 2>/dev/null || true' EXIT
sleep 2

CSV=$OUT/rq4_shard_scaling_${DIST}.csv
"$BIN/RQ4_ShardScaling" "$ROWS" "$REGIONS" "$EPS" "$REPEATS" "$BASE" "$CSV" \
    "$SHARDS" "$OFFERED" "$WARMUP" "$DIST" "$POOL" 2>&1 | tee "$OUT/rq4_${DIST}.log"
echo "-> $CSV"
