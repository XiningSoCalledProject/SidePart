#!/usr/bin/env bash
# run_rq2.sh — RQ2 privacy budget, eps in {0 (zero-information), 0.5, 1, 1.5, 2}.
#   bash Testing/Experiment1/run_rq2.sh            (from the repo root, or anywhere)
# Plain EC2 loopback: NO network emulation.  Args are the RQ2 defaults; change
# them here if your earlier run used other values.
set -euo pipefail
K=${K:-4}; ROWS=${ROWS:-20000}; REGIONS=${REGIONS:-40}; REPEATS=${REPEATS:-10}
MODE=${MODE:-both}; BASE=${BASE:-8881}; OFFERED=${OFFERED:-0}; RESERVE=${RESERVE:-1024}; CAP_T=${CAP_T:-0}
export RQ2_EPSILONS=${RQ2_EPSILONS:-0,0.5,1,1.5,2}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd); REPO=$(cd "$HERE/../.." && pwd)
BIN=$REPO/build/bin; OUT=$REPO/results_rq2_eps; mkdir -p "$OUT"
unset DEPART_NET_RTT_MS DEPART_NET_BW_MBPS            # no simulated network
if tc qdisc show dev lo | grep -q netem; then echo "netem is active on lo — run: sudo TPC-C/net_emulate.sh off"; exit 1; fi

pkill -f Servers_MultiRingORAM 2>/dev/null || true; sleep 1
for ((i = 0; i < K; i++)); do "$BIN/Servers_MultiRingORAM" $((BASE + i)) > "$OUT/listener_$((BASE + i)).log" 2>&1 & done
trap 'pkill -f Servers_MultiRingORAM 2>/dev/null || true' EXIT
sleep 2
"$BIN/RQ2_PrivateBudget" "$K" "$ROWS" "$REGIONS" "$REPEATS" "$MODE" "$BASE" "$OUT/rq2" "$OFFERED" "$RESERVE" "$CAP_T" \
    2>&1 | tee "$OUT/rq2.log"
echo "-> $OUT/rq2_{quality,perf,epochs}.csv"
