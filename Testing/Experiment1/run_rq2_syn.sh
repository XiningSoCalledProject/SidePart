#!/usr/bin/env bash
# run_rq2_syn.sh — RQ2 synopsis ablation: same data-aware placement and eps,
# aggregates answered via ORAM vs at the trusted proxy (exact / noisy synopsis).
#   bash run_rq2_syn.sh                         # eps = 0.5 (TPC-C), arms oram exact random
#   SMOKE=1 bash run_rq2_syn.sh                 # small quick check (~1 min)
#   RQ2_SYN_EPS="0.5,1,1.5,2" bash run_rq2_syn.sh
# Plain EC2 loopback: NO network emulation.
set -euo pipefail
REPO=${REPO:-$HOME/DePart/DePart}
K=${K:-4}; ROWS=${ROWS:-20000}; REGIONS=${REGIONS:-40}; BASE=${BASE:-8881}
OFFERED=${OFFERED:-0}; RESERVE=${RESERVE:-1024}
export RQ2_SYN_EPS=${RQ2_SYN_EPS:-0.5}   # TPC-C data-aware budget (eps_part = 0.5)
export RQ2_SYN_ARMS=${RQ2_SYN_ARMS:-oram,exact,random}
export RQ2_AGG_FRAC=${RQ2_AGG_FRAC:-0.1}; export RQ2_AGG_GROUP=${RQ2_AGG_GROUP:-32}; export RQ2_AGG_EPS=${RQ2_AGG_EPS:-0.5}
OUT=${OUT:-$REPO/results_rq2_syn}
if [ "${SMOKE:-0}" = 1 ]; then ROWS=4000; OUT=$REPO/results_rq2_syn_smoke; fi
BIN=$REPO/build/bin; mkdir -p "$OUT"
[ -x "$BIN/RQ2_PrivateBudget" ] || { echo "no $BIN/RQ2_PrivateBudget — build it first"; exit 1; }
unset DEPART_NET_RTT_MS DEPART_NET_BW_MBPS
if tc qdisc show dev lo | grep -q netem; then echo "netem is active on lo — run: sudo TPC-C/net_emulate.sh off"; exit 1; fi
pkill -f Servers_MultiRingORAM 2>/dev/null || true; sleep 1
for ((i = 0; i < K; i++)); do "$BIN/Servers_MultiRingORAM" $((BASE + i)) > "$OUT/listener_$((BASE + i)).log" 2>&1 & done
trap 'pkill -f Servers_MultiRingORAM 2>/dev/null || true' EXIT
sleep 2
"$BIN/RQ2_PrivateBudget" "$K" "$ROWS" "$REGIONS" 1 synopsis "$BASE" "$OUT/rq2" "$OFFERED" "$RESERVE" 0 \
    2>&1 | tee "$OUT/rq2_syn.log"
echo "-> $OUT/rq2_synopsis.csv, $OUT/rq2_synopsis_epochs.csv"
