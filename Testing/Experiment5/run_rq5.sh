#!/usr/bin/env bash
# run_rq5.sh — growth-rate sweep, rho in {0.25, 0.5, 1, 1.5, 2}, for each overflow policy.
#   bash ~/DePart/DePart/Testing/Experiment1/run_rq5.sh
#   POLICIES="backpressure standby early" E_MAX=20 bash run_rq5.sh
# Plain EC2 loopback: NO network emulation.
set -euo pipefail
REPO=${REPO:-$HOME/DePart/DePart}
K=${K:-4}; ROWS=${ROWS:-20000}; REGIONS=${REGIONS:-40}
E_MAX=${E_MAX:-20}; EPOCHS=${EPOCHS:-0}; C_IOO=${C_IOO:-4096}; EPS=${EPS:-1.0}; BASE=${BASE:-8881}
POLICIES=${POLICIES:-"backpressure standby early"}
export RQ5_RHOS=${RQ5_RHOS:-0.25,0.5,1,1.5,2}
export RQ5_STANDBY_FACTOR=${RQ5_STANDBY_FACTOR:-1.0}
BIN=$REPO/build/bin; OUT=${OUT:-$REPO/results_rq5}; mkdir -p "$OUT"
[ -x "$BIN/RQ5_GrowthRate" ] || { echo "no $BIN/RQ5_GrowthRate — build it first"; exit 1; }

unset DEPART_NET_RTT_MS DEPART_NET_BW_MBPS
if tc qdisc show dev lo | grep -q netem; then echo "netem is active on lo — run: sudo TPC-C/net_emulate.sh off"; exit 1; fi

for POL in $POLICIES; do
    pkill -f Servers_MultiRingORAM 2>/dev/null || true; sleep 1
    for ((i = 0; i < K; i++)); do
        "$BIN/Servers_MultiRingORAM" $((BASE + i)) > "$OUT/listener_${POL}_$((BASE + i)).log" 2>&1 &
    done
    sleep 2
    echo "=== policy $POL ==="
    "$BIN/RQ5_GrowthRate" "$K" "$ROWS" "$REGIONS" "$E_MAX" "$EPOCHS" "$C_IOO" "$EPS" "$POL" "$BASE" \
        "$OUT/growth_${POL}" 2>&1 | tee "$OUT/growth_${POL}.log"
done
pkill -f Servers_MultiRingORAM 2>/dev/null || true
echo "-> $OUT/growth_<policy>_{epochs,summary}.csv"
