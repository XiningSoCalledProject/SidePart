#!/usr/bin/env bash
# ============================================================================
#  run_rq3_alg3_point.sh — RQ3 (distribution shift) with DePart on Algorithm 3 (fixed k = 16)
#
#  Same workload and arguments as run_rq3_sweep.sh, plus the 14th argument
#  cap_T > 0: DePart packs partitions of ~CAP_T noisy rows (K decided by the
#  data), and Random + index uses the same K for the same (seed, setting).
#  Listeners are restarted before every invocation (one invocation opens
#  2 systems x 4 settings x 3 seeds = 24 connections, after which a listener
#  exits).  Finished CSVs are skipped, so the script can be restarted.
#
#  Usage (nohup; tmux has been flaky on this host):
#     nohup bash run_rq3_alg3_point.sh load   > ~/rq3_alg3_point_load.log 2>&1 &     # sweep A
#     OFF_B=<rate> nohup bash run_rq3_alg3_point.sh skew > ~/rq3_alg3_point_skew.log 2>&1 &   # sweep B
#  Run sweep A first and pick OFF_B just below the balanced saturation point.
# ============================================================================
set -u
cd ~/DePart/DePart
BIN=./build/bin/RQ3_DistShift
SRV=./build/bin/Servers_MultiRingORAM
OUT=rq3_alg3_point_out
ROWS=20000; REGIONS=200; ALPHA_D=1.0; EPS=1.0; PORT=8881
RANGE=0; REPEATS=10; SEEDS=3
CAP_T=0
K_MAX=16
# Throughput sweep: 500 1000 1500 2000 txn/s.  1000/1500/2000 already exist in $OUT and are
# skipped by one() (a finished CSV has > 24 rows), so only 500 actually runs.
# Override without editing:  LOAD_RATES="500" bash run_rq3_alg3_point.sh load
LOAD_RATES=${LOAD_RATES:-"500 1000 1500 2000"}
SKEW_AQ="0 0.5 1.0 1.5 2.0"
OFF_B=${OFF_B:-}

mkdir -p "$OUT"
say() { echo "[$(date '+%m-%d %H:%M:%S')] $*"; }
n_listen() { lsof -nP -iTCP:${PORT}-$((PORT + K_MAX - 1)) -sTCP:LISTEN 2>/dev/null | grep -c LISTEN; }
restart_listeners() {
    pkill -f RQ3_DistShift 2>/dev/null; pkill -f Servers_MultiRingORAM 2>/dev/null
    for _ in $(seq 1 30); do [ "$(n_listen)" -eq 0 ] && break; sleep 1; done
    pkill -9 -f Servers_MultiRingORAM 2>/dev/null; sleep 2
    for p in $(seq $PORT $((PORT + K_MAX - 1))); do "$SRV" "$p" > "$OUT/listener_$p.log" 2>&1 & done
    for _ in $(seq 1 60); do [ "$(n_listen)" -ge "$K_MAX" ] && return 0; sleep 1; done
    say "listeners did not start"; return 1
}
one() {   # one <csv-name> <alpha_q> <offered>
    local csv="$OUT/$1.csv" aq=$2 off=$3
    if [ -s "$csv" ] && [ "$(wc -l < "$csv")" -gt 24 ]; then say "$1: done, skipping"; return; fi
    restart_listeners || return 1
    say "$1: alpha_q=$aq offered=$off cap_T=$CAP_T"
    $BIN $K_MAX $ROWS $REGIONS $ALPHA_D $EPS $PORT "$csv" $RANGE "$aq" "$off" $REPEATS $SEEDS 1 $CAP_T \
        > "$OUT/$1.log" 2>&1
    grep -h "K=" "$OUT/$1.log" | sort | uniq -c | head -8 | sed 's/^/    /'
    say "  rows written: $(($(wc -l < "$csv") - 1))"
}
trap 'say interrupted; pkill -f RQ3_DistShift; pkill -f Servers_MultiRingORAM; exit 130' INT TERM
case "${1:-load}" in
  load) for OFF in $LOAD_RATES; do one "load_off${OFF}" 1.0 "$OFF"; done ;;
  skew) [ -n "$OFF_B" ] || { echo "set OFF_B (from sweep A) first"; exit 1; }
        for AQ in $SKEW_AQ; do one "skew_aq${AQ}_off${OFF_B}" "$AQ" "$OFF_B"; done ;;
  pilot) one "pilot_off400" 1.0 400 ;;
  *) echo "usage: $0 load|skew|pilot"; exit 1 ;;
esac
pkill -f Servers_MultiRingORAM 2>/dev/null
say "done -> $OUT"
