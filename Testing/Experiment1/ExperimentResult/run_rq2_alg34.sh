#!/usr/bin/env bash
# ============================================================================
#  run_rq2_alg34.sh — RQ2 (privacy budget) with DePart on Algorithm 3 AND 4
#
#  Same settings as the earlier paced RQ2 run:
#     rows=20000, regions=2000 (10 rows/bin), 10 repeats, 300 txn/s paced,
#     insert reserve 96 blocks/shard
#  Algorithm 3: exactly K3=4 partitions (as before).
#  Algorithm 4: fixed threshold CAP_T=5000 -> ~4-5 partitions at large eps
#               (same resources as Algorithm 3), growing as eps shrinks.
#  Figure 3 (quality) needs no cluster.  Figure 4 (perf) runs ONE eps per
#  invocation with freshly started listeners, then the per-eps CSVs are merged.
#  Finished pieces are skipped, so the script can be restarted.
#
#  Usage:  nohup bash run_rq2_alg34.sh > ~/rq2_alg34.log 2>&1 &
#          nohup bash run_rq2_alg34.sh quality > ... &      # only Figure 3
#          nohup bash run_rq2_alg34.sh perf    > ... &      # only Figure 4
#          nohup bash run_rq2_alg34.sh random  > ... &      # eps = 0 random placement
#                     (Figure 4 only; k = K3 shards, same growth / repartitioning
#                      pipeline; writes $OUT/rand_perf.csv and rand_epochs.csv)
# ============================================================================
set -u
cd ~/DePart/DePart
BIN=./build/bin/RQ2_PrivateBudget
SRV=./build/bin/Servers_MultiRingORAM
OUT=rq2_alg34_out
ROWS=20000; REGIONS=2000; REPEATS=10; PORT=8881; OFFERED=300; RESERVE=96
K3=4
CAP_T=5000
K_MAX=40            # listeners for Figure 4 (Algorithm 4 needs up to ~25 at eps=0.01)
EPS_GRID="0.01 0.025 0.05 0.1 0.25 0.5 1.0 2.0 4.0 8.0"
WHAT=${1:-all}

mkdir -p "$OUT"
say() { echo "[$(date '+%m-%d %H:%M:%S')] $*"; }
n_listen() { lsof -nP -iTCP:${PORT}-$((PORT + K_MAX - 1)) -sTCP:LISTEN 2>/dev/null | grep -c LISTEN; }
restart_listeners() {
    pkill -f RQ2_PrivateBudget 2>/dev/null; pkill -f Servers_MultiRingORAM 2>/dev/null
    for _ in $(seq 1 30); do [ "$(n_listen)" -eq 0 ] && break; sleep 1; done
    pkill -9 -f Servers_MultiRingORAM 2>/dev/null; sleep 2
    for p in $(seq $PORT $((PORT + K_MAX - 1))); do "$SRV" "$p" > "$OUT/listener_$p.log" 2>&1 & done
    for _ in $(seq 1 60); do [ "$(n_listen)" -ge "$K_MAX" ] && return 0; sleep 1; done
    say "listeners did not start"; return 1
}
# alg_args <3|4>  ->  "<k> ... <cap_T>" pieces
kfor()   { [ "$1" = 3 ] && echo $K3 || echo $K_MAX; }
capfor() { [ "$1" = 3 ] && echo 0   || echo $CAP_T; }

quality() {   # quality <alg>
    local a=$1 pre="$OUT/alg$1"
    if [ -s "${pre}_quality.csv" ]; then say "alg$a quality: done, skipping"; return; fi
    say "alg$a quality (Figure 3)"
    $BIN "$(kfor $a)" $ROWS $REGIONS $REPEATS quality $PORT "$pre" $OFFERED $RESERVE "$(capfor $a)" \
        > "${pre}_quality.log" 2>&1
    grep -E "eps=" "${pre}_quality.log" | sed 's/^/    /'
}
perf_one() {  # perf_one <alg> <eps>
    local a=$1 e=$2 pre="$OUT/alg$1_eps$2"
    if [ -s "${pre}_perf.csv" ] && [ "$(wc -l < "${pre}_perf.csv")" -ge 2 ]; then
        say "alg$a eps=$e perf: done, skipping"; return; fi
    restart_listeners || return 1
    say "alg$a eps=$e perf (Figure 4)"
    RQ2_EPSILONS=$e $BIN "$(kfor $a)" $ROWS $REGIONS $REPEATS perf $PORT "$pre" $OFFERED $RESERVE "$(capfor $a)" \
        > "${pre}.log" 2>&1
    grep -E "→ cut=|FAILED|\[layout\]" "${pre}.log" | head -4 | sed 's/^/    /'
}
merge() {     # merge <alg>
    local a=$1
    for kind in perf epochs; do
        local out="$OUT/alg${a}_${kind}.csv" first=1
        : > "$out"
        for e in $EPS_GRID; do
            local f="$OUT/alg${a}_eps${e}_${kind}.csv"
            [ -s "$f" ] || continue
            if [ $first = 1 ]; then cat "$f" >> "$out"; first=0; else tail -n +2 "$f" >> "$out"; fi
        done
        say "  merged -> $out ($(($(wc -l < "$out") - 1)) rows)"
    done
}

perf_random() {   # eps = 0: zero-information placement, k = K3, one run
    local pre="$OUT/rand"
    if [ -s "${pre}_perf.csv" ] && [ "$(wc -l < "${pre}_perf.csv")" -ge 2 ]; then
        say "random (eps=0) perf: done, skipping"; return; fi
    restart_listeners || return 1
    say "random (eps=0) perf, k=$K3 (Figure 4)"
    $BIN $K3 $ROWS $REGIONS $REPEATS perf $PORT "$pre" $OFFERED $RESERVE 0 random > "${pre}.log" 2>&1
    grep -E "Placement|\[layout\]|REFRESH|FAILED" "${pre}.log" | head -6 | sed 's/^/    /'
}

trap 'say interrupted; pkill -f RQ2_PrivateBudget; pkill -f Servers_MultiRingORAM; exit 130' INT TERM
if [ "$WHAT" = all ] || [ "$WHAT" = quality ]; then quality 3; quality 4; fi
if [ "$WHAT" = random ]; then perf_random; fi
if [ "$WHAT" = all ] || [ "$WHAT" = perf ]; then
    for a in 3 4; do for e in $EPS_GRID; do perf_one $a $e; done; merge $a; done
fi
pkill -f Servers_MultiRingORAM 2>/dev/null
say "done -> $OUT"
