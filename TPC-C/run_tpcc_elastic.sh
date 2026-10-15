#!/usr/bin/env bash
# run_tpcc_elastic.sh — standard TPC-C with an ELASTIC number of shards:
# SidePart's Algorithm 4 decides K from a public threshold instead of a fixed k.
#
#   sudo bash TPC-C/run_tpcc_elastic.sh                 # plan + calibrate + final (3 seeds)
#   sudo PHASE=plan      bash TPC-C/run_tpcc_elastic.sh # only: K for every threshold (no shards, seconds)
#   sudo PHASE=calibrate bash TPC-C/run_tpcc_elastic.sh # only: pick the threshold
#   sudo PHASE=final M=2 bash TPC-C/run_tpcc_elastic.sh # only: final runs with threshold m = 2
#
# Threshold.  m = warehouses per partition (public; the per-table threshold is
#   T = (m - 0.5) x rows_per_warehouse, a TPC-C schema constant), so
#   K = ceil(W / m):  W = 8 -> m = 1, 2, 3, 4 gives K = 8, 4, 3, 2.
#   (K <= W: SidePart bins are whole warehouses, a warehouse is never split.)
#
# Calibration (PHASE=calibrate) under the REAL network of the main runs
#   (1 Gbps shared proxy link, 10 ms RTT, tc netem): one SidePart run per m at
#   the highest offered rate, on a PILOT seed that is not reported
#   (CAL_SEED, default 7).  The m with the highest committed throughput wins
#   (ties -> fewer shards).  -> $OUT/calibration.csv
#
# Final (PHASE=final): seeds 42 43 44 x RATES, same systems as the k = 4 runs:
#   sidepart_dp_syn   SidePart data-aware + Stock-Level synopsis, threshold m*
#   sidepart_random   zero-information placement on the SAME K* shards
#   obladi            one ORAM with the same TOTAL batch budget (bread = 256 K*)
#   -> $OUT/s<seed>/<system>_r<rate>_{summary,epochs,rounds,types}.csv
#      (same file names as results_tpccstd_*; plot_tpcc_std_panels.py reads them)
set -uo pipefail
W=${W:-8}; N=${N:-4000}; RATES=${RATES:-"20 40 60 80 120 160"}; BASE=${BASE:-8881}
EPS=${EPS:-0.5}; SL_EPS=${SL_EPS:-0.5}; SL_SENS=${SL_SENS:-20}; R=${R:-4}
M_LIST=${M_LIST:-"1 2 3 4"}; CAL_SEED=${CAL_SEED:-7}; CAL_RATE=${CAL_RATE:-160}; CAL_N=${CAL_N:-$N}
SEEDS=${SEEDS:-"42 43 44"}; PHASE=${PHASE:-all}; M=${M:-}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)       # <repo>/TPC-C
REPO=$(cd "$HERE/.." && pwd)
BIN=$REPO/build/bin; NET=$HERE/net_emulate.sh
DATA=$HERE/tpcc_w${W}; OUT=${OUT:-$HERE/results_tpccstd_w${W}_elastic}
mkdir -p "$OUT"
for b in TPCC_Std tpcc-generator Servers_MultiRingORAM; do
  [ -x "$BIN/$b" ] || { echo "$BIN/$b missing — build first"; exit 1; }
done
[ -d "$DATA" ] || $BIN/tpcc-generator "$W" "$DATA"
KMAX=$W                                        # m = 1 needs W shards

n_listen() { lsof -nP -iTCP:"$BASE"-$((BASE + $1 - 1)) -sTCP:LISTEN 2>/dev/null | grep -c LISTEN || true; }
stop_listeners() {
  pkill -f "$BIN/TPCC_Std run" 2>/dev/null || true
  pkill -f Servers_MultiRingORAM 2>/dev/null || true
  for _ in $(seq 1 30); do [ "$(n_listen 16)" -eq 0 ] && return 0; sleep 1; done
  pkill -9 -f Servers_MultiRingORAM 2>/dev/null || true; sleep 2
}
start_listeners() {   # $1 = number of shards, $2 = log dir
  stop_listeners
  for ((i = 0; i < $1; i++)); do "$BIN/Servers_MultiRingORAM" $((BASE + i)) > "$2/listener_$((BASE + i)).log" 2>&1 & done
  for _ in $(seq 1 60); do [ "$(n_listen "$1")" -ge "$1" ] && return 0; sleep 1; done
  echo "listeners did not start"; return 1
}
col() {   # col <summary.csv> <column name>
  awk -F, -v c="$2" 'NR==1{for(i=1;i<=NF;i++) if($i==c) j=i} NR==2{print $j}' "$1"
}
# run <dir> <name> <k> <placement> <eps> <bread> <bwrite> <rate> <escrow> <synopsis> <seed> <n> [m]
run() {
  local dir=$1 name=$2 k=$3 place=$4 eps=$5 br=$6 bw=$7 rate=$8 esc=$9 syn=${10} seed=${11} n=${12} m=${13:-}
  start_listeners "$KMAX" "$dir" || return 1
  TPCC_WH_PER_PART=$m TPCC_ESCROW=$esc TPCC_SL_SYNOPSIS=$syn TPCC_SL_EPS=$SL_EPS TPCC_SL_SENS=$SL_SENS \
    $BIN/TPCC_Std run "$DATA" "$k" "$n" "$rate" "$place" "$eps" "$BASE" "$dir/${name}_r${rate}" "$seed" "$W" \
      "$br" "$bw" 0 "$R" > "$dir/${name}_r${rate}.log" 2>&1
  local s="$dir/${name}_r${rate}_summary.csv"
  [ -f "$s" ] && echo "    $name rate=$rate seed=$seed K=$(col "$s" k) tps=$(col "$s" committed_tps)" \
              || echo "    $name rate=$rate seed=$seed FAILED (see $dir/${name}_r${rate}.log)"
}

# ── plan: K for every m (STOCK only, no shards) ─────────────────────────────
if [ "$PHASE" = plan ] || [ "$PHASE" = all ]; then
  echo "== plan: partitions per threshold (W=$W, eps=$EPS) =="
  for m in $M_LIST; do $BIN/TPCC_Std plan "$DATA" "$W" "$m" "$EPS" | grep -E "^W=|partition"; done | tee "$OUT/plan.txt"
  [ "$PHASE" = plan ] && exit 0
fi

sudo "$NET" on "$BASE" "$KMAX" shared 1gbit 10
trap 'stop_listeners; sudo "$NET" off' EXIT

# ── calibrate: one SidePart run per m, pilot seed, highest rate ─────────────
if [ "$PHASE" = calibrate ] || [ "$PHASE" = all ]; then
  CAL=$OUT/calibration; mkdir -p "$CAL"
  echo "== calibrate: rate=$CAL_RATE seed=$CAL_SEED (pilot, not reported), m in {$M_LIST} =="
  echo "m,K,committed_tps,tpmC,epoch_ms_mean,txn_shards_mean,read_rounds_rejected_frac" > "$OUT/calibration.csv"
  for m in $M_LIST; do
    run "$CAL" "sidepart_dp_syn_m$m" "$W" depart "$EPS" 256 512 "$CAL_RATE" 1 1 "$CAL_SEED" "$CAL_N" "$m"
    s="$CAL/sidepart_dp_syn_m${m}_r${CAL_RATE}_summary.csv"; e="$CAL/sidepart_dp_syn_m${m}_r${CAL_RATE}_epochs.csv"
    [ -f "$s" ] || continue
    ems=$(awk -F, 'NR>1{s+=$2;n++} END{if(n) printf "%.1f", s/n}' "$e")
    echo "$m,$(col "$s" k),$(col "$s" committed_tps),$(col "$s" tpmC),$ems,$(col "$s" txn_shards_mean),$(col "$s" read_rounds_rejected_frac)" >> "$OUT/calibration.csv"
  done
  column -s, -t "$OUT/calibration.csv"
  # best throughput; ties (within 1%) -> fewer shards (larger m)
  M=$(awk -F, 'NR>1{print $1, $2, $3}' "$OUT/calibration.csv" | sort -k3,3gr -k2,2n | awk 'NR==1{best=$3; m=$1; k=$2; next}
        $3 >= 0.99*best && $2 < k {m=$1; k=$2} END{print m}')
  echo "== chosen threshold: m=$M  (K=$(awk -F, -v m="$M" '$1==m{print $2}' "$OUT/calibration.csv")) =="
  echo "$M" > "$OUT/chosen_m.txt"
  [ "$PHASE" = calibrate ] && exit 0
fi

# ── final: 3 seeds x all rates, SidePart (m*), random (K*), Obladi (budget K*) ──
if [ "$PHASE" = final ] || [ "$PHASE" = all ]; then
  [ -n "$M" ] || M=$(cat "$OUT/chosen_m.txt" 2>/dev/null)
  [ -n "$M" ] || { echo "no threshold: run PHASE=calibrate first or pass M=<m>"; exit 1; }
  K=$($BIN/TPCC_Std plan "$DATA" "$W" "$M" "$EPS" | awk -F= '/^K=/{print $2}')
  echo "== final: m=$M -> K=$K, seeds {$SEEDS}, rates {$RATES} =="
  [ "$K" = 4 ] && echo "   (K = 4: same shard count as the existing k = 4 runs)"
  for seed in $SEEDS; do
    D=$OUT/s$seed; mkdir -p "$D"
    for r in $RATES; do
      run "$D" sidepart_dp_syn "$K" depart "$EPS" 256 512 "$r" 1 1 "$seed" "$N" "$M"
      run "$D" sidepart_random "$K" random 0      256 512 "$r" 1 0 "$seed" "$N"
      run "$D" obladi          1    obladi 0 $((256 * K)) $((512 * K)) "$r" 0 0 "$seed" "$N"
    done
    SUMS=$(ls "$D"/*_r*_summary.csv)
    { head -n1 "$(echo "$SUMS" | head -n1)"; for f in $SUMS; do tail -n +2 "$f"; done; } > "$D/all_summary.csv"
  done
  echo "-> $OUT/s{$(echo $SEEDS | tr ' ' ',')}/all_summary.csv"
fi
