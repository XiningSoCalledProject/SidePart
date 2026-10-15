#!/usr/bin/env bash
# run_tpcc_std.sh — standard TPC-C under 1 Gbps / 10 ms RTT.
#
#   sudo TPC-C/run_tpcc_std.sh [W=10] [K=5] [business_txns=4000] [rates="10 20 40 80"] [base_port=8881]
#   sudo ORACLE=1 TPC-C/run_tpcc_std.sh ...      # also run the non-private oracle
#   sudo ESCROW=0 TPC-C/run_tpcc_std.sh ...      # Payment without escrow (old behaviour);
#                                                # results go to ..._noescrow/
#
# Payment escrow (part of SidePart): W_YTD / D_YTD increments are summed by
# the proxy and written once per row per epoch, so Payments of one warehouse
# no longer conflict (see include/tpcc_std_txns.h).
#   SidePart (dp, random) : escrow = $ESCROW   (default 1)
#   Obladi                : plain MVTSO, escrow OFF (Obladi has no escrow)
#   ABLATION=1            : also run obladi_escrow (Obladi + our escrow), to
#                           separate the gain of escrow from that of placement
# The column `escrow` in *_summary.csv records what each run used.
#
# Stock-Level DP synopsis (tpcc_stock_synopsis.h): Stock-Level answered by
# the proxy from a noised synopsis, 0 ORAM ops, approximate.  The synopsis is
# released ONCE at partitioning and reused unchanged for the whole run.
# SL_EPS=1.0 (eps of that release), SL_SENS=20 (L1 sensitivity, see the header).
#   sidepart_dp / sidepart_random : NO synopsis (default; SYNOPSIS=1 turns it on)
#   SYN=1 adds two separate lines (own file names, nothing is overwritten):
#     sidepart_dp_syn      DP placement + synopsis
#     sidepart_random_syn  random placement + synopsis  (control: is the gain
#                          specific to data-aware placement?)
#   Only the synopsis lines, same workload as an earlier run:
#     sudo SYN=1 ONLY="sidepart_dp_syn sidepart_random_syn" TPC-C/run_tpcc_std.sh 8 4 2000 "20 40 80 160"
#   Columns sl_* in *_summary.csv: on/off, eps, #answers, mean |error|.
#
#   ONLY="sidepart_dp sidepart_random"   run only these systems
#   TAG=final                            results go to results_tpccstd_w<W>_k<K>_final/
#   SEED=43                              workload seed (default 42); results go
#                                        to results_..._s43/ when SEED != 42
# Per-shard read load per round -> *_rounds.csv; summarize with
#   python3 TPC-C/analyze_shard_load.py <results dir>
#
# Main comparison (every rate):
#   1. sidepart_dp      SidePart, DP data-aware        placement depart, eps_part = 0.5
#   2. sidepart_random  SidePart, eps = 0 (zero info)  placement random (data-independent)
#   3. obladi           Obladi-style single ORAM       placement obladi
# Optional (ORACLE=1):
#   4. exact_oracle     NON-private exact data-aware partitioning — the upper
#                       bound on locality; NOT an eps = 0 setting.
#
# W > K by default (W=10, K=5: two warehouses per shard) so data-aware placement
# is not an artificial "one warehouse = one shard" layout.  W = K is fine for
# smoke tests:  sudo TPC-C/run_tpcc_std.sh 4 4 500 "100"
# depart / exact_oracle need K <= W, and W a multiple of K (e.g. 10/5, 10/2, 12/4).
#
# Lives in <repo>/TPC-C/ next to net_emulate.sh; can be called from anywhere.
# Binaries: <repo>/build/bin.  Data (tpcc_w<W>/) and results
# (results_tpccstd_w<W>_k<K>/) go to <repo>/TPC-C/.
#
# Shard listeners (Servers_MultiRingORAM, one per port) are (re)started by this
# script before EVERY run, as run_tpcc_w5.sh does; logs: $OUT/listener_<port>.log
# Obladi gets the same TOTAL batch capacity (bread/bwrite x K) and the same
# 1 Gbps link (shared mode), so the comparison isolates placement.
set -euo pipefail
W=${1:-10}; K=${2:-5}; N=${3:-4000}; RATES=${4:-"10 20 40 80"}; BASE=${5:-8881}
EPS=${EPS:-0.5}; R=${R:-4}; ORACLE=${ORACLE:-0}; ESCROW=${ESCROW:-1}; ABLATION=${ABLATION:-0}
ONLY=${ONLY:-}; SEED=${SEED:-42}; SYNOPSIS=${SYNOPSIS:-0}; SL_EPS=${SL_EPS:-1.0}; SL_SENS=${SL_SENS:-20}; SYN=${SYN:-0}
want() { [ -z "$ONLY" ] || [[ " $ONLY " == *" $1 "* ]]; }
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)       # <repo>/TPC-C
REPO=$(cd "$HERE/.." && pwd)
BIN=$REPO/build/bin; NET=$HERE/net_emulate.sh
DATA=$HERE/tpcc_w${W}; OUT=$HERE/results_tpccstd_w${W}_k${K}
[ "$ESCROW" = 0 ] && OUT=${OUT}_noescrow
[ "$SEED" != 42 ] && OUT=${OUT}_s${SEED}
[ -n "${TAG:-}" ] && OUT=${OUT}_${TAG}          # e.g. TAG=final -> results_..._final, or ..._s43_final with SEED=43
for b in TPCC_Std tpcc-generator Servers_MultiRingORAM; do
  [ -x "$BIN/$b" ] || { echo "$BIN/$b missing — build first (see header)"; exit 1; }
done
[ "$K" -le "$W" ] || { echo "need K <= W (depart partitions on the warehouse column)"; exit 1; }
[ $((W % K)) -eq 0 ] || { echo "need W to be a multiple of K (each shard gets W/K whole warehouses)"; exit 1; }
mkdir -p "$OUT"
[ -d "$DATA" ] || $BIN/tpcc-generator "$W" "$DATA"

n_listen() { lsof -nP -iTCP:"$BASE"-$((BASE + $1 - 1)) -sTCP:LISTEN 2>/dev/null | grep -c LISTEN || true; }
stop_listeners() {
  pkill -f "$BIN/TPCC_Std" 2>/dev/null || true
  pkill -f Servers_MultiRingORAM 2>/dev/null || true
  for _ in $(seq 1 30); do [ "$(n_listen 16)" -eq 0 ] && return 0; sleep 1; done
  pkill -9 -f Servers_MultiRingORAM 2>/dev/null || true; sleep 2
}
start_listeners() {   # $1 = number of shards; ports BASE..BASE+$1-1
  stop_listeners
  for ((i = 0; i < $1; i++)); do
    p=$((BASE + i))
    "$BIN/Servers_MultiRingORAM" "$p" > "$OUT/listener_$p.log" 2>&1 &
  done
  for _ in $(seq 1 60); do [ "$(n_listen "$1")" -ge "$1" ] && return 0; sleep 1; done
  echo "listeners did not start (see $OUT/listener_*.log)"; return 1
}

sudo "$NET" on "$BASE" "$K" shared 1gbit 10
trap 'stop_listeners; sudo "$NET" off' EXIT

run() {   # name k placement eps bread bwrite rate escrow synopsis
  want "$1" || return 0
  start_listeners "$2"
  # 2>&1: the ORAM children report fatal errors on stderr — keep them in the log.
  TPCC_ESCROW=$8 TPCC_SL_SYNOPSIS=${9:-0} TPCC_SL_EPS=$SL_EPS TPCC_SL_SENS=$SL_SENS $BIN/TPCC_Std run "$DATA" "$2" "$N" "$7" "$3" "$4" "$BASE" "$OUT/$1_r$7" "$SEED" "$W" "$5" "$6" 0 "$R" \
      2>&1 | tee "$OUT/$1_r$7.log"
}
for r in $RATES; do
  run sidepart_dp     "$K" depart       "$EPS" 256 512 "$r" "$ESCROW" "$SYNOPSIS"
  run sidepart_random "$K" random       0      256 512 "$r" "$ESCROW" "$SYNOPSIS"
  if [ "$SYN" = 1 ]; then
    run sidepart_dp_syn     "$K" depart "$EPS" 256 512 "$r" "$ESCROW" 1
    run sidepart_random_syn "$K" random 0      256 512 "$r" "$ESCROW" 1
  fi
  run obladi          1    obladi       0      $((256 * K)) $((512 * K)) "$r" 0 0
  if [ "$ABLATION" = 1 ]; then run obladi_escrow 1 obladi 0 $((256 * K)) $((512 * K)) "$r" 1 0; fi
  if [ "$ORACLE" = 1 ]; then run exact_oracle "$K" exact_oracle 0 256 512 "$r" "$ESCROW" "$SYNOPSIS"; fi
done

# one table: system, k, W, rate, tps, tpmC, shards per round, ...
# (all_summary.csv itself also matches *_summary.csv -> excluded)
SUMS=$(ls "$OUT"/*_r*_summary.csv)
{ head -n1 "$(echo "$SUMS" | head -n1)"
  for f in $SUMS; do tail -n +2 "$f"; done; } > "$OUT/all_summary.tmp" && mv "$OUT/all_summary.tmp" "$OUT/all_summary.csv"
echo "-> $OUT/all_summary.csv"
