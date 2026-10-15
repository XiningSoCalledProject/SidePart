#!/usr/bin/env bash
# ============================================================================
#  run_rq3_net.sh — network sensitivity of placement (RQ3 workload, one EC2 host)
#
#  The shards all run on this host, so proxy <-> shard traffic is loopback.  NetEmu.h adds the
#  cost of a real network to every padded shard batch:   delay = RTT + bytes / BW
#  (bytes = all batches of the round when the proxy NIC is shared, the default).
#  SidePart (epsilon > 0) and random placement (epsilon = 0) run at the SAME shard count, for
#  point-only, 50% range and range-only transactions, under two sweeps:
#     rtt : RTT in RTTS (ms) at bandwidth BW_FIXED
#     bw  : bandwidth in BWS (Mbit/s) at RTT RTT_FIXED
#  plus one "base" run per mix with emulation off (should match the existing RQ3 numbers).
#
#  Usage (on the EC2 host, from ~/DePart/DePart; never run two copies at once — same ports):
#     nohup bash run_rq3_net.sh pilot > ~/rq3_net_pilot.log 2>&1 &   # 1 run: range, RTT 100 ms
#     nohup bash run_rq3_net.sh all   > ~/rq3_net.log 2>&1 &         # base + rtt + bw sweeps
#     nohup bash run_rq3_net.sh rtt   > ~/rq3_net_rtt.log 2>&1 &     # only the RTT sweep
#     ALG=4 nohup bash run_rq3_net.sh all > ~/rq3_net_alg4.log 2>&1 &   # Algorithm 4 (elastic K)
#  Finished CSVs are skipped, so the script can simply be restarted.
# ============================================================================
set -u
cd ~/DePart/DePart
BIN=./build/bin/RQ3_DistShift
SRV=./build/bin/Servers_MultiRingORAM

# ── workload: identical to run_rq3_alg3_point.sh / run_rq3_alg4_point.sh ──
ROWS=20000; REGIONS=200; ALPHA_D=1.0; EPS=1.0; PORT=8881
REPEATS=10; SEEDS=3
AQ=${AQ:-1.0}                    # query skew (1.0 = the load sweep's value)
OFF=${OFF:-100}                  # offered load, txn/s (low load: per-round shard sets differ)
ALG=${ALG:-3}
if [ "$ALG" = 4 ]; then CAP_T=1250; K_MAX=32; else CAP_T=0; K_MAX=16; fi
OUT=rq3_net_alg${ALG}_off${OFF}_out      # one directory per offered load

# ── network grid ──
MIXES=${MIXES:-"point:0 mix:0.5 range:1.0"}   # name:range_fraction
RTTS=${RTTS:-"1 10 50 100 200"}                # ms, at BW_FIXED
BW_FIXED=${BW_FIXED:-1000}                     # Mbit/s (1 Gbps)
BWS=${BWS:-"10000 1000 500 100"}               # Mbit/s, at RTT_FIXED
RTT_FIXED=${RTT_FIXED:-1}                      # ms
SHARED=${SHARED:-1}                            # 1 = proxy NIC shared by all shard batches
PATH_FACTOR=${PATH_FACTOR:-1}                  # >1 adds Ring ORAM eviction traffic

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
one() {   # one <mix-name> <range_fraction> <rtt_ms> <bw_mbps>      (rtt=0 and bw=0 = emulation off)
    local name="net_$1_rtt$3_bw$4" range=$2 rtt=$3 bw=$4
    local csv="$OUT/$name.csv"
    if [ -s "$csv" ] && [ "$(wc -l < "$csv")" -gt 24 ]; then say "$name: done, skipping"; return; fi
    restart_listeners || return 1
    say "$name: range=$range RTT=${rtt}ms BW=${bw}Mbps shared=$SHARED offered=$OFF alg=$ALG"
    DEPART_NET_RTT_MS=$rtt DEPART_NET_BW_MBPS=$bw DEPART_NET_SHARED=$SHARED \
    DEPART_NET_PATH_FACTOR=$PATH_FACTOR \
        $BIN $K_MAX $ROWS $REGIONS $ALPHA_D $EPS $PORT "$csv" "$range" "$AQ" "$OFF" \
             $REPEATS $SEEDS 1 $CAP_T > "$OUT/$name.log" 2>&1
    grep -h "Network emulation" "$OUT/$name.log" | head -1 | sed 's/^/    /'
    say "  rows written: $(($(wc -l < "$csv" 2>/dev/null || echo 1) - 1))"
}
sweep_base() { for m in $MIXES; do one "${m%%:*}" "${m##*:}" 0 0; done; }
sweep_rtt()  { for m in $MIXES; do for r in $RTTS; do one "${m%%:*}" "${m##*:}" "$r" "$BW_FIXED"; done; done; }
sweep_bw()   { for m in $MIXES; do for b in $BWS;  do one "${m%%:*}" "${m##*:}" "$RTT_FIXED" "$b"; done; done; }

trap 'say interrupted; pkill -f RQ3_DistShift; pkill -f Servers_MultiRingORAM; exit 130' INT TERM
case "${1:-all}" in
  pilot) one range 1.0 100 "$BW_FIXED" ;;
  base)  sweep_base ;;
  rtt)   sweep_rtt ;;
  bw)    sweep_bw ;;
  all)   sweep_base; sweep_rtt; sweep_bw ;;
  *) echo "usage: $0 pilot|base|rtt|bw|all"; exit 1 ;;
esac
pkill -f Servers_MultiRingORAM 2>/dev/null
say "done -> $OUT"
