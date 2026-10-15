#!/usr/bin/env bash
# net_emulate.sh — emulate a 1 Gbps / 10 ms RTT link between the proxy and the
# ORAM shards on loopback (shards listen on 127.0.0.1:BASE .. BASE+K-1).
#
#   sudo ./net_emulate.sh on     <base_port> <k> [shared|per-shard] [rate=1gbit] [rtt_ms=10]
#   sudo IMPL=netemrate ./net_emulate.sh on ...   # alternative layout, see below
#   sudo ./net_emulate.sh verify <port>          # run BEFORE starting the shards
#   sudo ./net_emulate.sh show
#   sudo ./net_emulate.sh off
#
# shared    : the proxy has ONE 1 Gbps full-duplex NIC. All proxy->shard traffic
#             shares one 1 Gbps class, all shard->proxy traffic another.
#             Main setting: Obladi (k=1) and SidePart (k shards) get the same link.
# per-shard : every shard sits behind its own 1 Gbps link (proxy side uncapped).
#
# netem on lo delays each direction once, so each class gets RTT/2.
# Only IPv4 TCP to/from the shard ports is shaped; other lo traffic is untouched.
# `on` also sets lo's MTU to 1500 (a real NIC's), saving the old value, and
# `off` restores it: lo's default 64 KB MTU puts packets larger than the HTB
# quantum into the shaper, which stalls bulk transfers.
set -euo pipefail
DEV=lo
cmd=${1:-}

MTU_SAVE=/tmp/net_emulate_lo_mtu
off() {
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  if [ -f "$MTU_SAVE" ]; then ip link set "$DEV" mtu "$(cat "$MTU_SAVE")"; rm -f "$MTU_SAVE"; fi
}

case "$cmd" in
off)  off; echo "shaping removed from $DEV (MTU $(cat /sys/class/net/$DEV/mtu))"; exit 0 ;;
show) tc -s qdisc show dev "$DEV"; tc -s class show dev "$DEV"; exit 0 ;;
verify)
  P=${2:?port}
  echo "== RTT (TCP handshake to port $P, expect ~10 ms) =="
  python3 - "$P" <<'EOF'
import socket, sys, time, statistics
p = int(sys.argv[1])
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", p)); srv.listen(64)
ts = []
for _ in range(20):
    c = socket.socket(); t = time.perf_counter(); c.connect(("127.0.0.1", p))
    ts.append((time.perf_counter() - t) * 1e3); s, _ = srv.accept(); s.close(); c.close()
print(f"connect p50 = {statistics.median(ts):.2f} ms")
EOF
  echo "== Request/response after idle gaps (ORAM-like; expect ~10 ms, no hang) =="
  if ! timeout 30 python3 - "$P" <<'PYEOF'
import socket, sys, threading, time, statistics
p = int(sys.argv[1])
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", p)); srv.listen(1)
def echo():
    s, _ = srv.accept()
    while True:
        d = s.recv(65536)
        if not d: break
        s.sendall(d)
threading.Thread(target=echo, daemon=True).start()
c = socket.create_connection(("127.0.0.1", p))
c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
msg = b"x" * 4096; ts = []
for i in range(15):
    t = time.perf_counter(); c.sendall(msg); got = 0
    while got < len(msg): got += len(c.recv(65536))
    ts.append((time.perf_counter() - t) * 1e3)
    time.sleep(0.2)            # idle gap: nothing else pushes the queue
print(f"4 KB round trip p50 = {statistics.median(ts):.2f} ms, max = {max(ts):.2f} ms")
PYEOF
  then echo "request/response TIMED OUT — packets stuck in the shaper after idle (BAD)"; fi
  sleep 0.5
  echo "== Bandwidth (expect ~940 Mbit/s every second, then sender + receiver totals) =="
  if command -v iperf3 >/dev/null; then
    timeout 20 iperf3 -s -p "$P" -1 >/dev/null 2>&1 & sleep 0.5
    timeout 15 iperf3 -c 127.0.0.1 -p "$P" -t 5 -i 1 \
      || echo "iperf3 timed out — shaping stalls bulk traffic (check: tc -s qdisc show dev $DEV)"
    wait 2>/dev/null || true
  else echo "iperf3 not installed — skip (apt install iperf3)"; fi
  exit 0 ;;
on) ;;
*) sed -n '2,18p' "$0"; exit 1 ;;
esac

BASE=${2:?base_port}; K=${3:?k}; MODE=${4:-shared}; RATE=${5:-1gbit}; RTT=${6:-10}
HALF=$(awk "BEGIN{print $RTT/2}")

# GSO/TSO hand 64 KB super-packets to the shaper; disable so the rate is
# enforced per wire-sized packet.
ethtool -K "$DEV" gso off tso off gro off >/dev/null 2>&1 \
  || echo "warn: ethtool -K $DEV failed; rate shaping may be coarse"

off
cat /sys/class/net/"$DEV"/mtu > "$MTU_SAVE"
ip link set "$DEV" mtu 1500

# Two layouts.  Never put netem UNDER a rate-limiting HTB class: netem holds
# packets until their send time, HTB sees a non-work-conserving child and the
# last packets of a burst are never sent once traffic stops (seen on EC2:
# iperf3 sends 5 s at 950 Mbit/s, then its final exchange hangs forever) —
# fatal for ORAM's burst-then-idle batches.
#
# IMPL=root (default, the layout the netem docs recommend for delay + rate):
#   root netem (delay RTT/2, every lo packet)  ->  child HTB: shard ports
#   -> 1 Gbps classes, everything else -> unshaped class.  Non-shard lo
#   traffic also gets the delay, which is harmless on a benchmark box.
# IMPL=netemrate (fallback only: netem is still a leaf under HTB, so it can
#   show the same stall):
#   root HTB used only to CLASSIFY (all classes 100 Gbit, never throttles);
#   each shard class has a netem leaf doing both delay and rate.
IMPL=${IMPL:-root}
if [ "$IMPL" = root ]; then
  tc qdisc add dev "$DEV" root handle 1: netem delay "${HALF}ms" limit 100000
  H=2
  tc qdisc add dev "$DEV" parent 1:1 handle 2: htb default 1
  tc class add dev "$DEV" parent 2: classid 2:1 htb rate 100gbit quantum 1514      # unshaped
  add_class() { tc class add dev "$DEV" parent 2: classid "2:$1" htb rate "$RATE" ceil "$RATE" quantum 1514; }
elif [ "$IMPL" = netemrate ]; then
  H=1
  tc qdisc add dev "$DEV" root handle 1: htb default 1
  tc class add dev "$DEV" parent 1: classid 1:1 htb rate 100gbit quantum 1514      # unshaped
  add_class() {
    tc class add dev "$DEV" parent 1: classid "1:$1" htb rate 100gbit quantum 1514
    tc qdisc add dev "$DEV" parent "1:$1" handle "$1:" netem delay "${HALF}ms" rate "$RATE" limit 100000
  }
else
  echo "IMPL must be root or netemrate"; exit 1
fi
filt() {        # $1 = dport|sport, $2 = port, $3 = class minor
  tc filter add dev "$DEV" parent "$H": protocol ip prio 1 u32 \
     match ip "$1" "$2" 0xffff flowid "$H:$3"
}

if [ "$MODE" = shared ]; then
  add_class 10; add_class 20
  for ((i = 0; i < K; i++)); do p=$((BASE + i)); filt dport $p 10; filt sport $p 20; done
elif [ "$MODE" = per-shard ]; then
  for ((i = 0; i < K; i++)); do
    up=$(printf '%x' $((0x100 + i))); dn=$(printf '%x' $((0x800 + i)))
    add_class "$up"; add_class "$dn"
    p=$((BASE + i)); filt dport $p "$up"; filt sport $p "$dn"
  done
else
  echo "mode must be shared or per-shard"; exit 1
fi
echo "$DEV: ports $BASE..$((BASE + K - 1)) -> $RATE, RTT ${RTT} ms ($MODE), MTU 1500, IMPL=$IMPL"
