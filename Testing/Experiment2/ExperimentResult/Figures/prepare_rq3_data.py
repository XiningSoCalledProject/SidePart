#!/usr/bin/env python3
"""Collect the six RQ3 result directories into three tidy CSVs.
Usage: python3 prepare_rq3_data.py ROOT OUT_DIR
  ROOT holds rq3_alg{3,4}_{point,mix,range}_out/ with load_off*.csv and skew_aq*.csv.
Writes (mean / std over the 3 seeds; system 'DePart' = DPart, 'Random' = epsilon = 0):
  rq3_throughput.csv  committed throughput, admitted rate, commit/abort rate     (load + skew sweeps)
  rq3_fanout.csv      mean shards per transaction (all / point / range txns),
                      pooled over every load of the load sweep (fan-out does not depend on load)
  rq3_overhead.csv    padded ORAM slots per committed txn and per admitted txn   (load + skew sweeps)"""
import csv, glob, os, re, statistics as st, sys
from collections import defaultdict
ROOT, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)
ALG = {"alg3": "Alg3", "alg4": "Alg4"}
MIX = {"point": "point-only", "mix": "mix (50% range)", "range": "range-only"}
PLACE = {"DePart": "DPart", "Random": "DPart eps=0 (random)"}

runs = []
for d in sorted(glob.glob(os.path.join(ROOT, "rq3_alg*_*_out"))):
    m = re.search(r"rq3_(alg[34])_(point|mix|range)_out$", d)
    if not m: continue
    for f in glob.glob(os.path.join(d, "*.csv")):
        name = os.path.basename(f)
        if "subepoch" in name or not (name.startswith("load_off") or name.startswith("skew_aq")): continue
        sweep = "load" if name.startswith("load_off") else "skew"
        for r in csv.DictReader(open(f)):
            r.update(algorithm=ALG[m.group(1)], query_mix=MIX[m.group(2)], sweep=sweep)
            runs.append(r)

def agg(keys, metrics):
    g = defaultdict(list)
    for r in runs:
        g[tuple(r[k] for k in keys)].append(r)
    out = []
    for key, L in sorted(g.items()):
        row = dict(zip(keys, key))
        row["placement"] = PLACE[row.pop("system")]
        row["K"] = "/".join(sorted({x["k"] for x in L}, key=int))
        row["n_runs"] = len(L)
        for name, fn in metrics:
            v = [fn(x) for x in L]
            row[f"{name}_mean"] = round(st.mean(v), 6)
            row[f"{name}_std"] = round(st.stdev(v), 6) if len(v) > 1 else 0.0
        out.append(row)
    return out

def write(name, rows):
    with open(os.path.join(OUT, name), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n"); w.writeheader(); w.writerows(rows)
    print(f"{name}: {len(rows)} rows")

f_ = lambda c: (lambda x: float(x[c]))
KEYS = ["algorithm", "query_mix", "sweep", "setting", "system", "offered_txn_s", "alpha_query"]
write("rq3_throughput.csv", agg(KEYS, [
    ("throughput_txn_s", f_("throughput_txn_s")),
    ("admitted_txn_s", lambda x: float(x["throughput_txn_s"]) / max(float(x["commit_rate"]), 1e-9)),
    ("commit_rate", f_("commit_rate")),
    ("abort_rate", lambda x: 1 - float(x["commit_rate"])),
    ("abort_read_batch_full", f_("abort_read_batch_full"))]))
write("rq3_overhead.csv", agg(KEYS, [
    ("slots_per_committed_txn", f_("slots_per_commit")),
    ("slots_per_admitted_txn", lambda x: (float(x["read_slots_sent"]) + float(x["write_slots_sent"])) / float(x["txns"])),
    ("contacted_frac", f_("contacted_frac")),
    ("imbalance_exec", f_("imbalance_exec"))]))
saved = runs
runs = [r for r in saved if r["sweep"] == "load"]            # fan-out: pooled over the load sweep
nan_ok = lambda c: (lambda x: float(x[c]) if x[c] not in ("", "nan") else float("nan"))
fan = agg(["algorithm", "query_mix", "setting", "system"], [
    ("fanout_all", f_("mean_fanout")),
    ("fanout_point_txns", nan_ok("mean_fanout_point")),
    ("fanout_range_txns", nan_ok("mean_fanout_range")),
    ("range_txn_frac", f_("range_txn_frac"))])
for r in fan:   # a txn type that does not occur in a query mix has no fan-out
    if r["query_mix"] == "point-only": r["fanout_range_txns_mean"] = r["fanout_range_txns_std"] = ""
    if r["query_mix"] == "range-only": r["fanout_point_txns_mean"] = r["fanout_point_txns_std"] = ""
write("rq3_fanout.csv", fan)
