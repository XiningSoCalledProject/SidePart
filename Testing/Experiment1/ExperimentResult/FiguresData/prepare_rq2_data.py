#!/usr/bin/env python3
"""
prepare_rq2_data.py — rebuild the two tidy CSVs used by RQ2Figures.ipynb from the raw RQ2 output.

Usage : python3 prepare_rq2_data.py [ROOT] [OUT_DIR]
        ROOT is searched recursively for the two run directories:
          rq2_alg34_out/   offered load 300 txn/s  (also holds alg3_quality.csv / alg4_quality.csv)
          rq2_alg34_hi/    offered load 1000 txn/s
        The offered load is read from rand_epochs.csv / alg*_epochs.csv (column offered_txn_s),
        not from the directory name.

Naming: code Algorithm 3 (alg3, fixed k) = paper Alg. 2; code Algorithm 4 (alg4, threshold T) = paper Alg. 3.

rq2_fig3_quality.csv  (one row per algorithm x epsilon; layout statistics over `repeats` noise draws)
    row_assignment_error_pct_mean/_sd = 100 * churn_vs_noiseless_mean / churn_sd
    partition_size_cv                 = size_cv_mean
    partitions_mean/_min/_max         = mean_partitions / min_partitions / max_partitions
    dp_padding_pct                    = 100 * total_dummy / (rows * repeats)
rq2_fig4_perf.csv     (one row per offered load x placement x epsilon; one run each)
    throughput_txn_s_incl_repartitioning = throughput_txn_s_with_refresh
    throughput_txn_s                     = throughput_txn_s (repartitioning pauses excluded)
    repartitioning_pause_pct             = amortized_refresh_pct
    repartitionings                      = refreshes
"""
import csv, glob, os, sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
OUT = sys.argv[2] if len(sys.argv) > 2 else "."
os.makedirs(OUT, exist_ok=True)
ALG = {"alg3": "Alg2", "alg4": "Alg3"}
f = lambda v: float(v)

def run_dirs():
    """every directory holding rand_perf.csv, with its offered load"""
    out = []
    for p in sorted(glob.glob(os.path.join(ROOT, "**", "rand_perf.csv"), recursive=True)):
        d = os.path.dirname(p)
        ep = next(csv.DictReader(open(os.path.join(d, "rand_epochs.csv"))))
        out.append((d, int(round(f(ep["offered_txn_s"])))))
    return out

# ---- figure 3: layout quality ----
Q = []
for p in sorted(glob.glob(os.path.join(ROOT, "**", "alg*_quality.csv"), recursive=True)):
    alg = ALG[os.path.basename(p).split("_")[0]]
    for r in csv.DictReader(open(p)):
        Q.append(dict(algorithm=alg, epsilon=f"{f(r['epsilon']):g}", k=r["k"], cap_T=f"{f(r['cap_T']):g}",
                      rows=r["rows"], regions=r["regions"], repeats=r["repeats"],
                      row_assignment_error_pct_mean=f"{100 * f(r['churn_vs_noiseless_mean']):.4f}",
                      row_assignment_error_pct_sd=f"{100 * f(r['churn_sd']):.4f}",
                      partition_size_cv=f"{f(r['size_cv_mean']):.6f}",
                      partitions_mean=f"{f(r['mean_partitions']):g}",
                      partitions_min=r["min_partitions"], partitions_max=r["max_partitions"],
                      dp_padding_pct=f"{100 * f(r['total_dummy']) / (f(r['rows']) * f(r['repeats'])):.4f}",
                      fanout_mean=r["fanout_mean"], affinity_cut_mean=r["affinity_cut_mean"]))
    if len({q["epsilon"] for q in Q if q["algorithm"] == alg}) != sum(1 for q in Q if q["algorithm"] == alg):
        print("warning: duplicate epsilon rows for", alg, "(more than one quality file?)")
Q.sort(key=lambda r: (r["algorithm"], float(r["epsilon"])))

# ---- figure 4: runtime ----
P = []
for d, load in run_dirs():
    for name, place in (("rand", "Random(eps=0)"), ("alg3", "Alg2"), ("alg4", "Alg3")):
        for r in csv.DictReader(open(os.path.join(d, f"{name}_perf.csv"))):
            P.append(dict(offered_txn_s=load, placement=place, epsilon=f"{f(r['epsilon']):g}",
                          partitions=r["partitions"],
                          throughput_txn_s_incl_repartitioning=r["throughput_txn_s_with_refresh"],
                          throughput_txn_s=r["throughput_txn_s"],
                          repartitioning_pause_pct=r["amortized_refresh_pct"],
                          repartitionings=r["refreshes"], mean_repartition_pause_ms=r["mean_refresh_pause_ms"],
                          committed=r["committed"], aborted=r["aborted"], epochs=r["epochs"],
                          dummy_total=r["dummy_total"], fanout=r["fanout"]))
P.sort(key=lambda r: (r["offered_txn_s"], r["placement"] != "Random(eps=0)", r["placement"], float(r["epsilon"])))

for name, rows in (("rq2_fig3_quality.csv", Q), ("rq2_fig4_perf.csv", P)):
    with open(os.path.join(OUT, name), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    print(f"wrote {name}: {len(rows)} rows")
print("epsilon values:", sorted({float(r["epsilon"]) for r in Q}))
print("offered loads :", sorted({r["offered_txn_s"] for r in P}))
