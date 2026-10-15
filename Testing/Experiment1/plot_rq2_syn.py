#!/usr/bin/env python3
"""plot_rq2_syn.py — RQ2 synopsis ablation (mechanism evidence), one file per panel.

    python3 plot_rq2_syn.py  results_rq2_syn  rq2_syn_figs  [--eps 0.5] [--arms oram exact random]

  In Jupyter: run this cell, then   main(["results_rq2_syn", "rq2_syn_figs"])

Reads rq2_synopsis.csv (RQ2_PrivateBudget mode "synopsis" / run_rq2_syn.sh).
Arms (same data-aware placement, same eps; only how aggregates are answered):
  ORAM lookup            aggregate reads every row of its group through ORAM
  Proxy synopsis (exact) trusted proxy answers from exact counts, 0 ORAM reads
  Proxy synopsis (noisy) proxy answers from a DP release (eps_load)
  Zero-info + ORAM       reference: eps = 0 placement, aggregates via ORAM
With one eps the x axis is the arm (bar chart); with several eps it is eps
(one line per arm).

Writes, each as .pdf and .png:
  rq2syn_throughput      committed txn/s (aggregates answered at the proxy count as served)
  rq2syn_reads_per_txn   real ORAM reads per served txn
  rq2syn_occupancy       read-batch occupancy (real / slots sent)
  rq2syn_rbf_rate        READ_BATCH_FULL aborts / admitted txns
  rq2syn_summary.csv     the numbers behind the figures
"""
import argparse, csv, glob, os, sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"font.family": "serif", "font.size": 8, "axes.labelsize": 8.5, "legend.fontsize": 7,
                     "xtick.labelsize": 7.5, "ytick.labelsize": 8, "pdf.fonttype": 42, "ps.fonttype": 42,
                     "mathtext.fontset": "dejavuserif"})

ARMS = [  # key, label, colour, hatch
    ("oram",   "ORAM\nlookup",       "#7f7f7f", ""),
    ("exact",  "Proxy\nsynopsis",       "#0b4f9c", ""),
    ("noisy",  "Proxy synopsis\n(noisy)", "#5b8fd0", ""),
    ("random", "Zero-info\n+ ORAM",     "#ff7f0e", "///"),
]


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def arm_key(r):
    return "random" if r.get("placement") == "random" else r["arm"]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_dir")
    ap.add_argument("out")
    ap.add_argument("--width", type=float, default=3.4)
    ap.add_argument("--height", type=float, default=2.4)
    ap.add_argument("--arms", nargs="+", default=["oram", "exact", "random"],
                    help="arms to draw, in order (oram exact noisy random); default without noisy")
    ap.add_argument("--eps", type=float, default=None,
                    help="draw only this placement eps (e.g. 0.5, the TPC-C budget); default: all in the csv")
    a = ap.parse_args(argv)       # argv=None -> command line; in Jupyter pass a list
    a.csv_dir, a.out = os.path.expanduser(a.csv_dir), os.path.expanduser(a.out)

    files = glob.glob(os.path.join(a.csv_dir, "*_synopsis.csv")) or \
            glob.glob(os.path.join(a.csv_dir, "**", "*_synopsis.csv"), recursive=True)
    rows = []
    for p in files:
        with open(p) as fh:
            rows += list(csv.DictReader(fh))
    if not rows:
        print("!! no *_synopsis.csv in", os.path.abspath(a.csv_dir), " (cwd:", os.getcwd() + ")")
        return
    os.makedirs(a.out, exist_ok=True)
    print(f"  {len(files)} csv, {len(rows)} rows  ->", os.path.abspath(a.out))

    by = {}                                            # (arm, eps) -> row
    for r in rows:
        by[(arm_key(r), f(r["epsilon"]))] = r
    eps_vals = sorted({e for (k, e) in by if k != "random"})
    if a.eps is not None:
        eps_vals = [e for e in eps_vals if abs(e - a.eps) < 1e-9]
        if not eps_vals:
            print(f"!! no rows with eps={a.eps:g} in the csv"); return
    arms = [x for key in a.arms for x in ARMS if x[0] == key and any(k == key for (k, _) in by)]

    def save(fig, name):
        fig.tight_layout(pad=0.3)
        for ext in ("pdf", "png"):
            fig.savefig(os.path.join(a.out, f"{name}.{ext}"), dpi=300, bbox_inches="tight")
        plt.close(fig)
        print("  wrote", name + ".pdf/.png")

    def get(arm, eps):
        if arm == "random":
            return next((r for (k, _), r in by.items() if k == "random"), None)
        return by.get((arm, eps))

    def fig_metric(name, ylabel, key, second=None, second_label=None, pct=False):
        scale = 100.0 if pct else 1.0
        fig, ax = plt.subplots(figsize=(a.width, a.height))
        if len(eps_vals) <= 1:                         # bar chart over arms
            e = eps_vals[0] if eps_vals else 0.0
            xs = range(len(arms))
            for i, (k, lab, c, h) in enumerate(arms):
                r = get(k, e)
                if r is None:
                    continue
                v = f(r[key]) * scale
                ax.bar(i, v, width=0.62, color=c, hatch=h, edgecolor="white", zorder=3)
                ax.text(i, v, f"{v:.1f}" if v < 99.95 else f"{v:.0f}", ha="center", va="bottom", fontsize=6.5)
                if second:
                    v2 = f(r[second]) * scale
                    ax.bar(i, v2, width=0.62, color="none", edgecolor="black", hatch="....", linewidth=0.6, zorder=4)
            ax.set_xticks(list(xs), [lab for _, lab, _, _ in arms], fontsize=7)
            ax.set_xlabel(f"Aggregate answering path (data-aware placement, $\\epsilon$={e:g})")
            if second and second_label:
                from matplotlib.patches import Patch
                ax.legend([Patch(facecolor="white", edgecolor="black", hatch="....", linewidth=0.6)],
                          [second_label], frameon=False, loc="upper left")
        else:                                          # lines over eps
            for k, lab, c, h in arms:
                if k == "random":
                    r = get(k, 0)
                    if r is not None:
                        ax.axhline(f(r[key]) * scale, color=c, linestyle="--", linewidth=1,
                                   label=lab.replace("\n", " ") + " (ref.)")
                    continue
                pts = [(e, f(by[(k, e)][key]) * scale) for e in eps_vals if (k, e) in by]
                ax.plot([p[0] for p in pts], [p[1] for p in pts], color=c, marker="o", markersize=4,
                        linewidth=1.5, label=lab.replace("\n", " "))
            ax.set_xlabel("Privacy budget $\\epsilon$ (placement)")
            ax.set_xticks(eps_vals, [f"{e:g}" for e in eps_vals])
            ax.legend(frameon=False, loc="best", fontsize=6.5)
        ax.set_ylabel(ylabel)
        ax.set_ylim(bottom=0, top=ax.get_ylim()[1] * 1.12)
        ax.grid(True, axis="y", alpha=0.3, linewidth=0.6)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        save(fig, name)

    fig_metric("rq2syn_throughput", "Committed throughput (txn/s)", "throughput_txn_s")
    fig_metric("rq2syn_reads_per_txn", "ORAM reads per served txn", "oram_reads_per_served_txn")
    fig_metric("rq2syn_occupancy", "Read-batch occupancy (%)", "read_batch_occupancy", pct=True)
    fig_metric("rq2syn_rbf_rate", "READ_BATCH_FULL aborts (% of admitted)", "read_batch_full_rate", pct=True)

    keys = ["epsilon", "arm", "placement", "throughput_txn_s", "regular_txn_s", "proxy_answered",
            "agg_committed_oram", "oram_reads_per_served_txn", "read_batch_occupancy",
            "read_batch_full_rate", "noisy_mean_abs_err", "agg_releases", "cumulative_agg_eps"]
    with open(os.path.join(a.out, "rq2syn_summary.csv"), "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["arm_label"] + keys)
        for k, lab, _, _ in arms:
            for e in (eps_vals if k != "random" else [0.0]):
                r = get(k, e)
                if r is not None:
                    w.writerow([lab.replace("\n", " ")] + [r.get(x, "") for x in keys])
    print("  wrote rq2syn_summary.csv")
    print(f"  {'arm':28s} {'eps':>4s} {'served/s':>9s} {'regular/s':>9s} {'reads/txn':>9s} {'occ%':>6s} {'RBF%':>6s} {'|err|':>6s}")
    for k, lab, _, _ in arms:
        for e in (eps_vals if k != "random" else [0.0]):
            r = get(k, e)
            if r is None:
                continue
            print(f"  {lab.replace(chr(10), ' '):28s} {f(r['epsilon']):4g} {f(r['throughput_txn_s']):9.1f} "
                  f"{f(r['regular_txn_s']):9.1f} {f(r['oram_reads_per_served_txn']):9.2f} "
                  f"{100 * f(r['read_batch_occupancy']):6.1f} {100 * f(r['read_batch_full_rate']):6.1f} "
                  f"{f(r['noisy_mean_abs_err']):6.2f}")


if __name__ == "__main__" and "ipykernel" not in sys.modules:
    main()
