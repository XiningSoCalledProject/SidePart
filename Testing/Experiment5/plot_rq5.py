#!/usr/bin/env python3
"""plot_rq5.py — growth-rate sweep figures, one file per panel.

    python3 plot_rq5.py  results_rq5  rq5_figs   [--rhos 0.25 1 2]

  In Jupyter: run this cell, then in a new cell
    main(["results_rq5", "rq5_figs"])

Reads growth_<policy>_epochs.csv and growth_<policy>_summary.csv (RQ5_GrowthRate,
run_rq5.sh).  rho = inserts expected before E_max / C_IOO.

Writes to OUT_DIR, each as .pdf and .png:
  NAMING: "occupancy" here is BUFFERED inserts (active IOO + standby) normalised
  by ONE IOO capacity C_IOO, so it exceeds 100 % under the standby policy.  The
  figures and table call it "buffered inserts (% of one IOO capacity)", never
  "IOO utilisation"; the table also gives the split active IOO (<= 100) + standby.
  rq5_occupancy_<policy>     Figure A: buffered inserts (% of one C_IOO) over epochs for
                             --rhos (default 0.25, 1, 2); dashed vertical lines =
                             repartitionings (E = E_max, 2 E_max, ...)
  rq5_eff_throughput         Figure B: effective foreground throughput (repartition
                             pauses included) vs rho, one line per policy;
                             hollow markers = epochs only
  rq5_a_priv                 inserts per privacy release A_priv = N_ins / eps_r vs rho
  rq5_overflow               inserts rejected (backpressure) / standby blocks activated vs rho
  rq5_buffered_at_repartition  buffered inserts at repartitioning (% of one C_IOO) vs rho
  rq5_repartitions           number of repartitionings (= DP releases after the first) vs rho
  rq5_table.csv / .tex       the table: buffered inserts at repartition (+ active / standby split), repartition
                             duration, inserts per repartition, standby / rejects, A_priv
"""
import argparse, csv, glob, os, re, sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"font.family": "serif", "font.size": 8, "axes.labelsize": 8.5, "legend.fontsize": 7,
                     "xtick.labelsize": 8, "ytick.labelsize": 8, "pdf.fonttype": 42, "ps.fonttype": 42,
                     "mathtext.fontset": "dejavuserif"})

POLICY_STYLE = {   # colour, marker, legend
    "backpressure": ("#1f77b4", "o", "no standby (backpressure)"),
    "standby":      ("#2ca02c", "^", "standby IOO"),
    "early":        ("#d62728", "s", "repartition when full"),
}
RHO_COLORS = ["#9ec2ea", "#5b8fd0", "#1f77b4", "#0b4f9c", "#08306b"]
RHO_NAME = {0.25: "slow", 1.0: "matched", 2.0: "fast"}


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def find_all(d, pattern):
    hits = glob.glob(os.path.join(d, pattern))
    return hits or glob.glob(os.path.join(d, "**", pattern), recursive=True)


def style(ax):
    ax.grid(True, alpha=0.3, linewidth=0.6)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_dir")
    ap.add_argument("out")
    ap.add_argument("--rhos", type=float, nargs="+", default=[0.25, 1, 2], help="rho values in Figure A")
    ap.add_argument("--width", type=float, default=3.4)
    ap.add_argument("--height", type=float, default=2.4)
    a = ap.parse_args(argv)       # argv=None -> command line; in Jupyter pass a list
    a.csv_dir, a.out = os.path.expanduser(a.csv_dir), os.path.expanduser(a.out)   # allow ~ in paths

    epochs = defaultdict(lambda: defaultdict(list))   # policy -> rho -> rows
    summ = defaultdict(dict)                          # policy -> rho -> row
    for p in find_all(a.csv_dir, "growth_*_epochs.csv"):
        with open(p) as fh:
            for r in csv.DictReader(fh):
                epochs[r["policy"]][round(f(r["rho"]), 4)].append(r)
    for p in find_all(a.csv_dir, "growth_*_summary.csv"):
        with open(p) as fh:
            for r in csv.DictReader(fh):
                summ[r["policy"]][round(f(r["rho"]), 4)] = r
    print("  policies:", sorted(set(epochs) | set(summ)))
    if not epochs and not summ:
        print("!! no growth_*_epochs.csv / growth_*_summary.csv in", os.path.abspath(a.csv_dir),
              " (cwd:", os.getcwd() + ")")
        return
    os.makedirs(a.out, exist_ok=True)
    print("  output ->", os.path.abspath(a.out))
    policies = [p for p in POLICY_STYLE if p in summ or p in epochs] + \
               [p for p in sorted(set(summ) | set(epochs)) if p not in POLICY_STYLE]

    def save(fig, name):
        fig.tight_layout(pad=0.3)
        for ext in ("pdf", "png"):
            fig.savefig(os.path.join(a.out, f"{name}.{ext}"), dpi=300, bbox_inches="tight")
        plt.close(fig)
        print("  wrote", name + ".pdf/.png")

    def pstyle(p):
        return POLICY_STYLE.get(p, ("#7f7f7f", "D", p))

    # ---- Figure A: occupancy over epochs, one file per policy -------------
    for pol in policies:
        by_rho = epochs.get(pol, {})
        if not by_rho:
            continue
        fig, ax = plt.subplots(figsize=(a.width, a.height))
        top = 100.0
        wanted = [r for r in sorted(by_rho) if any(abs(r - w) < 1e-6 for w in a.rhos)] or sorted(by_rho)
        reps = {}                                   # x -> colours of the rhos that repartitioned there
        for i, rho in enumerate(wanted):
            rr = sorted(by_rho[rho], key=lambda r: int(f(r["epoch"])))
            c = RHO_COLORS[min(len(RHO_COLORS) - 1, int(round(i * (len(RHO_COLORS) - 1) / max(1, len(wanted) - 1))))]
            # epoch e's occupancy is measured at its END: plot at x = e + 1, starting from 0 at x = 0;
            # after a repartitioning the IOO is empty again, so drop to 0 at the same x
            X, Y = [0], [0.0]
            for r in rr:
                x, y = int(f(r["epoch"])) + 1, f(r["occupancy_total_pct"])
                X.append(x); Y.append(y); top = max(top, y)
                if f(r["repartitioned"]) >= 1:
                    X.append(x); Y.append(0.0)
                    reps.setdefault(x, []).append(c)
            name = RHO_NAME.get(rho)
            ax.plot(X, Y, color=c, linewidth=1.5, label=f"$\\rho$={rho:g}" + (f" ({name})" if name else ""))
        E_max = int(f(next(iter(by_rho.values()))[0]["E_max"]))
        for x, cs in sorted(reps.items()):          # scheduled repartitions coincide -> black
            col = "black" if len(set(cs)) > 1 or len(wanted) == 1 else cs[0]
            ax.axvline(x, color=col, linestyle="--", linewidth=0.8, alpha=0.7, zorder=0)
        ax.plot([], [], color="black", linestyle="--", linewidth=0.8, label="repartition")
        ax.axhline(100, color="grey", linestyle=":", linewidth=0.8)
        ax.set_ylim(0, top * 1.08); ax.set_xlim(left=0)
        if top > 100.5:                             # buffered beyond one IOO = standby blocks
            ax.axhspan(100, top * 1.08, color="#2ca02c", alpha=0.07, zorder=0)
            ax.text(0.99, (100 + top * 1.08) / 2, "standby IOO", transform=ax.get_yaxis_transform(),
                    ha="right", va="center", fontsize=6.5, color="#2ca02c")
            ax.text(0.99, 50, "active IOO", transform=ax.get_yaxis_transform(),
                    ha="right", va="center", fontsize=6.5, color="grey")
        ax.set_xlabel(f"Epoch ($E_{{max}}$ = {E_max})")
        ax.set_ylabel("Buffered inserts (% of one $C_{IOO}$)")
        style(ax)
        ax.legend(frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=2, fontsize=6.5)
        save(fig, f"rq5_occupancy_{pol}")

    # ---- per-rho line figures over policies --------------------------------
    def line_fig(name, ylabel, getter, hollow_getter=None, hollow_label=None, ylim0=True, percent=False):
        fig, ax = plt.subplots(figsize=(a.width, a.height))
        drew = False
        for pol in policies:
            rows = summ.get(pol, {})
            if not rows:
                continue
            c, m, lab = pstyle(pol)
            xs = sorted(rows)
            ys = [getter(rows[x]) for x in xs]
            if all(y != y for y in ys):
                continue
            ax.plot(xs, ys, color=c, marker=m, markersize=5, linewidth=1.5, label=lab)
            if hollow_getter:
                ax.plot(xs, [hollow_getter(rows[x]) for x in xs], color=c, marker=m, markersize=5,
                        markerfacecolor="white", linestyle=":", linewidth=1)
            drew = True
        if not drew:
            plt.close(fig); return
        if hollow_getter and hollow_label:
            ax.plot([], [], color="grey", marker="o", markerfacecolor="white", linestyle=":", label=hollow_label)
        ax.set_xlabel("Growth rate $\\rho$ = inserts before $E_{max}$ / $C_{IOO}$")
        ax.set_ylabel(ylabel)
        if ylim0:
            ax.set_ylim(bottom=0)
        allx = sorted({x for p in policies for x in summ.get(p, {})})
        ax.set_xticks(allx, [f"{x:g}" for x in allx])
        style(ax)
        ax.legend(frameon=False, loc="best")
        save(fig, name)

    line_fig("rq5_eff_throughput", "Effective throughput (txn/s)",
             lambda r: f(r["throughput_eff_txn_s"]), lambda r: f(r["throughput_fg_txn_s"]),
             "epochs only (no repartition pause)")
    line_fig("rq5_a_priv", "Inserts per privacy release $A_{priv}$ (= $N_{ins}/\\epsilon_r$)",
             lambda r: f(r["A_priv"]))
    line_fig("rq5_buffered_at_repartition", "Buffered inserts at repartition\n(% of one $C_{IOO}$)",
             lambda r: f(r["ioo_util_at_repartition_pct"]))
    line_fig("rq5_repartitions", "Repartitionings (DP releases)",
             lambda r: f(r["repartitions"]))
    line_fig("rq5_overflow", "Overflow inserts (rejected or on standby)",
             lambda r: f(r["rejected_ins"]) if r["policy"] != "standby" else f(r["standby_blocks_max"]))

    # ---- the table -----------------------------------------------------------
    # active / standby split: exact columns from the 10/10/26 build, else derived (exact when
    # shards fill evenly; the per-shard split is only in the new columns)
    for p_ in summ.values():
        for r in p_.values():
            x = f(r.get("ioo_util_at_repartition_pct"))
            if not r.get("active_ioo_at_repartition_pct"):
                r["active_ioo_at_repartition_pct"] = f"{min(100.0, x):.2f}"
            if not r.get("standby_at_repartition_pct"):
                r["standby_at_repartition_pct"] = f"{max(0.0, x - 100.0):.2f}"
    cols = [("policy", "policy"), ("rho", "rho"),
            ("ioo_util_at_repartition_pct", "buffered @ rep. (% of one C_IOO)"),
            ("active_ioo_at_repartition_pct", "active IOO (%)"),
            ("standby_at_repartition_pct", "standby (% of C_IOO)"),
            ("mean_repartition_ms", "rep. duration (ms)"), ("inserts_per_repartition", "inserts / rep."),
            ("standby_blocks_max", "standby blocks"), ("rejected_ins", "rejected inserts"),
            ("insert_reject_pct", "rejected (%)"), ("repartitions", "#rep."), ("A_priv", "A_priv"),
            ("cumulative_eps", "cum. eps"), ("throughput_eff_txn_s", "eff. txn/s")]
    rows = [summ[p][r] for p in policies for r in sorted(summ.get(p, {}))]
    if rows:
        with open(os.path.join(a.out, "rq5_table.csv"), "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow([c[0] for c in cols])
            for r in rows:
                w.writerow([r.get(c[0], "") for c in cols])
        with open(os.path.join(a.out, "rq5_table.tex"), "w") as fh:
            fh.write("\\begin{tabular}{ll" + "r" * (len(cols) - 2) + "}\n\\toprule\n")
            fh.write(" & ".join(c[1].replace("%", "\\%").replace("_", "\\_").replace("#", "\\#") for c in cols) + " \\\\\n\\midrule\n")
            for r in rows:
                cells = []
                for key, _ in cols:
                    v = r.get(key, "")
                    if key == "policy":
                        cells.append(v)
                    elif key in ("rho", "repartitions", "standby_blocks_max", "rejected_ins"):
                        cells.append(f"{f(v):g}")
                    else:
                        cells.append(f"{f(v):.1f}")
                fh.write(" & ".join(cells) + " \\\\\n")
            fh.write("\\bottomrule\n\\end{tabular}\n")
        print("  wrote rq5_table.csv / rq5_table.tex")
        print(f"  {'policy':13s} {'rho':>5s} {'buf@rep%':>9s} {'rep ms':>7s} {'ins/rep':>8s} {'standby':>7s} "
              f"{'reject':>6s} {'#rep':>4s} {'A_priv':>8s} {'eff txn/s':>9s}")
        for r in rows:
            print(f"  {r['policy']:13s} {f(r['rho']):5g} {f(r['ioo_util_at_repartition_pct']):9.1f} "
                  f"{f(r['mean_repartition_ms']):7.0f} {f(r['inserts_per_repartition']):8.0f} "
                  f"{f(r['standby_blocks_max']):7.0f} {f(r['rejected_ins']):6.0f} {f(r['repartitions']):4.0f} "
                  f"{f(r['A_priv']):8.0f} {f(r['throughput_eff_txn_s']):9.1f}")


if __name__ == "__main__" and "ipykernel" not in sys.modules:
    main()
