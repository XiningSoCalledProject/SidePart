#!/usr/bin/env python3
"""RQ3 figures from the three CSVs written by prepare_rq3_data.py.
Usage: python3 plot_rq3.py DATA_DIR OUT_DIR
For every case (algorithm x query mix) it writes, as PDF + PNG:
  rq3_<alg>_<mix>_throughput        committed throughput vs offered load (2x2 settings)
  rq3_<alg>_<mix>_fanout            mean shards per transaction per setting
  rq3_<alg>_<mix>_overhead_commit   padded ORAM slots per COMMITTED txn vs load
  rq3_<alg>_<mix>_overhead_admit    padded ORAM slots per ADMITTED txn vs load
  rq3_<alg>_<mix>_throughput_skew   throughput vs query skew (only where the skew sweep exists)
Paper-sized (7 in wide; fan-out 3.4 in), TrueType fonts, no figure titles."""
import csv, os, sys
import numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
DATA, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)
plt.rcParams.update({"pdf.fonttype": 42, "ps.fonttype": 42, "font.family": "DejaVu Sans", "font.size": 8,
                     "axes.labelsize": 8, "axes.titlesize": 8.5, "xtick.labelsize": 7.5, "ytick.labelsize": 7.5,
                     "legend.fontsize": 7.5, "hatch.linewidth": 0.6})
rd = lambda n: list(csv.DictReader(open(os.path.join(DATA, n))))
TP, OV, FO = rd("rq3_throughput.csv"), rd("rq3_overhead.csv"), rd("rq3_fanout.csv")
SET = [("uniform_data+uniform_queries", "Uniform data, uniform queries"),
       ("skewed_data+matched_queries", "Skewed data, matched queries"),
       ("uniform_data+skewed_queries", "Uniform data, skewed queries"),
       ("skewed_data+mismatched_queries", "Skewed data, mismatched queries")]
SHORT = {"uniform_data+uniform_queries": "Unif. data\nunif. queries", "skewed_data+matched_queries": "Skew. data\nmatched q.",
         "uniform_data+skewed_queries": "Unif. data\nskewed q.", "skewed_data+mismatched_queries": "Skew. data\nmismatched q."}
PL = ["DPart", "DPart eps=0 (random)"]
STY = {"DPart": dict(color="#08519c", edgecolor="none"),
       "DPart eps=0 (random)": dict(color="white", edgecolor="#ff7f0e", hatch="/////", linewidth=0.5)}
MIXTAG = {"point-only": "point", "mix (50% range)": "mix", "range-only": "range"}
MIXTXT = {"point-only": "point queries only", "mix (50% range)": "50% point + 50% range", "range-only": "range queries only"}

def lab(alg, p):
    return f"DPart ({alg[:3]}. {alg[3:]})" if p == "DPart" else r"DPart, $\epsilon$=0 (random placement)"
def style(ax):
    ax.grid(True, axis="y", which="major", alpha=0.3, lw=0.5); ax.set_axisbelow(True)
    for sp in ax.spines.values(): sp.set_linewidth(0.6)
    ax.tick_params(width=0.6, length=2.5)
def save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(OUT, f"{name}.{ext}"), dpi=300, bbox_inches="tight", pad_inches=0.01)
    plt.close(fig)
def pick(T, alg, mix, sweep, s, p):
    return [r for r in T if r["algorithm"] == alg and r["query_mix"] == mix and r["sweep"] == sweep
            and r["setting"] == s and r["placement"] == p]

def grid(T, alg, mix, sweep, metric, ylabel, name, xcol, xlabel, log=False, ylim=None, pct=False, admitted=False):
    rows_any = [r for r in T if r["algorithm"] == alg and r["query_mix"] == mix and r["sweep"] == sweep]
    if not rows_any: return False
    xs_vals = sorted({float(r[xcol]) for r in rows_any})
    fig, axs = plt.subplots(2, 2, figsize=(7.0, 4.3), sharey=not log)
    x = np.arange(len(xs_vals)); w = 0.4
    for ax, (s, n) in zip(axs.flat, SET):
        vals = {}
        for i, p in enumerate(PL):
            R = {float(r[xcol]): r for r in pick(T, alg, mix, sweep, s, p)}
            vals[p] = [(float(R[v][f"{metric}_mean"]), float(R[v][f"{metric}_std"])) for v in xs_vals]
            ax.bar(x + (i - 0.5) * w, [m for m, _ in vals[p]], w, yerr=[e for _, e in vals[p]], capsize=1,
                   error_kw=dict(lw=0.5), label=lab(alg, p), **STY[p])
            K = R[xs_vals[0]]["K"]
        if admitted:
            for j, v in enumerate(xs_vals):
                a = np.mean([float({float(r[xcol]): r for r in pick(T, alg, mix, sweep, s, p)}[v]["admitted_txn_s_mean"])
                             for p in PL])
                ax.plot([x[j] - 0.45, x[j] + 0.45], [a, a], color="black", lw=0.7, ls=":",
                        label="admitted load" if j == 0 else None)
        if pct:
            for j in range(len(xs_vals)):
                d, q = vals["DPart"][j][0], vals["DPart eps=0 (random)"][j][0]; pc = round((d / q - 1) * 100)
                ax.text(x[j] - w / 2, d * (1.12 if log else 1.02) + (0 if log else q * 0.01),
                        "0%" if pc == 0 else f"{pc:+d}%".replace("-", "\u2212"), ha="center", va="bottom",
                        fontsize=6, rotation=90, color="#555" if pc == 0 else ("#08519c" if pc < 0 else "#b30000"))
        if log: ax.set_yscale("log")
        if ylim: ax.set_ylim(*ylim)
        ax.set_xticks(x); ax.set_xticklabels([f"{v:g}" for v in xs_vals]); ax.set_title(f"{n} ($k$={K})"); style(ax)
    for ax in axs[1]: ax.set_xlabel(xlabel)
    fig.supylabel(ylabel, fontsize=8, x=0.005)
    h, l = axs[0, 0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=len(h), frameon=False, bbox_to_anchor=(0.5, 1.03))
    fig.tight_layout(pad=0.3, rect=(0, 0, 1, 0.965)); save(fig, name)
    return True

def fanout(alg, mix, name):
    fig, ax = plt.subplots(figsize=(3.4, 2.3))
    x = np.arange(len(SET)); w = 0.38
    for i, p in enumerate(PL):
        R = {r["setting"]: r for r in FO if r["algorithm"] == alg and r["query_mix"] == mix and r["placement"] == p}
        m = [float(R[s]["fanout_all_mean"]) for s, _ in SET]; e = [float(R[s]["fanout_all_std"]) for s, _ in SET]
        ax.bar(x + (i - 0.5) * w, m, w, yerr=e, capsize=1, error_kw=dict(lw=0.5), label=lab(alg, p), **STY[p])
        for xi, v in zip(x + (i - 0.5) * w, m):
            ax.text(xi, v + 0.12, f"{v:.1f}", ha="center", va="bottom", fontsize=6.3)
    ax.set_xticks(x); ax.set_xticklabels([f"{SHORT[s]}\n($k$={R[s]['K']})" for s, _ in SET], fontsize=6.3)
    ax.set_ylabel("Shards per transaction"); ax.set_ylim(0, 10); style(ax)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=1, frameon=False, borderaxespad=0.15)
    fig.tight_layout(pad=0.2); save(fig, name)

made = []
for alg in ("Alg3", "Alg4"):
    for mix in ("point-only", "mix (50% range)", "range-only"):
        base = f"rq3_{alg.lower()}_{MIXTAG[mix]}"
        grid(TP, alg, mix, "load", "throughput_txn_s", "Committed throughput (txn/s)", f"{base}_throughput",
             "offered_txn_s", "Offered load (txn/s)", admitted=True)
        grid(OV, alg, mix, "load", "slots_per_committed_txn", "Padded ORAM slots / committed txn",
             f"{base}_overhead_commit", "offered_txn_s", "Offered load (txn/s)", log=True, ylim=(3, 3000), pct=True)
        grid(OV, alg, mix, "load", "slots_per_admitted_txn", "Padded ORAM slots / admitted txn",
             f"{base}_overhead_admit", "offered_txn_s", "Offered load (txn/s)", log=True, ylim=(3, 3000), pct=True)
        fanout(alg, mix, f"{base}_fanout")
        if grid(TP, alg, mix, "skew", "throughput_txn_s", "Committed throughput (txn/s)", f"{base}_throughput_skew",
                "alpha_query", r"Query skew $\alpha_q$ ($\alpha_d$ = 1; 2000 txn/s offered)", admitted=True):
            made.append(f"{base}_throughput_skew")
        made += [f"{base}_{k}" for k in ("throughput", "fanout", "overhead_commit", "overhead_admit")]
print(len(made), "figures:", ", ".join(made))
