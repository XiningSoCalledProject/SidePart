//
// RQ2_PrivateBudget.cpp   (v4, 10/13/26)
// ----------------------------------------------------------------------------
// v4 (10/13/26): mode "synopsis" = synopsis ablation (mechanism evidence).
//   SAME data-aware placement and SAME eps; only the way AGGREGATE queries
//   are answered changes:
//     oram   : the aggregate reads every row of its group through the ORAM
//              read batches (no shortcut)
//     exact  : the TRUSTED proxy answers from its exact per-group histogram,
//              0 ORAM reads (the proxy is trusted and may hold real counts;
//              the answer goes to the client, never to a server)
//     noisy  : the proxy answers from a DP release of that histogram
//              (eps_load = RQ2_AGG_EPS), released at partitioning and at every
//              repartitioning only, reused unchanged in between
//     random : reference — eps = 0 zero-information placement, aggregates via ORAM
//   Aggregate = COUNT(rows of group g with value < t); group = RQ2_AGG_GROUP
//   consecutive rows of one region (see RQ2_AggSynopsis.h).  A share
//   RQ2_AGG_FRAC of the non-insert txns are aggregates; the arrival stream is
//   identical in every arm (same RNG draws), only the answering path differs.
//   Reported: committed / proxy-answered / regular-txn throughput, ORAM real
//   reads per served txn, read-batch occupancy, READ_BATCH_FULL rate, and the
//   mean |error| of the noisy answers.
//   Env:  RQ2_SYN_ARMS="oram,exact,noisy[,random]"   RQ2_SYN_EPS="1"  (eps grid)
//         RQ2_AGG_FRAC=0.1   RQ2_AGG_GROUP=32   RQ2_AGG_EPS=0.5
//   Out:  <prefix>_synopsis.csv (one row per eps x arm), <prefix>_synopsis_epochs.csv
//   Modes quality / perf / both are unchanged (no aggregates, same RNG draws).
// ----------------------------------------------------------------------------
// v3 (10/10/26): default eps grid = {0, 0.5, 1, 1.5, 2}.  eps = 0 is the
// zero-information placement (uniformly random shard per row, no synopsis, no
// dummies, exact index) and now runs IN THE SAME SWEEP as the DP points, for
// both Figure 3 (quality) and Figure 4 (perf); argv[11] = "random" is no longer
// needed (still accepted: it forces eps = 0 only).  New CSV column `placement`
// (random | dpart).  Requires the listeners / ORAM client of 10/8/26
// (Servers_MultiRingORAM with opcode 11, batched RingORAM): rebuild both and
// restart the listeners before running.  No network emulation here: leave
// DEPART_NET_RTT_MS / DEPART_NET_BW_MBPS unset and do not run net_emulate.sh.
// ----------------------------------------------------------------------------
// RQ2: How does the privacy budget influence partition quality and refresh
// behavior, and how do these effects translate into system performance?
//
//   RQ2a / Figure 3 — x: ε,  y: Affinity Cut
//        Accuracy AND stability of the private partition layout.  Every ε is
//        repeated REPEATS times (Laplace noise is redrawn each run) and we
//        report mean / sd / min / max plus layout churn against the noiseless
//        layout, because the relationship need not be monotonic.
//        No ORAM cluster needed.
//
//   RQ2b / Figure 4 — x: ε,  y: transaction throughput (txn/s)
//        ε → partition layout → { fan-out, stash / IOO overhead, refresh
//        behavior } → throughput.
//
// v2 fixes three things the first version got wrong:
//
//   (1) THROUGHPUT IS NOW END-TO-END EPOCH THROUGHPUT.
//       v1 timed only executeOramOps() on the dedup'd op list, i.e.
//       committed txns / raw ORAM execution time.  Figure 4 says "transaction
//       throughput (txn/s)", so this version drives the full Obladi-style
//       epoch — runEpoch_RBR_W(): R fixed read batches of bread, padding,
//       staged transactions, MVTSO, and the write phase — and divides by the
//       end-to-end epoch wall time.
//
//   (2) REFRESH BEHAVIOR IS NOW ACTUALLY EXERCISED.
//       v1's repartition_ms was just "time to partition once at the start of
//       this ε".  This version runs EPOCHS_PER_EPSILON epochs of a workload
//       that contains INSERTs, watches the refresh trigger, and reports
//       refresh count, epochs between refreshes, refresh frequency,
//       refresh-induced pause, and amortized refresh overhead.
//
//   (3) IOO IS MEASURED, NOT JUST STASH.
//       This repo has no separate insert-only ORAM; new rows take a block
//       from the partition's PUBLIC INSERT RESERVE (a fixed, data-independent
//       number of filler blocks — see setInsertReservePerPartition and the
//       pos_map).  That reserve IS the IOO here, so we report its occupancy,
//       the accesses that land in it, when it activates, and the refresh it
//       triggers.  Rename these columns if you implement a separate IOO.
//
// Refresh trigger (configurable): refresh when the fullest shard's insert
// reserve is ≥ IOO_REFRESH_THRESHOLD occupied.  Refresh = re-run
// Repartition_Main over (original ∪ inserted) rows and rebuild the shards, so
// the inserted rows are folded back into the partitions and the reserve is
// empty again.  That is the "IOO → R&U refresh" path.
//
// PREREQUISITE for Figure 4: k listeners must already be running:
//       for p in $(seq 8881 8884); do ./build/bin/Servers_MultiRingORAM $p & done
//
// Usage:
//   ./RQ2_PrivacyBudget [k] [rows] [regions] [repeats] [mode] [base_port] [csv_prefix]
//                       [offered_txn_s]
//     mode = quality | perf | both | synopsis   (default both; synopsis: see v4)
//     offered_txn_s  open-loop arrival RATE in txn/s summed over all clients,
//                    evenly paced in wall-clock time (same as RQ3).  A rejected
//                    push (intake closed between epochs) is dropped, not
//                    retried.  0 = legacy BURST: every client pushes
//                    TXNS_PER_CLIENT txns at once each epoch, so nearly all of
//                    them land in read sub-epoch 1 (default 0)
//     insert_reserve IOO blocks per shard (default 1024).  In paced mode pick
//                    it so refreshes still happen within the run, e.g. ~64
//                    at 300 txn/s (see the note printed at start-up).
//     cap_T          (argv[10], 9/27/26) 0 = Algorithm 3: exactly k partitions
//                    (DEFAULT, all earlier runs).  > 0 = Algorithm 4: fixed
//                    public threshold cap_T; a partition closes once its
//                    packing size (sum of max(0, noisy bin count)) reaches
//                    cap_T, so the number of partitions K depends on the data
//                    AND on eps (small eps inflates the clamped packing sizes,
//                    so K grows).  k is then the number of listeners available;
//                    a layout with K > k is reported as FAILED for that eps.
//     placement      (argv[11], 10/3/26) "dpart" (default) or "random".
//                    "random" = DPart with eps = 0: zero-information placement.
//                    Every row goes to a uniformly random shard (no DP, no
//                    synopsis, no dummies) and is located through an exact
//                    index; inserts also go to a uniformly random shard.  The
//                    SAME growth / IOO / repartitioning pipeline runs: when the
//                    insert reserve fills, the cluster is rebuilt over the grown
//                    data with a fresh random placement of the new rows (existing
//                    rows keep their shard, same seed).  Only Figure 4 (perf) is
//                    meaningful; the eps grid is replaced by the single point 0.
//   Env RQ2_EPSILONS="0,0.5,1"  overrides the eps grid (e.g. one eps per
//                    run, so listeners can be restarted between points).
//                    eps = 0 (or < 0) = zero-information random placement.
//   → <prefix>_quality.csv (Figure 3), <prefix>_perf.csv (Figure 4),
//     <prefix>_epochs.csv  (per-epoch trace: throughput, IOO, refresh events)
// ----------------------------------------------------------------------------

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <stdexcept>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"
#include "Repartition.h"
#include "RQ2_Affinity.h"
#include "RQ2_AggSynopsis.h"
#include "RQ3_Workload.h"
#include "RQ_Preflight.h"

using clk = std::chrono::high_resolution_clock;
static double ms_between(clk::time_point a, clk::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ── Sweep / workload configuration ──────────────────────────────────────────
// The interesting range is where the Laplace noise (scale 1/eps) is COMPARABLE
// to the per-bin row count.  With 100 rows/bin the curve moves between eps 0.01
// and 0.2 and is flat above that, so the grid starts well below 0.1.
// v3: 0 = zero-information placement (random), then the DP budgets.
static std::vector<double> EPSILONS = {0.0, 0.5, 1.0, 1.5, 2.0};
static constexpr double   NOISELESS_EPS      = 1e6;  // reference layout for churn
static constexpr double   SKEW_ALPHA         = 1.0;
static constexpr uint32_t WORKLOAD_TXNS      = 3000; // txns behind the affinity graph
static constexpr uint32_t OPS_PER_TXN        = 3;
// Fraction of a transaction's keys drawn from its ANCHOR region; the rest come
// from a neighbouring region.  This must be < 1.0 or Figure 3 is a flat zero
// line: the partitioner assigns whole bins, so a transaction confined to one
// bin can NEVER have an affinity edge cut, whatever eps does to the boundaries.
// 0.6 = strong but imperfect locality, like a TPC-C order touching stock rows
// that mostly, not always, live in its own warehouse.
static constexpr double   WORKLOAD_LOCALITY   = 0.6;
static constexpr int      NEIGHBOUR_SPAN      = 2;   // +/- this many regions
// Figure 4 must measure the SYSTEM, not the client loop.  With 4 clients x 25
// txns and a 3 ms pause between them, every epoch committed exactly 100 txns
// with 0 aborts in ~1.35 s, so throughput was pinned at ~75 txn/s for every
// eps — the number described how fast the harness could submit, not how fast
// the shards could serve.  Submit enough to keep the read batches full.
static constexpr int      NUM_CLIENTS        = 8;
static constexpr int      TXNS_PER_CLIENT    = 200;  // per epoch
static constexpr uint32_t EPOCHS_PER_EPSILON = 8;
static constexpr double   INSERT_FRACTION    = 0.15; // txns that insert a new row
// IOO capacity per shard.  Must comfortably exceed one epoch's inserts
// (NUM_CLIENTS * TXNS_PER_CLIENT * INSERT_FRACTION), or a shard runs dry in the
// middle of an epoch — see the headroom trigger below.
// Overridable from argv[9]: with paced arrivals an epoch admits far fewer
// inserts than the 240 of burst mode, so a 1024-block reserve would never
// reach the refresh threshold within EPOCHS_PER_EPSILON epochs.
static uint32_t INSERT_RESERVE = 1024;
static constexpr double   IOO_REFRESH_THRESHOLD = 0.60;
static constexpr uint32_t BUCKET_SIZE        = 8;
static constexpr uint32_t S_PARAM            = 4;

// Set once from argv in main().  0 = burst (legacy), > 0 = paced open loop.
static double g_offered_txn_s = 0.0;
// Set once from argv[10].  0 = Algorithm 3 (k partitions), > 0 = Algorithm 4.
static double g_cap_T = 0.0;
// Set once from argv[11].  true = eps = 0 random placement (see Usage).
static bool g_random = false;
static constexpr uint64_t RANDOM_PLACEMENT_SEED = 42;
// eps <= 0 means zero-information placement (no private information is used).
static bool isRandom(double eps) { return g_random || eps <= 0.0; }

// ── v4: synopsis ablation (mode "synopsis") ─────────────────────────────────
// NONE = the original Figure 4 workload (no aggregates, identical RNG draws).
enum class AggArm { NONE, ORAM, PROXY_EXACT, PROXY_NOISY };
static const char* armName(AggArm a) {
    switch (a) {
        case AggArm::ORAM:        return "oram";
        case AggArm::PROXY_EXACT: return "exact";
        case AggArm::PROXY_NOISY: return "noisy";
        default:                  return "none";
    }
}
static double   g_agg_frac  = 0.1;    // share of non-insert txns that are aggregates
static uint32_t g_agg_group = 32;     // rows per aggregate group
static double   g_agg_eps   = 0.5;    // eps_load of each noisy release
static constexpr uint64_t AGG_TXN_TAG = 0xA66u;   // client_txn_id of an aggregate txn

// eps = 0 placement: each row is its own bin and goes to a uniformly random
// shard; exact counts, no dummies, no noise stash reserve.  Same as
// rowRandomPlacement() in RQ3_DistShift.cpp.  A fixed seed means a row keeps
// its shard across repartitionings; rows inserted since the last build get new
// random shards.  `out_bins` must outlive the cluster (servers keep a reference).
static std::vector<Partition> rowRandomPlacement(uint32_t num_rows, uint32_t k,
                                                 uint64_t seed, BinInfo& out_bins) {
    std::mt19937_64 place_rng(seed ^ 0x9E3779B97F4A7C15ULL);
    std::vector<Partition> parts(k);
    for (uint32_t r = 0; r < num_rows; ++r) {
        const std::string b = std::to_string(r);
        out_bins.bin_key_to_data_indices[b] = {r};
        out_bins.synopsis[b]        = 1;
        out_bins.noised_synopsis[b] = 1;
        parts[place_rng() % k].index.push_back(b);
    }
    for (auto& p : parts) {
        p.synopsis = p.noisy_synopsis =
            static_cast<uint32_t>(std::max<size_t>(1, p.index.size()));
        p.dummy_num            = 0;
        p.noise_stash_reserve  = 0;
        p.stash_overflow_count = 0;
    }
    return parts;
}

// Every DePart layout in this file goes through here, so Algorithm 3 and
// Algorithm 4 differ in exactly one place.
static std::vector<Partition> partitionRows(Repartition& rp, const std::vector<std::string>& rows,
                                            const std::string& schema, const std::vector<int>& attr,
                                            double eps, uint32_t k) {
    if (g_cap_T > 0.0)
        return rp.Repartition_Main(rows, schema, attr, eps, 0.0, g_cap_T, /*fixed_number_part=*/false, 0, 0, 0);
    return rp.Repartition_Main(rows, schema, attr, eps, 0.0, 0.0, /*fixed_number_part=*/true, k, 0, k);
}

static EpochConfig epochConfig() {
    EpochConfig c;
    c.R            = 3;
    c.bread        = 256;
    c.bwrite       = 512;
    c.sub_epoch_ms = 60;
    c.pad_batches  = true;     // security mode: fixed-size batches
    // Same padding semantics as RQ3 (the paper's threat model): only shards
    // with real ops in a round get a batch, padded to bread / bwrite; the
    // contacted set C is visible, per-shard volume inside it is hidden.
    c.pad_uncontacted_shards = false;
    // Obladi admission, stated explicitly rather than relying on defaults:
    // one bread-sized batch per shard per read round; overflow aborts.
    c.per_server_capacity    = true;
    c.enforce_batch_capacity = true;
    return c;
}

// ── Workload ────────────────────────────────────────────────────────────────
struct Workload {
    std::vector<std::vector<uint32_t>> txns;
    rq2::AffinityGraph graph;
};

static Workload buildWorkload(const rq3::Dataset& d, uint32_t num_txns,
                              uint32_t ops_per_txn, double alpha, uint64_t seed) {
    Workload w;
    rq3::QueryKeyGen pick(d, rq3::QueryDist::SKEWED_MATCHED, alpha, seed);
    std::mt19937_64 rng(seed ^ 0x9E3779B9u);
    for (uint32_t t = 0; t < num_txns; ++t) {
        const uint32_t anchor    = pick.nextKey();
        const uint32_t anchor_r  = d.region_of_row[anchor];
        std::vector<uint32_t> keys{anchor};
        for (uint32_t q = 1; q < ops_per_txn; ++q) {
            uint32_t r = anchor_r;
            if ((rng() % 1000) >= static_cast<uint64_t>(WORKLOAD_LOCALITY * 1000)) {
                const int delta = static_cast<int>(rng() % (2 * NEIGHBOUR_SPAN + 1)) - NEIGHBOUR_SPAN;
                r = static_cast<uint32_t>(std::min<int>(std::max<int>(0, int(anchor_r) + delta),
                                                       int(d.num_regions) - 1));
            }
            const auto& rows = d.rows_of_region[r];
            if (!rows.empty()) keys.push_back(rows[rng() % rows.size()]);
        }
        if (keys.size() < 2) continue;      // a 1-key txn contributes no edge
        w.graph.addTransaction(keys);
        w.txns.push_back(std::move(keys));
    }
    return w;
}

static std::vector<int> shardOfRows(size_t num_rows, const BinInfo& bin_info,
                                    const std::vector<Partition>& parts) {
    std::vector<int> shard(num_rows, -1);
    for (size_t p = 0; p < parts.size(); ++p)
        for (const auto& bin_key : parts[p].index) {
            auto it = bin_info.bin_key_to_data_indices.find(bin_key);
            if (it == bin_info.bin_key_to_data_indices.end()) continue;
            for (uint32_t row : it->second)
                if (row < shard.size()) shard[row] = static_cast<int>(p);
        }
    return shard;
}

// ══════════════════════════════════════════════════════════════════════════
// Figure 3 — partition quality vs ε (no cluster)
// ══════════════════════════════════════════════════════════════════════════
struct QualityRow {
    double epsilon = 0;
    rq2::Stats cut, fanout, size_cv, churn, noisy_ratio;
    double   mean_partitions = 0;
    uint32_t min_partitions = 0, max_partitions = 0;
    uint64_t total_dummy = 0, total_stash_reserve = 0;
};

static std::vector<QualityRow> runQualitySweep(const rq3::Dataset& d, const Workload& wl,
                                               uint32_t k, uint32_t repeats) {
    const std::string schema = d.schema();
    const std::vector<int> attr = {1};

    Repartition ref_rp;
    auto ref_parts = partitionRows(ref_rp, d.rows, schema, attr, NOISELESS_EPS, k);
    const std::vector<int> ref_shard = shardOfRows(d.rows.size(), ref_rp.getBinInfo(), ref_parts);
    std::cout << "  reference (no noise): affinity_cut=" << std::fixed << std::setprecision(4)
              << wl.graph.cut([&](uint32_t key) { return ref_shard[key]; })
              << "  partitions=" << ref_parts.size() << "\n";

    std::vector<QualityRow> rows;
    for (double eps : EPSILONS) {
        std::vector<double> cuts, fans, cvs, churns, noisy_ratios;
        QualityRow r; r.epsilon = eps;
        double parts_sum = 0;
        for (uint32_t rep = 0; rep < repeats; ++rep) {
            Repartition rp;   // seeds from random_device → different noise each run
            BinInfo rbins;    // eps = 0: random placement, a fresh layout per repeat
            auto parts = isRandom(eps)
                ? rowRandomPlacement(static_cast<uint32_t>(d.rows.size()), k, RANDOM_PLACEMENT_SEED + rep, rbins)
                : partitionRows(rp, d.rows, schema, attr, eps, k);
            const std::vector<int> shard =
                shardOfRows(d.rows.size(), isRandom(eps) ? rbins : rp.getBinInfo(), parts);
            cuts.push_back(wl.graph.cut([&](uint32_t key) { return shard[key]; }));
            fans.push_back(rq2::meanFanout(wl.txns, [&](uint32_t key) { return shard[key]; }));
            churns.push_back(rq2::layoutChurn(ref_shard, shard));
            std::vector<uint32_t> sizes;
            uint64_t real = 0, noisy = 0;
            for (const auto& p : parts) {
                sizes.push_back(p.synopsis);
                real += p.synopsis; noisy += p.noisy_synopsis;
                r.total_dummy += p.dummy_num; r.total_stash_reserve += p.noise_stash_reserve;
            }
            cvs.push_back(rq2::sizeCV(sizes));
            noisy_ratios.push_back(real ? static_cast<double>(noisy) / real : 0.0);
            parts_sum += parts.size();
            const uint32_t np = static_cast<uint32_t>(parts.size());
            r.min_partitions = rep == 0 ? np : std::min(r.min_partitions, np);
            r.max_partitions = std::max(r.max_partitions, np);
        }
        r.cut = rq2::summarize(cuts);   r.fanout      = rq2::summarize(fans);
        r.size_cv = rq2::summarize(cvs); r.churn      = rq2::summarize(churns);
        r.noisy_ratio = rq2::summarize(noisy_ratios);
        r.mean_partitions = parts_sum / repeats;
        rows.push_back(r);
        std::cout << "  eps=" << std::setw(6) << eps << (isRandom(eps) ? " (random)" : "")
                  << "  cut=" << std::setprecision(4) << r.cut.mean << " ±" << r.cut.sd
                  << " [" << r.cut.min << ", " << r.cut.max << "]"
                  << "  fanout=" << std::setprecision(2) << r.fanout.mean
                  << "  churn=" << std::setprecision(3) << r.churn.mean
                  << "  noisy/real=" << std::setprecision(2) << r.noisy_ratio.mean
                  << "  partitions=" << std::setprecision(1) << r.mean_partitions
                  << " [" << r.min_partitions << ", " << r.max_partitions << "]\n";
    }
    return rows;
}

// ══════════════════════════════════════════════════════════════════════════
// Figure 4 — end-to-end epoch throughput, refresh behavior, IOO overhead
// ══════════════════════════════════════════════════════════════════════════

// A brought-up cluster.  Rebuilding one IS a refresh.
struct Cluster {
    std::unique_ptr<Repartition>   rp;
    std::unique_ptr<BinInfo>       rbins;          // eps = 0 random placement only
                                                   // (declared before mrs: outlives it)
    std::vector<Partition>         parts;
    std::vector<ServerInfo>        servers;
    std::unique_ptr<MultiRingORAM_Servers> mrs;
    std::vector<int>               shard_of_row;   // by primary key
    // Free blocks each shard STARTED with.  This is not INSERT_RESERVE: the
    // pos_map's free list also holds the partition's DP dummy fillers
    // (dummy_num), so a shard can begin with e.g. 2145 free blocks while the
    // reserve knob says 256.  Measuring occupancy against the constant made
    // used = 256 - 2145 < 0 -> clamped to 0, so the IOO looked permanently
    // empty and the refresh trigger never fired.
    std::map<int, size_t>          ioo_baseline;
    uint64_t                       ioo_capacity = 0;   // Σ of those baselines
    double                         build_ms = 0;

    uint64_t iooUsed() const {
        uint64_t used = 0;
        for (const auto& s : servers) {
            const auto it = ioo_baseline.find(s.server_id);
            if (it == ioo_baseline.end()) continue;
            const size_t free_now = mrs->posMap().freeBlocks(s.server_id);
            used += (free_now < it->second) ? (it->second - free_now) : 0;
        }
        return used;
    }
    // Free blocks left on the TIGHTEST shard.  The refresh has to happen while
    // this still covers a whole epoch's inserts: if a shard runs dry mid-epoch,
    // allocate() fails, the dispatch fails, and EVERY txn of that epoch aborts
    // — reads included.  Worse, the failed allocate is rolled back by
    // release(), so the free blocks come back and occupancy never crosses the
    // threshold: the run then aborts every epoch forever at a frozen
    // occupancy (observed: stuck at 0.746 with committed=0 for six epochs).
    size_t iooMinFree() const {
        size_t worst = SIZE_MAX;
        for (const auto& s : servers)
            worst = std::min(worst, mrs->posMap().freeBlocks(s.server_id));
        return worst == SIZE_MAX ? 0 : worst;
    }

    // Occupancy of the FULLEST shard — that is what runs out first, and a
    // shard that started with few free blocks fills long before the others.
    double iooMaxOccupancy() const {
        double worst = 0.0;
        for (const auto& s : servers) {
            const auto it = ioo_baseline.find(s.server_id);
            if (it == ioo_baseline.end() || it->second == 0) continue;
            const size_t free_now = mrs->posMap().freeBlocks(s.server_id);
            const double used = (free_now < it->second) ? double(it->second - free_now) : 0.0;
            worst = std::max(worst, used / double(it->second));
        }
        return worst;
    }
};

// Bring up (or refresh) the cluster over `rows`.

// Bulk init streams the whole dummy tree in 10 MB chunks, each chunk ending in
// a barrier byte the listener must ACK.  If that handshake wedges, the child
// blocks in recv(&ack,1) and the run hangs after "[init] forking ... waiting".
// Set DEPART_BULK_INIT=0 to fall back to the per-record path: far slower
// (one access(INSERT) per row, tens of RTTs each) but it uses only the
// insert_slot / find_slot opcodes, so it isolates whether the barrier
// handshake is the problem.
static bool useBulkInit() {
    const char* e = std::getenv("DEPART_BULK_INIT");
    return !(e && std::string(e) == "0");
}

static Cluster bringUp(const std::vector<std::string>& rows, const std::string& schema,
                       double eps, uint32_t k, int base_port) {
    Cluster c;
    auto t0 = clk::now();
    const std::vector<int> attr = {1};
    c.rp = std::make_unique<Repartition>();
    if (isRandom(eps)) {
        c.rbins = std::make_unique<BinInfo>();
        c.parts = rowRandomPlacement(static_cast<uint32_t>(rows.size()), k, RANDOM_PLACEMENT_SEED, *c.rbins);
    } else {
        c.parts = partitionRows(*c.rp, rows, schema, attr, eps, k);
    }
    if (c.parts.size() > k)
        throw std::runtime_error("layout has " + std::to_string(c.parts.size()) +
                                 " partitions but only k=" + std::to_string(k) +
                                 " listeners are available (Algorithm 4 at this eps); start more");
    std::cerr << "    [layout] " << c.parts.size() << " partitions"
              << (isRandom(eps) ? " (eps = 0 random placement)" : "") << std::endl;
    const BinInfo& bi = isRandom(eps) ? *c.rbins : c.rp->getBinInfo();

    c.servers.resize(c.parts.size());
    for (size_t i = 0; i < c.parts.size(); ++i) {
        c.servers[i].server_id     = static_cast<int>(i);
        c.servers[i].port          = base_port + static_cast<int>(i);
        c.servers[i].partition_id  = static_cast<uint32_t>(i);
        c.servers[i].assigned_bins = c.parts[i].index;
    }

    c.mrs = std::make_unique<MultiRingORAM_Servers>();
    c.mrs->setInsertReservePerPartition(INSERT_RESERVE);   // the IOO, public + fixed
    c.mrs->distributeDataToPartitions(bi, c.parts, c.servers, rows);

    const uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    const uint32_t block_size  = tuple_width + 16 + 2 * sizeof(uint32_t);  // + AES::BLOCKSIZE
    // BULK init (not ServerInitialization): the per-record path calls
    // oram->access(block, INSERT, value) for EVERY row, and each of those is a
    // ReadPath + eviction, i.e. tens to hundreds of network round-trips.  At
    // 6000 rows per shard that is millions of RTTs and the run looks hung.
    // ServerInitializationBulk loads the records straight into the stash
    // (O(N), local) and streams the whole dummy tree up in one pass.
    for (size_t i = 0; i < c.parts.size(); ++i)
        if (useBulkInit())
            c.mrs->ServerInitializationBulk(c.servers[i], c.parts[i], bi, rows, schema, BUCKET_SIZE,
                                        "RQ2_S" + std::to_string(i), block_size, "127.0.0.1", S_PARAM);
        else
            c.mrs->ServerInitialization(c.servers[i], c.parts[i], bi, rows, schema, BUCKET_SIZE,
                                        "RQ2_S" + std::to_string(i), block_size, "127.0.0.1", S_PARAM);
    std::cerr << "    [init] forking " << c.parts.size() << " shards, waiting..." << std::endl;
    c.mrs->waitForAllServersReady();
    std::cerr << "    [init] all shards ready" << std::endl;

    c.shard_of_row.assign(rows.size(), -1);
    for (uint32_t pk = 0; pk < rows.size(); ++pk)
        c.shard_of_row[pk] = c.mrs->serverOfKey(ObjectKey{0, 0, 0, pk});

    for (const auto& s : c.servers) {
        const size_t free_now = c.mrs->posMap().freeBlocks(s.server_id);
        c.ioo_baseline[s.server_id] = free_now;
        c.ioo_capacity += free_now;
    }
    {   // The refresh fires on the FULLEST shard, so the smallest baseline is
        // what matters.  Printing it makes a "refreshes=0" run explainable.
        size_t smallest = SIZE_MAX;
        std::cerr << "    [init] IOO free blocks per shard:";
        for (const auto& s : c.servers) {
            const size_t b = c.ioo_baseline[s.server_id];
            std::cerr << " s" << s.server_id << "=" << b;
            smallest = std::min(smallest, b);
        }
        std::cerr << "  -> refresh at " << IOO_REFRESH_THRESHOLD
                  << " of the fullest; the tightest shard needs "
                  << static_cast<uint64_t>(IOO_REFRESH_THRESHOLD * smallest)
                  << " inserts" << std::endl;
    }
    c.build_ms = ms_between(t0, clk::now());
    return c;
}

struct EpochRow {
    double   epsilon = 0;
    uint32_t epoch = 0;
    uint32_t txns = 0, committed = 0, aborted = 0;
    uint64_t inserts = 0, ioo_accesses = 0, ioo_used = 0, ioo_capacity = 0;
    double   ioo_max_occupancy = 0;
    double   epoch_ms = 0, throughput_txn_s = 0;
    bool     refreshed = false;
    double   refresh_ms = 0;
    std::string refresh_reason;    // occupancy | headroom | wedged
    uint64_t pushes_outside = 0;   // paced mode: arrivals rejected (intake closed)
    uint32_t partitions = 0;       // shards in use during this epoch
    // v4 synopsis ablation (all 0 when arm == NONE)
    uint64_t proxy_answered = 0;   // aggregates answered at the proxy (0 ORAM reads)
    uint64_t agg_committed  = 0;   // aggregates committed through ORAM
    uint64_t agg_offered    = 0;   // aggregates offered (both paths)
    uint64_t read_real = 0, read_capacity = 0;   // real read slots / R x bread x contacted-or-all shards
    uint64_t read_batch_full = 0;  // aborts with READ_BATCH_FULL
    uint64_t noisy_answers = 0;  double noisy_abs_err = 0;
};

struct PerfRow {
    double   epsilon = 0;
    uint32_t partitions = 0, partitions_max = 0;   // first layout / max over refreshes
    double   affinity_cut = 0, fanout = 0;
    uint32_t epochs = 0;
    uint64_t committed = 0, aborted = 0, inserts = 0, ioo_accesses = 0;
    uint32_t real_total = 0, noisy_total = 0;
    uint64_t dummy_total = 0, stash_reserve_total = 0, ioo_capacity = 0;
    // refresh behaviour
    uint32_t refreshes = 0;
    double   epochs_between_refreshes = 0, refresh_frequency = 0;
    double   mean_refresh_pause_ms = 0, total_refresh_ms = 0, amortized_refresh_pct = 0;
    // throughput: end-to-end epoch time, excluding and including refresh pauses
    double   epoch_ms_total = 0;
    double   throughput_txn_s = 0, throughput_txn_s_with_refresh = 0;
    // v4 synopsis ablation
    AggArm   arm = AggArm::NONE;
    uint64_t proxy_answered = 0, agg_committed = 0, agg_offered = 0;
    uint64_t read_real = 0, read_capacity = 0, read_batch_full = 0, txns_admitted = 0;
    uint64_t noisy_answers = 0, agg_releases = 0;
    double   noisy_abs_err = 0;
};

// One ε: bring up, run EPOCHS_PER_EPSILON epochs, refresh when the IOO fills.
static PerfRow runPerfPoint(const rq3::Dataset& d, const Workload& wl, double eps,
                            uint32_t k, int base_port, uint64_t seed,
                            std::vector<EpochRow>& trace, AggArm arm = AggArm::NONE) {
    PerfRow R; R.epsilon = eps; R.arm = arm;
    uint32_t partitions_max = 0;
    const std::string schema = d.schema();

    std::vector<std::string> rows = d.rows;        // grows with inserts; the
                                                   // refresh repartitions over this
    Cluster cl = bringUp(rows, schema, eps, k, base_port);

    R.partitions  = static_cast<uint32_t>(cl.parts.size());
    R.ioo_capacity = cl.ioo_capacity;
    for (const auto& p : cl.parts) {
        R.real_total += p.synopsis; R.noisy_total += p.noisy_synopsis;
        R.dummy_total += p.dummy_num; R.stash_reserve_total += p.noise_stash_reserve;
    }
    R.affinity_cut = wl.graph.cut([&](uint32_t key) {
        return key < cl.shard_of_row.size() ? cl.shard_of_row[key] : -1; });
    R.fanout = rq2::meanFanout(wl.txns, [&](uint32_t key) {
        return key < cl.shard_of_row.size() ? cl.shard_of_row[key] : -1; });

    std::set<uint32_t> inserted_keys;             // rows that live in the IOO
    const uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    std::mt19937_64 rng(seed);

    // v4: aggregate groups + the proxy's synopsis over the ORIGINAL rows
    // (inserts never join a group, so the exact histogram stays exact).
    rq2::AggGroups agg_groups;
    std::unique_ptr<rq2::AggSynopsis> agg_syn;
    if (arm != AggArm::NONE) {
        agg_groups = rq2::buildAggGroups(d.rows_of_region, d.rows.size(), g_agg_group);
        std::vector<int64_t> values(d.rows.size());
        for (size_t i = 0; i < d.rows.size(); ++i) values[i] = rq2::aggValueOfRow(d.rows[i]);
        agg_syn = std::make_unique<rq2::AggSynopsis>(agg_groups, values);
        if (arm == AggArm::PROXY_NOISY) agg_syn->release(g_agg_eps, seed * 1000003ULL);   // at partitioning
        std::cerr << "    [synopsis] arm=" << armName(arm) << "  groups=" << agg_syn->numGroups()
                  << "  agg_frac=" << g_agg_frac << "  group_size=" << g_agg_group
                  << (arm == AggArm::PROXY_NOISY ? "  eps_load=" + std::to_string(g_agg_eps) : std::string())
                  << std::endl;
    }

    for (uint32_t e = 0; e < EPOCHS_PER_EPSILON; ++e) {
        EpochRow ER; ER.epsilon = eps; ER.epoch = e;
        ER.partitions = static_cast<uint32_t>(cl.parts.size());
        partitions_max = std::max(partitions_max, ER.partitions);

        QueriesReceiving qr;
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> pushed{0}, inserts{0}, ioo_hits{0};
        std::vector<std::thread> clients;
        std::mutex new_rows_mu;
        std::vector<std::pair<uint32_t, std::string>> new_rows;   // pk, csv line

        const uint32_t next_pk_base = static_cast<uint32_t>(rows.size());
        std::atomic<uint32_t> next_pk{next_pk_base};

        // Rejected paced pushes hand their pk back so the pk space stays
        // dense (pk == row index; a gap would leave an empty row that the
        // refresh would then repartition).
        std::mutex pk_mu;
        std::vector<uint32_t> free_pks;
        auto takePk = [&]() -> uint32_t {
            std::lock_guard<std::mutex> lk(pk_mu);
            if (!free_pks.empty()) { uint32_t pk = free_pks.back(); free_pks.pop_back(); return pk; }
            return next_pk++;
        };
        auto givePkBack = [&](uint32_t pk) {
            std::lock_guard<std::mutex> lk(pk_mu); free_pks.push_back(pk);
        };
        std::atomic<uint64_t> outside{0};
        std::atomic<uint64_t> proxy_answered{0}, agg_offered{0};
        std::mutex err_mu; double noisy_err = 0; uint64_t noisy_n = 0;

        // Build one txn.  Pure: records nothing.  For an INSERT it returns the
        // new row in *ins so the caller records it only once the push is
        // accepted.
        auto buildTxn = [&](std::mt19937_64& r, bool* is_ins,
                            std::pair<uint32_t, std::string>* ins, uint64_t* hits,
                            bool* at_proxy) {
            txn t;
            *is_ins = (r() % 1000) < static_cast<uint64_t>(INSERT_FRACTION * 1000);
            *hits = 0;
            *at_proxy = false;
            // v4: an aggregate (only when an ablation arm is active, so the
            // NONE workload draws exactly the same random numbers as before)
            if (!*is_ins && arm != AggArm::NONE &&
                (r() % 1000) < static_cast<uint64_t>(g_agg_frac * 1000)) {
                const auto& anchor_txn = wl.txns[r() % wl.txns.size()];   // same skew as the reads
                const int64_t thr = rq2::AggSynopsis::threshold(r());
                const int32_t g = anchor_txn.empty() || anchor_txn[0] >= agg_groups.group_of_row.size()
                                  ? -1 : agg_groups.group_of_row[anchor_txn[0]];
                if (g >= 0) {
                    ++agg_offered;
                    if (arm == AggArm::ORAM) {          // no shortcut: fetch every row of g
                        const auto& mem = agg_groups.members[g];
                        for (size_t q = 0; q < mem.size(); ++q) {
                            Operat op;
                            op.type             = OpType::READ;
                            op.data_primary_key = mem[q];
                            op.server_id        = cl.shard_of_row[mem[q]];
                            op.last_one         = (q + 1 == mem.size());
                            t.operations.push_back(op);
                        }
                        t.client_txn_id = AGG_TXN_TAG;
                        return t;
                    }
                    // answered at the trusted proxy: never enters an ORAM batch
                    *at_proxy = true;
                    if (arm == AggArm::PROXY_NOISY) {
                        const double err = std::fabs(agg_syn->answerNoisy(g, thr) -
                                                     double(agg_syn->answerExact(g, thr)));
                        std::lock_guard<std::mutex> lk(err_mu);
                        noisy_err += err; ++noisy_n;
                    } else {
                        (void)agg_syn->answerExact(g, thr);
                    }
                    return t;
                }
            }
            if (*is_ins) {
                // New row: takes a block from that shard's insert reserve (IOO).
                const uint32_t pk     = takePk();
                const uint32_t region = static_cast<uint32_t>(r() % d.num_regions);
                std::string line = std::to_string(pk) + "," + std::to_string(region) +
                                   "," + std::to_string(r() % 10000) + "," +
                                   std::to_string(r() % 100000);
                int shard = -1;
                if (isRandom(eps)) {                // eps = 0: uniformly random shard
                    shard = static_cast<int>(r() % cl.parts.size());
                } else {                            // place with its region's rows
                    for (uint32_t probe = 0; probe < d.rows_of_region[region].size() && shard < 0; ++probe)
                        shard = cl.shard_of_row[d.rows_of_region[region][probe]];
                    if (shard < 0) shard = static_cast<int>(pk % cl.parts.size());
                }
                Operat op;
                op.type             = OpType::INSERT;
                op.data_primary_key = pk;
                op.server_id        = shard;
                op.data_value       = line.substr(0, tuple_width);
                op.last_one         = true;
                t.operations.push_back(op);
                *ins = {pk, std::move(line)};
            } else {
                const auto& keys = wl.txns[r() % wl.txns.size()];
                for (size_t q = 0; q < keys.size(); ++q) {
                    Operat op;
                    op.type             = OpType::READ;
                    op.data_primary_key = keys[q];
                    op.server_id        = cl.shard_of_row[keys[q]];
                    op.last_one         = (q + 1 == keys.size());
                    t.operations.push_back(op);
                    if (inserted_keys.count(keys[q])) ++*hits;   // access inside the IOO
                }
            }
            return t;
        };
        // Record a txn that the intake ACCEPTED.
        auto accepted = [&](bool is_ins, std::pair<uint32_t, std::string>& ins, uint64_t hits) {
            if (is_ins) {
                std::lock_guard<std::mutex> lk(new_rows_mu);
                new_rows.push_back(std::move(ins));
                ++inserts;
            }
            ioo_hits += hits;
            ++pushed;
        };

        for (int c = 0; c < NUM_CLIENTS; ++c) {
            clients.emplace_back([&, c]() {
                std::mt19937_64 r(seed + 100 * e + c);
                bool is_ins; std::pair<uint32_t, std::string> ins; uint64_t hits; bool at_proxy;

                if (g_offered_txn_s <= 0.0) {
                    // Legacy BURST: TXNS_PER_CLIENT txns pushed at once
                    // (reproduces the earlier CSVs).
                    for (int i = 0; i < TXNS_PER_CLIENT && !stop; ++i) {
                        txn t = buildTxn(r, &is_ins, &ins, &hits, &at_proxy);
                        if (at_proxy) { ++proxy_answered; continue; }   // served, no ORAM
                        bool ok = false;
                        for (int a = 0; a < 2000 && !(ok = qr.push_txn(t)); ++a) {
                            if (stop) break;
                            std::this_thread::sleep_for(std::chrono::microseconds(100));
                        }
                        // Burst mode kept the old accounting: every attempt
                        // counts, and an INSERT is recorded even if its push
                        // failed (serverOfKey() filters it out below).
                        accepted(is_ins, ins, hits);
                        (void)ok;
                    }
                    return;
                }

                // PACED open loop (same as RQ3): one txn every interval per
                // client, so all clients together offer g_offered_txn_s txn/s.
                // Pacing is in wall-clock time; intake stays open while a read
                // batch executes, so a round's arrivals = rate x round period.
                const auto interval = std::chrono::microseconds(static_cast<int64_t>(
                    1e6 * NUM_CLIENTS / g_offered_txn_s));
                auto next = clk::now() + (interval * c) / NUM_CLIENTS;   // stagger
                while (!stop) {
                    std::this_thread::sleep_until(next);
                    next += interval;
                    if (stop) break;
                    txn t = buildTxn(r, &is_ins, &ins, &hits, &at_proxy);
                    if (at_proxy) { ++proxy_answered; continue; }       // served, no ORAM
                    if (qr.push_txn(t)) {
                        accepted(is_ins, ins, hits);
                    } else {                       // outside this epoch's window
                        ++outside;
                        if (is_ins) givePkBack(ins.first);
                    }
                }
            });
        }

        // ── the measured quantity: a full Obladi-style epoch ────────────
        auto t_epoch0 = clk::now();
        std::vector<AssembledTxnResult> results;
        static const TxnEngine no_staged_types;     // RQ2 txns are single-stage
        EpochStats es;                              // observation only (same epoch)
        try {
            results = runEpoch_RBR_W(qr, *cl.mrs, epochConfig(), no_staged_types, &es);
        } catch (...) { stop = true; for (auto& c : clients) c.join(); throw; }
        ER.epoch_ms = ms_between(t_epoch0, clk::now());
        stop = true;
        for (auto& c : clients) c.join();

        ER.txns = static_cast<uint32_t>(results.size());
        for (const auto& r : results) {
            (r.committed ? ER.committed : ER.aborted)++;
            if (r.committed && r.client_txn_id == AGG_TXN_TAG) ++ER.agg_committed;
            if (!r.committed && r.abort_reason == AbortReason::READ_BATCH_FULL) ++ER.read_batch_full;
        }
        ER.proxy_answered = proxy_answered;
        ER.agg_offered    = agg_offered;
        ER.noisy_answers  = noisy_n; ER.noisy_abs_err = noisy_err;
        {   // read-batch occupancy: real slots / slots of the batches put on the wire
            const EpochConfig cfg = epochConfig();
            for (const auto& m : es.read_real_per_bucket) {
                uint32_t contacted = 0;
                for (const auto& [b, n] : m) if (b >= 0) { ER.read_real += n; contacted += (n > 0); }
                const uint64_t batches = cfg.pad_uncontacted_shards ? cl.parts.size() : contacted;
                ER.read_capacity += batches * cfg.bread;
            }
        }
        ER.inserts          = inserts;
        ER.ioo_accesses     = ioo_hits;
        ER.pushes_outside   = outside;
        // served = committed through the epoch + answered at the proxy (v4; 0 otherwise)
        ER.throughput_txn_s = ER.epoch_ms > 0 ? (ER.committed + ER.proxy_answered) * 1000.0 / ER.epoch_ms : 0.0;

        // Rows that actually got a block are now IOO residents.
        {
            std::lock_guard<std::mutex> lk(new_rows_mu);
            for (const auto& [pk, line] : new_rows)
                if (cl.mrs->serverOfKey(ObjectKey{0, 0, 0, pk}) >= 0) {
                    inserted_keys.insert(pk);
                    if (pk >= rows.size()) rows.resize(pk + 1);
                    rows[pk] = line;   // carries its own region column, so the
                                       // refresh re-bins it correctly
                }
        }
        ER.ioo_used          = cl.iooUsed();
        ER.ioo_capacity      = cl.ioo_capacity;
        ER.ioo_max_occupancy = cl.iooMaxOccupancy();

        // ── refresh trigger ─────────────────────────────────────────────
        // Three ways to fire, in order of how bad the situation is:
        //   (a) the fullest shard passed the occupancy threshold — the normal,
        //       planned refresh;
        //   (b) the tightest shard no longer has room for another epoch's
        //       inserts — pre-empt the mid-epoch exhaustion described above;
        //   (c) the epoch committed NOTHING while inserts were offered — the
        //       reserve already ran dry and was rolled back, so occupancy will
        //       never rise on its own.  Without this the run wedges forever.
        // Burst: known per-epoch insert count.  Paced: use what this epoch
        // actually admitted (arrivals depend on epoch length), plus 25% slack.
        const uint64_t inserts_per_epoch = (g_offered_txn_s <= 0.0)
            ? static_cast<uint64_t>(NUM_CLIENTS * TXNS_PER_CLIENT * INSERT_FRACTION)
            : static_cast<uint64_t>(std::ceil(1.25 * std::max<uint64_t>(ER.inserts, 1)));
        const bool trig_occupancy = ER.ioo_max_occupancy >= IOO_REFRESH_THRESHOLD;
        const bool trig_headroom  = cl.iooMinFree() < inserts_per_epoch * 2;
        const bool trig_wedged    = (ER.committed == 0 && ER.inserts > 0);
        if (trig_occupancy || trig_headroom || trig_wedged) {
            ER.refresh_reason = trig_occupancy ? "occupancy"
                              : (trig_headroom ? "headroom" : "wedged");
            auto t_r0 = clk::now();
            cl.mrs->shutdownAllServers();
            cl = bringUp(rows, schema, eps, k, base_port);   // IOO → R&U refresh
            ER.refresh_ms = ms_between(t_r0, clk::now());
            ER.refreshed  = true;
            inserted_keys.clear();                           // folded back into partitions
            if (arm == AggArm::PROXY_NOISY)                  // new release ONLY at repartitioning
                agg_syn->release(g_agg_eps, seed * 1000003ULL + R.refreshes + 1);
            ++R.refreshes;
            R.total_refresh_ms += ER.refresh_ms;
        }

        R.committed    += ER.committed + ER.proxy_answered;   // served (v4: + proxy answers)
        R.aborted      += ER.aborted;
        R.proxy_answered += ER.proxy_answered;  R.agg_committed += ER.agg_committed;
        R.agg_offered    += ER.agg_offered;     R.read_real     += ER.read_real;
        R.read_capacity  += ER.read_capacity;   R.read_batch_full += ER.read_batch_full;
        R.txns_admitted  += ER.txns;
        R.noisy_answers  += ER.noisy_answers;   R.noisy_abs_err += ER.noisy_abs_err;
        R.inserts      += ER.inserts;
        R.ioo_accesses += ER.ioo_accesses;
        R.epoch_ms_total += ER.epoch_ms;
        ++R.epochs;
        trace.push_back(ER);

        std::cout << "    epoch " << std::setw(2) << e
                  << "  committed=" << std::setw(4) << ER.committed
                  << (arm != AggArm::NONE ? "  proxy=" + std::to_string(ER.proxy_answered) +
                                            "  agg_oram=" + std::to_string(ER.agg_committed) +
                                            "  rbf=" + std::to_string(ER.read_batch_full) : std::string())
                  << "  ins=" << std::setw(3) << ER.inserts
                  << "  IOO=" << std::setw(4) << ER.ioo_used << "/" << ER.ioo_capacity
                  << " (max " << std::fixed << std::setprecision(2) << ER.ioo_max_occupancy << ")"
                  << "  epoch_ms=" << std::setprecision(1) << ER.epoch_ms
                  << "  txn/s=" << ER.throughput_txn_s
                  << (ER.refreshed ? ("  REFRESH[" + ER.refresh_reason + "] "
                                      + std::to_string((int)ER.refresh_ms) + "ms") : "")
                  << "\n";
    }

    cl.mrs->shutdownAllServers();
    R.agg_releases = agg_syn ? agg_syn->releases() : 0;

    R.partitions_max = partitions_max;
    R.epochs_between_refreshes = R.refreshes ? double(R.epochs) / R.refreshes : 0.0;
    R.refresh_frequency        = R.epochs ? double(R.refreshes) / R.epochs : 0.0;
    R.mean_refresh_pause_ms    = R.refreshes ? R.total_refresh_ms / R.refreshes : 0.0;
    const double wall_with_refresh = R.epoch_ms_total + R.total_refresh_ms;
    R.amortized_refresh_pct = wall_with_refresh > 0
        ? 100.0 * R.total_refresh_ms / wall_with_refresh : 0.0;
    R.throughput_txn_s = R.epoch_ms_total > 0 ? R.committed * 1000.0 / R.epoch_ms_total : 0.0;
    R.throughput_txn_s_with_refresh = wall_with_refresh > 0
        ? R.committed * 1000.0 / wall_with_refresh : 0.0;
    return R;
}

// ── CSV ─────────────────────────────────────────────────────────────────────
static void writeQualityCsv(const std::string& path, const std::vector<QualityRow>& rows,
                            uint32_t k, uint32_t repeats, uint32_t rowcount, uint32_t regions) {
    std::ofstream f(path);
    f << "epsilon,k,rows,regions,repeats,"
         "affinity_cut_mean,affinity_cut_sd,affinity_cut_min,affinity_cut_max,"
         "fanout_mean,fanout_sd,size_cv_mean,churn_vs_noiseless_mean,churn_sd,"
         "noisy_over_real_mean,mean_partitions,total_dummy,total_stash_reserve,"
         "min_partitions,max_partitions,cap_T,placement\n";
    for (const auto& r : rows)
        f << r.epsilon << ',' << k << ',' << rowcount << ',' << regions << ',' << repeats << ','
          << std::fixed << std::setprecision(6)
          << r.cut.mean << ',' << r.cut.sd << ',' << r.cut.min << ',' << r.cut.max << ','
          << r.fanout.mean << ',' << r.fanout.sd << ',' << r.size_cv.mean << ','
          << r.churn.mean << ',' << r.churn.sd << ',' << r.noisy_ratio.mean << ','
          << r.mean_partitions << ',' << r.total_dummy << ',' << r.total_stash_reserve << ','
          << r.min_partitions << ',' << r.max_partitions << ',' << g_cap_T << ','
          << (isRandom(r.epsilon) ? "random" : "dpart") << '\n';
    std::cout << "Wrote " << path << "  (Figure 3)\n";
}

static void writePerfCsv(const std::string& path, const std::vector<PerfRow>& rows) {
    std::ofstream f(path);
    f << "epsilon,partitions,affinity_cut,fanout,epochs,committed,aborted,inserts,"
         "ioo_accesses,ioo_capacity,real_total,noisy_total,dummy_total,stash_reserve_total,"
         "refreshes,epochs_between_refreshes,refresh_frequency,mean_refresh_pause_ms,"
         "total_refresh_ms,amortized_refresh_pct,epoch_ms_total,"
         "throughput_txn_s,throughput_txn_s_with_refresh,partitions_max,cap_T,placement\n";
    for (const auto& r : rows)
        f << r.epsilon << ',' << r.partitions << ',' << std::fixed << std::setprecision(6)
          << r.affinity_cut << ',' << r.fanout << ',' << r.epochs << ',' << r.committed << ','
          << r.aborted << ',' << r.inserts << ',' << r.ioo_accesses << ',' << r.ioo_capacity << ','
          << r.real_total << ',' << r.noisy_total << ',' << r.dummy_total << ','
          << r.stash_reserve_total << ',' << r.refreshes << ','
          << std::setprecision(3) << r.epochs_between_refreshes << ',' << r.refresh_frequency << ','
          << std::setprecision(2) << r.mean_refresh_pause_ms << ',' << r.total_refresh_ms << ','
          << r.amortized_refresh_pct << ',' << r.epoch_ms_total << ','
          << r.throughput_txn_s << ',' << r.throughput_txn_s_with_refresh << ','
          << r.partitions_max << ',' << g_cap_T << ',' << (isRandom(r.epsilon) ? "random" : "dpart") << '\n';
    std::cout << "Wrote " << path << "  (Figure 4)\n";
}

static void writeEpochCsv(const std::string& path, const std::vector<EpochRow>& rows) {
    std::ofstream f(path);
    f << "epsilon,epoch,txns,committed,aborted,inserts,ioo_accesses,ioo_used,ioo_capacity,"
         "ioo_max_occupancy,epoch_ms,throughput_txn_s,refreshed,refresh_ms,refresh_reason,"
         "offered_txn_s,commit_rate,pushes_outside,partitions,placement\n";
    for (const auto& r : rows)
        f << r.epsilon << ',' << r.epoch << ',' << r.txns << ',' << r.committed << ','
          << r.aborted << ',' << r.inserts << ',' << r.ioo_accesses << ',' << r.ioo_used << ','
          << r.ioo_capacity << ',' << std::fixed << std::setprecision(4) << r.ioo_max_occupancy
          << ',' << std::setprecision(2) << r.epoch_ms << ',' << r.throughput_txn_s << ','
          << (r.refreshed ? 1 : 0) << ',' << r.refresh_ms << ','
          << (r.refresh_reason.empty() ? "-" : r.refresh_reason) << ','
          << g_offered_txn_s << ','
          << std::setprecision(4) << (r.txns ? double(r.committed) / r.txns : 0.0) << ','
          << r.pushes_outside << ',' << r.partitions << ',' << (isRandom(r.epsilon) ? "random" : "dpart") << '\n';
    std::cout << "Wrote " << path << "  (per-epoch trace)\n";
}

// ── v4: synopsis ablation CSVs ──────────────────────────────────────────────
static void writeSynopsisCsv(const std::string& path, const std::vector<PerfRow>& rows) {
    std::ofstream f(path);
    f << "epsilon,arm,placement,agg_frac,agg_group,agg_eps,epochs,served,proxy_answered,"
         "agg_offered,agg_committed_oram,regular_committed,aborted,read_batch_full,"
         "read_batch_full_rate,oram_real_reads,oram_reads_per_served_txn,read_slots_sent,"
         "read_batch_occupancy,epoch_ms_total,throughput_txn_s,regular_txn_s,"
         "throughput_txn_s_with_refresh,refreshes,agg_releases,cumulative_agg_eps,"
         "noisy_answers,noisy_mean_abs_err,fanout,affinity_cut,partitions\n";
    for (const auto& r : rows) {
        const uint64_t regular = r.committed - r.proxy_answered - r.agg_committed;
        f << r.epsilon << ',' << armName(r.arm) << ',' << (isRandom(r.epsilon) ? "random" : "dpart") << ','
          << g_agg_frac << ',' << g_agg_group << ',' << (r.arm == AggArm::PROXY_NOISY ? g_agg_eps : 0.0) << ','
          << r.epochs << ',' << r.committed << ',' << r.proxy_answered << ',' << r.agg_offered << ','
          << r.agg_committed << ',' << regular << ',' << r.aborted << ',' << r.read_batch_full << ','
          << std::fixed << std::setprecision(4)
          << (r.txns_admitted ? double(r.read_batch_full) / r.txns_admitted : 0.0) << ','
          << r.read_real << ',' << (r.committed ? double(r.read_real) / r.committed : 0.0) << ','
          << r.read_capacity << ',' << (r.read_capacity ? double(r.read_real) / r.read_capacity : 0.0) << ','
          << std::setprecision(2) << r.epoch_ms_total << ',' << r.throughput_txn_s << ','
          << (r.epoch_ms_total > 0 ? regular * 1000.0 / r.epoch_ms_total : 0.0) << ','
          << r.throughput_txn_s_with_refresh << ',' << r.refreshes << ',' << r.agg_releases << ','
          << r.agg_releases * (r.arm == AggArm::PROXY_NOISY ? g_agg_eps : 0.0) << ','
          << r.noisy_answers << ',' << std::setprecision(3)
          << (r.noisy_answers ? r.noisy_abs_err / r.noisy_answers : 0.0) << ','
          << std::setprecision(4) << r.fanout << ',' << r.affinity_cut << ',' << r.partitions << '\n';
    }
    std::cout << "Wrote " << path << "  (synopsis ablation)\n";
}

static void writeSynEpochCsv(const std::string& path, const std::vector<EpochRow>& rows,
                             const std::vector<std::string>& arm_of_row) {
    std::ofstream f(path);
    f << "epsilon,arm,epoch,txns,committed,proxy_answered,agg_offered,agg_committed_oram,aborted,"
         "read_batch_full,read_real,read_capacity,epoch_ms,throughput_txn_s,refreshed,refresh_ms\n";
    for (size_t i = 0; i < rows.size(); ++i) {
        const auto& r = rows[i];
        f << r.epsilon << ',' << arm_of_row[i] << ',' << r.epoch << ',' << r.txns << ',' << r.committed << ','
          << r.proxy_answered << ',' << r.agg_offered << ',' << r.agg_committed << ',' << r.aborted << ','
          << r.read_batch_full << ',' << r.read_real << ',' << r.read_capacity << ','
          << std::fixed << std::setprecision(2) << r.epoch_ms << ',' << r.throughput_txn_s << ','
          << (r.refreshed ? 1 : 0) << ',' << r.refresh_ms << '\n';
    }
    std::cout << "Wrote " << path << "\n";
}

static std::vector<std::string> splitList(const char* ev, const std::string& dflt) {
    std::vector<std::string> out; std::string tok, all = (ev && *ev) ? ev : dflt;
    for (size_t i = 0; i <= all.size(); ++i) {
        if (i == all.size() || all[i] == ',') { if (!tok.empty()) out.push_back(tok); tok.clear(); }
        else if (all[i] != ' ') tok += all[i];
    }
    return out;
}

int main(int argc, char** argv) {
    const uint32_t k       = argc > 1 ? std::stoul(argv[1]) : 4;
    const uint32_t rows    = argc > 2 ? std::stoul(argv[2]) : 20000;
    const uint32_t regions = argc > 3 ? std::stoul(argv[3]) : 40;
    const uint32_t repeats = argc > 4 ? std::stoul(argv[4]) : 10;
    const std::string mode = argc > 5 ? argv[5] : "both";
    const int base_port    = argc > 6 ? std::stoi(argv[6]) : 8881;
    const std::string pre  = argc > 7 ? argv[7] : "rq2";
    g_offered_txn_s        = argc > 8 ? std::stod(argv[8]) : 0.0;
    if (argc > 9) INSERT_RESERVE = static_cast<uint32_t>(std::stoul(argv[9]));
    g_cap_T                = argc > 10 ? std::stod(argv[10]) : 0.0;
    if (const char* ev = std::getenv("RQ2_EPSILONS"); ev && *ev) {
        std::vector<double> grid; std::string tok; std::string all(ev);
        for (size_t i = 0; i <= all.size(); ++i) {
            if (i == all.size() || all[i] == ',') { if (!tok.empty()) grid.push_back(std::stod(tok)); tok.clear(); }
            else tok += all[i];
        }
        if (!grid.empty()) EPSILONS = grid;
    }
    g_random = argc > 11 && std::string(argv[11]) == "random";
    if (g_random) {
        EPSILONS = {0.0};                 // legacy switch: eps = 0 only
        std::cout << "Placement: eps = 0 random placement only, k=" << k << " shards\n";
    }
    if (std::find_if(EPSILONS.begin(), EPSILONS.end(), [](double e) { return e <= 0.0; }) != EPSILONS.end())
        std::cout << "eps = 0 -> zero-information placement: uniformly random shard per row, no DP,\n"
                     "           no synopsis, no dummies; same growth / IOO / refresh pipeline\n";
    std::cout << "Partitioning: " << (g_cap_T > 0.0
                  ? "Algorithm 4, cap_T=" + std::to_string(g_cap_T) + " (K depends on data and eps; k="
                    + std::to_string(k) + " listeners available)"
                  : "Algorithm 3, exactly k=" + std::to_string(k) + " partitions")
              << "   eps grid:";
    for (double e : EPSILONS) std::cout << ' ' << e;
    std::cout << "\n";

    std::cout << "RQ2 — privacy budget:  k=" << k << "  rows=" << rows
              << "  regions=" << regions << "  repeats=" << repeats << "  mode=" << mode << "\n"
              << "AffinityCut = cut weight / total weight of the workload affinity graph\n"
              << "IOO         = the per-shard public insert reserve ("
              << INSERT_RESERVE << " blocks/shard); refresh at "
              << IOO_REFRESH_THRESHOLD << " occupancy of the fullest shard\n"
              << "Throughput  = committed txns / END-TO-END epoch time (runEpoch_RBR_W:"
                 " R=" << epochConfig().R << ", bread=" << epochConfig().bread
              << ", bwrite=" << epochConfig().bwrite << ", padding: "
              << (epochConfig().pad_uncontacted_shards ? "all shards" : "contacted-only")
              << ")\n"
              << "Arrivals    = " << (g_offered_txn_s > 0
                     ? std::to_string(g_offered_txn_s) + " txn/s (paced, open loop, as in RQ3)"
                     : std::string("BURST (legacy): ") + std::to_string(NUM_CLIENTS * TXNS_PER_CLIENT)
                       + " txns pushed at once per epoch")
              << "\n\n";

    rq3::Dataset d = rq3::generateDataset(rows, regions, rq3::DataDist::SKEWED, SKEW_ALPHA, 42);
    Workload wl = buildWorkload(d, WORKLOAD_TXNS, OPS_PER_TXN, SKEW_ALPHA, 43);
    std::cout << "Granularity: " << (rows / std::max(1u, regions))
              << " rows/bin — eps matters while 1/eps is comparable to that\n"
              << "Locality:    " << WORKLOAD_LOCALITY << " of a txn's keys stay in its own region\n"
              << "Workload: " << wl.graph.numTxns() << " txns, "
              << wl.graph.numEdges() << " affinity edges, weight "
              << wl.graph.totalWeight() << "\n\n";

    if (mode == "quality" || mode == "both") {
        std::cout << "── Figure 3: partition quality vs epsilon ──\n";
        auto q = runQualitySweep(d, wl, k, repeats);
        double lo = 1e9, hi = -1e9;
        for (const auto& r : q) { lo = std::min(lo, r.cut.mean); hi = std::max(hi, r.cut.mean); }
        if (hi <= 1e-9) {
            std::cout << "\n  !! affinity cut is 0 at every eps.  Every transaction fits inside\n"
                         "     one bin, and bins are never split across shards, so no edge can\n"
                         "     be cut.  Lower WORKLOAD_LOCALITY (currently "
                      << WORKLOAD_LOCALITY << ").\n";
        } else if (hi - lo <= 0.005) {
            std::cout << "\n  !! affinity cut barely moves (" << std::fixed << std::setprecision(4)
                      << lo << " to " << hi << ").  The Laplace noise (scale 1/eps) is small\n"
                         "     next to " << (rows / std::max(1u, regions)) << " rows/bin, so the"
                         " partition boundaries never shift.\n"
                         "     Use finer bins (raise `regions`) or extend the grid to smaller eps.\n";
        }
        writeQualityCsv(pre + "_quality.csv", q, k, repeats, rows, regions);
    }
    if (mode == "synopsis") {
        if (const char* v = std::getenv("RQ2_AGG_FRAC");  v && *v) g_agg_frac  = std::stod(v);
        if (const char* v = std::getenv("RQ2_AGG_GROUP"); v && *v) g_agg_group = static_cast<uint32_t>(std::stoul(v));
        if (const char* v = std::getenv("RQ2_AGG_EPS");   v && *v) g_agg_eps   = std::stod(v);
        std::vector<double> eps_list;
        for (const auto& t : splitList(std::getenv("RQ2_SYN_EPS"), "1")) eps_list.push_back(std::stod(t));
        const auto arms = splitList(std::getenv("RQ2_SYN_ARMS"), "oram,exact,noisy");
        requireListeners("127.0.0.1", base_port, k, "RQ2 synopsis ablation");
        std::cout << "\n── Synopsis ablation: same placement and eps, aggregates answered via ORAM\n"
                     "   vs at the trusted proxy (exact / noisy synopsis) ──\n"
                  << "   agg_frac=" << g_agg_frac << " of non-insert txns, group=" << g_agg_group
                  << " rows, noisy eps_load=" << g_agg_eps << "   arms:";
        for (const auto& a : arms) std::cout << ' ' << a;
        std::cout << "\n";
        std::vector<PerfRow> rowsOut; std::vector<EpochRow> trace; std::vector<std::string> trace_arm;
        for (double eps : eps_list) {
            for (const auto& a : arms) {
                AggArm arm = a == "exact" ? AggArm::PROXY_EXACT : a == "noisy" ? AggArm::PROXY_NOISY
                                                                                : AggArm::ORAM;
                if (a != "oram" && a != "exact" && a != "noisy" && a != "random") {
                    std::cerr << "  unknown arm '" << a << "' (oram | exact | noisy | random)\n"; continue;
                }
                const double e = (a == "random") ? 0.0 : eps;   // reference: zero-information + ORAM
                if (a == "random" && eps != eps_list.front()) continue;   // one reference row is enough
                std::cout << "  eps=" << e << "  arm=" << a << "\n";
                try {
                    const size_t before = trace.size();
                    PerfRow r = runPerfPoint(d, wl, e, k, base_port, 7, trace, arm);
                    for (size_t i = before; i < trace.size(); ++i) trace_arm.push_back(a);
                    const uint64_t regular = r.committed - r.proxy_answered - r.agg_committed;
                    std::cout << "    → served txn/s=" << std::fixed << std::setprecision(1) << r.throughput_txn_s
                              << "  regular txn/s=" << (r.epoch_ms_total > 0 ? regular * 1000.0 / r.epoch_ms_total : 0.0)
                              << "  proxy=" << r.proxy_answered << "  agg_oram=" << r.agg_committed
                              << "  READ_BATCH_FULL=" << r.read_batch_full
                              << "  reads/served=" << std::setprecision(2)
                              << (r.committed ? double(r.read_real) / r.committed : 0.0)
                              << "  occupancy=" << (r.read_capacity ? double(r.read_real) / r.read_capacity : 0.0)
                              << (r.noisy_answers ? "  noisy|err|=" + std::to_string(r.noisy_abs_err / r.noisy_answers) : std::string())
                              << "\n";
                    if (a == "random") r.arm = AggArm::ORAM;
                    rowsOut.push_back(r);
                } catch (const std::exception& ex) {
                    std::cerr << "    FAILED: " << ex.what() << "\n";
                }
            }
        }
        writeSynopsisCsv(pre + "_synopsis.csv", rowsOut);
        writeSynEpochCsv(pre + "_synopsis_epochs.csv", trace, trace_arm);
        return 0;
    }
    if (mode == "perf" || mode == "both") {
        requireListeners("127.0.0.1", base_port, k, "RQ2 Figure 4");
        std::cout << "\n── Figure 4: throughput / refresh / IOO vs epsilon (needs " << k
                  << " listeners on ports " << base_port << ".."
                  << base_port + static_cast<int>(k) - 1 << ") ──\n";
        std::vector<PerfRow>  perf;
        std::vector<EpochRow> trace;
        for (double eps : EPSILONS) {
            std::cout << "  eps=" << eps << (isRandom(eps) ? "  (zero-information random placement)" : "") << "\n";
            try {
                PerfRow r = runPerfPoint(d, wl, eps, k, base_port, 7, trace);
                std::cout << "    → cut=" << std::fixed << std::setprecision(4) << r.affinity_cut
                          << "  fanout=" << std::setprecision(2) << r.fanout
                          << "  refreshes=" << r.refreshes
                          << " (every " << std::setprecision(1) << r.epochs_between_refreshes
                          << " epochs, pause " << std::setprecision(0) << r.mean_refresh_pause_ms
                          << "ms, " << std::setprecision(1) << r.amortized_refresh_pct << "% of wall)"
                          << "  txn/s=" << std::setprecision(1) << r.throughput_txn_s
                          << " (with refresh " << r.throughput_txn_s_with_refresh << ")\n";
                perf.push_back(r);
            } catch (const std::exception& e) {
                std::cerr << "    FAILED: " << e.what() << "\n";
            }
        }
        writePerfCsv(pre + "_perf.csv", perf);
        writeEpochCsv(pre + "_epochs.csv", trace);
    }
    return 0;
}