//
// RQ3_DistributionShift.cpp
// ----------------------------------------------------------------------------
// RQ3: How robust is DePart to mismatches between the DATA distribution and
// the QUERY-ACCESS distribution?
//
// Four settings (paper §RQ3):
//   1. uniform data  + uniform queries
//   2. skewed data   + matching skewed queries
//   3. uniform data  + skewed queries
//   4. skewed data   + mismatched skewed queries
//
// Produces both figures in one run:
//   Figure 5 — shard load imbalance:  Imbalance = max_i L_i / ((1/k) Σ_i L_i)
//              L_i = workload assigned to shard i.  Reported twice:
//                * over submitted queries  (what clients asked for)
//                * over ORAM ops           (what shard i actually executed,
//                                           i.e. after MVTSO dedup)
//   Figure 6 — transaction throughput (txn/s) for the same settings, measured
//              END-TO-END over a full DePart epoch: runEpoch_RBR_W() with R
//              fixed read sub-epochs, bread / bwrite, dummy padding ON
//              (security mode), the version cache, staged transactions, MVTSO
//              and the write phase.  An earlier version timed only
//              executeOramOps() on the dedup'd op list with padding OFF, which
//              is "deduplicated real ORAM op execution time", not transaction
//              throughput.
//
// Systems compared here:
//   * DePart: DP-guided partitioning on the `region` attribute (Algorithm 3),
//     DP-sized ORAMs.
//   * Random (+ secondary index): the same baseline as the TPC-C "index"
//     placement.  Every ROW is placed on a uniformly random shard by primary
//     key; there is no DP, no synopsis and no attribute partitioning, and each
//     shard's ORAM is sized to its exact row count.  The proxy holds an exact
//     secondary index region -> {PK}, so a range predicate resolves to the same
//     qualifying key set Q that DePart gets from its exact synopsis; the
//     records are then fetched by PK through the pos_map.  Only physical
//     placement differs between the two systems.
//     (The previous Random variant dealt DePart's DP bins to shards at random,
//     which kept every region's rows together and so handed the baseline part
//     of DePart's locality.  It has been removed.)
// OasisDB is not built in this repo — run it separately and merge its column
// into the CSV.
//
// WHY THIS FILE EXISTS (vs Oram_Performance_*_Skewed.cpp):
// the old skewed experiments drew the shard uniformly and applied Zipf INSIDE
// the shard:
//       int sid = rng() % servers.size();
//       uint32_t block = zipf_within_shard(rng);
// Skew inside a shard cannot produce cross-shard imbalance, so Figure 5 would
// have measured Imbalance ≡ 1 no matter the setting.  Here the skew is on the
// KEY SPACE (the partitioning attribute) and the shard is whatever the
// partitioning maps that key to — see RQ3_Workload.h.
//
// PREREQUISITE: k storage listeners must already be running, one per shard:
//       for p in $(seq 8881 8884); do ./build/bin/Servers_MultiRingORAM $p & done
//
// Usage:
//   ./RQ3_DistShift [k] [rows] [regions] [alpha_data] [epsilon] [base_port] [csv]
//                   [range_fraction] [alpha_query]
//                   [offered_txn_s] [repeats] [num_seeds] [contacted_only]
//                   [cap_T]
//     cap_T                 0 = Algorithm 3: DePart splits into exactly k
//                           partitions (DEFAULT, all earlier runs).
//                           > 0 = Algorithm 4 (9/27/26): fixed public threshold
//                           cap_T; a partition closes once it holds >= cap_T
//                           noisy rows, so the shard count K is decided by the
//                           data (and the DP noise).  One partition per shard.
//                           Random + index then uses the SAME K as the DePart
//                           run of that (seed, setting), so both systems get
//                           the same number of servers.  k becomes the number
//                           of listeners available (must be >= every K).
//     offered_txn_s         open-loop arrival RATE in txn/s, summed over all
//                           clients, evenly paced in wall-clock time.  A read
//                           round lasts sub_epoch_ms + batch execution time,
//                           so arrivals per round = rate x round period (see
//                           arrivals_per_subepoch in the CSV).  0 = legacy
//                           burst: every client pushes TXNS_PER_CLIENT txns at
//                           once, so ~all arrive in sub-epoch 1 (default 0)
//     repeats               epochs per (system, setting, seed)   (default 10)
//     num_seeds             seeds 42, 43, ... (dataset + workload) (default 1)
//     contacted_only        1 = only shards with real ops get a batch, padded
//                           to bread/bwrite; the contacted set C of each round
//                           is visible, as the paper's threat model allows
//                           (DEFAULT — this is where locality pays off).
//                           0 = every shard gets a padded batch every round
//                           (hides C; stricter baseline for comparison).
//                           When timing, prefer separate machines: on one
//                           host, skipped dummy batches also remove CPU
//                           contention between listeners.
//   Also writes <csv minus .csv>_subepoch.csv: one row per (seed, rep,
//   sub-epoch) with the real ORAM slots each shard used in that read batch.
//     k        shards                      (default 4)
//     rows     dataset rows                (default 20000)
//     regions  distinct partition-attribute values (default 40)
//     alpha    Zipf exponent for both data and query skew (default 1.0)
//     epsilon  DP budget for partitioning  (default 1.0)
//     base_port first listener port        (default 8881)
//     csv      output path                 (default rq3_distribution_shift.csv)
// ----------------------------------------------------------------------------

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <atomic>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"
#include "Repartition.h"
#include "RQ3_Workload.h"
#include "RQ_Preflight.h"
#include "NetEmu.h"          // emulated proxy <-> shard network (DEPART_NET_*)

using clk = std::chrono::high_resolution_clock;
static double ms_between(clk::time_point a, clk::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ── Workload knobs ──────────────────────────────────────────────────────────
// Figure 6 asks whether shard load imbalance TRANSLATES INTO throughput loss.
// That only shows up when the shards are the bottleneck: with 4 clients x 25
// txns and a 3 ms pause, the smoke run committed exactly 300 txns with 0
// aborts in every setting, so throughput sat at ~85 txn/s whether the
// imbalance was 1.05 or 3.33 — the number measured the client loop, not the
// cluster.  An epoch ends when the SLOWEST shard finishes, so a hot shard can
// only cost throughput once there is enough offered load to keep it busy.
static constexpr int NUM_CLIENTS      = 8;
static constexpr int TXNS_PER_CLIENT  = 200;
static constexpr int OPS_PER_TXN      = 3;
static constexpr uint32_t BUCKET_SIZE = 8;
static constexpr uint32_t S_PARAM     = 4;
static constexpr uint32_t DEFAULT_REPEATS = 10;  // epochs per (setting, seed); report median + IQR

// ── Query mix ───────────────────────────────────────────────────────────────
// A POINT query names one primary key.  Both systems route it through their own
// pos_map, so neither pays a penalty and DePart's locality buys nothing — which
// is why a point-only workload made Random look uniformly better.
//
// A RANGE query names a predicate over the partitioning attribute
// (region in [r, r+RANGE_WIDTH_REGIONS)) and must read every matching row.  That
// is where placement decides cost:
//   * DePart keeps a region's rows together and adjacent regions on the same or
//     neighbouring shards, so a narrow window touches 1-2 shards.
//   * RANDOM + FULL POSITION INDEX (the baseline modelled here): the proxy knows
//     exactly which rows match and where each lives, so it issues no wasted
//     access — but the matching rows are scattered over all k shards, so fan-out
//     is ~k.  This is the STRONGEST form of the random baseline; a variant with
//     no index would additionally have to probe shards that hold nothing.
//
// Both systems run the identical generator below and resolve a range predicate
// to the SAME qualifying key set Q (ds.rows_of_region plays the role of
// DePart's exact synopsis and of Random's exact secondary index region -> PK).
// Every record in Q is then fetched by PK — these are index-lookup + record-
// fetch queries, not index-only ones — so the fan-out difference comes only
// from where each system physically put the rows.
static constexpr uint32_t RANGE_WIDTH_REGIONS = 4;
// A 4-region window matches ~4 x (rows/regions) rows — 400 at 20000/200, far
// more than one epoch batch can carry.  A real system would stream that; here a
// range query samples at most this many matching rows, so the per-txn op count
// stays comparable to the point case and the fan-out comparison is not swamped
// by batch overflow.
static constexpr uint32_t RANGE_MAX_ROWS      = 12;

// The epoch the throughput number refers to.  pad_batches = true is the
// security default: every batch is exactly bread / bwrite on the wire.
// Set once from argv in main(); read by epochConfig().
static bool g_contacted_only = true;
static double g_cap_T = 0.0;          // > 0: DePart uses Algorithm 4 (see Usage)

static EpochConfig epochConfig() {
    EpochConfig c;
    c.R            = 3;
    c.bread        = 256;
    c.bwrite       = 512;
    c.sub_epoch_ms = 60;
    c.pad_batches  = true;
    // Obladi semantics, stated explicitly rather than relying on defaults:
    // each shard gets exactly ONE bread-sized batch per read sub-epoch, and a
    // stage whose new fetches do not fit aborts with READ_BATCH_FULL (no
    // extra batch, which would leak per-shard volume).
    c.per_server_capacity    = true;
    c.enforce_batch_capacity = true;
    c.pad_uncontacted_shards = !g_contacted_only;
    return c;
}

static double median_of(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return (n % 2) ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}
// Linear-interpolated quantile, q in [0,1].
static double quantile_of(std::vector<double> v, double q) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const double pos = q * (v.size() - 1);
    const size_t lo = static_cast<size_t>(std::floor(pos));
    const size_t hi = std::min(lo + 1, v.size() - 1);
    return v[lo] + (pos - lo) * (v[hi] - v[lo]);
}

// One read sub-epoch of one epoch, as dispatched (written to *_subepoch.csv).
struct SubEpochRow {
    std::string variant, setting;
    uint64_t seed = 0;
    uint32_t rep = 0, sub_epoch = 0;
    size_t   arrivals = 0;              // txns drained into this sub-epoch
    std::vector<uint32_t> real;         // real ORAM slots per shard (of bread)
};

enum class Variant { DEPART, RANDOM };
static const char* variantName(Variant v) { return v == Variant::DEPART ? "DePart" : "Random"; }

// ── One measurement ─────────────────────────────────────────────────────────
struct Result {
    std::string variant, setting;
    uint32_t k = 0, rows = 0, regions = 0;
    double   alpha_data = 0, alpha_query = 0, epsilon = 0;
    uint32_t txns = 0, committed = 0, aborted = 0;   // summed over REPEATS epochs
    uint64_t query_ops = 0, oram_ops = 0;
    double   imbalance_queries = 1.0, imbalance_oram = 1.0;
    double   mean_fanout = 0.0;          // distinct shards touched per txn (all kinds)
    // Split by query kind.  Point fan-out comes out the same for both systems
    // (each routes one key through its own pos_map); the RANGE number is the
    // one placement decides — DePart keeps a region window on 1-2 shards,
    // row-level random placement scatters it over ~min(k, |Q|) shards.
    double   mean_fanout_point = 0.0;
    double   mean_fanout_range = 0.0;
    double   range_txn_frac    = 0.0;
    double   epoch_ms = 0.0;                 // median end-to-end epoch time
    double   throughput_txn_s = 0.0, throughput_ops_s = 0.0;
    std::vector<uint64_t> L_queries, L_oram;
    std::vector<uint32_t> partition_rows;

    // Why the aborted txns aborted.  Without this, "DePart commits 53% while
    // Random commits 100% in the mismatched setting" is an unexplained gap;
    // with it the cause is visible — a hot shard cannot fit its reads into
    // R x bread, so its overflow txns are rejected with READ_BATCH_FULL while
    // the cold shards' txns sail through.
    std::map<AbortReason, uint64_t> abort_by_reason;

    // ── Executed-load metrics (from the dispatched batches, not the generator)
    uint64_t seed = 0;
    double   offered_per_subepoch = 0;   // 0 = burst
    uint32_t repeats = 0;
    uint64_t pushes_outside_window = 0;  // paced pushes rejected (intake closed)
    std::vector<double> arrivals_per_subepoch_mean;   // mean over repeats
    std::vector<uint64_t> L_exec;        // real read slots per shard, summed over repeats
    double commit_rate          = 0.0;   // committed / admitted
    double tput_p25 = 0, tput_p75 = 0;   // txn/s IQR over repeats
    // eta_read = sum_{r,i} real_i(r) / (k * R * bread): share of read slots
    // doing real work; the rest of every padded batch is dummy.
    double eta_read_med = 0, eta_read_p25 = 0, eta_read_p75 = 0;
    double eta_write_med = 0;            // same for the single write batch
    // peak_util = max_{r,i} real_i(r) / bread  (1.0 = some shard's batch full)
    double peak_util_med = 0;
    // max_share = max_i real_i / sum_i real_i per non-empty sub-epoch, averaged;
    // 1/k = perfectly spread, 1 = one shard does all real work.
    double max_share_med = 0;
    double imbalance_exec = 1.0;         // imbalance of L_exec

    // ── Contacted-set leakage (meaningful in both modes) ─────────────────
    // A shard is "contacted" in a sub-epoch iff it has >= 1 real read slot.
    // In contacted_only mode exactly these shards receive a batch, so C is
    // what the adversary sees.  In the default mode the same numbers say how
    // much C WOULD reveal if padding of uncontacted shards were dropped.
    std::string padding_mode = "all_shards";
    double contacted_frac        = 0.0;  // mean |C_r| / k over sub-epochs
    double partial_contact_frac  = 0.0;  // share of sub-epochs with 0 < |C_r| < k
    double eta_read_sent_med     = 0.0;  // real / read slots actually sent

    // ── Cost per committed txn (K sweep) ─────────────────────────────────
    // ORAM slots (real + dummy) actually put on the wire, summed over all
    // repeats, read rounds + write round.  This is a COUNT, so it is not
    // distorted by running all listeners on one host.
    uint64_t read_slots_sent  = 0;
    uint64_t write_slots_sent = 0;
    double   slots_per_commit = 0.0;     // (read + write slots) / committed

    // ── Emulated network (NetEmu.h); all 0 when DEPART_NET_* is unset ──
    double   net_rtt_ms = 0.0, net_bw_mbps = 0.0;
    int      net_shared = 1;
    uint64_t net_bytes = 0;              // emulated bytes on the wire, summed over repeats
    double   net_bytes_per_commit = 0.0;
    double   net_delay_ms_per_epoch = 0.0;   // sum over rounds of the largest shard delay / repeats
    double   net_bytes_per_slot_mean = 0.0;  // mean over shards of (L+1) * block_size * path_factor
};

// One transaction's keys.  A `range_fraction` share are range queries.  Shared
// by the client threads and by the Figure 5 load accounting, so the measured
// load and the executed load can never drift apart.
static std::vector<uint32_t> makeTxnKeys(const rq3::Dataset& ds, rq3::QueryKeyGen& keys,
                                         std::mt19937_64& rng, double range_fraction,
                                         bool* was_range = nullptr) {
    const bool is_range = (rng() % 1000) < static_cast<uint64_t>(range_fraction * 1000);
    if (was_range) *was_range = is_range;

    if (!is_range) {                        // point query: OPS_PER_TXN keys
        std::vector<uint32_t> out;
        out.reserve(OPS_PER_TXN);
        for (int q = 0; q < OPS_PER_TXN; ++q) out.push_back(keys.nextKey());
        return out;
    }

    // The anchor is drawn from the same query distribution, so skew decides
    // WHERE the range lands exactly as it decides where point reads land.
    const uint32_t anchor = ds.region_of_row[keys.nextKey()];
    const uint32_t width  = std::min<uint32_t>(RANGE_WIDTH_REGIONS, ds.num_regions);
    const uint32_t start  = (anchor + width <= ds.num_regions) ? anchor
                                                              : ds.num_regions - width;
    std::vector<uint32_t> matched;
    for (uint32_t r = start; r < start + width; ++r)
        matched.insert(matched.end(), ds.rows_of_region[r].begin(),
                       ds.rows_of_region[r].end());
    if (matched.empty()) return {keys.nextKey()};
    if (matched.size() > RANGE_MAX_ROWS) {
        for (uint32_t i = 0; i < RANGE_MAX_ROWS; ++i)
            std::swap(matched[i], matched[i + rng() % (matched.size() - i)]);
        matched.resize(RANGE_MAX_ROWS);
    }
    return matched;
}

// ── Random + secondary index baseline: row-level random placement ──────────
// Mirrors the TPC-C "index" placement exactly:
//   * one bin per ROW (bin key = row index = primary key), so no two rows are
//     forced onto the same shard by the partitioning attribute;
//   * each row goes to a uniformly random shard;
//   * no DP: synopsis == noisy_synopsis == exact row count, no dummies, no
//     noise stash reserve.  Exact (smaller) ORAMs are conservative for DePart.
// `out_bins` must outlive the cluster, since the servers keep a reference.
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

// ── One (variant × setting) measurement ─────────────────────────────────────

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

// alpha_data and alpha_query are SEPARATE on purpose.  With a single alpha a
// sweep would be meaningless: raising it sharpens the data distribution and the
// query distribution together, so "matched" stays matched by construction and
// the mismatch curve can never diverge.  Pinning the data skew and turning up
// only the query skew is what isolates
//     "as access skew increases, mismatch progressively creates hotspots".
static Result runOne(Variant variant, const rq3::Setting& st, uint32_t k,
                     uint32_t rows, uint32_t regions, double alpha_data,
                     double alpha_query, double epsilon,
                     int base_port, uint64_t seed, double range_fraction,
                     double offered_per_subepoch, uint32_t repeats,
                     std::vector<SubEpochRow>* sub_rows) {
    Result R;
    R.seed = seed; R.offered_per_subepoch = offered_per_subepoch; R.repeats = repeats;
    R.variant = variantName(variant);
    R.setting = st.name;
    R.k = k; R.rows = rows; R.regions = regions;
    R.alpha_data = alpha_data; R.alpha_query = alpha_query; R.epsilon = epsilon;

    // ── 1. Data with the requested DATA distribution ────────────────────
    rq3::Dataset ds = rq3::generateDataset(rows, regions, st.data, alpha_data, seed);
    const std::string schema = ds.schema();
    std::vector<int> attr = {1};                 // partition by `region`

    // ── 2. Placement ───────────────────────────────────────────────────
    // DePart runs DP partitioning; the Random baseline never touches any
    // DePart structure (no Repartition, no synopsis, no DP noise).
    Repartition repartitioner;          // DePart only (owns its BinInfo)
    BinInfo     rr_bins;                // Random only: one bin per row
    std::vector<Partition> partitions;
    if (variant == Variant::DEPART && g_cap_T > 0.0) {
        partitions = repartitioner.Repartition_Main(
            ds.rows, schema, attr,
            epsilon,      // epsilon_max
            0.0,          // epsilon_threshold (unused)
            g_cap_T,      // threshold_pub → Algorithm 4
            false,        // fixed_number_part = false → Algorithm 4
            0, 0, 0);
        if (partitions.size() > k)
            throw std::runtime_error("Algorithm 4 produced " + std::to_string(partitions.size()) +
                                     " partitions but only k=" + std::to_string(k) +
                                     " listeners are available; start more listeners");
    } else if (variant == Variant::DEPART) {
        partitions = repartitioner.Repartition_Main(
            ds.rows, schema, attr,
            epsilon,      // epsilon_max
            0.0,          // epsilon_threshold (unused for Algorithm 3)
            0.0,          // threshold_pub    (unused for Algorithm 3)
            true,         // fixed_number_part → Algorithm 3
            k,            // fixed_part_num
            0,            // primary_key_col = customer_id
            k);           // num_servers cap
    } else {
        partitions = rowRandomPlacement(static_cast<uint32_t>(ds.rows.size()), k, seed, rr_bins);
    }
    const BinInfo& bin_info = (variant == Variant::DEPART) ? repartitioner.getBinInfo()
                                                           : rr_bins;

    for (const auto& p : partitions) R.partition_rows.push_back(p.synopsis);
    R.k = static_cast<uint32_t>(partitions.size());     // Algorithm 4: K, not the k argument
    if (g_cap_T > 0.0) {
        std::cerr << "    [" << R.variant << " / " << st.name << " / seed " << seed << "] K="
                  << partitions.size() << " partitions, rows:";
        for (const auto& p : partitions) std::cerr << ' ' << p.synopsis;
        std::cerr << "\n";
    }

    // ── 3. Cluster ─────────────────────────────────────────────────────
    std::vector<ServerInfo> servers(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        servers[i].server_id    = static_cast<int>(i);
        servers[i].port         = base_port + static_cast<int>(i);
        servers[i].partition_id = static_cast<uint32_t>(i);
        servers[i].assigned_bins = partitions[i].index;
    }

    MultiRingORAM_Servers mrs;
    // Identity key mapper: pk == data index, ObjectKey = (0, 0, 0, pk).
    mrs.distributeDataToPartitions(bin_info, partitions, servers, ds.rows);

    const uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    const uint32_t block_size  = tuple_width + 16 + 2 * sizeof(uint32_t);   // + AES::BLOCKSIZE

    // Emulated network: bytes one padded slot costs on shard i = one Ring ORAM path of its tree.
    netemu::clearBytesPerSlot();
    {
        double sum = 0.0;
        for (size_t i = 0; i < partitions.size(); ++i) {
            const uint64_t n = std::max<uint64_t>(1, partitions[i].noisy_synopsis);
            const uint64_t b = netemu::ringOramBytesPerSlot(n, BUCKET_SIZE, block_size);
            netemu::setBytesPerSlot(static_cast<int>(i), b);
            sum += double(b);
        }
        R.net_bytes_per_slot_mean = partitions.empty() ? 0.0 : sum / partitions.size();
    }
    // BULK init (not ServerInitialization): the per-record path calls
    // oram->access(block, INSERT, value) for EVERY row, and each of those is a
    // ReadPath + eviction, i.e. tens to hundreds of network round-trips.  At
    // 6000 rows per shard that is millions of RTTs and the run looks hung.
    // ServerInitializationBulk loads the records straight into the stash
    // (O(N), local) and streams the whole dummy tree up in one pass.
    for (size_t i = 0; i < partitions.size(); ++i) {
        if (useBulkInit())
            mrs.ServerInitializationBulk(servers[i], partitions[i], bin_info, ds.rows, schema,
                                     BUCKET_SIZE, "RQ3_S" + std::to_string(i),
                                     block_size, "127.0.0.1", S_PARAM);
        else
            mrs.ServerInitialization(servers[i], partitions[i], bin_info, ds.rows, schema,
                                     BUCKET_SIZE, "RQ3_S" + std::to_string(i),
                                     block_size, "127.0.0.1", S_PARAM);
    }
    std::cerr << "    [init] forking " << partitions.size() << " shards, waiting..." << std::endl;
    mrs.waitForAllServersReady();
    std::cerr << "    [init] all shards ready" << std::endl;

    // Shard of every row, straight from the pos_map (this is the mapping the
    // query distribution gets pushed through).
    std::vector<int> shard_of_row(ds.rows.size(), -1);
    for (uint32_t pk = 0; pk < ds.rows.size(); ++pk)
        shard_of_row[pk] = mrs.serverOfKey(ObjectKey{0, 0, 0, pk});

    // ── 4. REPEATS full epochs with the requested QUERY distribution ────
    // Each epoch is measured end-to-end: the clients push while
    // runEpoch_RBR_W() collects over R read sub-epochs, dedups against the
    // version cache, dispatches padded read batches, runs MVTSO, and finishes
    // with the write phase.  That whole wall time is the denominator.
    rq3::ShardLoad load(partitions.size());
    std::vector<double> tput_txn, tput_ops, epoch_ms_samples;
    std::vector<double> eta_read_s, eta_write_s, peak_util_s, max_share_s, eta_sent_s;
    uint64_t sub_total = 0, sub_partial = 0; double contacted_sum = 0.0;
    uint64_t fanout_sum = 0, committed_all = 0;
    const EpochConfig cfg = epochConfig();
    R.padding_mode = cfg.pad_uncontacted_shards ? "all_shards" : "contacted_only";
    const uint32_t nshard = static_cast<uint32_t>(partitions.size());
    R.L_exec.assign(nshard, 0);
    R.arrivals_per_subepoch_mean.assign(cfg.R, 0.0);
    static const TxnEngine no_staged_types;   // RQ3 txns are single-stage reads
    std::atomic<uint64_t> outside{0};

    // Build one txn from the shared generator (used by both client modes).
    auto buildTxn = [&](rq3::QueryKeyGen& keys, std::mt19937_64& mix) {
        txn t;
        const std::vector<uint32_t> pks = makeTxnKeys(ds, keys, mix, range_fraction);
        for (size_t q = 0; q < pks.size(); ++q) {
            Operat op;
            op.type             = OpType::READ;
            op.data_primary_key = pks[q];
            op.server_id        = shard_of_row[pks[q]];  // routing from the pos_map
            op.last_one         = (q + 1 == pks.size());
            t.operations.push_back(op);
        }
        return t;
    };

    netemu::resetCounters();             // count only the measured epochs (not initialization)
    for (uint32_t rep = 0; rep < repeats; ++rep) {
        QueriesReceiving qr;
        std::atomic<bool> stop{false};
        std::vector<std::thread> clients;
        for (int c = 0; c < NUM_CLIENTS; ++c) {
            clients.emplace_back([&, c]() {
                rq3::QueryKeyGen keys(ds, st.query, alpha_query, seed + 1000 + 37 * rep + c);
                std::mt19937_64 mix(seed + 7000 + 37 * rep + c);

                if (offered_per_subepoch <= 0.0) {
                    // Legacy BURST mode (reproduces the earlier CSVs): all
                    // txns are pushed immediately, so nearly all of them land
                    // in read sub-epoch 1 and sub-epochs 2..R carry only dummies.
                    for (int i = 0; i < TXNS_PER_CLIENT && !stop; ++i) {
                        txn t = buildTxn(keys, mix);
                        for (int a = 0; a < 2000 && !qr.push_txn(t); ++a) {
                            if (stop) break;
                            std::this_thread::sleep_for(std::chrono::microseconds(100));
                        }
                    }
                    return;
                }

                // PACED open-loop mode: each client emits one txn every
                // interval so all clients together offer `offered_per_subepoch`
                // txn/s (field name kept for struct compatibility; unit is
                // txn/s).  Pacing is in wall-clock time, NOT per sub_epoch_ms:
                // intake stays open while a read batch executes, so a round's
                // arrivals = rate x (sub_epoch_ms + batch execution time).
                // A rejected push (intake not yet open / already closed) is
                // NOT retried — it falls outside this epoch's window and is
                // counted separately, so admitted == offered-within-window.
                const auto interval = std::chrono::microseconds(static_cast<int64_t>(
                    1e6 * NUM_CLIENTS / offered_per_subepoch));
                // Stagger clients so arrivals interleave instead of clumping.
                auto next = clk::now() + (interval * c) / NUM_CLIENTS;
                while (!stop) {
                    std::this_thread::sleep_until(next);
                    next += interval;
                    if (stop) break;
                    if (!qr.push_txn(buildTxn(keys, mix))) ++outside;
                }
            });
        }

        auto t_epoch0 = clk::now();
        std::vector<AssembledTxnResult> results;
        EpochStats es;
        try {
            results = runEpoch_RBR_W(qr, mrs, cfg, no_staged_types, &es);
        } catch (...) { stop = true; for (auto& c : clients) c.join(); throw; }
        const double epoch_ms = ms_between(t_epoch0, clk::now());
        stop = true;
        for (auto& c : clients) c.join();

        uint64_t committed = 0, ops = 0;
        for (const auto& r : results) {
            if (r.committed) { ++committed; ops += r.per_op_result.size(); }
            else { ++R.aborted; ++R.abort_by_reason[r.abort_reason]; }
        }
        R.txns      += static_cast<uint32_t>(results.size());
        R.committed += static_cast<uint32_t>(committed);
        committed_all += committed;

        epoch_ms_samples.push_back(epoch_ms);
        tput_txn.push_back(epoch_ms > 0 ? committed * 1000.0 / epoch_ms : 0.0);
        tput_ops.push_back(epoch_ms > 0 ? ops * 1000.0 / epoch_ms : 0.0);

        // ── Executed load from the dispatched batches ───────────────────
        bool global_bucket = es.write_real_per_bucket.count(-1) > 0;
        for (const auto& m : es.read_real_per_bucket) global_bucket |= m.count(-1) > 0;
        if (global_bucket) {
            std::cerr << "    [warn] per_server_capacity is OFF: batch load is one global "
                         "bucket, per-shard metrics below are meaningless\n";
        }
        uint64_t real_read = 0; double peak = 0.0; uint64_t slots_sent = 0;
        std::vector<double> shares;
        for (uint32_t r = 0; r < es.read_real_per_bucket.size(); ++r) {
            SubEpochRow row;
            row.variant = R.variant; row.setting = R.setting; row.seed = seed;
            row.rep = rep; row.sub_epoch = r;
            row.arrivals = r < es.arrivals_per_sub_epoch.size() ? es.arrivals_per_sub_epoch[r] : 0;
            row.real.assign(nshard, 0);
            uint64_t sum = 0; uint32_t mx = 0;
            for (const auto& [b, n] : es.read_real_per_bucket[r]) {
                if (b < 0 || static_cast<uint32_t>(b) >= nshard) continue;
                row.real[b] = n; sum += n; mx = std::max(mx, n);
                R.L_exec[b] += n;
            }
            real_read += sum;
            peak = std::max(peak, cfg.bread ? double(mx) / cfg.bread : 0.0);
            uint32_t contacted = 0;
            for (auto n : row.real) contacted += (n > 0);
            ++sub_total;
            contacted_sum += nshard ? double(contacted) / nshard : 0.0;
            if (contacted > 0 && contacted < nshard) ++sub_partial;
            // Batches actually on the wire this sub-epoch (one per shard,
            // since admission keeps every shard within bread).
            slots_sent += uint64_t(cfg.pad_uncontacted_shards ? nshard : contacted) * cfg.bread;
            if (sum > 0) shares.push_back(double(mx) / sum);
            if (r < R.arrivals_per_subepoch_mean.size())
                R.arrivals_per_subepoch_mean[r] += double(row.arrivals) / repeats;
            if (sub_rows) sub_rows->push_back(std::move(row));
        }
        uint64_t real_write = 0; uint32_t write_contacted = 0;
        for (const auto& [b, n] : es.write_real_per_bucket)
            if (b >= 0) { real_write += n; write_contacted += (n > 0); }
        R.read_slots_sent  += slots_sent;
        R.write_slots_sent += uint64_t(cfg.pad_uncontacted_shards ? nshard : write_contacted) * cfg.bwrite;
        const double read_slots  = double(nshard) * cfg.R * cfg.bread;
        const double write_slots = double(nshard) * cfg.bwrite;
        eta_read_s.push_back(read_slots > 0 ? real_read / read_slots : 0.0);
        eta_write_s.push_back(write_slots > 0 ? real_write / write_slots : 0.0);
        peak_util_s.push_back(peak);
        eta_sent_s.push_back(slots_sent ? double(real_read) / slots_sent : 0.0);
        double ms = 0; for (double x : shares) ms += x;
        max_share_s.push_back(shares.empty() ? 0.0 : ms / shares.size());
    }

    {
        const netemu::Config& nc = netemu::config();
        R.net_rtt_ms  = nc.rtt_ms;
        R.net_bw_mbps = nc.bw_mbps;
        R.net_shared  = nc.shared ? 1 : 0;
        R.net_bytes   = netemu::counters().bytes.load();
        R.net_bytes_per_commit   = committed_all ? double(R.net_bytes) / committed_all : 0.0;
        R.net_delay_ms_per_epoch = repeats ? netemu::counters().round_delay_us.load() / 1000.0 / repeats : 0.0;
    }
    R.epoch_ms         = median_of(epoch_ms_samples);
    R.throughput_txn_s = median_of(tput_txn);
    R.throughput_ops_s = median_of(tput_ops);
    R.tput_p25         = quantile_of(tput_txn, 0.25);
    R.tput_p75         = quantile_of(tput_txn, 0.75);
    R.eta_read_med     = median_of(eta_read_s);
    R.eta_read_p25     = quantile_of(eta_read_s, 0.25);
    R.eta_read_p75     = quantile_of(eta_read_s, 0.75);
    R.eta_write_med    = median_of(eta_write_s);
    R.peak_util_med    = median_of(peak_util_s);
    R.max_share_med    = median_of(max_share_s);
    R.imbalance_exec   = rq3::imbalance(R.L_exec);
    R.commit_rate      = R.txns ? double(R.committed) / R.txns : 0.0;
    R.pushes_outside_window = outside.load();
    R.contacted_frac       = sub_total ? contacted_sum / sub_total : 0.0;
    R.partial_contact_frac = sub_total ? double(sub_partial) / sub_total : 0.0;
    R.eta_read_sent_med    = median_of(eta_sent_s);
    R.slots_per_commit     = R.committed
        ? double(R.read_slots_sent + R.write_slots_sent) / R.committed : 0.0;

    // ── 5. Figure 5: L_i, the workload assigned to each shard ───────────
    // Measured on the workload itself (one pass of the same generator), so the
    // load picture is independent of how many epochs happened to run:
    //   L_i(queries)  = ops routed to shard i
    //   L_i(oram)     = DISTINCT keys touched on shard i.  Within one epoch the
    //                   proxy coalesces repeated reads of a key into a single
    //                   ORAM op (and keeps only the last write per key), so the
    //                   distinct-key count is the ORAM work shard i performs.
    {
        rq3::QueryKeyGen keys(ds, st.query, alpha_query, seed + 1000);
        std::mt19937_64 mix(seed + 7000);
        std::set<std::pair<int, uint32_t>> distinct;
        const int txns_for_load = NUM_CLIENTS * TXNS_PER_CLIENT;
        uint64_t fan_point = 0, fan_range = 0, n_point = 0, n_range = 0;
        for (int i = 0; i < txns_for_load; ++i) {
            bool is_range = false;
            const std::vector<uint32_t> pks = makeTxnKeys(ds, keys, mix, range_fraction, &is_range);
            std::set<int> touched;
            for (uint32_t pk : pks) {
                const int sh = shard_of_row[pk];
                load.addQuery(sh);
                touched.insert(sh);
                distinct.insert({sh, pk});
                ++R.query_ops;
            }
            fanout_sum += touched.size();
            // Fan-out split by query kind: the point number should be the same
            // for both systems, the range number is where placement shows up.
            if (is_range) { fan_range += touched.size(); ++n_range; }
            else          { fan_point += touched.size(); ++n_point; }
        }
        R.mean_fanout_point = n_point ? double(fan_point) / n_point : 0.0;
        R.mean_fanout_range = n_range ? double(fan_range) / n_range : 0.0;
        R.range_txn_frac    = txns_for_load ? double(n_range) / txns_for_load : 0.0;
        for (const auto& [sh, pk] : distinct) { (void)pk; load.addOramOp(sh); ++R.oram_ops; }
        R.mean_fanout = txns_for_load ? static_cast<double>(fanout_sum) / txns_for_load : 0.0;
    }

    R.L_queries         = load.queries;
    R.L_oram            = load.oram_ops;
    R.imbalance_queries = rq3::imbalance(load.queries);
    R.imbalance_oram    = rq3::imbalance(load.oram_ops);

    mrs.shutdownAllServers();
    return R;
}

// ── Reporting ───────────────────────────────────────────────────────────────
static void printHeader() {
    std::cout << "\n" << std::string(112, '=') << "\n"
              << std::left << std::setw(8)  << "system"
              << std::setw(34) << "setting"
              << std::right << std::setw(8) << "txns"
              << std::setw(8)  << "commit"
              << std::setw(10) << "oram_ops"
              << std::setw(11) << "imbal(q)"
              << std::setw(12) << "imbal(oram)"
              << std::setw(9)  << "fanout"
              << std::setw(12) << "txn/s"
              << std::setw(9)  << "commit%"
              << std::setw(9)  << "eta_rd"
              << std::setw(9)  << "peak" << "\n"
              << std::string(139, '=') << "\n";
}
static void printRow(const Result& r) {
    std::cout << std::left << std::setw(8) << r.variant
              << std::setw(34) << r.setting
              << std::right << std::setw(8) << r.txns
              << std::setw(8)  << r.committed
              << std::setw(10) << r.oram_ops
              << std::setw(11) << std::fixed << std::setprecision(2) << r.imbalance_queries
              << std::setw(12) << r.imbalance_oram
              << std::setw(9)  << std::setprecision(2) << r.mean_fanout
              << std::setw(12) << std::setprecision(1) << r.throughput_txn_s
              << std::setw(9)  << std::setprecision(1) << 100.0 * r.commit_rate
              << std::setw(9)  << std::setprecision(3) << r.eta_read_med
              << std::setw(9)  << std::setprecision(2) << r.peak_util_med << "\n"
              << "        seed=" << r.seed
              << "  L_i(queries)=[" << rq3::joinLoads(r.L_queries, ',')
              << "]  L_i(oram)=[" << rq3::joinLoads(r.L_oram, ',')
              << "]  L_i(exec)=[" << rq3::joinLoads(r.L_exec, ',') << "]"
              << "  arrivals/sub-epoch=[";
    for (size_t i = 0; i < r.arrivals_per_subepoch_mean.size(); ++i)
        std::cout << (i ? "," : "") << std::setprecision(0) << r.arrivals_per_subepoch_mean[i];
    std::cout << "]\n";
}

static void writeCsv(const std::string& path, const std::vector<Result>& rs) {
    std::ofstream f(path);
    f << "system,setting,seed,offered_txn_s,repeats,"
         "k,rows,regions,alpha_data,alpha_query,epsilon,txns,committed,aborted,"
         "query_ops,oram_ops,imbalance_queries,imbalance_oram,mean_fanout,"
         "mean_fanout_point,mean_fanout_range,range_txn_frac,"
         "epoch_ms_median,throughput_txn_s,throughput_ops_s,L_queries,L_oram,partition_rows,"
         "abort_read_batch_full,abort_write_batch_full,abort_write_too_late,abort_cascade,"
         "abort_incomplete,abort_unfinished,abort_dispatch_failed,abort_other,"
         // ── executed-load columns (new) ──
         "commit_rate,throughput_p25,throughput_p75,"
         "eta_read_median,eta_read_p25,eta_read_p75,eta_write_median,"
         "peak_util_median,max_share_median,imbalance_exec,L_exec,"
         "arrivals_per_subepoch,pushes_outside_window,"
         "padding_mode,contacted_frac,partial_contact_frac,eta_read_sent_median,"
         "read_slots_sent,write_slots_sent,slots_per_commit,"
         "net_rtt_ms,net_bw_mbps,net_shared,net_bytes,net_bytes_per_commit,"
         "net_delay_ms_per_epoch,net_bytes_per_slot\n";
    for (const auto& r : rs) {
        f << std::defaultfloat
          << r.variant << ',' << r.setting << ',' << r.seed << ','
          << r.offered_per_subepoch << ',' << r.repeats << ','
          << r.k << ',' << r.rows << ','
          << r.regions << ',' << r.alpha_data << ',' << r.alpha_query << ','
          << r.epsilon << ',' << r.txns << ','
          << r.committed << ',' << r.aborted << ',' << r.query_ops << ',' << r.oram_ops << ','
          << std::fixed << std::setprecision(4) << r.imbalance_queries << ','
          << r.imbalance_oram << ',' << r.mean_fanout << ','
          << r.mean_fanout_point << ',' << r.mean_fanout_range << ','
          << r.range_txn_frac << ','
          << std::setprecision(2) << r.epoch_ms << ','
          << std::setprecision(2) << r.throughput_txn_s << ',' << r.throughput_ops_s << ",\""
          << rq3::joinLoads(r.L_queries, ' ') << "\",\""
          << rq3::joinLoads(r.L_oram, ' ') << "\",\"";
        for (size_t i = 0; i < r.partition_rows.size(); ++i)
            f << (i ? " " : "") << r.partition_rows[i];
        f << '"';

        auto n = [&](AbortReason why) {
            auto it = r.abort_by_reason.find(why);
            return it == r.abort_by_reason.end() ? uint64_t{0} : it->second;
        };
        uint64_t named = 0, all = 0;
        for (AbortReason why : {AbortReason::READ_BATCH_FULL, AbortReason::WRITE_BATCH_FULL,
                                AbortReason::WRITE_TOO_LATE, AbortReason::CASCADE,
                                AbortReason::INCOMPLETE, AbortReason::UNFINISHED,
                                AbortReason::DISPATCH_FAILED}) {
            f << ',' << n(why);
            named += n(why);
        }
        for (const auto& kv : r.abort_by_reason) all += kv.second;
        f << ',' << (all - named);

        f << ',' << std::fixed << std::setprecision(4) << r.commit_rate
          << ',' << std::setprecision(2) << r.tput_p25 << ',' << r.tput_p75
          << ',' << std::setprecision(4) << r.eta_read_med << ',' << r.eta_read_p25
          << ',' << r.eta_read_p75 << ',' << r.eta_write_med
          << ',' << r.peak_util_med << ',' << r.max_share_med << ',' << r.imbalance_exec
          << ",\"" << rq3::joinLoads(r.L_exec, ' ') << "\",\"";
        for (size_t i = 0; i < r.arrivals_per_subepoch_mean.size(); ++i)
            f << (i ? " " : "") << std::setprecision(1) << r.arrivals_per_subepoch_mean[i];
        f << "\"," << r.pushes_outside_window
          << ',' << r.padding_mode << ',' << std::setprecision(4) << r.contacted_frac
          << ',' << r.partial_contact_frac << ',' << r.eta_read_sent_med
          << ',' << r.read_slots_sent << ',' << r.write_slots_sent
          << ',' << std::setprecision(3) << r.slots_per_commit
          << ',' << std::setprecision(3) << r.net_rtt_ms << ',' << r.net_bw_mbps << ',' << r.net_shared
          << ',' << r.net_bytes << ',' << std::setprecision(1) << r.net_bytes_per_commit
          << ',' << std::setprecision(2) << r.net_delay_ms_per_epoch
          << ',' << std::setprecision(1) << r.net_bytes_per_slot_mean << '\n';
    }
    std::cout << "\nWrote " << path << "\n";
}

// One row per (system, setting, seed, rep, read sub-epoch).  This is the raw
// material for max_i n_i(E), padding efficiency and the per-shard migration
// plots; everything in the summary CSV can be recomputed from it.
static void writeSubEpochCsv(const std::string& path, const std::vector<SubEpochRow>& rows,
                             uint32_t k, uint32_t bread) {
    std::ofstream f(path);
    const char* mode = g_contacted_only ? "contacted_only" : "all_shards";
    f << "system,setting,padding_mode,seed,rep,sub_epoch,arrivals,bread,sum_real,max_real,contacted";
    for (uint32_t i = 0; i < k; ++i) f << ",real_s" << i;
    f << "\n";
    for (const auto& r : rows) {
        uint64_t sum = 0; uint32_t mx = 0;
        for (auto n : r.real) { sum += n; mx = std::max(mx, n); }
        uint32_t contacted = 0;
        for (auto n : r.real) contacted += (n > 0);
        f << r.variant << ',' << r.setting << ',' << mode << ',' << r.seed << ',' << r.rep << ','
          << r.sub_epoch << ',' << r.arrivals << ',' << bread << ',' << sum << ',' << mx
          << ',' << contacted;
        for (uint32_t i = 0; i < k; ++i) f << ',' << (i < r.real.size() ? r.real[i] : 0);
        f << "\n";
    }
    std::cout << "Wrote " << path << "\n";
}

int main(int argc, char** argv) {
    const uint32_t k        = argc > 1 ? std::stoul(argv[1]) : 4;
    const uint32_t rows     = argc > 2 ? std::stoul(argv[2]) : 20000;
    const uint32_t regions  = argc > 3 ? std::stoul(argv[3]) : 40;
    const double   alpha    = argc > 4 ? std::stod(argv[4])  : 1.0;
    const double   epsilon  = argc > 5 ? std::stod(argv[5])  : 1.0;
    const int      base_port= argc > 6 ? std::stoi(argv[6])  : 8881;
    const std::string csv   = argc > 7 ? argv[7] : "rq3_distribution_shift.csv";
    // Share of transactions that are RANGE queries (a predicate over the
    // partitioning attribute) rather than point lookups.  0 reproduces the
    // point-only workload, in which placement cannot help DePart at all.
    const double range_fraction = argc > 8 ? std::stod(argv[8]) : 0.5;
    // Query-skew strength, independent of the data skew.  Sweep it (0, 0.5,
    // 1.0, 1.5, 2.0) with the data skew pinned to answer "is this result
    // specific to the chosen skew?".  Defaults to alpha, which reproduces every
    // earlier run bit for bit.
    const double alpha_query = argc > 9 ? std::stod(argv[9]) : alpha;
    // Offered load as an open-loop arrival rate in txn/s across all clients.
    // 0 = legacy burst.  Balanced capacity at k=4, bread=256, mixed workload
    // is roughly 500 committed txn/s, so sweep ~50..1500 to span
    // under-load -> saturation -> overload.
    const double offered   = argc > 10 ? std::stod(argv[10]) : 0.0;
    const uint32_t repeats = argc > 11 ? std::stoul(argv[11]) : DEFAULT_REPEATS;
    const uint32_t nseeds  = argc > 12 ? std::stoul(argv[12]) : 1;
    g_contacted_only       = argc > 13 ? (std::stoi(argv[13]) != 0) : true;
    g_cap_T                = argc > 14 ? std::stod(argv[14]) : 0.0;
    std::cout << "Partitioning: " << (g_cap_T > 0.0
                  ? "Algorithm 4, cap_T=" + std::to_string(g_cap_T) +
                    " (K decided per run; Random uses the same K; k=" + std::to_string(k) +
                    " listeners available)"
                  : "Algorithm 3, k=" + std::to_string(k) + " partitions") << "\n";
    std::cout << "Padding: " << (g_contacted_only
                  ? "contacted_only (only shards with real ops get a padded batch; C visible)"
                  : "all_shards (every shard gets a padded batch; C hidden)") << "\n";
    std::string sub_csv = csv;
    if (sub_csv.size() > 4 && sub_csv.substr(sub_csv.size() - 4) == ".csv")
        sub_csv.resize(sub_csv.size() - 4);
    sub_csv += "_subepoch.csv";

    std::cout << "RQ3 — distribution shift:  k=" << k << "  rows=" << rows
              << "  regions=" << regions << "  alpha=" << alpha
              << "  epsilon=" << epsilon << "  ports=" << base_port << ".."
              << (base_port + static_cast<int>(k) - 1) << "\n"
              << "Query mix: " << static_cast<int>(range_fraction * 100)
              << "% range queries (window = " << RANGE_WIDTH_REGIONS << " regions, <= "
              << RANGE_MAX_ROWS << " rows each), rest point lookups\n"
              << "Imbalance = max_i L_i / ((1/k) * sum_i L_i);  1.0 = balanced, "
              << k << " = one shard holds everything.\n"
              << "Arrivals: " << (offered > 0 ? std::to_string(offered) + " txn/s (paced, open loop)"
                                               : std::string("BURST (legacy)"))
              << "   repeats=" << repeats << "   seeds=" << nseeds << "\n"
              << "Network emulation: " << netemu::describe() << "\n";

    requireListeners("127.0.0.1", base_port, k, "RQ3");
    std::vector<Result> results;
    std::vector<SubEpochRow> sub_rows;
    printHeader();
    for (uint32_t si = 0; si < nseeds; ++si) {
        const uint64_t seed = 42 + si;
        std::map<std::string, uint32_t> depart_K;     // Algorithm 4: setting -> K
        for (Variant v : {Variant::DEPART, Variant::RANDOM}) {
            for (const auto& st : rq3::settings()) {
                try {
                    uint32_t kk = k;
                    // Matched shard counts: Random uses the K that DePart actually produced for
                    // this (seed, setting) — under Algorithm 4 (K decided by cap_T) AND under
                    // Algorithm 3, where MaxPartCount may return FEWER than k partitions (e.g. 13
                    // of 16 on skewed data).  Previously Algorithm 3 gave Random all k shards.
                    // DEPART_RANDOM_FIXED_K=1 restores that old behaviour (to reproduce old CSVs).
                    static const bool fixed_k = [] {
                        const char* e = std::getenv("DEPART_RANDOM_FIXED_K");
                        return e && std::string(e) == "1";
                    }();
                    if (v == Variant::RANDOM && (g_cap_T > 0.0 || !fixed_k)) {
                        auto it = depart_K.find(st.name);
                        if (it == depart_K.end()) {
                            std::cerr << "  [Random / " << st.name << "] no DePart K for this setting, skipping\n";
                            continue;
                        }
                        kk = it->second;
                    }
                    Result r = runOne(v, st, kk, rows, regions, alpha, alpha_query, epsilon,
                                      base_port, seed, range_fraction,
                                      offered, repeats, &sub_rows);
                    if (v == Variant::DEPART) depart_K[st.name] = r.k;
                    printRow(r);
                    results.push_back(std::move(r));
                } catch (const std::exception& e) {
                    std::cerr << "  [" << variantName(v) << " / " << st.name << " / seed "
                              << seed << "] FAILED: " << e.what() << "\n";
                }
            }
        }
        // Flush after every seed so a crash late in a long sweep loses little.
        writeCsv(csv, results);
        writeSubEpochCsv(sub_csv, sub_rows, k, epochConfig().bread);
    }
    std::cout << "\nOasisDB is not built in this repo — run it separately and add its\n"
                 "rows to the CSV with system=OasisDB to complete Figure 6.\n";
    return 0;
}