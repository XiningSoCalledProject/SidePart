//
// RQ4_ShardScaling.cpp
// ----------------------------------------------------------------------------
// RQ4: How does DePart scale with the number of ORAM shards, and where does its
// performance saturate?
//
//   Figure 7 — x: number of ORAM shards / servers (1, 2, 4, 8)
//              y: transaction throughput (txn/s)
//
// Everything except the shard count is held FIXED, as the doc requires:
//   * workload            — the same transaction list is replayed at every k
//   * total database size — the same rows, only their partitioning changes
//   * privacy configuration — same ε, same DP partitioning algorithm
//   * IOO policy          — TOTAL insert-reserve capacity is constant, so each
//                           shard gets IOO_TOTAL / k blocks.  (The reserve is
//                           per shard, so leaving it per-shard-constant would
//                           silently GROW total insert capacity with k and make
//                           the comparison unfair.)
//
// Reported alongside throughput, because they are what explains saturation:
//   speedup(k), efficiency(k)   — vs the k=1 point (1.0 = linear scaling)
//   marginal gain               — the diminishing-returns curve
//   saturation point            — first k whose marginal gain < MIN_GAIN
//   mean shard fan-out          — parallelism the workload actually exposes
//   shard load imbalance        — max_i L_i / mean_i L_i (a straggler caps the
//                                 epoch, since an epoch ends when the slowest
//                                 shard finishes)
//   ORAM tree height per shard  — smaller shards = shallower trees = cheaper ops
//
// This is not a comparison against systems built for large-scale distributed
// execution; it measures how well DePart uses additional shard-level
// parallelism and where adding shards stops paying off.
//
// PREREQUISITE: max(SHARD_COUNTS) listeners must be running:
//       for p in $(seq 8881 8888); do ./build/bin/Servers_MultiRingORAM $p & done
//
// Usage:
//   ./RQ4_ShardScaling [rows] [regions] [epsilon] [repeats] [base_port] [csv]
//     rows      dataset rows (total DB size, fixed across k)   default 20000
//     regions   distinct partition-attribute values            default 64
//     epsilon   DP budget (fixed across k)                     default 1.0
//     repeats   epochs per k; throughput is the median         default 5
//     base_port first listener port                            default 8881
//     csv       output path                        default rq4_shard_scaling.csv
//     shards    [arg 7] comma-separated shard counts           default 1,2,4,8
//     offered   [arg 8] open-loop arrival RATE in txn/s, paced in
//               wall-clock time exactly as in RQ2/RQ3.  Identical at every k,
//               so pick it high enough to saturate the LARGEST k; throughput
//               is then each k's service capacity.  0 = legacy BURST
//               (TXNS_PER_CLIENT pushed at once per epoch)     default 0
//     dist      [arg 10] uniform | skewed                     default uniform
//               uniform: uniform data AND uniform query anchors.  RQ4 isolates
//               shard-count scaling; under Zipf skew, hot keys are read by many
//               txns in the same round and COALESCED into one ORAM slot, so
//               measured throughput grows with offered load (k=4: 2092 txn/s at
//               4000 offered vs 5438 at 16000) and the speedup mixes shard
//               capacity with dedup gains.  Skew is studied in RQ3.
//               skewed: the original setting (Zipf alpha=1 data and queries).
//     pool      [arg 11] replayed transaction pool size       default 200000
//     warmup    [arg 9] leading epochs per k run but NOT measured
//               (bulk init leaves every record in the stash; the first
//               epochs pay the drain cost, and a bigger shard pays more,
//               which would inflate small-k latency)           default 1
//
// Padding and admission match RQ2/RQ3: contacted-only padding, one bread-sized
// batch per shard per read round, overflow aborts with READ_BATCH_FULL.
// ----------------------------------------------------------------------------

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"
#include "Repartition.h"
#include "RQ2_Affinity.h"
#include "RQ3_Workload.h"
#include "RQ4_Scaling.h"
#include "RQ_Preflight.h"

using clk = std::chrono::high_resolution_clock;
static double ms_between(clk::time_point a, clk::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ── Fixed across the whole sweep ────────────────────────────────────────────
static std::vector<uint32_t> SHARD_COUNTS = {1, 2, 4, 8};   // argv[7] overrides
static double   g_offered_txn_s = 0.0;                        // argv[8]; 0 = burst
static uint32_t g_warmup        = 1;                          // argv[9]
static bool     g_uniform       = true;                       // argv[10]
static constexpr double   SKEW_ALPHA      = 1.0;
// Size of the transaction pool the clients draw from (argv[11]).  It used to
// be 3000: at ~8000 admitted txns per epoch every pool entry then recurs ~3x
// per epoch, identical reads coalesce into one ORAM slot, and throughput
// measures the pool's repetition rate instead of shard capacity (k=1 served
// ~4.6 reads per slot even with UNIFORM keys).  A large pool keeps repeats
// rare while still replaying the SAME sequence at every k (fixed seed).
static uint32_t WORKLOAD_TXNS = 200000;
static constexpr uint32_t OPS_PER_TXN     = 3;
static constexpr int      NUM_CLIENTS     = 8;
static constexpr int      TXNS_PER_CLIENT = 400;
static constexpr uint32_t IOO_TOTAL       = 1024;   // TOTAL insert reserve, split over k
static constexpr uint32_t BUCKET_SIZE     = 8;
static constexpr uint32_t S_PARAM         = 4;
static constexpr double   MIN_GAIN        = 0.10;   // saturation threshold (+10%)

static EpochConfig epochConfig() {
    EpochConfig c;
    c.R            = 3;
    c.bread        = 256;
    c.bwrite       = 512;
    c.sub_epoch_ms = 60;
    c.pad_batches  = true;      // security mode: fixed-size batches
    // Same as RQ2 / RQ3 (the paper's threat model): only shards with real ops
    // in a round get a batch, padded to bread / bwrite; C is visible.
    c.pad_uncontacted_shards = false;
    c.per_server_capacity    = true;
    c.enforce_batch_capacity = true;
    return c;
}

struct Workload { std::vector<std::vector<uint32_t>> txns; };

static Workload buildWorkload(const rq3::Dataset& d, uint32_t num_txns,
                              uint32_t ops_per_txn, double alpha, uint64_t seed,
                              rq3::QueryDist qdist) {
    Workload w;
    rq3::QueryKeyGen pick(d, qdist, alpha, seed);
    std::mt19937_64 rng(seed ^ 0x9E3779B9u);
    for (uint32_t t = 0; t < num_txns; ++t) {
        const uint32_t anchor = pick.nextKey();
        const auto& rows = d.rows_of_region[d.region_of_row[anchor]];
        std::vector<uint32_t> keys{anchor};
        for (uint32_t q = 1; q < ops_per_txn && !rows.empty(); ++q)
            keys.push_back(rows[rng() % rows.size()]);
        w.txns.push_back(std::move(keys));
    }
    return w;
}

static double median_of(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return (n % 2) ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

static double quantile_of(std::vector<double> v, double q) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const double pos = q * (v.size() - 1);
    const size_t lo = static_cast<size_t>(std::floor(pos));
    const size_t hi = std::min(lo + 1, v.size() - 1);
    return v[lo] + (pos - lo) * (v[hi] - v[lo]);
}

struct Row {
    uint32_t k = 0;
    uint32_t partitions = 0, ioo_per_shard = 0;
    uint32_t real_total = 0, noisy_total = 0;
    uint64_t dummy_total = 0, stash_reserve_total = 0;
    double   mean_tree_height = 0, max_tree_height = 0;
    double   fanout = 0, imbalance_queries = 0;
    uint64_t committed = 0, aborted = 0, committed_ops = 0;
    double   init_ms = 0, epoch_ms_median = 0;
    double   throughput_txn_s = 0, throughput_ops_s = 0;
    double   speedup = 0, efficiency = 0, marginal_gain = 0;
    std::vector<uint64_t> L_queries;
    // Why work was rejected.  At k=8 throughput fell while commit rate rose;
    // the reason distribution says whether the extra shards were starved of
    // parallelism (fan-out already 1.0) or whether one hot shard could not fit
    // its reads into R x bread.  Those two have opposite implications.
    std::map<AbortReason, uint64_t> abort_by_reason;
    uint64_t pushes_outside = 0;      // paced: arrivals rejected (intake closed)
    double   epoch_ms_total = 0;      // measured epochs only
    double   epoch_ms_p25 = 0, epoch_ms_p75 = 0;
    double   tput_p25 = 0, tput_p75 = 0;
};


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

static Row runShardCount(const rq3::Dataset& d, const Workload& wl, uint32_t k,
                         double epsilon, uint32_t repeats, int base_port, uint64_t seed) {
    Row R; R.k = k;
    const std::string schema = d.schema();
    const std::vector<int> attr = {1};

    // ── partition the SAME data with the SAME epsilon into k shards ─────
    Repartition rp;
    auto parts = rp.Repartition_Main(d.rows, schema, attr, epsilon, 0.0, 0.0, true, k, 0, k);
    const BinInfo& bi = rp.getBinInfo();
    R.partitions = static_cast<uint32_t>(parts.size());

    std::vector<ServerInfo> servers(parts.size());
    for (size_t i = 0; i < parts.size(); ++i) {
        servers[i].server_id     = static_cast<int>(i);
        servers[i].port          = base_port + static_cast<int>(i);
        servers[i].partition_id  = static_cast<uint32_t>(i);
        servers[i].assigned_bins = parts[i].index;
    }

    MultiRingORAM_Servers mrs;
    // IOO policy unchanged: TOTAL reserve fixed, so per shard it is IOO_TOTAL / k.
    R.ioo_per_shard = std::max<uint32_t>(1, IOO_TOTAL / std::max<uint32_t>(1, R.partitions));
    mrs.setInsertReservePerPartition(R.ioo_per_shard);
    mrs.distributeDataToPartitions(bi, parts, servers, d.rows);

    const uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    const uint32_t block_size  = tuple_width + 16 + 2 * sizeof(uint32_t);   // + AES::BLOCKSIZE

    auto t_init = clk::now();
    // BULK init (not ServerInitialization): the per-record path calls
    // oram->access(block, INSERT, value) for EVERY row, and each of those is a
    // ReadPath + eviction, i.e. tens to hundreds of network round-trips.  At
    // 6000 rows per shard that is millions of RTTs and the run looks hung.
    // ServerInitializationBulk loads the records straight into the stash
    // (O(N), local) and streams the whole dummy tree up in one pass.
    for (size_t i = 0; i < parts.size(); ++i)
        if (useBulkInit())
            mrs.ServerInitializationBulk(servers[i], parts[i], bi, d.rows, schema, BUCKET_SIZE,
                                     "RQ4_S" + std::to_string(i), block_size, "127.0.0.1", S_PARAM);
        else
            mrs.ServerInitialization(servers[i], parts[i], bi, d.rows, schema, BUCKET_SIZE,
                                     "RQ4_S" + std::to_string(i), block_size, "127.0.0.1", S_PARAM);
    std::cerr << "    [init] forking " << parts.size() << " shards, waiting..." << std::endl;
    mrs.waitForAllServersReady();
    std::cerr << "    [init] all shards ready" << std::endl;
    R.init_ms = ms_between(t_init, clk::now());

    // Per-shard ORAM geometry: smaller shards → shallower trees → cheaper ops.
    double h_sum = 0;
    for (const auto& p : parts) {
        const uint32_t n = std::max<uint32_t>(1, p.noisy_synopsis + R.ioo_per_shard);
        const double h = std::ceil(std::log2(static_cast<double>(n))) + 1;
        h_sum += h;
        R.max_tree_height = std::max(R.max_tree_height, h);
        R.real_total          += p.synopsis;
        R.noisy_total         += p.noisy_synopsis;
        R.dummy_total         += p.dummy_num;
        R.stash_reserve_total += p.noise_stash_reserve;
    }
    R.mean_tree_height = parts.empty() ? 0 : h_sum / parts.size();

    std::vector<int> shard_of_row(d.rows.size(), -1);
    for (uint32_t pk = 0; pk < d.rows.size(); ++pk)
        shard_of_row[pk] = mrs.serverOfKey(ObjectKey{0, 0, 0, pk});
    R.fanout = rq2::meanFanout(wl.txns, [&](uint32_t key) { return shard_of_row[key]; });

    // ── repeats epochs of the SAME workload; report the median ─────────
    std::vector<double> tput_txn, tput_ops, epoch_ms;
    rq3::ShardLoad load(parts.size());
    std::atomic<uint64_t> outside{0};
    auto buildTxn = [&](std::mt19937_64& rng) {
        const auto& keys = wl.txns[rng() % wl.txns.size()];
        txn t;
        for (size_t q = 0; q < keys.size(); ++q) {
            Operat op;
            op.type             = OpType::READ;
            op.data_primary_key = keys[q];
            op.server_id        = shard_of_row[keys[q]];
            op.last_one         = (q + 1 == keys.size());
            t.operations.push_back(op);
        }
        return t;
    };
    for (uint32_t rep = 0; rep < g_warmup + repeats; ++rep) {
        const bool measured = rep >= g_warmup;
        QueriesReceiving qr;
        std::atomic<bool> stop{false};
        std::vector<std::thread> clients;
        for (int c = 0; c < NUM_CLIENTS; ++c) {
            clients.emplace_back([&, c]() {
                std::mt19937_64 rng(seed + 31 * rep + c);
                if (g_offered_txn_s <= 0.0) {
                    // Legacy BURST (reproduces the earlier CSVs).  NO `&& !stop`:
                    // every client pushes exactly TXNS_PER_CLIENT so the offered
                    // count is identical at every k; what the epoch cannot admit
                    // is dropped after a short retry.
                    for (int i = 0; i < TXNS_PER_CLIENT; ++i) {
                        txn t = buildTxn(rng);
                        for (int a = 0; a < 200 && !qr.push_txn(t); ++a)
                            std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                    return;
                }
                // PACED open loop (as RQ2 / RQ3).  The RATE is identical at
                // every k by construction, so the offered load no longer
                // depends on epoch length.  Rejected pushes are dropped.
                const auto interval = std::chrono::microseconds(static_cast<int64_t>(
                    1e6 * NUM_CLIENTS / g_offered_txn_s));
                auto next = clk::now() + (interval * c) / NUM_CLIENTS;
                while (!stop) {
                    std::this_thread::sleep_until(next);
                    next += interval;
                    if (stop) break;
                    if (!qr.push_txn(buildTxn(rng)) && measured) ++outside;
                }
            });
        }

        auto t0 = clk::now();
        std::vector<AssembledTxnResult> results;
        try {
            results = runEpoch_RBR_W(qr, mrs, epochConfig());
        } catch (...) { stop = true; for (auto& c : clients) c.join(); throw; }
        const double ms = ms_between(t0, clk::now());
        stop = true;
        for (auto& c : clients) c.join();

        if (!measured) {
            std::cerr << "    [warmup] epoch " << rep << ": " << results.size()
                      << " txns, " << std::fixed << std::setprecision(0) << ms << " ms (not measured)\n";
            continue;
        }
        uint64_t committed = 0, aborted = 0, ops = 0;
        for (const auto& r : results) {
            if (r.committed) { ++committed; ops += r.per_op_result.size(); }  // ops of committed txns
            else { ++aborted; ++R.abort_by_reason[r.abort_reason]; }
        }
        R.committed += committed; R.aborted += aborted; R.committed_ops += ops;
        epoch_ms.push_back(ms);
        R.epoch_ms_total += ms;
        tput_txn.push_back(ms > 0 ? committed * 1000.0 / ms : 0.0);
        tput_ops.push_back(ms > 0 ? ops * 1000.0 / ms : 0.0);   // committed ops/s
    }

    // Load distribution of the workload under THIS partitioning (Figure 7's
    // explanation: a straggler shard caps the epoch).
    for (const auto& keys : wl.txns)
        for (uint32_t key : keys) load.addQuery(shard_of_row[key]);
    R.L_queries         = load.queries;
    R.imbalance_queries = rq3::imbalance(load.queries);

    R.epoch_ms_median  = median_of(epoch_ms);
    R.throughput_txn_s = median_of(tput_txn);
    R.throughput_ops_s = median_of(tput_ops);
    R.tput_p25 = quantile_of(tput_txn, .25); R.tput_p75 = quantile_of(tput_txn, .75);
    R.epoch_ms_p25 = quantile_of(epoch_ms, .25); R.epoch_ms_p75 = quantile_of(epoch_ms, .75);
    R.pushes_outside = outside;

    mrs.shutdownAllServers();
    return R;
}

static void writeCsv(const std::string& path, const std::vector<Row>& rows,
                     uint32_t dataset_rows, double epsilon, uint32_t repeats) {
    std::ofstream f(path);
    f << "shards,partitions,rows,epsilon,repeats,ioo_per_shard,ioo_total,"
         "real_total,noisy_total,dummy_total,stash_reserve_total,"
         "mean_tree_height,max_tree_height,fanout,imbalance_queries,"
         "committed,aborted,offered,init_ms,epoch_ms_median,throughput_txn_s,"
         "throughput_committed_ops_s,"
         // Header order must match the write order in the loop below: the abort
         // columns are appended AFTER L_queries.  Listing them here instead
         // shifted every later field by five, which silently turned speedup and
         // efficiency into 0.000 and put efficiency values in the abort columns.
         "speedup,efficiency,marginal_gain,L_queries,"
         "abort_read_batch_full,abort_write_batch_full,abort_cascade,"
         "abort_incomplete,abort_other,"
         "offered_txn_s,warmup_epochs,commit_rate,admitted_txn_s,pushes_outside,"
         "throughput_p25,throughput_p75,epoch_ms_p25,epoch_ms_p75,distribution\n";
    for (const auto& r : rows) {
        f << r.k << ',' << r.partitions << ',' << dataset_rows << ',' << epsilon << ','
          << repeats << ',' << r.ioo_per_shard << ',' << (r.ioo_per_shard * r.partitions) << ','
          << r.real_total << ',' << r.noisy_total << ',' << r.dummy_total << ','
          << r.stash_reserve_total << ',' << std::fixed << std::setprecision(2)
          << r.mean_tree_height << ',' << r.max_tree_height << ','
          << std::setprecision(3) << r.fanout << ',' << r.imbalance_queries << ','
          << r.committed << ',' << r.aborted << ','
          << (r.committed + r.aborted) << ','
          << std::setprecision(2) << r.init_ms << ',' << r.epoch_ms_median << ','
          << r.throughput_txn_s << ',' << r.throughput_ops_s << ','
          << std::setprecision(3) << r.speedup << ',' << r.efficiency << ','
          << r.marginal_gain << ",\"" << rq3::joinLoads(r.L_queries, ' ') << "\"";
        auto n = [&](AbortReason why) {
            auto it = r.abort_by_reason.find(why);
            return it == r.abort_by_reason.end() ? uint64_t{0} : it->second;
        };
        uint64_t named = 0, all = 0;
        for (AbortReason why : {AbortReason::READ_BATCH_FULL, AbortReason::WRITE_BATCH_FULL,
                                AbortReason::CASCADE, AbortReason::INCOMPLETE}) {
            f << ',' << n(why);
            named += n(why);
        }
        for (const auto& kv : r.abort_by_reason) all += kv.second;
        f << ',' << (all - named);
        const uint64_t admitted = r.committed + r.aborted;
        f << ',' << g_offered_txn_s << ',' << g_warmup << ','
          << std::setprecision(4) << (admitted ? double(r.committed) / admitted : 0.0) << ','
          << std::setprecision(2)
          << (r.epoch_ms_total > 0 ? admitted * 1000.0 / r.epoch_ms_total : 0.0) << ','
          << r.pushes_outside << ',' << r.tput_p25 << ',' << r.tput_p75 << ','
          << r.epoch_ms_p25 << ',' << r.epoch_ms_p75 << ','
          << (g_uniform ? "uniform" : "skewed") << '\n';
    }
    std::cout << "Wrote " << path << "  (Figure 7)\n";
}

int main(int argc, char** argv) {
    const uint32_t rows    = argc > 1 ? std::stoul(argv[1]) : 20000;
    const uint32_t regions = argc > 2 ? std::stoul(argv[2]) : 64;
    const double   epsilon = argc > 3 ? std::stod(argv[3])  : 1.0;
    const uint32_t repeats = argc > 4 ? std::stoul(argv[4]) : 5;
    const int      base_port = argc > 5 ? std::stoi(argv[5]) : 8881;
    const std::string csv  = argc > 6 ? argv[6] : "rq4_shard_scaling.csv";
    if (argc > 7) {
        SHARD_COUNTS.clear();
        std::stringstream ss(argv[7]); std::string tok;
        while (std::getline(ss, tok, ',')) if (!tok.empty()) SHARD_COUNTS.push_back(std::stoul(tok));
    }
    g_offered_txn_s = argc > 8 ? std::stod(argv[8]) : 0.0;
    g_warmup        = argc > 9 ? static_cast<uint32_t>(std::stoul(argv[9])) : 1;
    if (argc > 10) g_uniform = std::string(argv[10]) != "skewed";
    if (argc > 11) WORKLOAD_TXNS = static_cast<uint32_t>(std::stoul(argv[11]));

    uint32_t max_k = 0;
    for (uint32_t k : SHARD_COUNTS) max_k = std::max(max_k, k);
    std::cout << "RQ4 — shard scaling:  rows=" << rows << "  regions=" << regions
              << "  epsilon=" << epsilon << "  repeats=" << repeats << "\n"
              << "Held fixed across k: workload, total DB size, privacy config, and\n"
              << "IOO policy (TOTAL reserve = " << IOO_TOTAL
              << " blocks, so each shard gets " << IOO_TOTAL << "/k).\n"
              << "Needs " << max_k << " listeners on ports " << base_port << ".."
              << base_port + static_cast<int>(max_k) - 1 << "\n"
              << "Padding: " << (epochConfig().pad_uncontacted_shards ? "all shards" : "contacted-only")
              << "   Arrivals: " << (g_offered_txn_s > 0
                     ? std::to_string(g_offered_txn_s) + " txn/s (paced, open loop)"
                     : std::string("BURST (legacy)"))
              << "   warmup epochs/k: " << g_warmup
              << "   distribution: " << (g_uniform ? "uniform data + queries" : "skewed (Zipf 1.0)")
              << "   txn pool: " << WORKLOAD_TXNS
              << "\n\n";

    rq3::Dataset d = rq3::generateDataset(rows, regions,
        g_uniform ? rq3::DataDist::UNIFORM : rq3::DataDist::SKEWED, SKEW_ALPHA, 42);
    Workload wl = buildWorkload(d, WORKLOAD_TXNS, OPS_PER_TXN, SKEW_ALPHA, 43,
        g_uniform ? rq3::QueryDist::UNIFORM : rq3::QueryDist::SKEWED_MATCHED);

    requireListeners("127.0.0.1", base_port, max_k, "RQ4");
    std::vector<Row> results;
    for (uint32_t k : SHARD_COUNTS) {
        std::cout << "── k=" << k << " shards ──\n";
        try {
            Row r = runShardCount(d, wl, k, epsilon, repeats, base_port, 7);
            std::cout << "   partitions=" << r.partitions
                      << "  ioo/shard=" << r.ioo_per_shard
                      << "  tree_h(mean/max)=" << std::fixed << std::setprecision(1)
                      << r.mean_tree_height << "/" << r.max_tree_height
                      << "  fanout=" << std::setprecision(2) << r.fanout
                      << "  imbalance=" << r.imbalance_queries
                      << "  epoch_ms=" << std::setprecision(1) << r.epoch_ms_median
                      << "  txn/s=" << r.throughput_txn_s << "\n";
            if (r.partitions != k)
                std::cerr << "   [warn] k=" << k << " but the partitioner produced "
                          << r.partitions << " partitions — this point is NOT a "
                             "k=" << k << " measurement.  Raise `regions` so the DP "
                             "algorithm has enough bins to split.\n";
            results.push_back(r);
        } catch (const std::exception& e) {
            std::cerr << "   FAILED: " << e.what() << "\n";
        }
    }

    // ── scaling summary (Figure 7's takeaway) ──────────────────────────
    std::vector<rq4::Point> pts;
    for (const auto& r : results) pts.push_back({r.k, r.throughput_txn_s});
    for (size_t i = 0; i < results.size(); ++i) {
        results[i].speedup       = rq4::speedup(pts, i);
        results[i].efficiency    = rq4::efficiency(pts, i);
        results[i].marginal_gain = rq4::marginalGain(pts, i);
    }

    std::cout << "\n" << std::string(78, '=') << "\n"
              << std::left << std::setw(8) << "shards" << std::right
              << std::setw(12) << "txn/s" << std::setw(10) << "speedup"
              << std::setw(12) << "efficiency" << std::setw(14) << "marginal gain"
              << std::setw(10) << "fanout" << "\n" << std::string(78, '=') << "\n";
    for (const auto& r : results)
        std::cout << std::left << std::setw(8) << r.k << std::right
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.throughput_txn_s
                  << std::setw(10) << std::setprecision(2) << r.speedup
                  << std::setw(12) << r.efficiency
                  << std::setw(13) << std::setprecision(1) << (r.marginal_gain * 100) << "%"
                  << std::setw(10) << std::setprecision(2) << r.fanout << "\n";

    if (!pts.empty()) {
        const uint32_t knee = rq4::saturationK(pts, MIN_GAIN);
        std::cout << "\nSaturation point: k=" << knee
                  << "  (first shard count whose next step gains < "
                  << static_cast<int>(MIN_GAIN * 100) << "%)\n";
        if (knee == pts.back().k)
            std::cout << "  → still scaling at k=" << knee
                      << "; extend SHARD_COUNTS to find the knee.\n";
    }
    writeCsv(csv, results, rows, epsilon, repeats);
    return 0;
}