//
// RQ5_GrowthRate.cpp   (10/10/26)
// ----------------------------------------------------------------------------
// Growth-rate sweep: how does the insert rate interact with the IOO and the
// repartitioning schedule, and what does each DP layout release buy?
//
// Held FIXED for every point:
//   E_max   epochs between two scheduled repartitionings (each repartitioning
//           is one DP layout release costing eps_r)
//   C_IOO   TOTAL logical IOO capacity = k x C_IOO / k blocks per shard
//   eps_r   privacy budget of one layout release
//   the read workload, the dataset, k, the epoch configuration
// VARIED: only the insert rate lambda_ins, chosen through
//     rho = (inserts expected before E_max) / C_IOO,
//     lambda_ins = rho x C_IOO / E_max   inserts per epoch.
// Default rho grid {0.25, 0.5, 1, 1.5, 2} = slow / matched / fast growth.
//
// Overflow policy (argv[8], what happens once a shard's IOO is full before E_max):
//   backpressure  no standby IOO: an insert routed to a full shard is rejected
//                 at admission (counted as inserts_rejected); repartitioning
//                 still waits for E_max                              (default)
//   standby       each shard has a pre-provisioned, public standby IOO of
//                 STANDBY_FACTOR x its IOO (physical reserve = (1 + f) x C);
//                 inserts past C use it and are reported as standby blocks
//                 activated; still repartition at E_max.  Rejects only once
//                 the standby is also full.
//   early         repartition AS SOON AS the tightest shard cannot take the
//                 next epoch's inserts (the RQ2 trigger), then restart the
//                 E_max clock: no rejections, but more releases.
//
// Insert placement follows the data: the region of a new row is the region of
// a uniformly drawn existing row, so the DP layout (balanced by row count)
// receives roughly C_IOO / k inserts per shard at rho = 1.  eps <= 0 runs the
// zero-information placement instead (uniformly random shard per row and per
// insert, no DP release; A_priv is then undefined and written as 0).
//
// Inserts are offered in closed loop: an insert whose txn aborted (no block
// allocated) is retried in the next epoch with the same primary key, so the
// realised growth tracks the target unless the policy rejects it.  Inserts
// still pending at a repartitioning are loaded by the rebuild.
//
// Outputs (prefix = argv[10], default "growth"):
//   NAMING: occupancy_* / ioo_util_* columns are BUFFERED inserts (active IOO
//   + standby) normalised by ONE IOO capacity C_IOO, so they exceed 100 % under
//   the standby policy.  Call them "buffered inserts (% of one IOO capacity)"
//   in the paper, not "IOO utilisation"; active_ioo_* (<= 100) and standby_*
//   are the split.
//   <prefix>_epochs.csv   per epoch: buffered inserts (% of one C_IOO),
//                         inserts offered / placed / rejected, standby used,
//                         committed, epoch_ms, repartition events  -> Figure A
//   <prefix>_summary.csv  per rho: effective foreground throughput (repartition
//                         pauses included), IOO utilisation at repartition,
//                         repartition duration, inserts per repartition,
//                         standby activated / inserts rejected,
//                         A_priv = inserts between repartitions / eps_r  -> Figure B + table
//
// PREREQUISITE: k listeners (Servers_MultiRingORAM, 10/8/26 build) running.
// No network emulation: leave DEPART_NET_RTT_MS / DEPART_NET_BW_MBPS unset.
//
// Usage:
//   ./RQ5_GrowthRate [k] [rows] [regions] [E_max] [epochs] [C_IOO_total] [eps]
//                    [policy] [base_port] [prefix]
//     k            shards / listeners                         default 4
//     rows         initial dataset rows                       default 20000
//     regions      partition-attribute values                 default 40
//     E_max        epochs per scheduled repartitioning        default 20
//     epochs       epochs per rho (0 = 2 x E_max)             default 0
//     C_IOO_total  logical IOO blocks over all shards         default 4096
//     eps          eps_r per layout release (<= 0: random)    default 1.0
//     policy       backpressure | standby | early             default backpressure
//     base_port    first listener port                        default 8881
//     prefix       output prefix                              default growth
//   Env RQ5_RHOS="0.25,0.5,1,1.5,2"  overrides the rho grid.
//   Env RQ5_STANDBY_FACTOR=1.0       standby IOO per shard, as a multiple of C.
// ----------------------------------------------------------------------------

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"
#include "Repartition.h"
#include "RQ2_Affinity.h"
#include "RQ3_Workload.h"
#include "RQ_Preflight.h"

using clk = std::chrono::high_resolution_clock;
static double ms_between(clk::time_point a, clk::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ── Fixed configuration (same as RQ2 where it overlaps) ─────────────────────
static std::vector<double> RHOS = {0.25, 0.5, 1.0, 1.5, 2.0};
static constexpr double   SKEW_ALPHA        = 1.0;
static constexpr uint32_t WORKLOAD_TXNS     = 3000;
static constexpr uint32_t OPS_PER_TXN       = 3;
static constexpr double   WORKLOAD_LOCALITY = 0.6;
static constexpr int      NEIGHBOUR_SPAN    = 2;
static constexpr int      NUM_CLIENTS       = 8;
static constexpr uint32_t READS_PER_EPOCH   = 1360;   // read txns offered per epoch (RQ2: 1600 x 0.85)
static constexpr uint32_t BUCKET_SIZE       = 8;
static constexpr uint32_t S_PARAM           = 4;
static constexpr uint64_t RANDOM_PLACEMENT_SEED = 42;
static double g_standby_factor = 1.0;

enum class Policy { BACKPRESSURE, STANDBY, EARLY };
static Policy g_policy = Policy::BACKPRESSURE;
static const char* policyName(Policy p) {
    return p == Policy::STANDBY ? "standby" : (p == Policy::EARLY ? "early" : "backpressure");
}
static bool isRandom(double eps) { return eps <= 0.0; }

static EpochConfig epochConfig() {
    EpochConfig c;
    c.R            = 3;
    c.bread        = 256;
    c.bwrite       = 512;
    c.sub_epoch_ms = 60;
    c.pad_batches  = true;
    c.pad_uncontacted_shards = false;   // contacted-only padding, as RQ2 / RQ3 / RQ4
    c.per_server_capacity    = true;
    c.enforce_batch_capacity = true;
    return c;
}

// ── Read workload (RQ2's: region-local txns with some neighbour keys) ───────
static std::vector<std::vector<uint32_t>> buildReads(const rq3::Dataset& d, uint32_t n, uint64_t seed) {
    std::vector<std::vector<uint32_t>> out;
    rq3::QueryKeyGen pick(d, rq3::QueryDist::SKEWED_MATCHED, SKEW_ALPHA, seed);
    std::mt19937_64 rng(seed ^ 0x9E3779B9u);
    for (uint32_t t = 0; t < n; ++t) {
        const uint32_t anchor = pick.nextKey();
        const uint32_t ar     = d.region_of_row[anchor];
        std::vector<uint32_t> keys{anchor};
        for (uint32_t q = 1; q < OPS_PER_TXN; ++q) {
            uint32_t r = ar;
            if ((rng() % 1000) >= static_cast<uint64_t>(WORKLOAD_LOCALITY * 1000)) {
                const int delta = static_cast<int>(rng() % (2 * NEIGHBOUR_SPAN + 1)) - NEIGHBOUR_SPAN;
                r = static_cast<uint32_t>(std::min<int>(std::max<int>(0, int(ar) + delta), int(d.num_regions) - 1));
            }
            const auto& rows = d.rows_of_region[r];
            if (!rows.empty()) keys.push_back(rows[rng() % rows.size()]);
        }
        out.push_back(std::move(keys));
    }
    return out;
}

// eps <= 0: every row its own bin on a uniformly random shard (as RQ2 v3).
static std::vector<Partition> rowRandomPlacement(uint32_t num_rows, uint32_t k, uint64_t seed, BinInfo& bins) {
    std::mt19937_64 rng(seed ^ 0x9E3779B97F4A7C15ULL);
    std::vector<Partition> parts(k);
    for (uint32_t r = 0; r < num_rows; ++r) {
        const std::string b = std::to_string(r);
        bins.bin_key_to_data_indices[b] = {r};
        bins.synopsis[b] = 1; bins.noised_synopsis[b] = 1;
        parts[rng() % k].index.push_back(b);
    }
    for (auto& p : parts) {
        p.synopsis = p.noisy_synopsis = static_cast<uint32_t>(std::max<size_t>(1, p.index.size()));
        p.dummy_num = 0; p.noise_stash_reserve = 0; p.stash_overflow_count = 0;
    }
    return parts;
}

static bool useBulkInit() {
    const char* e = std::getenv("DEPART_BULK_INIT");
    return !(e && std::string(e) == "0");
}

// ── A brought-up cluster; rebuilding one IS a repartitioning ────────────────
struct Cluster {
    std::unique_ptr<Repartition> rp;
    std::unique_ptr<BinInfo>     rbins;
    std::vector<Partition>       parts;
    std::vector<ServerInfo>      servers;
    std::unique_ptr<MultiRingORAM_Servers> mrs;
    std::vector<int>             shard_of_row;
    std::vector<int>             shard_of_region;   // where a new row of region r goes (DP)
    std::map<int, size_t>        baseline;          // free blocks at build (IOO + standby + DP dummies)
    double                       build_ms = 0;

    size_t usedOn(int s) const {
        const size_t f = mrs->posMap().freeBlocks(s);
        const auto it  = baseline.find(s);
        return (it != baseline.end() && f < it->second) ? it->second - f : 0;
    }
    size_t freeOn(int s) const { return mrs->posMap().freeBlocks(s); }
};

static Cluster bringUp(const std::vector<std::string>& rows, const std::vector<uint32_t>& region_of_row,
                       uint32_t num_regions, const std::string& schema, double eps, uint32_t k,
                       uint32_t physical_reserve, int base_port) {
    Cluster c;
    auto t0 = clk::now();
    c.rp = std::make_unique<Repartition>();
    if (isRandom(eps)) {
        c.rbins = std::make_unique<BinInfo>();
        c.parts = rowRandomPlacement(static_cast<uint32_t>(rows.size()), k, RANDOM_PLACEMENT_SEED, *c.rbins);
    } else {
        c.parts = c.rp->Repartition_Main(rows, schema, {1}, eps, 0.0, 0.0, /*fixed_number_part=*/true, k, 0, k);
    }
    if (c.parts.size() != k)
        throw std::runtime_error("layout has " + std::to_string(c.parts.size()) + " partitions, expected k=" +
                                 std::to_string(k) + " (raise `regions`)");
    const BinInfo& bi = isRandom(eps) ? *c.rbins : c.rp->getBinInfo();

    c.servers.resize(c.parts.size());
    for (size_t i = 0; i < c.parts.size(); ++i) {
        c.servers[i].server_id     = static_cast<int>(i);
        c.servers[i].port          = base_port + static_cast<int>(i);
        c.servers[i].partition_id  = static_cast<uint32_t>(i);
        c.servers[i].assigned_bins = c.parts[i].index;
    }
    c.mrs = std::make_unique<MultiRingORAM_Servers>();
    c.mrs->setInsertReservePerPartition(physical_reserve);
    c.mrs->distributeDataToPartitions(bi, c.parts, c.servers, rows);

    const uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    const uint32_t block_size  = tuple_width + 16 + 2 * sizeof(uint32_t);
    for (size_t i = 0; i < c.parts.size(); ++i)
        if (useBulkInit())
            c.mrs->ServerInitializationBulk(c.servers[i], c.parts[i], bi, rows, schema, BUCKET_SIZE,
                                            "RQ5_S" + std::to_string(i), block_size, "127.0.0.1", S_PARAM);
        else
            c.mrs->ServerInitialization(c.servers[i], c.parts[i], bi, rows, schema, BUCKET_SIZE,
                                        "RQ5_S" + std::to_string(i), block_size, "127.0.0.1", S_PARAM);
    c.mrs->waitForAllServersReady();

    c.shard_of_row.assign(rows.size(), -1);
    for (uint32_t pk = 0; pk < rows.size(); ++pk)
        c.shard_of_row[pk] = c.mrs->serverOfKey(ObjectKey{0, 0, 0, pk});
    c.shard_of_region.assign(num_regions, -1);
    for (uint32_t pk = 0; pk < rows.size() && pk < region_of_row.size(); ++pk) {
        const uint32_t r = region_of_row[pk];
        if (r < num_regions && c.shard_of_region[r] < 0) c.shard_of_region[r] = c.shard_of_row[pk];
    }
    for (const auto& s : c.servers) c.baseline[s.server_id] = c.mrs->posMap().freeBlocks(s.server_id);
    c.build_ms = ms_between(t0, clk::now());
    return c;
}

// ── Records ─────────────────────────────────────────────────────────────────
struct EpochRow {
    double   rho = 0;
    uint32_t epoch = 0, period = 0, epoch_in_period = 0;
    uint64_t offered_ins = 0, pushed_ins = 0, placed_ins = 0, rejected_ins = 0, pending_ins = 0;
    uint64_t ioo_used = 0, ioo_logical = 0, standby_used = 0;
    // occ_* = BUFFERED inserts (active IOO + standby) / ONE IOO capacity C_IOO: can exceed 100 %
    // under the standby policy.  active_pct (<= 100) and standby_pct split it.
    double   occ_total_pct = 0, occ_max_shard_pct = 0, active_pct = 0, standby_pct = 0;
    uint32_t txns = 0, committed = 0, aborted = 0;
    double   epoch_ms = 0, throughput_txn_s = 0;
    bool     repartitioned = false;
    double   repartition_ms = 0;
    std::string reason;   // scheduled | early | final
};

struct SummaryRow {
    double   rho = 0, target_ins_per_epoch = 0;
    uint32_t epochs = 0, repartitions = 0;
    uint64_t offered_ins = 0, placed_ins = 0, rejected_ins = 0, absorbed_by_rebuild = 0;
    uint64_t standby_max = 0;
    uint64_t committed = 0, aborted = 0;
    double   epoch_ms_total = 0, repartition_ms_total = 0;
    double   util_at_rep_total_pct = 0, util_at_rep_max_shard_pct = 0;   // buffered / C_IOO, means over repartitions
    double   active_at_rep_pct = 0, standby_at_rep_pct = 0;              // its split: active IOO (<= 100) + standby
    double   inserts_per_repartition = 0, a_priv = 0, cumulative_eps = 0;
    double   throughput_fg = 0, throughput_eff = 0;
    double   realized_rho = 0;
    uint32_t initial_rows = 0, final_rows = 0;
};

// ── One rho ─────────────────────────────────────────────────────────────────
static SummaryRow runRho(const rq3::Dataset& d, const std::vector<std::vector<uint32_t>>& reads,
                         double rho, uint32_t k, uint32_t E_max, uint32_t epochs, uint32_t C_total,
                         double eps, int base_port, std::vector<EpochRow>& trace) {
    SummaryRow S; S.rho = rho; S.initial_rows = static_cast<uint32_t>(d.rows.size());
    const std::string schema = d.schema();
    const uint32_t C_shard   = std::max<uint32_t>(1, C_total / k);
    const uint32_t standby   = (g_policy == Policy::STANDBY)
                               ? static_cast<uint32_t>(std::lround(g_standby_factor * C_shard)) : 0;
    const uint32_t physical  = C_shard + standby;
    const double   lambda    = rho * double(C_shard) * k / double(E_max);    // inserts per epoch
    S.target_ins_per_epoch   = lambda;
    const uint32_t tuple_width = tupleWidthBytesFromSchema(schema);

    std::vector<std::string> rows = d.rows;
    std::vector<uint32_t> region_of_row = d.region_of_row;
    Cluster cl = bringUp(rows, region_of_row, d.num_regions, schema, eps, k, physical, base_port);

    struct PendingIns { uint32_t pk; uint32_t region; int shard; std::string line; };   // shard fixed at creation
    std::vector<PendingIns> pending;            // aborted inserts, retried with the same pk
    std::mt19937_64 rng(1000 + static_cast<uint64_t>(rho * 1000));
    double carry = 0.0;                          // fractional inserts per epoch
    uint32_t period = 0, since_rep = 0;
    uint64_t period_placed = 0;
    std::vector<double> util_tot, util_max, ins_per_rep, act_rep, sb_rep;

    auto shardFor = [&](uint32_t region) -> int {
        if (isRandom(eps)) return static_cast<int>(rng() % k);
        int s = region < cl.shard_of_region.size() ? cl.shard_of_region[region] : -1;
        return s >= 0 ? s : static_cast<int>(region % k);
    };

    for (uint32_t e = 0; e < epochs; ++e) {
        EpochRow ER; ER.rho = rho; ER.epoch = e; ER.period = period; ER.epoch_in_period = since_rep;

        // ── this epoch's inserts: retries of aborted ones + new ones at rate lambda ──
        // A primary key is assigned only when an insert is ADMITTED, so a
        // rejected insert leaves no hole in the pk space (pk == row index).
        carry += lambda;
        const uint64_t fresh = static_cast<uint64_t>(std::floor(carry));
        carry -= double(fresh);
        std::vector<PendingIns> retries = std::move(pending);
        pending.clear();
        uint32_t next_pk = static_cast<uint32_t>(rows.size());
        for (const auto& p : retries) next_pk = std::max(next_pk, p.pk + 1);
        ER.offered_ins = retries.size() + fresh;

        // ── admission: per-shard room in the logical IOO (+ standby) ────
        std::vector<int64_t> room(k, 0);
        for (uint32_t s = 0; s < k; ++s) {
            const int64_t used  = static_cast<int64_t>(cl.usedOn(static_cast<int>(s)));
            const int64_t phys  = static_cast<int64_t>(cl.freeOn(static_cast<int>(s)));
            const int64_t limit = (g_policy == Policy::STANDBY) ? int64_t(physical) : int64_t(C_shard);
            room[s] = std::max<int64_t>(0, std::min<int64_t>(limit - used, phys));
        }
        std::vector<txn> batch;
        std::vector<PendingIns> pushed;
        auto admit = [&](PendingIns&& p) {
            const int s = p.shard;
            --room[s];
            txn t; Operat op;
            op.type = OpType::INSERT; op.data_primary_key = p.pk; op.server_id = s;
            op.data_value = p.line.substr(0, tuple_width); op.last_one = true;
            t.operations.push_back(op);
            batch.push_back(std::move(t));
            pushed.push_back(std::move(p));
        };
        for (auto& p : retries) {                  // already own a pk: defer, never drop
            if (room[p.shard] > 0) admit(std::move(p));
            else pending.push_back(std::move(p));
        }
        for (uint64_t i = 0; i < fresh; ++i) {
            const uint32_t region = d.region_of_row[rng() % d.region_of_row.size()];   // growth follows data
            const int shard = shardFor(region);
            if (room[shard] <= 0) { ++ER.rejected_ins; continue; }                 // backpressure
            const uint32_t pk = next_pk++;
            std::string line = std::to_string(pk) + "," + std::to_string(region) + "," +
                               std::to_string(rng() % 10000) + "," + std::to_string(rng() % 100000);
            admit({pk, region, shard, std::move(line)});
        }
        ER.pushed_ins = pushed.size();
        for (uint32_t i = 0; i < READS_PER_EPOCH; ++i) {
            const auto& keys = reads[rng() % reads.size()];
            txn t;
            for (size_t q = 0; q < keys.size(); ++q) {
                Operat op;
                op.type = OpType::READ; op.data_primary_key = keys[q];
                op.server_id = cl.shard_of_row[keys[q]]; op.last_one = (q + 1 == keys.size());
                t.operations.push_back(op);
            }
            batch.push_back(std::move(t));
        }
        std::shuffle(batch.begin(), batch.end(), rng);

        // ── run the epoch (burst intake, as RQ2's default) ──────────────
        QueriesReceiving qr;
        std::atomic<bool> stop{false};
        std::vector<std::thread> clients;
        for (int c = 0; c < NUM_CLIENTS; ++c)
            clients.emplace_back([&, c]() {
                for (size_t i = c; i < batch.size() && !stop; i += NUM_CLIENTS)
                    for (int a = 0; a < 2000 && !qr.push_txn(batch[i]) && !stop; ++a)
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
            });
        auto t0 = clk::now();
        std::vector<AssembledTxnResult> results;
        try {
            results = runEpoch_RBR_W(qr, *cl.mrs, epochConfig());
        } catch (...) { stop = true; for (auto& t : clients) t.join(); throw; }
        ER.epoch_ms = ms_between(t0, clk::now());
        stop = true;
        for (auto& t : clients) t.join();

        ER.txns = static_cast<uint32_t>(results.size());
        for (const auto& r : results) (r.committed ? ER.committed : ER.aborted)++;
        ER.throughput_txn_s = ER.epoch_ms > 0 ? ER.committed * 1000.0 / ER.epoch_ms : 0.0;

        // ── which inserts got a block; the rest are retried ─────────────
        for (auto& p : pushed) {
            if (cl.mrs->serverOfKey(ObjectKey{0, 0, 0, p.pk}) >= 0) {
                if (p.pk >= rows.size()) { rows.resize(p.pk + 1); region_of_row.resize(p.pk + 1, 0); }
                rows[p.pk] = p.line; region_of_row[p.pk] = p.region;
                ++ER.placed_ins;
            } else {
                pending.push_back(std::move(p));
            }
        }
        ER.pending_ins = pending.size();
        period_placed += ER.placed_ins;

        // ── occupancy (of the LOGICAL IOO, so standby shows as > 100 %) ─
        uint64_t used = 0, sb = 0; double occ_max = 0;
        for (uint32_t s = 0; s < k; ++s) {
            const uint64_t u = cl.usedOn(static_cast<int>(s));
            used += u;
            sb   += u > C_shard ? u - C_shard : 0;
            occ_max = std::max(occ_max, 100.0 * double(u) / double(C_shard));
        }
        ER.ioo_used = used; ER.ioo_logical = uint64_t(C_shard) * k; ER.standby_used = sb;
        ER.occ_total_pct = 100.0 * double(used) / double(ER.ioo_logical);
        ER.occ_max_shard_pct = occ_max;
        ER.active_pct  = 100.0 * double(used - sb) / double(ER.ioo_logical);   // sum_s min(u_s, C)
        ER.standby_pct = 100.0 * double(sb) / double(ER.ioo_logical);
        S.standby_max = std::max(S.standby_max, sb);

        // ── repartition? scheduled at E_max, or early (policy) ──────────
        ++since_rep;
        bool rep = since_rep >= E_max;
        std::string why = "scheduled";
        if (!rep && g_policy == Policy::EARLY) {
            // the tightest shard must still fit next epoch's expected inserts (+25 %)
            const double per_shard_next = 1.25 * (lambda + double(pending.size())) / double(k);
            for (uint32_t s = 0; s < k && !rep; ++s)
                if (double(C_shard) - double(cl.usedOn(static_cast<int>(s))) < per_shard_next) rep = true;
            why = "early";
        }
        if (rep) {
            util_tot.push_back(ER.occ_total_pct);
            util_max.push_back(ER.occ_max_shard_pct);
            act_rep.push_back(ER.active_pct);
            sb_rep.push_back(ER.standby_pct);
            // pending inserts are loaded by the rebuild (keeps the pk space dense)
            for (auto& p : pending) {
                if (p.pk >= rows.size()) { rows.resize(p.pk + 1); region_of_row.resize(p.pk + 1, 0); }
                rows[p.pk] = p.line; region_of_row[p.pk] = p.region;
            }
            S.absorbed_by_rebuild += pending.size();
            ins_per_rep.push_back(double(period_placed + pending.size()));
            pending.clear();
            for (size_t i = 0; i < rows.size(); ++i)              // no holes may reach the partitioner
                if (rows[i].empty())
                    throw std::runtime_error("internal: empty row " + std::to_string(i) + " before rebuild");
            auto tr = clk::now();
            cl.mrs->shutdownAllServers();
            cl = bringUp(rows, region_of_row, d.num_regions, schema, eps, k, physical, base_port);
            ER.repartition_ms = ms_between(tr, clk::now());
            ER.repartitioned = true; ER.reason = why;
            ++S.repartitions; S.repartition_ms_total += ER.repartition_ms;
            since_rep = 0; period_placed = 0; ++period;
        }

        S.offered_ins  += ER.offered_ins;
        S.placed_ins   += ER.placed_ins;
        S.rejected_ins += ER.rejected_ins;
        S.committed    += ER.committed;
        S.aborted      += ER.aborted;
        S.epoch_ms_total += ER.epoch_ms;
        ++S.epochs;
        trace.push_back(ER);

        std::cout << "    rho=" << rho << " epoch " << std::setw(2) << e
                  << "  ins offered/placed/rejected=" << ER.offered_ins << "/" << ER.placed_ins
                  << "/" << ER.rejected_ins
                  << "  buffered/C_IOO=" << std::fixed << std::setprecision(1) << ER.occ_total_pct << "% (max shard "
                  << ER.occ_max_shard_pct << "%)" << (sb ? "  standby=" + std::to_string(sb) : "")
                  << "  committed=" << ER.committed << "  epoch_ms=" << ER.epoch_ms
                  << (ER.repartitioned ? "  REPARTITION[" + why + "] " + std::to_string(int(ER.repartition_ms)) + "ms" : "")
                  << "\n";
    }
    cl.mrs->shutdownAllServers();

    auto mean = [](const std::vector<double>& v) { return v.empty() ? 0.0 : std::accumulate(v.begin(), v.end(), 0.0) / v.size(); };
    S.util_at_rep_total_pct     = mean(util_tot);
    S.util_at_rep_max_shard_pct = mean(util_max);
    S.active_at_rep_pct         = mean(act_rep);
    S.standby_at_rep_pct        = mean(sb_rep);
    S.inserts_per_repartition   = mean(ins_per_rep);
    S.a_priv          = (!isRandom(eps) && eps > 0) ? S.inserts_per_repartition / eps : 0.0;
    S.cumulative_eps  = isRandom(eps) ? 0.0 : eps * double(1 + S.repartitions);   // initial layout + each repartition
    S.throughput_fg   = S.epoch_ms_total > 0 ? S.committed * 1000.0 / S.epoch_ms_total : 0.0;
    const double wall = S.epoch_ms_total + S.repartition_ms_total;
    S.throughput_eff  = wall > 0 ? S.committed * 1000.0 / wall : 0.0;
    S.realized_rho    = (C_shard && S.epochs)
        ? double(S.placed_ins + S.absorbed_by_rebuild) / (double(C_shard) * k) * double(E_max) / double(S.epochs) : 0.0;
    S.final_rows      = static_cast<uint32_t>(rows.size());
    return S;
}

// ── CSV ─────────────────────────────────────────────────────────────────────
static void writeEpochs(const std::string& path, const std::vector<EpochRow>& rows, uint32_t E_max, double eps) {
    std::ofstream f(path);
    f << "rho,policy,E_max,epsilon,epoch,period,epoch_in_period,offered_ins,pushed_ins,placed_ins,"
         "rejected_ins,pending_ins,ioo_used,ioo_logical,standby_used,occupancy_total_pct,"
         "occupancy_max_shard_pct,txns,committed,aborted,epoch_ms,throughput_txn_s,"
         "repartitioned,repartition_ms,reason,active_ioo_pct,standby_pct\n";
    for (const auto& r : rows)
        f << r.rho << ',' << policyName(g_policy) << ',' << E_max << ',' << eps << ',' << r.epoch << ','
          << r.period << ',' << r.epoch_in_period << ',' << r.offered_ins << ',' << r.pushed_ins << ','
          << r.placed_ins << ',' << r.rejected_ins << ',' << r.pending_ins << ',' << r.ioo_used << ','
          << r.ioo_logical << ',' << r.standby_used << ',' << std::fixed << std::setprecision(2)
          << r.occ_total_pct << ',' << r.occ_max_shard_pct << ',' << r.txns << ',' << r.committed << ','
          << r.aborted << ',' << r.epoch_ms << ',' << r.throughput_txn_s << ','
          << (r.repartitioned ? 1 : 0) << ',' << r.repartition_ms << ','
          << (r.reason.empty() ? "-" : r.reason) << ',' << r.active_pct << ',' << r.standby_pct << '\n';
    std::cout << "Wrote " << path << "\n";
}

static void writeSummary(const std::string& path, const std::vector<SummaryRow>& rows, uint32_t k,
                         uint32_t E_max, uint32_t C_total, double eps) {
    std::ofstream f(path);
    f << "rho,policy,k,E_max,C_ioo,epsilon,standby_factor,target_ins_per_epoch,epochs,offered_ins,"
         "placed_ins,rejected_ins,insert_reject_pct,absorbed_by_rebuild,standby_blocks_max,repartitions,"
         "mean_repartition_ms,ioo_util_at_repartition_pct,ioo_util_at_repartition_max_shard_pct,"
         "inserts_per_repartition,A_priv,cumulative_eps,committed,aborted,epoch_ms_total,"
         "repartition_ms_total,throughput_fg_txn_s,throughput_eff_txn_s,realized_rho,initial_rows,final_rows,"
         "active_ioo_at_repartition_pct,standby_at_repartition_pct\n";
    for (const auto& r : rows)
        f << r.rho << ',' << policyName(g_policy) << ',' << k << ',' << E_max << ',' << C_total << ','
          << eps << ',' << (g_policy == Policy::STANDBY ? g_standby_factor : 0.0) << ','
          << std::fixed << std::setprecision(2) << r.target_ins_per_epoch << ',' << r.epochs << ','
          << r.offered_ins << ',' << r.placed_ins << ',' << r.rejected_ins << ','
          << (r.offered_ins ? 100.0 * r.rejected_ins / r.offered_ins : 0.0) << ','
          << r.absorbed_by_rebuild << ',' << r.standby_max << ',' << r.repartitions << ','
          << (r.repartitions ? r.repartition_ms_total / r.repartitions : 0.0) << ','
          << r.util_at_rep_total_pct << ',' << r.util_at_rep_max_shard_pct << ','
          << r.inserts_per_repartition << ',' << r.a_priv << ',' << r.cumulative_eps << ','
          << r.committed << ',' << r.aborted << ',' << r.epoch_ms_total << ',' << r.repartition_ms_total << ','
          << r.throughput_fg << ',' << r.throughput_eff << ',' << std::setprecision(3) << r.realized_rho << ','
          << r.initial_rows << ',' << r.final_rows << ',' << std::setprecision(2)
          << r.active_at_rep_pct << ',' << r.standby_at_rep_pct << '\n';
    std::cout << "Wrote " << path << "\n";
}

int main(int argc, char** argv) {
    const uint32_t k       = argc > 1 ? std::stoul(argv[1]) : 4;
    const uint32_t nrows   = argc > 2 ? std::stoul(argv[2]) : 20000;
    const uint32_t regions = argc > 3 ? std::stoul(argv[3]) : 40;
    const uint32_t E_max   = argc > 4 ? std::max<uint32_t>(1, std::stoul(argv[4])) : 20;
    uint32_t epochs        = argc > 5 ? std::stoul(argv[5]) : 0;
    const uint32_t C_total = argc > 6 ? std::stoul(argv[6]) : 4096;
    const double   eps     = argc > 7 ? std::stod(argv[7]) : 1.0;
    const std::string pol  = argc > 8 ? argv[8] : "backpressure";
    const int base_port    = argc > 9 ? std::stoi(argv[9]) : 8881;
    const std::string pre  = argc > 10 ? argv[10] : "growth";
    if (epochs == 0) epochs = 2 * E_max;
    if (pol == "standby") g_policy = Policy::STANDBY;
    else if (pol == "early") g_policy = Policy::EARLY;
    else if (pol != "backpressure") { std::cerr << "policy must be backpressure | standby | early\n"; return 1; }
    if (const char* ev = std::getenv("RQ5_RHOS"); ev && *ev) {
        std::vector<double> g; std::string tok, all(ev);
        for (size_t i = 0; i <= all.size(); ++i) {
            if (i == all.size() || all[i] == ',') { if (!tok.empty()) g.push_back(std::stod(tok)); tok.clear(); }
            else tok += all[i];
        }
        if (!g.empty()) RHOS = g;
    }
    if (const char* sf = std::getenv("RQ5_STANDBY_FACTOR"); sf && *sf) g_standby_factor = std::stod(sf);

    const uint32_t C_shard = std::max<uint32_t>(1, C_total / k);
    std::cout << "RQ5 — growth-rate sweep:  k=" << k << "  rows=" << nrows << "  regions=" << regions
              << "\n  E_max=" << E_max << " epochs between scheduled repartitionings, " << epochs
              << " epochs per rho\n  C_IOO=" << C_shard * k << " logical blocks (" << C_shard << "/shard)"
              << "  eps_r=" << eps << (isRandom(eps) ? " (zero-information placement, no release)" : "")
              << "\n  policy=" << policyName(g_policy)
              << (g_policy == Policy::STANDBY ? "  standby/shard=" + std::to_string(
                     static_cast<uint32_t>(std::lround(g_standby_factor * C_shard))) : std::string())
              << "\n  reads/epoch=" << READS_PER_EPOCH << "  rho grid:";
    for (double r : RHOS) std::cout << ' ' << r << " (" << std::fixed << std::setprecision(1)
                                    << r * C_shard * k / E_max << " ins/epoch)";
    std::cout << "\n\n";

    rq3::Dataset d = rq3::generateDataset(nrows, regions, rq3::DataDist::SKEWED, SKEW_ALPHA, 42);
    const auto reads = buildReads(d, WORKLOAD_TXNS, 43);
    requireListeners("127.0.0.1", base_port, k, "RQ5");

    std::vector<SummaryRow> summary;
    std::vector<EpochRow> trace;
    for (double rho : RHOS) {
        std::cout << "── rho=" << rho << " ──\n";
        try {
            SummaryRow s = runRho(d, reads, rho, k, E_max, epochs, C_total, eps, base_port, trace);
            std::cout << "   → placed=" << s.placed_ins << "  rejected=" << s.rejected_ins
                      << "  standby_max=" << s.standby_max << "  repartitions=" << s.repartitions
                      << "  util@rep=" << std::setprecision(1) << s.util_at_rep_total_pct << "%"
                      << "  ins/rep=" << s.inserts_per_repartition << "  A_priv=" << s.a_priv
                      << "  txn/s fg=" << s.throughput_fg << " eff=" << s.throughput_eff << "\n";
            summary.push_back(s);
        } catch (const std::exception& ex) {
            std::cerr << "   FAILED: " << ex.what() << "\n";
        }
    }
    writeEpochs(pre + "_epochs.csv", trace, E_max, eps);
    writeSummary(pre + "_summary.csv", summary, k, E_max, C_total, eps);
    return 0;
}
