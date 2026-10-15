//
// RQ3_Workload.h — data / query distribution generation and shard-load metrics
// for RQ3 ("How robust is DePart to mismatches between data distribution and
// query-access distribution?").  Created 9/22/26.
//
// The four settings of the paper:
//
//   1. UNIFORM_UNIFORM   uniform data  + uniform queries
//   2. SKEWED_MATCHED    skewed data   + matching skewed queries
//   3. UNIFORM_SKEWED    uniform data  + skewed queries
//   4. SKEWED_MISMATCHED skewed data   + mismatched skewed queries
//
// Why this header exists — the old skewed experiment could not measure
// Figure 5 at all:
//
//     int sid = rng() % servers.size();      // shard chosen UNIFORMLY
//     uint32_t block = zipf_within_shard(rng);
//
// Skew applied inside a shard cannot create cross-shard imbalance, so
// Imbalance ≡ 1 by construction.  Here the skew is applied to the KEY SPACE
// (the partitioning attribute), and the shard a query lands on is whatever
// the partitioning maps that key to.  That is the quantity Figure 5 is about.
//
// Definitions (Figure 5):
//     L_i        = workload assigned to shard i (queries, or ORAM ops)
//     Imbalance  = max_i L_i / ( (1/k) * Σ_i L_i )
//     1.0        = perfectly balanced;  k = everything on one shard.
//
#ifndef RQ3_WORKLOAD_H
#define RQ3_WORKLOAD_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace rq3 {

// ── The four settings ───────────────────────────────────────────────────────
enum class DataDist  { UNIFORM, SKEWED };
enum class QueryDist { UNIFORM, SKEWED_MATCHED, SKEWED_MISMATCHED };

struct Setting {
    const char* name;
    DataDist    data;
    QueryDist   query;
};

inline const std::vector<Setting>& settings() {
    static const std::vector<Setting> s = {
        {"uniform_data+uniform_queries",   DataDist::UNIFORM, QueryDist::UNIFORM},
        {"skewed_data+matched_queries",    DataDist::SKEWED,  QueryDist::SKEWED_MATCHED},
        {"uniform_data+skewed_queries",    DataDist::UNIFORM, QueryDist::SKEWED_MATCHED},
        {"skewed_data+mismatched_queries", DataDist::SKEWED,  QueryDist::SKEWED_MISMATCHED},
    };
    return s;
}

// ── Zipf weights over `n` ranks: P(rank r) ∝ 1/(r+1)^alpha ──────────────────
inline std::vector<double> zipfWeights(size_t n, double alpha) {
    std::vector<double> w(n);
    for (size_t i = 0; i < n; ++i)
        w[i] = 1.0 / std::pow(static_cast<double>(i + 1), alpha);
    return w;
}

// ── Dataset ─────────────────────────────────────────────────────────────────
// Rows are CSV lines matching the schema
//     "customer_id:uint32,region:uint32,order_amount:uint32,timestamp:uint32"
// `region` is the PARTITIONING ATTRIBUTE (the bin key), so the data
// distribution is the distribution of rows over regions.
//   UNIFORM : every region holds the same number of rows
//   SKEWED  : rows per region follow Zipf(alpha) over region ranks
struct Dataset {
    std::vector<std::string> rows;          // CSV lines, index = data_idx = pk
    std::vector<uint32_t>    region_of_row; // region (bin key) of each row
    std::vector<std::vector<uint32_t>> rows_of_region;   // region → row indices
    uint32_t num_regions = 0;

    // NOTE the types.  csv_reader.cpp's fieldWidthBytes() understands
    //     int8 int16 int32 float double date char(N) varchar(N) numeric(a-b)
    // and NOTHING else — an unknown type silently contributes width 0.  This
    // schema used to say uint32, so every field measured 0, the tuple width
    // came out 0, block_size collapsed to 0+16+8 = 24, and the bulk loader
    // died in
    //     dummy_value.substr(sizeof(uint32_t))   // substr(4) on an empty string
    // with the un-Googleable message "ERROR: basic_string".
    // char(24) for the last field keeps the tuple (36 B) wider than the
    // longest row this generator emits, so records are never truncated.
    std::string schema() const {
        return "customer_id:int32,region:int32,order_amount:int32,timestamp:char(24)";
    }
};

inline Dataset generateDataset(uint32_t num_rows, uint32_t num_regions,
                               DataDist dist, double alpha, uint64_t seed) {
    Dataset d;
    d.num_regions = num_regions;
    d.rows_of_region.resize(num_regions);
    std::mt19937_64 rng(seed);

    // How many rows each region gets.
    std::vector<uint32_t> per_region(num_regions, 0);
    if (dist == DataDist::UNIFORM) {
        for (uint32_t r = 0; r < num_regions; ++r) per_region[r] = num_rows / num_regions;
        for (uint32_t r = 0; r < num_rows % num_regions; ++r) per_region[r]++;
    } else {
        auto w = zipfWeights(num_regions, alpha);
        const double sum = std::accumulate(w.begin(), w.end(), 0.0);
        uint32_t placed = 0;
        for (uint32_t r = 0; r < num_regions; ++r) {
            per_region[r] = static_cast<uint32_t>(num_rows * w[r] / sum);
            placed += per_region[r];
        }
        for (uint32_t r = 0; placed < num_rows; ++r, ++placed) per_region[r % num_regions]++;
    }

    d.rows.reserve(num_rows);
    d.region_of_row.reserve(num_rows);
    for (uint32_t r = 0; r < num_regions; ++r) {
        for (uint32_t i = 0; i < per_region[r]; ++i) {
            const uint32_t pk  = static_cast<uint32_t>(d.rows.size());   // pk == data index
            const uint32_t amt = static_cast<uint32_t>(rng() % 10000);
            const uint32_t ts  = static_cast<uint32_t>(rng() % 100000);
            d.rows_of_region[r].push_back(pk);
            d.region_of_row.push_back(r);
            d.rows.push_back(std::to_string(pk) + "," + std::to_string(r) + "," +
                             std::to_string(amt) + "," + std::to_string(ts));
        }
    }
    return d;
}

// ── Query key generator ─────────────────────────────────────────────────────
// A query picks a REGION first (this is where skew lives), then a row inside
// it.  Returned value is the row index == primary key.
//
//   UNIFORM           : region ∝ its share of rows (i.e. uniform over rows)
//   SKEWED_MATCHED    : region ∝ Zipf(rank), SAME ranking as the data skew,
//                       so hot regions are the big ones
//   SKEWED_MISMATCHED : region ∝ Zipf(rank) over the REVERSED ranking, so the
//                       hot regions are the small ones — queries concentrate
//                       where the data is sparse, which is the case the
//                       partitioning was not built for
class QueryKeyGen {
public:
    QueryKeyGen(const Dataset& d, QueryDist qd, double alpha, uint64_t seed)
        : data_(&d), rng_(seed) {
        std::vector<double> w(d.num_regions, 0.0);
        if (qd == QueryDist::UNIFORM) {
            for (uint32_t r = 0; r < d.num_regions; ++r)
                w[r] = static_cast<double>(d.rows_of_region[r].size());
        } else {
            auto zw = zipfWeights(d.num_regions, alpha);
            for (uint32_t r = 0; r < d.num_regions; ++r) {
                // rank 0 is the hottest region.  MATCHED: region 0 (the
                // largest, since generateDataset fills regions in rank order).
                // MISMATCHED: the last region — the smallest one.
                const uint32_t rank = (qd == QueryDist::SKEWED_MATCHED)
                                          ? r : (d.num_regions - 1 - r);
                w[r] = d.rows_of_region[r].empty() ? 0.0 : zw[rank];
            }
        }
        region_pick_ = std::discrete_distribution<uint32_t>(w.begin(), w.end());
    }

    uint32_t nextKey() {
        for (int attempt = 0; attempt < 64; ++attempt) {
            const uint32_t r = region_pick_(rng_);
            const auto& rows = data_->rows_of_region[r];
            if (!rows.empty()) return rows[rng_() % rows.size()];
        }
        return 0;
    }

private:
    const Dataset* data_;
    std::mt19937_64 rng_;
    std::discrete_distribution<uint32_t> region_pick_;
};

// ── Shard load + imbalance (Figure 5) ───────────────────────────────────────
struct ShardLoad {
    std::vector<uint64_t> queries;    // L_i over submitted ops
    std::vector<uint64_t> oram_ops;   // L_i over ORAM ops actually executed

    explicit ShardLoad(size_t k) : queries(k, 0), oram_ops(k, 0) {}
    void addQuery(int shard)  { if (shard >= 0 && (size_t)shard < queries.size())  ++queries[shard]; }
    void addOramOp(int shard) { if (shard >= 0 && (size_t)shard < oram_ops.size()) ++oram_ops[shard]; }
};

// Imbalance = max_i L_i / mean_i L_i.  Returns 1.0 for an all-zero vector.
inline double imbalance(const std::vector<uint64_t>& L) {
    if (L.empty()) return 1.0;
    const uint64_t total = std::accumulate(L.begin(), L.end(), uint64_t{0});
    if (total == 0) return 1.0;
    const double mean = static_cast<double>(total) / static_cast<double>(L.size());
    const uint64_t mx = *std::max_element(L.begin(), L.end());
    return static_cast<double>(mx) / mean;
}

inline std::string joinLoads(const std::vector<uint64_t>& L, char sep = ' ') {
    std::string s;
    for (size_t i = 0; i < L.size(); ++i) {
        if (i) s += sep;
        s += std::to_string(L[i]);
    }
    return s;
}

}  // namespace rq3

#endif  // RQ3_WORKLOAD_H