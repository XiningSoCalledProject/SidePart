#pragma once
// ============================================================================
// RQ2_AggSynopsis.h — aggregate-query synopsis held by the trusted proxy
// (RQ2 synopsis ablation, 10/13/26).  Header-only, no repo dependencies, so
// Test_RQ2 can check it without a cluster.
//
// Aggregate query  AGG(g, t) = COUNT(rows r in group g with value(r) < t)
//   group g  : G consecutive rows of ONE region (rows_of_region chunked), so a
//              group lives on one shard under data-aware placement and is
//              scattered under zero-information placement.
//   value(r) : the 3rd CSV field of the row (0..9999 in the RQ2 dataset).
//   t        : a multiple of BUCKET_WIDTH (1000 .. 10000), so the answer is a
//              sum of whole buckets.
//
// Three ways to answer it (the ablation arms):
//   ORAM execution : read all |g| rows through the ORAM batch, count at the
//                    proxy                              -> exactFromValues()
//   proxy, exact   : the TRUSTED proxy keeps the exact per-group histogram and
//                    answers locally, 0 ORAM reads      -> answerExact()
//   proxy, noisy   : the proxy answers from a DP release of the histogram
//                    (Laplace(sens / eps) per bucket), made at partitioning /
//                    repartitioning only and reused unchanged in between
//                                                        -> answerNoisy()
// The answer goes to the client, never to a storage server, so answering at
// the proxy (exactly or not) changes nothing the servers observe beyond what
// the workload already shows; the noisy arm is there for the setting in which
// the synopsis itself must be private (e.g. persisted outside the proxy).
//
// Sensitivity of one release: changing one row's value moves it between two
// buckets of ONE group -> L1 change 2 -> sens = 2.
// ============================================================================
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

namespace rq2 {

struct AggGroups {
    std::vector<std::vector<uint32_t>> members;   // group -> row ids
    std::vector<int32_t> group_of_row;            // row id -> group (-1: none)
};

// Chunk every region's rows into groups of `G`; a tail shorter than
// `min_size` is merged into the region's previous group, so every group has
// at least min_size rows (an aggregate is then never mistaken for a 3-key txn).
inline AggGroups buildAggGroups(const std::vector<std::vector<uint32_t>>& rows_of_region,
                                size_t num_rows, uint32_t G, uint32_t min_size = 4) {
    AggGroups out;
    out.group_of_row.assign(num_rows, -1);
    G = std::max<uint32_t>(G, min_size);
    for (const auto& rows : rows_of_region) {
        const size_t first = out.members.size();
        for (size_t i = 0; i < rows.size(); i += G) {
            std::vector<uint32_t> grp(rows.begin() + i, rows.begin() + std::min(rows.size(), i + G));
            if (grp.size() < min_size && out.members.size() > first)
                out.members.back().insert(out.members.back().end(), grp.begin(), grp.end());
            else if (grp.size() >= min_size)
                out.members.push_back(std::move(grp));
        }
    }
    for (size_t g = 0; g < out.members.size(); ++g)
        for (uint32_t r : out.members[g])
            if (r < num_rows) out.group_of_row[r] = static_cast<int32_t>(g);
    return out;
}

// value(r): 3rd comma-separated field; falls back to a hash if it is missing.
inline int64_t aggValueOfRow(const std::string& row) {
    size_t a = row.find(',');
    size_t b = a == std::string::npos ? a : row.find(',', a + 1);
    if (b != std::string::npos) {
        const size_t c = row.find(',', b + 1);
        const std::string f = row.substr(b + 1, c == std::string::npos ? std::string::npos : c - b - 1);
        char* end = nullptr;
        const long long v = std::strtoll(f.c_str(), &end, 10);
        if (end && end != f.c_str()) return std::llabs(v) % 10000;
    }
    return static_cast<int64_t>(std::hash<std::string>{}(row) % 10000);
}

class AggSynopsis {
public:
    static constexpr int     kBuckets     = 10;
    static constexpr int64_t BUCKET_WIDTH = 1000;     // value range [0, 10000)
    static constexpr double  SENS         = 2.0;

    AggSynopsis() = default;
    AggSynopsis(const AggGroups& groups, const std::vector<int64_t>& value_of_row)
        : groups_(&groups), values_(value_of_row) {
        exact_.assign(groups.members.size(), std::vector<int64_t>(kBuckets, 0));
        for (size_t g = 0; g < groups.members.size(); ++g)
            for (uint32_t r : groups.members[g])
                if (r < values_.size()) ++exact_[g][bucketOf(values_[r])];
    }

    // One DP release (call at partitioning and at every repartitioning only).
    void release(double eps, uint64_t seed) {
        std::mt19937_64 rng(seed);
        noisy_.assign(exact_.size(), std::vector<double>(kBuckets, 0.0));
        for (size_t g = 0; g < exact_.size(); ++g)
            for (int b = 0; b < kBuckets; ++b)
                noisy_[g][b] = double(exact_[g][b]) + laplace(rng, SENS / eps);
        ++releases_;
    }

    size_t   numGroups() const { return exact_.size(); }
    uint64_t releases()  const { return releases_; }
    static int64_t threshold(uint64_t draw) { return BUCKET_WIDTH * int64_t(1 + draw % kBuckets); }

    int64_t answerExact(size_t g, int64_t t) const {
        int64_t s = 0;
        for (int b = 0; b < bucketsBelow(t); ++b) s += exact_[g][b];
        return s;
    }
    double answerNoisy(size_t g, int64_t t) const {
        double s = 0;
        for (int b = 0; b < bucketsBelow(t); ++b) s += noisy_[g][b];
        return std::max(0.0, s);
    }
    // What ORAM execution computes from the rows it fetched.
    int64_t exactFromValues(size_t g, int64_t t) const {
        int64_t s = 0;
        for (uint32_t r : groups_->members[g]) s += (r < values_.size() && values_[r] < t);
        return s;
    }

private:
    static int bucketOf(int64_t v) { return int(std::min<int64_t>(kBuckets - 1, std::max<int64_t>(0, v / BUCKET_WIDTH))); }
    static int bucketsBelow(int64_t t) { return int(std::min<int64_t>(kBuckets, std::max<int64_t>(0, t / BUCKET_WIDTH))); }
    static double laplace(std::mt19937_64& rng, double b) {
        std::uniform_real_distribution<double> u(-0.5, 0.5);
        const double x = u(rng);
        return -b * (x < 0 ? -1.0 : 1.0) * std::log(std::max(1e-300, 1.0 - 2.0 * std::fabs(x)));
    }
    const AggGroups* groups_ = nullptr;
    std::vector<int64_t> values_;
    std::vector<std::vector<int64_t>> exact_;
    std::vector<std::vector<double>>  noisy_;
    uint64_t releases_ = 0;
};

}  // namespace rq2
