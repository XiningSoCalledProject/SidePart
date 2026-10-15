//
// RQ2_Affinity.h — partition-quality metrics for RQ2 ("How does the privacy
// budget influence partition quality and refresh behavior?").  Created 9/22/26.
//
// Figure 3 plots AFFINITY CUT against ε.  The affinity graph is built from the
// workload, not from the data:
//
//     nodes  = records
//     w(u,v) = number of transactions that access BOTH u and v
//
// A partitioning assigns every record to a shard, which cuts every edge whose
// endpoints land on different shards:
//
//     AffinityCut = ( Σ_{(u,v) cut} w(u,v) ) / ( Σ_{(u,v)} w(u,v) )      ∈ [0,1]
//
//   0.0 = every co-accessed pair stayed together (perfect workload locality)
//   1.0 = every co-accessed pair was split
//
// This is the normalized edge cut of the classic affinity graph, so it is
// directly comparable across ε values and across partitioning schemes, and it
// does not depend on the number of shards the way raw fan-out does.
//
// ε also affects STABILITY: Laplace noise is redrawn on every run, so the same
// data can produce different boundaries.  Two measures here:
//   * spread of the affinity cut across repeats (mean / sd / min / max), and
//   * layoutChurn(): how much a layout differs from a reference layout,
//     measured on CO-LOCATION of record pairs so that it is invariant to
//     shard renaming (1 − Rand index).
//
#ifndef RQ2_AFFINITY_H
#define RQ2_AFFINITY_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <numeric>
#include <random>
#include <unordered_map>
#include <vector>

namespace rq2 {

// ── Affinity graph ──────────────────────────────────────────────────────────
// Undirected, keyed by (min,max).  Built from transactions, each given as the
// set of record ids (primary keys) it touches.
class AffinityGraph {
public:
    void addTransaction(const std::vector<uint32_t>& keys) {
        // De-duplicate within the transaction: a record read twice by one txn
        // is still one node, and a self-edge is not an affinity.
        std::vector<uint32_t> k = keys;
        std::sort(k.begin(), k.end());
        k.erase(std::unique(k.begin(), k.end()), k.end());
        for (size_t i = 0; i < k.size(); ++i)
            for (size_t j = i + 1; j < k.size(); ++j) {
                edges_[{k[i], k[j]}] += 1;
                total_ += 1;
            }
        ++txns_;
    }

    // AffinityCut ∈ [0,1]; shard_of(key) < 0 means "unknown", edge skipped.
    template <class ShardOf>
    double cut(ShardOf shard_of) const {
        uint64_t cut_w = 0, total_w = 0;
        for (const auto& [e, w] : edges_) {
            const int su = shard_of(e.first), sv = shard_of(e.second);
            if (su < 0 || sv < 0) continue;
            total_w += w;
            if (su != sv) cut_w += w;
        }
        return total_w ? static_cast<double>(cut_w) / static_cast<double>(total_w) : 0.0;
    }

    uint64_t totalWeight() const { return total_; }
    size_t   numEdges()    const { return edges_.size(); }
    uint64_t numTxns()     const { return txns_; }

private:
    std::map<std::pair<uint32_t, uint32_t>, uint64_t> edges_;
    uint64_t total_ = 0;
    uint64_t txns_  = 0;
};

// Mean number of DISTINCT shards a transaction touches (shard fan-out).
template <class ShardOf>
inline double meanFanout(const std::vector<std::vector<uint32_t>>& txn_keys,
                         ShardOf shard_of) {
    if (txn_keys.empty()) return 0.0;
    uint64_t sum = 0;
    for (const auto& keys : txn_keys) {
        std::vector<int> s;
        for (uint32_t k : keys) {
            const int sh = shard_of(k);
            if (sh >= 0) s.push_back(sh);
        }
        std::sort(s.begin(), s.end());
        s.erase(std::unique(s.begin(), s.end()), s.end());
        sum += s.size();
    }
    return static_cast<double>(sum) / static_cast<double>(txn_keys.size());
}

// ── Layout stability ────────────────────────────────────────────────────────
// Fraction of record PAIRS whose co-location differs between two layouts
// (0 = identical partitioning up to shard renaming, 1 = completely different).
// Exact over all pairs would be O(n²); we sample `samples` random pairs.
inline double layoutChurn(const std::vector<int>& a, const std::vector<int>& b,
                          size_t samples = 200000, uint64_t seed = 12345) {
    const size_t n = std::min(a.size(), b.size());
    if (n < 2) return 0.0;
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<size_t> pick(0, n - 1);
    size_t differ = 0, counted = 0;
    for (size_t s = 0; s < samples; ++s) {
        const size_t i = pick(rng), j = pick(rng);
        if (i == j || a[i] < 0 || a[j] < 0 || b[i] < 0 || b[j] < 0) continue;
        const bool together_a = (a[i] == a[j]);
        const bool together_b = (b[i] == b[j]);
        if (together_a != together_b) ++differ;
        ++counted;
    }
    return counted ? static_cast<double>(differ) / static_cast<double>(counted) : 0.0;
}

// ── Summary of repeated runs at one ε ───────────────────────────────────────
struct Stats {
    double mean = 0, sd = 0, min = 0, max = 0;
    size_t n = 0;
};

inline Stats summarize(const std::vector<double>& v) {
    Stats s;
    if (v.empty()) return s;
    s.n    = v.size();
    s.mean = std::accumulate(v.begin(), v.end(), 0.0) / v.size();
    double acc = 0;
    for (double x : v) acc += (x - s.mean) * (x - s.mean);
    s.sd  = v.size() > 1 ? std::sqrt(acc / (v.size() - 1)) : 0.0;
    s.min = *std::min_element(v.begin(), v.end());
    s.max = *std::max_element(v.begin(), v.end());
    return s;
}

// Coefficient of variation of partition sizes (0 = equal-sized partitions).
inline double sizeCV(const std::vector<uint32_t>& sizes) {
    if (sizes.empty()) return 0.0;
    const double mean = std::accumulate(sizes.begin(), sizes.end(), 0.0) / sizes.size();
    if (mean <= 0) return 0.0;
    double acc = 0;
    for (uint32_t x : sizes) acc += (x - mean) * (x - mean);
    return std::sqrt(acc / sizes.size()) / mean;
}

}  // namespace rq2

#endif  // RQ2_AFFINITY_H