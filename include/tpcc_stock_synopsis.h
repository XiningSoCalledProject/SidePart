#pragma once
// ============================================================================
// tpcc_stock_synopsis.h — DP load synopsis for TPC-C Stock-Level (SidePart).
//
// Stock-Level(w, d, threshold) = #distinct items in the order lines of the
// district's last 20 orders whose S_QUANTITY < threshold (threshold 10..20).
//
// Privacy model (10/9/26): the synopsis is GENERATED ONLY AT REPARTITIONING
// and then REUSED UNCHANGED until the next repartitioning.  In these runs the
// data is partitioned once, at load, so there is exactly one release per run
// (releases() == 1); call repartition() wherever the system repartitions.
//
// The release (one per repartitioning).  For every district (w, d) the proxy
// publishes a noisy histogram of the district's recent items by S_QUANTITY:
//     bucket 0      : S_QUANTITY < 10
//     bucket 1..10  : S_QUANTITY == 9 + b      (10, 11, ..., 19)
//     noisy[b] = exact[b] + Laplace(sens / eps)
// Any threshold t in [10, 20] is answered by post-processing (no extra
// budget):   answer(w, d, t) = max(0, sum_{b < t - 9} noisy[b]).
//
// Sensitivity (L1 of the whole release), neighbouring = one STOCK row's
// S_QUANTITY changed: the item moves between two buckets (L1 change 2) in
// every district whose recent orders contain it, at most the 10 districts of
// its warehouse  ->  sens = 2 x 10 = 20 (default, TPCC_SL_SENS).  Use a
// smaller value only if the paper's neighbouring relation justifies it.
//
// Between repartitionings the answers do NOT follow the data: they are those
// of the database at release time + noise.  The proxy still mirrors the
// committed state (S_QUANTITY per (w, i), last 20 orders per district,
// updated in commit order between epochs) only to MEASURE the error:
//     meanAbsErrorNoise() : |answer - exact at release time|   (DP noise)
//     meanAbsError()      : |answer - exact now|               (noise + staleness)
// The mirror is never used to answer.
// ============================================================================
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <deque>
#include <map>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "tpcc_proxy_index.h"   // csvField / csvU

namespace tpcc_std {

class StockLevelSynopsis {
public:
    static constexpr int kBuckets = 11;               // <10, 10, 11, ..., 19

    StockLevelSynopsis(double eps, uint64_t seed, double sens = 20.0)
        : eps_(eps), sens_(sens), rng_(seed) {}

    // Load the committed state and make the first release (= the
    // repartitioning at load time).
    // stock.csv: 0 S_I_ID, 1 S_W_ID, 2 S_QUANTITY
    // order_line.csv: 0 OL_O_ID, 1 OL_D_ID, 2 OL_W_ID, 3 OL_NUMBER, 4 OL_I_ID
    void build(const std::vector<std::string>& stock, const std::vector<std::string>& order_line) {
        for (const auto& r : stock) {
            const uint32_t i = csvU(r, 0), w = csvU(r, 1);
            qty_[w][i] = int32_t(std::stol(csvField(r, 2)));
        }
        std::map<std::pair<uint32_t, uint32_t>, std::map<uint32_t, std::vector<uint32_t>>> by_order;
        for (const auto& r : order_line)
            by_order[{csvU(r, 2), csvU(r, 1)}][csvU(r, 0)].push_back(csvU(r, 4));
        for (auto& [wd, orders] : by_order) {
            auto& dq = recent_[wd];
            for (auto& [o, items] : orders) {          // ascending O_ID; keep the last 20
                dq.push_back({o, std::move(items)});
                if (dq.size() > kWindow) dq.pop_front();
            }
        }
        repartition();
    }

    // New release from the current committed state.  Call ONLY when the
    // system repartitions; every call spends eps.
    void repartition() {
        noisy_.clear();
        exact_at_release_.clear();
        for (const auto& kv : recent_) {
            const auto ex = histogram(kv.first.first, kv.first.second);
            std::array<double, kBuckets> nz{};
            for (int b = 0; b < kBuckets; ++b) nz[size_t(b)] = double(ex[size_t(b)]) + laplace(sens_ / eps_);
            noisy_[kv.first] = nz;
            exact_at_release_[kv.first] = ex;
        }
        ++releases_;
    }

    // Committed New-Order (not rolled back), in commit order.  Updates only
    // the measurement mirror; the published synopsis stays unchanged.
    void onNewOrder(uint32_t w, uint32_t d, uint32_t o,
                    const std::vector<std::tuple<uint32_t, uint32_t, int64_t>>& lines) {
        std::vector<uint32_t> items;
        for (const auto& [i, sw, q] : lines) {
            auto& s = qty_[sw][i];
            s = (s >= q + 10) ? int32_t(s - q) : int32_t(s - q + 91);
            items.push_back(i);
        }
        auto& dq = recent_[{w, d}];
        dq.push_back({o, std::move(items)});
        while (dq.size() > kWindow) dq.pop_front();
    }

    // Answer from the published synopsis (post-processing only).
    double answer(uint32_t w, uint32_t d, int64_t threshold) {
        const int nb = bucketsBelow(threshold);
        double ans = 0;
        auto it = noisy_.find({w, d});
        if (it != noisy_.end())
            for (int b = 0; b < nb; ++b) ans += it->second[size_t(b)];
        ans = std::max(0.0, ans);

        double ex0 = 0;                                // exact at release time
        auto e0 = exact_at_release_.find({w, d});
        if (e0 != exact_at_release_.end())
            for (int b = 0; b < nb; ++b) ex0 += double(e0->second[size_t(b)]);
        const double now = double(exact(w, d, threshold));
        err_noise_sum_ += std::fabs(ans - ex0);
        err_total_sum_ += std::fabs(ans - now);
        exact_sum_     += now;
        ++queries_;
        return ans;
    }

    // Exact Stock-Level on the CURRENT committed state (measurement only).
    int64_t exact(uint32_t w, uint32_t d, int64_t threshold) const {
        auto it = recent_.find({w, d});
        if (it == recent_.end()) return 0;
        auto qw = qty_.find(w);
        if (qw == qty_.end()) return 0;
        std::set<uint32_t> low;
        for (const auto& [o, items] : it->second)
            for (uint32_t i : items) {
                auto q = qw->second.find(i);
                if (q != qw->second.end() && q->second < threshold) low.insert(i);
            }
        return int64_t(low.size());
    }

    int64_t  quantity(uint32_t w, uint32_t i) const {
        auto a = qty_.find(w);
        if (a == qty_.end()) return -1;
        auto b = a->second.find(i);
        return b == a->second.end() ? -1 : b->second;
    }
    double   eps() const { return eps_; }
    double   sensitivity() const { return sens_; }
    uint64_t queries() const { return queries_; }
    uint64_t releases() const { return releases_; }
    double   meanAbsError() const { return queries_ ? err_total_sum_ / double(queries_) : 0; }
    double   meanAbsErrorNoise() const { return queries_ ? err_noise_sum_ / double(queries_) : 0; }
    double   meanExact() const { return queries_ ? exact_sum_ / double(queries_) : 0; }

private:
    static constexpr size_t kWindow = 20;
    // #buckets whose quantities are all < threshold (threshold clamped to [10, 20]).
    static int bucketsBelow(int64_t t) { return int(std::min<int64_t>(20, std::max<int64_t>(10, t)) - 9); }
    static int bucketOf(int32_t q) { return q < 10 ? 0 : (q < 20 ? q - 9 : -1); }

    std::array<int64_t, kBuckets> histogram(uint32_t w, uint32_t d) const {
        std::array<int64_t, kBuckets> h{};
        auto it = recent_.find({w, d});
        auto qw = qty_.find(w);
        if (it == recent_.end() || qw == qty_.end()) return h;
        std::set<uint32_t> seen;                       // distinct items
        for (const auto& [o, items] : it->second)
            for (uint32_t i : items) {
                if (!seen.insert(i).second) continue;
                auto q = qw->second.find(i);
                if (q == qw->second.end()) continue;
                const int b = bucketOf(q->second);
                if (b >= 0) ++h[size_t(b)];
            }
        return h;
    }
    double laplace(double b) {
        std::uniform_real_distribution<double> u(-0.5, 0.5);
        const double x = u(rng_);
        const double m = std::max(1e-300, 1.0 - 2.0 * std::fabs(x));
        return -b * (x < 0 ? -1.0 : 1.0) * std::log(m);
    }

    double eps_, sens_;
    std::mt19937_64 rng_;
    std::map<uint32_t, std::map<uint32_t, int32_t>> qty_;                                // mirror: w -> i -> S_QUANTITY
    std::map<std::pair<uint32_t, uint32_t>, std::deque<std::pair<uint32_t, std::vector<uint32_t>>>> recent_;
    std::map<std::pair<uint32_t, uint32_t>, std::array<double, kBuckets>>  noisy_;       // the published synopsis
    std::map<std::pair<uint32_t, uint32_t>, std::array<int64_t, kBuckets>> exact_at_release_;
    uint64_t queries_ = 0, releases_ = 0;
    double   err_noise_sum_ = 0, err_total_sum_ = 0, exact_sum_ = 0;
};

}  // namespace tpcc_std
