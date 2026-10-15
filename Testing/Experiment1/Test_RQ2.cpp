// Sanity test for RQ2_Affinity.h: does AffinityCut behave like a locality metric?
#include "RQ2_Affinity.h"
#include "RQ3_Workload.h"
#include "RQ2_AggSynopsis.h"
#include <cstdio>
using namespace rq2;
static int fails = 0;
#define CHECK(c) do { if (!(c)) { printf("  FAIL line %d: %s\n", __LINE__, #c); ++fails; } } while (0)

// ── 5. Synopsis ablation (RQ2_AggSynopsis.h, 10/13/26) ──────────────────────
// BEGIN_AGG_TESTS
static void testAggSynopsis() {
    // regions of uneven size; values 0..9999 in the 3rd CSV field
    const uint32_t REGIONS = 7, G = 32;
    std::vector<std::vector<uint32_t>> rows_of_region(REGIONS);
    std::vector<std::string> rows;
    std::mt19937_64 rng(11);
    for (uint32_t r = 0; r < REGIONS; ++r)
        for (uint32_t i = 0; i < 20 + 37 * r; ++i) {           // 20, 57, 94, ... rows
            const uint32_t pk = static_cast<uint32_t>(rows.size());
            rows_of_region[r].push_back(pk);
            rows.push_back(std::to_string(pk) + "," + std::to_string(r) + "," +
                           std::to_string(rng() % 10000) + "," + std::to_string(rng() % 100000));
        }
    // 5a. groups: inside one region, every row in exactly one group, sizes in [4, G + 3]
    const rq2::AggGroups gr = rq2::buildAggGroups(rows_of_region, rows.size(), G);
    {   std::vector<int> seen(rows.size(), 0);
        bool one_region = true, sizes_ok = true;
        for (const auto& m : gr.members) {
            sizes_ok &= m.size() >= 4 && m.size() <= G + 3;
            for (uint32_t pk : m) { ++seen[pk]; one_region &= (rows[pk].find(',' + std::to_string(
                                        std::stoul(rows[m[0]].substr(rows[m[0]].find(',') + 1))) + ',') != std::string::npos); }
        }
        bool all_once = true;
        for (size_t i = 0; i < rows.size(); ++i) all_once &= (seen[i] == 1) && gr.group_of_row[i] >= 0;
        CHECK(all_once); CHECK(sizes_ok); CHECK(one_region);
        printf("5a: %zu rows -> %zu groups (G=%u), each row once, groups stay in one region\n",
               rows.size(), gr.members.size(), G); }
    // 5b. value parsing
    CHECK(rq2::aggValueOfRow("12,3,4567,89") == 4567);
    CHECK(rq2::aggValueOfRow("no-commas") >= 0 && rq2::aggValueOfRow("no-commas") < 10000);
    std::vector<int64_t> vals(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) vals[i] = rq2::aggValueOfRow(rows[i]);
    rq2::AggSynopsis syn(gr, vals);
    // 5c. proxy exact answer == what ORAM execution computes from the fetched rows
    {   bool same = true; uint64_t checked = 0;
        for (size_t g = 0; g < syn.numGroups(); ++g)
            for (int64_t t = 1000; t <= 10000; t += 1000, ++checked)
                same &= (syn.answerExact(g, t) == syn.exactFromValues(g, t));
        CHECK(same);
        CHECK(syn.answerExact(0, 10000) == static_cast<int64_t>(gr.members[0].size()));   // t = max: all rows
        printf("5c: exact proxy answer == ORAM-executed answer on %llu (group, t) pairs\n",
               (unsigned long long)checked); }
    // 5d. thresholds are whole buckets
    {   bool ok = true;
        for (uint64_t d = 0; d < 100; ++d) { const int64_t t = rq2::AggSynopsis::threshold(d); ok &= t >= 1000 && t <= 10000 && t % 1000 == 0; }
        CHECK(ok); }
    // 5e. noisy release: deterministic per seed, non-negative, error falls with eps
    auto meanErr = [&](double eps, uint64_t seed) {
        rq2::AggSynopsis s2(gr, vals); s2.release(eps, seed);
        double e = 0; uint64_t n = 0; bool nonneg = true;
        for (size_t g = 0; g < s2.numGroups(); ++g)
            for (int64_t t = 1000; t <= 10000; t += 1000, ++n) {
                const double a = s2.answerNoisy(g, t);
                nonneg &= a >= 0.0;
                e += std::fabs(a - double(s2.answerExact(g, t)));
            }
        CHECK(nonneg); CHECK(s2.releases() == 1);
        return e / double(n);
    };
    {   rq2::AggSynopsis a(gr, vals), b(gr, vals);
        a.release(0.5, 99); b.release(0.5, 99);
        CHECK(a.answerNoisy(1, 5000) == b.answerNoisy(1, 5000));       // same seed, same release
        a.release(0.5, 100);
        CHECK(a.releases() == 2);                                        // one count per release
        double e_lo = 0, e_mid = 0, e_hi = 0;
        for (uint64_t sd = 0; sd < 20; ++sd) { e_lo += meanErr(0.1, sd); e_mid += meanErr(1.0, sd); e_hi += meanErr(10.0, sd); }
        e_lo /= 20; e_mid /= 20; e_hi /= 20;
        CHECK(e_lo > e_mid && e_mid > e_hi);
        CHECK(e_mid > 0.5 && e_mid < 20.0);                            // Laplace(2/1) summed over <= 10 buckets
        printf("5e: noisy mean |error|  eps=0.1: %.2f  eps=1: %.2f  eps=10: %.3f (falls with eps)\n",
               e_lo, e_mid, e_hi); }
}
// END_AGG_TESTS

int main() {
    // ── 1. Hand-checkable graph: txns {1,2} {2,3} {4,5}
    {   AffinityGraph g;
        g.addTransaction({1,2}); g.addTransaction({2,3}); g.addTransaction({4,5});
        CHECK(g.numEdges() == 3 && g.totalWeight() == 3);
        std::vector<int> perfect = {0,0,0,0,1,1};            // idx = key
        CHECK(g.cut([&](uint32_t k){ return perfect[k]; }) == 0.0);
        std::vector<int> split12 = {0,0,1,1,0,1};            // cuts (1,2) and (4,5)
        CHECK(std::abs(g.cut([&](uint32_t k){ return split12[k]; }) - 2.0/3.0) < 1e-9);
        std::vector<int> all_apart = {0,1,2,3,4,5};
        CHECK(g.cut([&](uint32_t k){ return all_apart[k]; }) == 1.0);
        // a repeated pair weighs more
        AffinityGraph g2; for (int i=0;i<9;i++) g2.addTransaction({1,2}); g2.addTransaction({4,5});
        CHECK(std::abs(g2.cut([&](uint32_t k){ return split12[k]; }) - 1.0) < 1e-9);   // both cut
        CHECK(std::abs(g2.cut([&](uint32_t k){ return k <= 2 ? 0 : 1; }) - 0.0) < 1e-9);
        // duplicate key inside one txn must not create a self-edge
        AffinityGraph g3; g3.addTransaction({7,7}); CHECK(g3.numEdges() == 0);
        printf("1: hand-checked cuts OK\n"); }

    // ── 2. A partitioning that follows the workload beats a random one
    {   const uint32_t ROWS = 4000, REGIONS = 20; const int K = 4;
        rq3::Dataset d = rq3::generateDataset(ROWS, REGIONS, rq3::DataDist::UNIFORM, 1.0, 1);
        // Workload: each txn touches 3 rows of ONE region → affinity follows regions
        rq3::QueryKeyGen g(d, rq3::QueryDist::UNIFORM, 1.0, 2);
        AffinityGraph graph; std::vector<std::vector<uint32_t>> txns;
        std::mt19937_64 rng(3);
        for (int t = 0; t < 3000; ++t) {
            const uint32_t seed_key = g.nextKey();
            const auto& rows = d.rows_of_region[d.region_of_row[seed_key]];
            std::vector<uint32_t> keys;
            for (int q = 0; q < 3; ++q) keys.push_back(rows[rng() % rows.size()]);
            graph.addTransaction(keys); txns.push_back(keys);
        }
        // locality-aware: whole regions kept together
        std::vector<int> byRegion(ROWS), byHash(ROWS);
        for (uint32_t r = 0; r < ROWS; ++r) {
            byRegion[r] = d.region_of_row[r] % K;
            byHash[r]   = static_cast<int>((r * 2654435761u) % K);   // ignores affinity
        }
        const double cut_region = graph.cut([&](uint32_t k){ return byRegion[k]; });
        const double cut_hash   = graph.cut([&](uint32_t k){ return byHash[k]; });
        const double fan_region = meanFanout(txns, [&](uint32_t k){ return byRegion[k]; });
        const double fan_hash   = meanFanout(txns, [&](uint32_t k){ return byHash[k]; });
        printf("2: cut(region-aligned)=%.3f fanout=%.2f | cut(hash)=%.3f fanout=%.2f\n",
               cut_region, fan_region, cut_hash, fan_hash);
        CHECK(cut_region < 0.05);            // co-accessed rows stay together
        CHECK(cut_hash   > 0.60);            // hashing splits ~(1-1/K)
        CHECK(fan_region < 1.05 && fan_hash > 2.0); }

    // ── 3. Stability: churn is 0 vs itself, ~0 under renaming, high vs random
    {   const size_t N = 5000; std::vector<int> a(N), renamed(N), rnd(N);
        std::mt19937_64 rng(7);
        for (size_t i = 0; i < N; ++i) { a[i] = int(i % 4); renamed[i] = 3 - a[i]; rnd[i] = int(rng() % 4); }
        CHECK(layoutChurn(a, a) == 0.0);
        CHECK(layoutChurn(a, renamed) == 0.0);            // invariant to shard renaming
        const double churn_rnd = layoutChurn(a, rnd);
        printf("3: churn(self)=0  churn(renamed)=0  churn(random)=%.3f\n", churn_rnd);
        CHECK(churn_rnd > 0.3); }

    // ── 4. summarize / sizeCV
    {   Stats s = summarize({1.0, 2.0, 3.0});
        CHECK(s.n == 3 && std::abs(s.mean - 2.0) < 1e-9 && std::abs(s.sd - 1.0) < 1e-9);
        CHECK(s.min == 1.0 && s.max == 3.0);
        CHECK(sizeCV({100,100,100,100}) == 0.0);
        CHECK(sizeCV({10,10,10,370}) > 1.0);
        printf("4: summarize / sizeCV OK\n"); }

    testAggSynopsis();

    printf("%s\n", fails ? "RQ2 METRIC TESTS FAILED" : "ALL RQ2 METRIC TESTS PASSED");
    return fails;
}