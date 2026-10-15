// Sanity test for RQ3_Workload.h: does Imbalance separate the four settings?
// Two shard mappings: DePart-like (balance rows across shards, regions kept
// contiguous) and Random + index (every ROW on a uniformly random shard).
#include "RQ3_Workload.h"
#include <cstdio>
#include <map>
#include <random>
#include <set>
using namespace rq3;
static int fails = 0;
#define CHECK(c) do { if (!(c)) { printf("  FAIL line %d: %s\n", __LINE__, #c); ++fails; } } while (0)

// DePart-like: contiguous regions greedily packed so each shard holds ~equal ROWS.
static std::vector<int> balancedMap(const Dataset& d, int k) {
    std::vector<int> shard_of_region(d.num_regions, 0);
    uint64_t total = d.rows.size(), target = total / k, acc = 0; int s = 0;
    for (uint32_t r = 0; r < d.num_regions; ++r) {
        shard_of_region[r] = s;
        acc += d.rows_of_region[r].size();
        if (acc >= target && s < k - 1) { ++s; acc = 0; }
    }
    return shard_of_region;
}
// Random + index: row-level placement, same as rowRandomPlacement() in
// RQ3_DistShift.cpp and the TPC-C "index" baseline.  Indexed by ROW, not region.
static std::vector<int> randomRowMap(const Dataset& d, int k, uint64_t seed) {
    std::mt19937_64 rng(seed ^ 0x9E3779B97F4A7C15ULL);
    std::vector<int> m(d.rows.size(), 0);
    for (auto& s : m) s = static_cast<int>(rng() % k);
    return m;
}

int main() {
    const uint32_t ROWS = 20000, REGIONS = 40, QUERIES = 20000;
    const int K = 4; const double ALPHA = 1.0;
    printf("%-34s %10s %10s   %s\n", "setting", "DePart", "Random", "per-shard L_i (DePart)");
    for (const auto& st : settings()) {
        Dataset d = generateDataset(ROWS, REGIONS, st.data, ALPHA, 42);
        auto bal = balancedMap(d, K);          // indexed by region
        auto rnd = randomRowMap(d, K, 7);      // indexed by row
        ShardLoad lb(K), lr(K);
        QueryKeyGen g1(d, st.query, ALPHA, 99), g2(d, st.query, ALPHA, 99);
        for (uint32_t q = 0; q < QUERIES; ++q) {
            lb.addQuery(bal[d.region_of_row[g1.nextKey()]]);
            lr.addQuery(rnd[g2.nextKey()]);
        }
        printf("%-34s %10.2f %10.2f   [%s]\n", st.name,
               imbalance(lb.queries), imbalance(lr.queries), joinLoads(lb.queries, ',').c_str());
        // every query must be counted exactly once
        uint64_t tot = 0; for (auto v : lb.queries) tot += v; CHECK(tot == QUERIES);
        uint64_t totr = 0; for (auto v : lr.queries) totr += v; CHECK(totr == QUERIES);
    }
    // Explicit expectations
    {   Dataset d = generateDataset(ROWS, REGIONS, DataDist::UNIFORM, ALPHA, 42);
        auto bal = balancedMap(d, K);
        ShardLoad u(K), s(K);
        QueryKeyGen gu(d, QueryDist::UNIFORM, ALPHA, 1), gs(d, QueryDist::SKEWED_MATCHED, ALPHA, 1);
        for (uint32_t q = 0; q < QUERIES; ++q) { u.addQuery(bal[d.region_of_row[gu.nextKey()]]);
                                                 s.addQuery(bal[d.region_of_row[gs.nextKey()]]); }
        CHECK(imbalance(u.queries) < 1.05);        // uniform → balanced
        CHECK(imbalance(s.queries) > 1.5);         // skewed queries → hotspot
        printf("\nuniform data: uniform queries imbalance=%.3f, skewed queries imbalance=%.3f\n",
               imbalance(u.queries), imbalance(s.queries));
    }
    // Row-level random placement: a region's rows land on all shards, so a hot
    // REGION does not become a hot shard (imbalance printed for inspection).
    {   Dataset d = generateDataset(ROWS, REGIONS, DataDist::UNIFORM, ALPHA, 42);
        auto rnd = randomRowMap(d, K, 7);
        ShardLoad s(K);
        QueryKeyGen gs(d, QueryDist::SKEWED_MATCHED, ALPHA, 1);
        for (uint32_t q = 0; q < QUERIES; ++q) s.addQuery(rnd[gs.nextKey()]);
        // A single region's rows must not all sit on one shard.
        std::set<int> sh; for (uint32_t r : d.rows_of_region[0]) sh.insert(rnd[r]);
        CHECK(d.rows_of_region[0].size() < 8 || sh.size() == (size_t)K);
        printf("random+index: skewed queries imbalance=%.3f, region 0 spans %zu shards\n",
               imbalance(s.queries), sh.size());
    }
    CHECK(imbalance({0,0,0,0}) == 1.0);
    CHECK(std::abs(imbalance({100,0,0,0}) - 4.0) < 1e-9);   // all on one shard = k
    printf("%s\n", fails ? "RQ3 METRIC TESTS FAILED" : "ALL RQ3 METRIC TESTS PASSED");
    return fails;
}