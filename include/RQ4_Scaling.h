//
// RQ4_Scaling.h — shard-scaling summary for RQ4 ("How does DePart scale with
// the number of ORAM shards, and where does its performance saturate?").
// Created 9/22/26.
//
// Figure 7 plots transaction throughput against the number of ORAM shards
// (1, 2, 4, 8).  These helpers turn the raw per-k throughputs into the two
// statements the paper makes:
//
//   speedup(k)     = T(k) / T(k_min)             how much parallelism was won
//   efficiency(k)  = speedup(k) / (k / k_min)    1.0 = linear scaling
//   marginalGain   = T(k) / T(previous k) − 1    the diminishing-returns curve
//   saturationK    = the first k whose marginal gain over the previous point
//                    falls below `min_gain` — "the practical operating point".
//
#ifndef RQ4_SCALING_H
#define RQ4_SCALING_H

#include <cstdint>
#include <vector>

namespace rq4 {

struct Point {
    uint32_t k = 0;
    double   throughput = 0;   // txn/s
};

inline double speedup(const std::vector<Point>& pts, size_t i) {
    if (pts.empty() || i >= pts.size() || pts.front().throughput <= 0) return 0.0;
    return pts[i].throughput / pts.front().throughput;
}

inline double efficiency(const std::vector<Point>& pts, size_t i) {
    if (pts.empty() || i >= pts.size() || pts.front().k == 0) return 0.0;
    const double ideal = static_cast<double>(pts[i].k) / static_cast<double>(pts.front().k);
    return ideal > 0 ? speedup(pts, i) / ideal : 0.0;
}

// Relative gain of pts[i] over pts[i-1]; 0 for the first point.
inline double marginalGain(const std::vector<Point>& pts, size_t i) {
    if (i == 0 || i >= pts.size() || pts[i - 1].throughput <= 0) return 0.0;
    return pts[i].throughput / pts[i - 1].throughput - 1.0;
}

// First k whose marginal gain drops below `min_gain` (e.g. 0.10 = +10%).
// Returns the largest k if the curve never flattens — i.e. not saturated yet.
inline uint32_t saturationK(const std::vector<Point>& pts, double min_gain = 0.10) {
    if (pts.empty()) return 0;
    for (size_t i = 1; i < pts.size(); ++i)
        if (marginalGain(pts, i) < min_gain) return pts[i - 1].k;
    return pts.back().k;
}

}  // namespace rq4

#endif  // RQ4_SCALING_H