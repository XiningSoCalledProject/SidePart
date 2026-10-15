#include "RQ4_Scaling.h"
#include <cmath>
#include <cstdio>
using namespace rq4;
static int fails = 0;
#define CHECK(c) do { if (!(c)) { printf("  FAIL line %d: %s\n", __LINE__, #c); ++fails; } } while (0)
int main() {
    // linear scaling: 1,2,4,8 shards double the throughput each time
    std::vector<Point> lin = {{1,100},{2,200},{4,400},{8,800}};
    CHECK(std::abs(speedup(lin,3) - 8.0) < 1e-9);
    CHECK(std::abs(efficiency(lin,3) - 1.0) < 1e-9);
    CHECK(std::abs(marginalGain(lin,2) - 1.0) < 1e-9);
    CHECK(saturationK(lin, 0.10) == 8);                 // never flattens
    // saturating: gains die after k=4
    std::vector<Point> sat = {{1,100},{2,180},{4,300},{8,310}};
    CHECK(std::abs(speedup(sat,3) - 3.1) < 1e-9);
    CHECK(efficiency(sat,3) < 0.40);                    // far from linear at k=8
    CHECK(std::abs(marginalGain(sat,3) - (310.0/300.0 - 1.0)) < 1e-9);
    CHECK(saturationK(sat, 0.10) == 4);                 // k=4 is the operating point
    // regression: more shards actually hurt
    std::vector<Point> bad = {{1,100},{2,90}};
    CHECK(marginalGain(bad,1) < 0 && saturationK(bad, 0.10) == 1);
    CHECK(saturationK({}, 0.1) == 0);
    printf("sat curve: speedup(8)=%.2f efficiency(8)=%.2f knee=k%u\n",
           speedup(sat,3), efficiency(sat,3), saturationK(sat,0.10));
    printf("%s\n", fails ? "RQ4 SCALING TESTS FAILED" : "ALL RQ4 SCALING TESTS PASSED");
    return fails;
}