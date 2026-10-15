#pragma once
// ============================================================================
// tpcc_std_workload.h — standard TPC-C (5 transaction types) input generator
// for the SidePart vs Obladi benchmark.  Self-contained: no cluster code.
// "Terminal" inputs are generated up front, as in the current T1-T8 generator;
// keys that depend on data read inside the txn are produced by the stage functions.
//
// Wire params (std::vector<int64_t>) per type — the stage functions decode these:
//   NEW_ORDER    : w, d, c, ol_cnt, rbk, {i_id, supply_w, qty} x ol_cnt
//                  rbk = 1 -> the last i_id is unused (= items+1): must roll back
//   PAYMENT      : w, d, c_w, c_d, by_name, key, h_amount_cents
//                  by_name = 1 -> key = C_LAST number 0..999, else key = C_ID
//   ORDER_STATUS : w, d, by_name, key
//   DELIVERY     : w, carrier, d, business_id    (one DB txn per district, §2.7.4.2)
//   STOCK_LEVEL  : w, d, threshold
//
// Terminals (§2.4.1.1, §2.8.1.1): there are terminals_per_w terminals per
// warehouse.  Every request is issued by a terminal drawn uniformly; the
// terminal's HOME warehouse is fixed, so W_ID of every txn type is the
// terminal's warehouse, and Stock-Level also uses the terminal's FIXED
// district (terminal i of a warehouse -> district i).  New-Order / Payment /
// Order-Status still draw D_ID per txn, as the spec says.
// ============================================================================
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace tpcc_std {

enum class StdTxn : int { NEW_ORDER = 0, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL };
constexpr int kNumStdTxn = 5;

inline const char* txnName(StdTxn t) {
    static const char* s[] = {"NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"};
    return s[static_cast<int>(t)];
}

struct Config {
    int warehouses      = 1;
    int districts_per_w = 10;
    int customers_per_d = 3000;
    int terminals_per_w = 10;     // one per district (§4.2.2)
    int items           = 100000;
    // Mix (§5.2.3): Payment >= 43 %, the other three >= 4 %, New-Order the rest.
    std::array<double, kNumStdTxn> weight = {0.45, 0.43, 0.04, 0.04, 0.04};
    // NURand run-time constants (§2.1.6).  c_last_run must satisfy
    // 65 <= |c_last_run - C_LOAD| <= 119 and != 96, 112.  TpccGenerator's
    // makeNonUniformRandom() hard-codes C_LOAD = 42, so 150 (delta 108) is valid.
    int c_last_run  = 150;
    int c_id_run    = 259;
    int ol_i_id_run = 7911;
    // No duplicate (supply_w, i_id) inside one order, so one txn never
    // writes the same STOCK row twice.
    bool distinct_order_lines = true;
};

struct Request {
    StdTxn               type;
    std::vector<int64_t> params;
    int64_t              business_id;   // Delivery: its 10 district txns share it
    int                  terminal = 0;  // 0 .. W*terminals_per_w-1
};

// §4.3.2.3 last-name syllables
inline std::string lastName(int num) {
    static const char* syl[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
                                "ESE", "ANTI", "CALLY", "ATION", "EING"};
    return std::string(syl[num / 100]) + syl[(num / 10) % 10] + syl[num % 10];
}

class Generator {
public:
    Generator(Config c, uint64_t seed) : c_(std::move(c)), rng_(seed) {}

    // n business transactions, shuffled with a fixed seed.  Delivery expands
    // into districts_per_w DB txns, so the result is longer than n.
    std::vector<Request> generate(int n) {
        const std::vector<int> cnt = counts(n);
        std::vector<StdTxn> order;
        for (int t = 0; t < kNumStdTxn; ++t)
            order.insert(order.end(), cnt[t], static_cast<StdTxn>(t));
        std::shuffle(order.begin(), order.end(), rng_);

        std::vector<Request> out;
        out.reserve(order.size() + size_t(cnt[3]) * c_.districts_per_w);
        const int n_term = c_.warehouses * c_.terminals_per_w;
        for (int b = 0; b < static_cast<int>(order.size()); ++b) {
            const int term = uni(0, n_term - 1);
            const int w = term / c_.terminals_per_w + 1;                 // home warehouse
            const int td = term % c_.terminals_per_w % c_.districts_per_w + 1;  // terminal's district
            switch (order[b]) {
            case StdTxn::NEW_ORDER:    out.push_back({order[b], newOrder(w), b, term});        break;
            case StdTxn::PAYMENT:      out.push_back({order[b], payment(w), b, term});         break;
            case StdTxn::ORDER_STATUS: out.push_back({order[b], orderStatus(w), b, term});     break;
            case StdTxn::STOCK_LEVEL:  out.push_back({order[b], stockLevel(w, td), b, term});  break;
            case StdTxn::DELIVERY: {
                const int64_t carrier = uni(1, 10);
                for (int d = 1; d <= c_.districts_per_w; ++d)
                    out.push_back({StdTxn::DELIVERY, {w, carrier, d, b}, b, term});
                break;
            }
            }
        }
        return out;
    }

    // exact per-type counts for n business txns (largest remainder)
    std::vector<int> counts(int n) const {
        std::vector<int> cnt(kNumStdTxn);
        std::vector<std::pair<double, int>> rem;
        int assigned = 0;
        for (int t = 0; t < kNumStdTxn; ++t) {
            const double exact = c_.weight[t] * n;
            cnt[t] = static_cast<int>(std::floor(exact + 1e-9));
            assigned += cnt[t];
            rem.push_back({exact - cnt[t], t});
        }
        std::sort(rem.begin(), rem.end(), [](auto& a, auto& b) { return a.first > b.first; });
        for (int r = 0; r < n - assigned; ++r) cnt[rem[r].second]++;
        return cnt;
    }

private:
    Config          c_;
    std::mt19937_64 rng_;

    int uni(int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng_); }
    int nurand(int A, int x, int y, int C) {
        return (((uni(0, A) | uni(x, y)) + C) % (y - x + 1)) + x;
    }
    int otherWarehouse(int w) {
        if (c_.warehouses == 1) return w;
        int x;
        do { x = uni(1, c_.warehouses); } while (x == w);
        return x;
    }
    // 60 % by last name, 40 % by C_ID (§2.5.1.2, §2.6.1.2)
    std::pair<int, int> customerSelector() {
        if (uni(1, 100) <= 60) return {1, nurand(255, 0, 999, c_.c_last_run)};
        return {0, nurand(1023, 1, c_.customers_per_d, c_.c_id_run)};
    }

    // §2.4.1
    std::vector<int64_t> newOrder(int w) {
        const int  d = uni(1, c_.districts_per_w);
        const int  c = nurand(1023, 1, c_.customers_per_d, c_.c_id_run);
        const int  ol_cnt = uni(5, 15);
        const bool rbk = uni(1, 100) == 1;
        std::vector<int64_t> p = {w, d, c, ol_cnt, rbk ? 1 : 0};
        std::vector<std::pair<int, int>> seen;
        for (int n = 0; n < ol_cnt; ++n) {
            int i, sw;
            do {
                i  = nurand(8191, 1, c_.items, c_.ol_i_id_run);
                sw = (c_.warehouses > 1 && uni(1, 100) == 1) ? otherWarehouse(w) : w;
            } while (c_.distinct_order_lines &&
                     std::find(seen.begin(), seen.end(), std::make_pair(sw, i)) != seen.end());
            seen.push_back({sw, i});
            if (rbk && n == ol_cnt - 1) i = c_.items + 1;   // §2.4.1.4 unused item id
            p.insert(p.end(), {int64_t(i), int64_t(sw), int64_t(uni(1, 10))});
        }
        return p;
    }

    // §2.5.1
    std::vector<int64_t> payment(int w) {
        const int d = uni(1, c_.districts_per_w);
        int cw = w, cd = d;
        if (c_.warehouses > 1 && uni(1, 100) > 85) {      // 15 % remote customer
            cw = otherWarehouse(w);
            cd = uni(1, c_.districts_per_w);
        }
        const auto [by_name, key] = customerSelector();
        return {w, d, cw, cd, by_name, key, uni(100, 500000)};   // H_AMOUNT in cents
    }

    // §2.6.1
    std::vector<int64_t> orderStatus(int w) {
        const int d = uni(1, c_.districts_per_w);
        const auto [by_name, key] = customerSelector();
        return {w, d, by_name, key};
    }

    // §2.8.1: (w, d) are the terminal's fixed home warehouse and district
    std::vector<int64_t> stockLevel(int w, int d) {
        return {w, d, uni(10, 20)};
    }
};

}  // namespace tpcc_std
