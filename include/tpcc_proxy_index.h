#pragma once
// ============================================================================
// tpcc_proxy_index.h — trusted-proxy secondary indexes for standard TPC-C.
//
// These only DISCOVER KEYS; row contents are always read through ORAM.
// SidePart and Obladi get the identical index (same rule as the current
// "Random + secondary index" baseline), so it is not a source of advantage.
//
//   CUSTOMER (w,d,C_LAST) -> C_IDs sorted by C_FIRST  (C_FIRST/C_LAST immutable)
//   ORDERS   (w,d,c)      -> latest O_ID              (New-Order commit updates)
//   NEW_ORDER(w,d)        -> pending O_IDs ascending  (New-Order commit adds,
//                                                      Delivery commit removes)
//   ORDERS   (w,d,o)      -> O_OL_CNT                 (New-Order commit adds)
//   DISTRICT (w,d)        -> committed D_NEXT_O_ID    (New-Order commit raises)
//
// MVTSO visibility.  The index holds COMMITTED state only (updated between
// epochs).  Orders created by txns of the CURRENT epoch are exactly the ids
// in [committedNext(w,d), D_NEXT_O_ID), where D_NEXT_O_ID is read from the
// DISTRICT row through MVTSO.  The stage functions (tpcc_std_txns.h) read
// those orders' rows through MVTSO as well, so a reader sees an in-epoch
// order iff MVTSO orders its writer before the reader, and the DISTRICT
// read/write conflict makes the engine abort any writer that would
// otherwise slip in "behind" a reader (phantom protection).
//
// Column positions assume the TPC-C spec column order (§1.3), which the
// existing loader already relies on for STOCK / ORDERS / ORDER_LINE:
//   customer.csv : 0 C_ID, 1 C_D_ID, 2 C_W_ID, 3 C_FIRST, 5 C_LAST
//   order.csv    : 0 O_ID, 1 O_D_ID, 2 O_W_ID, 3 O_C_ID, 6 O_OL_CNT
//   new_order.csv: 0 NO_O_ID, 1 NO_D_ID, 2 NO_W_ID
// ============================================================================
#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "tpcc_std_workload.h"

namespace tpcc_std {

inline std::string csvField(const std::string& row, int i) {
    size_t b = 0;
    for (int c = 0; c < i; ++c) {
        b = row.find(',', b);
        if (b == std::string::npos) return {};
        ++b;
    }
    const size_t e = row.find(',', b);
    std::string v = row.substr(b, e == std::string::npos ? std::string::npos : e - b);
    if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size() - 2);
    return v;
}
inline uint32_t csvU(const std::string& r, int i) { return uint32_t(std::stoul(csvField(r, i))); }

class ProxyIndex {
public:
    void build(const std::vector<std::string>& customer,
               const std::vector<std::string>& orders,
               const std::vector<std::string>& new_order) {
        std::map<std::tuple<uint32_t, uint32_t, std::string>,
                 std::vector<std::pair<std::string, uint32_t>>> tmp;
        for (const auto& r : customer)
            tmp[{csvU(r, 2), csvU(r, 1), csvField(r, 5)}].push_back({csvField(r, 3), csvU(r, 0)});
        for (auto& [k, v] : tmp) {
            std::sort(v.begin(), v.end());
            auto& dst = by_last_[k];
            for (auto& fc : v) dst.push_back(fc.second);
        }
        for (const auto& r : orders) {
            auto& o = latest_[{csvU(r, 2), csvU(r, 1), csvU(r, 3)}];
            o = std::max(o, csvU(r, 0));
            ol_cnt_[{csvU(r, 2), csvU(r, 1), csvU(r, 0)}] = csvU(r, 6);
            auto& nx = next_[{csvU(r, 2), csvU(r, 1)}];
            nx = std::max(nx, csvU(r, 0) + 1);
        }
        for (const auto& r : new_order) pending_[{csvU(r, 2), csvU(r, 1)}].insert(csvU(r, 0));
    }

    // §2.5.2.2: sort by C_FIRST, take position ceil(n/2) (1-based).  -1 = none.
    int64_t customerByLastName(uint32_t w, uint32_t d, int name_num) const {
        auto it = by_last_.find({w, d, lastName(name_num)});
        if (it == by_last_.end() || it->second.empty()) return -1;
        return it->second[(it->second.size() - 1) / 2];
    }

    int64_t latestOrder(uint32_t w, uint32_t d, uint32_t c) const {
        std::lock_guard<std::mutex> g(mu_);
        auto it = latest_.find({w, d, c});
        return it == latest_.end() ? -1 : int64_t(it->second);
    }

    // O_OL_CNT of a COMMITTED order; -1 = unknown (e.g. not committed yet).
    int64_t olCount(uint32_t w, uint32_t d, uint32_t o) const {
        std::lock_guard<std::mutex> g(mu_);
        auto it = ol_cnt_.find({w, d, o});
        return it == ol_cnt_.end() ? -1 : int64_t(it->second);
    }

    // §2.7.4.2: oldest undelivered (committed) orders of (w,d), ascending.
    std::vector<uint32_t> pendingFirst(uint32_t w, uint32_t d, size_t n) const {
        std::lock_guard<std::mutex> g(mu_);
        std::vector<uint32_t> out;
        auto it = pending_.find({w, d});
        if (it == pending_.end()) return out;
        for (uint32_t o : it->second) { if (out.size() >= n) break; out.push_back(o); }
        return out;
    }
    int64_t oldestNewOrder(uint32_t w, uint32_t d) const {
        auto v = pendingFirst(w, d, 1);
        return v.empty() ? -1 : int64_t(v[0]);
    }
    // committed D_NEXT_O_ID (= max committed O_ID + 1)
    uint32_t committedNext(uint32_t w, uint32_t d) const {
        std::lock_guard<std::mutex> g(mu_);
        auto it = next_.find({w, d});
        return it == next_.end() ? 1u : it->second;
    }

    // Commit hooks — call ONLY after the txn commits.  Two Deliveries of the
    // same district in one epoch both pick the same O_ID; the write-write
    // conflict on that NEW_ORDER row aborts one, which then retries.
    void onNewOrderCommit(uint32_t w, uint32_t d, uint32_t c, uint32_t o, uint32_t ol_cnt) {
        std::lock_guard<std::mutex> g(mu_);
        ol_cnt_[{w, d, o}] = ol_cnt;
        auto& x = latest_[{w, d, c}];
        x = std::max(x, o);
        auto& nx = next_[{w, d}];
        nx = std::max(nx, o + 1);
        // A Delivery of the same epoch may already have delivered it (its hook
        // can never run first — results come in timestamp order — but be safe).
        if (!delivered_.count({w, d, o})) pending_[{w, d}].insert(o);
    }
    void onDeliveryCommit(uint32_t w, uint32_t d, uint32_t o) {
        std::lock_guard<std::mutex> g(mu_);
        pending_[{w, d}].erase(o);
        delivered_.insert({w, d, o});
    }

private:
    std::map<std::tuple<uint32_t, uint32_t, std::string>, std::vector<uint32_t>> by_last_;
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, uint32_t> latest_;
    std::map<std::pair<uint32_t, uint32_t>, std::set<uint32_t>>  pending_;
    std::map<std::tuple<uint32_t, uint32_t, uint32_t>, uint32_t> ol_cnt_;
    std::map<std::pair<uint32_t, uint32_t>, uint32_t>            next_;
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>>           delivered_;
    mutable std::mutex mu_;
};

}  // namespace tpcc_std
