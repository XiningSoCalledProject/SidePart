#pragma once
// ============================================================================
// tpcc_std_txns.h — proxy-side state machines of the 5 standard TPC-C txns
// (shared by TPCC_Std_Main.cpp and tests/tpcc_std_mvtso_test.cpp).
//
// Wire params: tpcc_std_workload.h, plus ONE trailing param = request tag
// (unique per request, stable across retries).
//
// MVTSO and the proxy indexes
// ---------------------------
// The indexes (tpcc_proxy_index.h) hold COMMITTED state and change only
// between epochs.  Everything an index cannot know — orders created, and
// orders delivered, by OTHER txns of the SAME epoch — is taken from rows
// read through the MVTSO version store, so every lookup sees exactly the
// writes that precede the reader in timestamp order:
//
//   in-epoch orders of (w,d) = [idx.committedNext(w,d), D_NEXT_O_ID), with
//                              D_NEXT_O_ID read from the DISTRICT row
//   "already delivered"       = O_CARRIER_ID set in the ORDERS row read
//
// A reader that reads DISTRICT(w,d) before an older New-Order writes it
// makes that New-Order abort (WRITE_TOO_LATE), so no order can appear
// "behind" a reader.  This is the predicate/phantom protection an index
// lock would give, done with the engine's own read markers.
//
// Payment escrow (StdCtx::escrow, default on; approved 10/9/26)
// -----------------------------------------------------------------
// W_YTD / D_YTD are pure running sums: no txn reads them for a decision.
// Payment therefore does not UPDATE WAREHOUSE / DISTRICT; it emits
// FieldDelta(W_YTD, +h) and FieldDelta(D_YTD, +h) (in cents), and the
// engine's write phase adds the committed sum once per row, on top of the
// row's latest version (a New-Order's D_NEXT_O_ID write) or its base value.
// Payment's reads of W and D are stable reads (only W_NAME / D_NAME, never
// written), so they set no read marker and cannot abort a New-Order.
// Without escrow, all Payments of one warehouse write the same WAREHOUSE row
// and MVTSO lets at most one of them commit per epoch (WRITE_TOO_LATE).
// D_NEXT_O_ID is NOT escrowed: New-Order reads it to pick O_ID.
//
// Stock-Level DP synopsis (StdCtx::sl_synopsis, SidePart only, 10/9/26):
// Stock-Level is answered by the proxy from tpcc_stock_synopsis.h with no
// ORAM access.  The synopsis is released once at (re)partitioning and reused
// unchanged until the next one (approximate: data at release time + noise).
//
// Read rounds per txn (cfg.R must cover them; arrival sub-epoch counts):
//   NewOrder 1 | Payment 1 | OrderStatus 2 (3 if the district has in-epoch
//   orders) | Delivery 2 (3 if the index has no undelivered order left)
//   | StockLevel 3
// ============================================================================
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <map>
#include <cstdio>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "DePart_TPCC_Driver.h"
#include "tpcc_proxy_index.h"
#include "tpcc_stock_synopsis.h"
#include "tpcc_std_workload.h"

namespace stdrun {
using tpcc_std::StdTxn;
using P     = std::vector<int64_t>;
using Prior = std::vector<std::vector<std::string>>;

namespace key {   // canonical encoders (ObjectKey.h)
using tpcc_key::warehouse;
using tpcc_key::district;
using tpcc_key::customer;
using tpcc_key::history;   // h_id: load row index + 1, then hist_base + tag
}  // namespace key

// ── Column positions (TPC-C §1.3 order = TpccGenerator.cpp) ────────────────
namespace col {
constexpr int W_NAME = 1, W_TAX = 7, W_YTD = 8;                                   // 9 cols
constexpr int D_NAME = 2, D_TAX = 8, D_YTD = 9, D_NEXT_O_ID = 10;                 // 11
constexpr int C_CREDIT = 13, C_DISCOUNT = 15, C_BALANCE = 16, C_YTD_PAYMENT = 17,
              C_PAYMENT_CNT = 18, C_DELIVERY_CNT = 19, C_DATA = 20;               // 21
constexpr int I_PRICE = 3;                                                        // 5
constexpr int S_QUANTITY = 2, S_DIST_01 = 3, S_YTD = 13, S_ORDER_CNT = 14,
              S_REMOTE_CNT = 15;                                                  // 17
constexpr int O_ID = 0, O_C_ID = 3, O_CARRIER_ID = 5, O_OL_CNT = 6;               // 8
constexpr int OL_O_ID = 0, OL_I_ID = 4, OL_DELIVERY_D = 6, OL_AMOUNT = 8;         // 10
}  // namespace col

// ── Tuple helpers ───────────────────────────────────────────────────────────
struct Row {
    std::vector<std::string> f;
    static Row parse(const std::string& oram_result) {
        Row r;
        const std::string t = depart::DePartRecordFormat::tupleOf(oram_result);
        if (t.empty()) return r;
        size_t b = 0;
        for (;;) {
            const size_t e = t.find(',', b);
            r.f.push_back(t.substr(b, e == std::string::npos ? std::string::npos : e - b));
            if (e == std::string::npos) break;
            b = e + 1;
        }
        return r;
    }
    bool ok(size_t min_cols) const { return f.size() >= min_cols; }
    std::string raw(int i) const {
        if (i < 0 || size_t(i) >= f.size()) return {};
        std::string v = f[size_t(i)];
        if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size() - 2);
        return v;
    }
    double  num(int i) const { try { return std::stod(raw(i)); } catch (...) { return 0.0; } }
    int64_t inum(int i) const { try { return std::stoll(raw(i)); } catch (...) { return 0; } }
    void set(int i, const std::string& v) { if (i >= 0 && size_t(i) < f.size()) f[size_t(i)] = v; }
    std::string str() const {
        std::string s;
        for (size_t i = 0; i < f.size(); ++i) { if (i) s += ','; s += f[i]; }
        return s;
    }
};
inline std::string money(double v) { char b[48]; std::snprintf(b, sizeof b, "%.2f", v); return b; }
inline int64_t nowSec() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
// Loader writes "null" for an undelivered order; New-Order here writes "0".
inline bool undelivered(const Row& o) {
    const std::string c = o.raw(col::O_CARRIER_ID);
    return c == "null" || c == "0" || c.empty();
}

// ── Shared proxy state ──────────────────────────────────────────────────────
struct StdCtx {
    std::shared_ptr<depart::DePartContext> cx;      // route + record format
    tpcc_std::ProxyIndex                   idx;
    uint32_t                               items = 100000;
    uint64_t                               hist_base = 1;   // first free HISTORY h_id
    size_t                                 delivery_window = 8;   // index candidates per Delivery
    // per request tag: O_ID chosen by its latest attempt (-1 = none / skipped)
    std::unique_ptr<std::atomic<int64_t>[]> assigned;
    size_t                                  n_assigned = 0;
    std::atomic<uint64_t> bad_rows{0};              // a row that must exist came back empty
    bool escrow = true;                             // Payment W_YTD / D_YTD as FieldDeltas
    // Stock-Level from the DP synopsis (tpcc_stock_synopsis.h): 0 ORAM ops,
    // approximate answer.  nullptr = exact Stock-Level through ORAM.
    std::shared_ptr<tpcc_std::StockLevelSynopsis> sl_synopsis;

    void resetAssigned(size_t n) {
        assigned.reset(new std::atomic<int64_t>[n]);
        n_assigned = n;
        for (size_t i = 0; i < n; ++i) assigned[i] = -1;
    }
};

inline Operat R(StdCtx& s, const ObjectKey& k) { return depart::makeOp(*s.cx, OpType::READ, k); }
inline Operat U(StdCtx& s, const ObjectKey& k, const std::string& t) {
    return depart::makeOp(*s.cx, OpType::UPDATE, k, s.cx->fmt.fit(t));
}
inline Operat I(StdCtx& s, const ObjectKey& k, const std::string& t) {
    return depart::makeOp(*s.cx, OpType::INSERT, k, s.cx->fmt.fit(t));
}
inline TxnTypeId tid(StdTxn t) { return static_cast<TxnTypeId>(static_cast<int>(t) + 1); }
inline uint64_t  tagOf(const P& p) { return static_cast<uint64_t>(p.back()); }
// No more ops, but not done: the engine runs empty stages locally until
// max_local_stages and then aborts the attempt with UNFINISHED; the driver
// retries it in the next epoch (after the index caught up).  Used only for
// the rare "every candidate was delivered by older txns of this epoch".
inline TxnStage retryNextEpoch() { return TxnStage{{}, false}; }

inline uint32_t resolveCustomer(const StdCtx& s, uint32_t w, uint32_t d, int64_t by_name, int64_t k) {
    if (by_name != 1) return uint32_t(k);
    const int64_t c = s.idx.customerByLastName(w, d, int(k));   // C_LAST/C_FIRST are immutable
    return c > 0 ? uint32_t(c) : 1u;
}

// ── 1. New-Order  p = w,d,c,ol_cnt,rbk,{i,sw,q}*n,tag ──────────────────────
inline TxnStage newOrder(StdCtx& s, uint32_t st, const P& p, const Prior& pr) {
    const uint32_t w = uint32_t(p[0]), d = uint32_t(p[1]), c = uint32_t(p[2]), n = uint32_t(p[3]);
    const bool rbk = p[4] == 1;
    auto item_of = [&](uint32_t j) { return uint32_t(p[5 + 3 * j]); };
    auto sw_of   = [&](uint32_t j) { return uint32_t(p[6 + 3 * j]); };
    auto q_of    = [&](uint32_t j) { return int64_t(p[7 + 3 * j]); };
    // Unused item id (1% rollback): read a real row of the same table instead,
    // so the access pattern does not reveal the rollback.
    auto real_item = [&](uint32_t i) { return (i >= 1 && i <= s.items) ? i : s.items; };

    if (st == 0) {
        s.assigned[tagOf(p)] = -1;
        std::vector<Operat> ops = {R(s, key::warehouse(w)), R(s, key::district(w, d)),
                                   R(s, key::customer(w, d, c))};
        for (uint32_t j = 0; j < n; ++j) ops.push_back(R(s, tpcc_key::item(real_item(item_of(j)))));
        for (uint32_t j = 0; j < n; ++j) ops.push_back(R(s, tpcc_key::stock(sw_of(j), real_item(item_of(j)))));
        return TxnStage{ops, false};
    }
    if (st != 1 || pr.empty() || pr[0].size() < 3 + 2 * size_t(n)) return TxnStage{{}, true};
    const auto& r = pr[0];
    Row W = Row::parse(r[0]), D = Row::parse(r[1]), C = Row::parse(r[2]);
    if (!W.ok(9) || !D.ok(11) || !C.ok(21)) { ++s.bad_rows; return TxnStage{{}, true}; }
    bool invalid = false;
    for (uint32_t j = 0; j < n; ++j) invalid |= (item_of(j) < 1 || item_of(j) > s.items);
    if (rbk || invalid) return TxnStage{{}, true};          // rollback: nothing written

    const uint32_t o = uint32_t(D.inum(col::D_NEXT_O_ID));
    s.assigned[tagOf(p)] = o;
    std::vector<Operat> ops;
    D.set(col::D_NEXT_O_ID, std::to_string(o + 1));
    ops.push_back(U(s, key::district(w, d), D.str()));
    bool all_local = true;
    for (uint32_t j = 0; j < n; ++j) all_local &= (sw_of(j) == w);
    ops.push_back(I(s, tpcc_key::orders(w, d, o), s.cx->fmt.orders_row(w, d, o, c, n, all_local)));
    ops.push_back(I(s, tpcc_key::newOrder(w, d, o), s.cx->fmt.new_order_row(w, d, o)));
    double total = 0;
    for (uint32_t j = 0; j < n; ++j) {
        Row It = Row::parse(r[3 + j]), S = Row::parse(r[3 + n + j]);
        if (!It.ok(5) || !S.ok(17)) { ++s.bad_rows; s.assigned[tagOf(p)] = -1; return TxnStage{{}, true}; }
        const uint32_t i = item_of(j), sw = sw_of(j);
        const int64_t q = q_of(j);
        int64_t sq = S.inum(col::S_QUANTITY);
        sq = (sq >= q + 10) ? sq - q : sq - q + 91;
        S.set(col::S_QUANTITY, std::to_string(sq));
        S.set(col::S_YTD, std::to_string(S.inum(col::S_YTD) + q));
        S.set(col::S_ORDER_CNT, std::to_string(S.inum(col::S_ORDER_CNT) + 1));
        if (sw != w) S.set(col::S_REMOTE_CNT, std::to_string(S.inum(col::S_REMOTE_CNT) + 1));
        ops.push_back(U(s, tpcc_key::stock(sw, i), S.str()));
        const double amount = double(q) * It.num(col::I_PRICE);
        total += amount;
        const std::string row = std::to_string(o) + "," + std::to_string(d) + "," + std::to_string(w) + "," +
                                std::to_string(j + 1) + "," + std::to_string(i) + "," + std::to_string(sw) +
                                ",0," + std::to_string(q) + "," + money(amount) + "," +
                                S.raw(col::S_DIST_01 + int(d) - 1);
        ops.push_back(I(s, tpcc_key::orderLine(w, d, o, j + 1), row));
    }
    (void)(total * (1 - C.num(col::C_DISCOUNT)) * (1 + W.num(col::W_TAX) + D.num(col::D_TAX)));
    return TxnStage{ops, true};
}

// ── 2. Payment  p = w,d,c_w,c_d,by_name,key,h_cents,tag ────────────────────
inline TxnStage payment(StdCtx& s, uint32_t st, const P& p, const Prior& pr) {
    const uint32_t w = uint32_t(p[0]), d = uint32_t(p[1]), cw = uint32_t(p[2]), cd = uint32_t(p[3]);
    const uint32_t c = resolveCustomer(s, cw, cd, p[4], p[5]);
    const double   h = double(p[6]) / 100.0;
    if (st == 0) {
        TxnStage t{{R(s, key::warehouse(w)), R(s, key::district(w, d)), R(s, key::customer(cw, cd, c))}, false};
        if (s.escrow) t.stable_reads = {0, 1};          // W_NAME / D_NAME only
        return t;
    }
    if (st != 1 || pr.empty() || pr[0].size() < 3) return TxnStage{{}, true};
    Row W = Row::parse(pr[0][0]), D = Row::parse(pr[0][1]), C = Row::parse(pr[0][2]);
    if (!W.ok(9) || !D.ok(11) || !C.ok(21)) { ++s.bad_rows; return TxnStage{{}, true}; }
    std::vector<Operat> ops;
    std::vector<FieldDelta> deltas;
    if (s.escrow) {
        deltas.push_back(FieldDelta{R(s, key::warehouse(w)),   uint32_t(col::W_YTD), p[6]});
        deltas.push_back(FieldDelta{R(s, key::district(w, d)), uint32_t(col::D_YTD), p[6]});
    } else {
        W.set(col::W_YTD, money(W.num(col::W_YTD) + h));
        D.set(col::D_YTD, money(D.num(col::D_YTD) + h));
        ops.push_back(U(s, key::warehouse(w), W.str()));
        ops.push_back(U(s, key::district(w, d), D.str()));
    }
    C.set(col::C_BALANCE, money(C.num(col::C_BALANCE) - h));
    C.set(col::C_YTD_PAYMENT, money(C.num(col::C_YTD_PAYMENT) + h));
    C.set(col::C_PAYMENT_CNT, std::to_string(C.inum(col::C_PAYMENT_CNT) + 1));
    if (C.raw(col::C_CREDIT) == "BC") {                       // §2.5.2.2
        std::string nd = std::to_string(c) + " " + std::to_string(cd) + " " + std::to_string(cw) + " " +
                         std::to_string(d) + " " + std::to_string(w) + " " + money(h) + " " + C.raw(col::C_DATA);
        if (nd.size() > 500) nd.resize(500);
        C.set(col::C_DATA, nd);
    }
    ops.push_back(U(s, key::customer(cw, cd, c), C.str()));
    const std::string hrow = std::to_string(c) + "," + std::to_string(cd) + "," + std::to_string(cw) + "," +
                             std::to_string(d) + "," + std::to_string(w) + "," + std::to_string(nowSec()) + "," +
                             money(h) + "," + W.raw(col::W_NAME) + "    " + D.raw(col::D_NAME);
    ops.push_back(I(s, key::history(w, d, s.hist_base + tagOf(p)), hrow));
    TxnStage t{ops, true};
    t.deltas = std::move(deltas);
    return t;
}

// Engine hook for FieldDelta: add the summed cents to the money column(s).
inline std::string applyEscrow(StdCtx& s, const ObjectKey&, const std::string& row,
                               const std::map<uint32_t, int64_t>& sums) {
    Row r = Row::parse(row);
    for (const auto& [field, cents] : sums) {
        if (!r.ok(size_t(field) + 1)) { ++s.bad_rows; return {}; }
        const int64_t cur = std::llround(r.num(int(field)) * 100.0);
        const int64_t v   = cur + cents;
        char b[48];
        std::snprintf(b, sizeof b, "%s%lld.%02lld", v < 0 ? "-" : "",
                      (long long)(std::llabs(v) / 100), (long long)(std::llabs(v) % 100));
        r.set(int(field), b);
    }
    return s.cx->fmt.fit(r.str());
}

// In-epoch order ids of (w,d) as seen by this txn: [committedNext, D_NEXT_O_ID).
inline std::vector<uint32_t> inEpochOrders(const StdCtx& s, uint32_t w, uint32_t d, const Row& D) {
    std::vector<uint32_t> out;
    const uint32_t lo = s.idx.committedNext(w, d), hi = uint32_t(D.inum(col::D_NEXT_O_ID));
    for (uint32_t o = lo; o < hi; ++o) out.push_back(o);
    return out;
}
inline std::vector<Operat> orderLineReads(StdCtx& s, uint32_t w, uint32_t d, const Row& O) {
    std::vector<Operat> ops;
    const uint32_t o = uint32_t(O.inum(col::O_ID)), n = uint32_t(O.inum(col::O_OL_CNT));
    for (uint32_t j = 1; j <= n; ++j) ops.push_back(R(s, tpcc_key::orderLine(w, d, o, j)));
    return ops;
}

// ── 3. Order-Status  p = w,d,by_name,key,tag ───────────────────────────────
//   R0: CUSTOMER, DISTRICT, ORDERS(latest committed order of c)
//   R1: ORDERS of the district's in-epoch orders (only if there are any)
//   R2: ORDER_LINE of the newest visible order of c
inline TxnStage orderStatus(StdCtx& s, uint32_t st, const P& p, const Prior& pr) {
    const uint32_t w = uint32_t(p[0]), d = uint32_t(p[1]);
    const uint32_t c = resolveCustomer(s, w, d, p[2], p[3]);
    const int64_t  o_idx = s.idx.latestOrder(w, d, c);
    if (st == 0) {
        std::vector<Operat> ops = {R(s, key::customer(w, d, c)), R(s, key::district(w, d))};
        if (o_idx >= 0) ops.push_back(R(s, tpcc_key::orders(w, d, uint32_t(o_idx))));
        return TxnStage{ops, false};
    }
    if (pr.empty() || pr[0].size() < 2) return TxnStage{{}, true};
    Row D = Row::parse(pr[0][1]);
    if (!D.ok(11)) { ++s.bad_rows; return TxnStage{{}, true}; }
    const auto fresh = inEpochOrders(s, w, d, D);
    Row committed = (o_idx >= 0 && pr[0].size() > 2) ? Row::parse(pr[0][2]) : Row{};

    if (st == 1 && !fresh.empty()) {                    // scan the in-epoch orders
        std::vector<Operat> ops;
        for (uint32_t o : fresh) ops.push_back(R(s, tpcc_key::orders(w, d, o)));
        return TxnStage{ops, false};
    }
    Row chosen = committed;
    if (!fresh.empty()) {                               // st == 2: newest visible order of c
        if (pr.size() < 2) return TxnStage{{}, true};
        for (size_t j = pr[1].size(); j-- > 0;) {
            Row O = Row::parse(pr[1][j]);
            if (O.ok(8) && uint32_t(O.inum(col::O_C_ID)) == c) { chosen = O; break; }
        }
    }
    if (!chosen.ok(8)) return TxnStage{{}, true};       // customer has no order
    return TxnStage{orderLineReads(s, w, d, chosen), true};
}

// ── 4. Delivery (one district)  p = w,carrier,d,business_id,tag ────────────
//   R0: ORDERS of the first `delivery_window` committed pending orders
//       (+ DISTRICT if the index has fewer than that many)
//   R1: [only if all of them are delivered and DISTRICT was read]
//       ORDERS of the district's in-epoch orders
//   R*: ORDER_LINE + CUSTOMER of the oldest undelivered visible order
//   W : NEW_ORDER tombstone, ORDERS carrier, ORDER_LINE delivery date, CUSTOMER
// Two Deliveries of one district in one epoch both read the candidate's
// ORDERS row and both write it, so MVTSO lets at most one of them deliver
// it; the other either sees O_CARRIER_ID set (and takes the next order) or
// aborts WRITE_TOO_LATE and retries.
inline TxnStage deliveryWrites(StdCtx& s, uint32_t w, uint32_t d, uint32_t carrier, Row O,
                               const std::vector<std::string>& lines_and_customer) {
    const uint32_t o = uint32_t(O.inum(col::O_ID)), c = uint32_t(O.inum(col::O_C_ID));
    const uint32_t n = uint32_t(O.inum(col::O_OL_CNT));
    if (lines_and_customer.size() < size_t(n) + 1) return TxnStage{{}, true};
    // NEW_ORDER / ORDERS / ORDER_LINE of an order created in THIS epoch exist
    // only as an INSERT version; the engine's write phase sends just the
    // latest version per key, so an UPDATE here would reach executeOramOps
    // as "UPDATE of unknown row" and fail the epoch.  An INSERT of an existing
    // row is sent as UPDATE by executeOramOps, so INSERT is correct for both.
    std::vector<Operat> ops;
    ops.push_back(I(s, tpcc_key::newOrder(w, d, o), "DELETED"));   // tombstone = delete
    O.set(col::O_CARRIER_ID, std::to_string(carrier));
    ops.push_back(I(s, tpcc_key::orders(w, d, o), O.str()));
    double sum = 0;
    const std::string now = std::to_string(nowSec());
    for (uint32_t j = 0; j < n; ++j) {
        Row L = Row::parse(lines_and_customer[j]);
        if (!L.ok(10)) { ++s.bad_rows; continue; }
        sum += L.num(col::OL_AMOUNT);
        L.set(col::OL_DELIVERY_D, now);
        ops.push_back(I(s, tpcc_key::orderLine(w, d, o, j + 1), L.str()));   // upsert, see above
    }
    Row C = Row::parse(lines_and_customer[n]);
    if (!C.ok(21)) { ++s.bad_rows; return TxnStage{{}, true}; }
    C.set(col::C_BALANCE, money(C.num(col::C_BALANCE) + sum));
    C.set(col::C_DELIVERY_CNT, std::to_string(C.inum(col::C_DELIVERY_CNT) + 1));
    ops.push_back(U(s, key::customer(w, d, c), C.str()));
    return TxnStage{ops, true};
}
inline int firstUndelivered(const std::vector<std::string>& res, size_t count) {
    for (size_t j = 0; j < count && j < res.size(); ++j) {
        Row O = Row::parse(res[j]);
        if (O.ok(8) && undelivered(O)) return int(j);
    }
    return -1;
}
inline TxnStage delivery(StdCtx& s, uint32_t st, const P& p, const Prior& pr) {
    const uint32_t w = uint32_t(p[0]), carrier = uint32_t(p[1]), d = uint32_t(p[2]);
    const auto cands = s.idx.pendingFirst(w, d, s.delivery_window);
    const bool read_district = cands.size() < s.delivery_window;
    auto deliverRead = [&](const Row& O) {
        std::vector<Operat> ops = orderLineReads(s, w, d, O);
        ops.push_back(R(s, key::customer(w, d, uint32_t(O.inum(col::O_C_ID)))));
        return ops;
    };

    if (st == 0) {
        s.assigned[tagOf(p)] = -1;
        std::vector<Operat> ops;
        for (uint32_t o : cands) ops.push_back(R(s, tpcc_key::orders(w, d, o)));
        if (read_district) ops.push_back(R(s, key::district(w, d)));
        return TxnStage{ops, false};
    }
    if (pr.empty()) return TxnStage{{}, true};
    const int j0 = firstUndelivered(pr[0], cands.size());
    if (j0 >= 0) {                                      // a committed pending order is free
        Row O = Row::parse(pr[0][size_t(j0)]);
        s.assigned[tagOf(p)] = O.inum(col::O_ID);
        if (st == 1) return TxnStage{deliverRead(O), false};
        return pr.size() > 1 ? deliveryWrites(s, w, d, carrier, O, pr[1]) : TxnStage{{}, true};
    }
    if (!read_district) return retryNextEpoch();        // window exhausted by older txns
    Row D = Row::parse(pr[0][cands.size()]);
    if (!D.ok(11)) { ++s.bad_rows; return TxnStage{{}, true}; }
    const auto fresh = inEpochOrders(s, w, d, D);
    if (fresh.empty()) return TxnStage{{}, true};       // §2.7.4.2: nothing to deliver -> skip
    if (st == 1) {
        std::vector<Operat> ops;
        for (uint32_t o : fresh) ops.push_back(R(s, tpcc_key::orders(w, d, o)));
        return TxnStage{ops, false};
    }
    if (pr.size() < 2) return TxnStage{{}, true};
    const int j1 = firstUndelivered(pr[1], fresh.size());
    if (j1 < 0) return TxnStage{{}, true};              // all delivered -> skip
    Row O = Row::parse(pr[1][size_t(j1)]);
    s.assigned[tagOf(p)] = O.inum(col::O_ID);
    if (st == 2) return TxnStage{deliverRead(O), false};
    return pr.size() > 2 ? deliveryWrites(s, w, d, carrier, O, pr[2]) : TxnStage{{}, true};
}

// ── 5. Stock-Level  p = w,d,threshold,tag ──────────────────────────────────
//   R0: DISTRICT   R1: ORDER_LINE of orders [D_NEXT_O_ID-20, D_NEXT_O_ID)
//   R2: STOCK of the distinct items
// Committed orders: O_OL_CNT from the index.  In-epoch orders (count not in
// the index yet): read OL_NUMBER 1..15 through MVTSO; numbers the order does
// not have come back empty (absent row), so the result equals that of a
// serial execution in timestamp order.
inline TxnStage stockLevel(StdCtx& s, uint32_t st, const P& p, const Prior& pr) {
    const uint32_t w = uint32_t(p[0]), d = uint32_t(p[1]);
    if (s.sl_synopsis) {                                     // DP synopsis: answered locally
        if (st == 0) (void)s.sl_synopsis->answer(w, d, p[2]);
        return TxnStage{{}, true};
    }
    if (st == 0) return TxnStage{{R(s, key::district(w, d))}, false};
    if (st == 1) {
        Row D = pr.empty() || pr[0].empty() ? Row{} : Row::parse(pr[0][0]);
        if (!D.ok(11)) { ++s.bad_rows; return TxnStage{{}, true}; }
        const int64_t next = D.inum(col::D_NEXT_O_ID);
        std::vector<Operat> ops;
        for (int64_t o = std::max<int64_t>(1, next - 20); o < next; ++o) {
            int64_t n = s.idx.olCount(w, d, uint32_t(o));
            if (n < 0) n = 15;                               // in-epoch order: max O_OL_CNT
            for (int64_t j = 1; j <= n; ++j)
                ops.push_back(R(s, tpcc_key::orderLine(w, d, uint32_t(o), uint32_t(j))));
        }
        return TxnStage{ops, ops.empty()};
    }
    if (st == 2) {
        std::set<uint32_t> items;                            // count each item once
        if (pr.size() > 1)
            for (const auto& res : pr[1]) {
                Row L = Row::parse(res);
                if (L.ok(10)) items.insert(uint32_t(L.inum(col::OL_I_ID)));
            }
        std::vector<Operat> ops;
        for (uint32_t i : items) ops.push_back(R(s, tpcc_key::stock(w, i)));
        return TxnStage{ops, true};   // low_stock = #(S_QUANTITY < threshold) on these results
    }
    return TxnStage{{}, true};
}

inline void registerStdTypes(TxnEngine& engine, std::shared_ptr<StdCtx> s) {
    engine.setDeltaApplier([s](const ObjectKey& k, const std::string& row,
                               const std::map<uint32_t, int64_t>& sums) { return applyEscrow(*s, k, row, sums); });
    engine.registerType(tid(StdTxn::NEW_ORDER), "NewOrder",
        [s](uint32_t st, const P& p, const Prior& pr) { return newOrder(*s, st, p, pr); });
    engine.registerType(tid(StdTxn::PAYMENT), "Payment",
        [s](uint32_t st, const P& p, const Prior& pr) { return payment(*s, st, p, pr); });
    engine.registerType(tid(StdTxn::ORDER_STATUS), "OrderStatus",
        [s](uint32_t st, const P& p, const Prior& pr) { return orderStatus(*s, st, p, pr); });
    engine.registerType(tid(StdTxn::DELIVERY), "Delivery",
        [s](uint32_t st, const P& p, const Prior& pr) { return delivery(*s, st, p, pr); });
    engine.registerType(tid(StdTxn::STOCK_LEVEL), "StockLevel",
        [s](uint32_t st, const P& p, const Prior& pr) { return stockLevel(*s, st, p, pr); });
}

// ── Commit hook: fold one COMMITTED attempt into the indexes ───────────────
// Call for committed results only, in result order (= timestamp order).
enum class CommitKind { OTHER, NEWORDER_OK, NEWORDER_ROLLBACK, DELIVERY_OK, DELIVERY_SKIPPED };
inline CommitKind onCommitted(StdCtx& s, const txn& t) {
    const auto& p = t.params;
    const int64_t o = s.assigned[tagOf(p)].load();
    if (t.txn_type == tid(StdTxn::NEW_ORDER)) {
        if (p[4] == 1) return CommitKind::NEWORDER_ROLLBACK;
        if (o < 0) return CommitKind::OTHER;
        s.idx.onNewOrderCommit(uint32_t(p[0]), uint32_t(p[1]), uint32_t(p[2]), uint32_t(o), uint32_t(p[3]));
        if (s.sl_synopsis) {
            std::vector<std::tuple<uint32_t, uint32_t, int64_t>> lines;
            for (uint32_t j = 0; j < uint32_t(p[3]); ++j)
                lines.emplace_back(uint32_t(p[5 + 3 * j]), uint32_t(p[6 + 3 * j]), p[7 + 3 * j]);
            s.sl_synopsis->onNewOrder(uint32_t(p[0]), uint32_t(p[1]), uint32_t(o), lines);
        }
        return CommitKind::NEWORDER_OK;
    }
    if (t.txn_type == tid(StdTxn::DELIVERY)) {
        if (o < 0) return CommitKind::DELIVERY_SKIPPED;
        s.idx.onDeliveryCommit(uint32_t(p[0]), uint32_t(p[2]), uint32_t(o));
        return CommitKind::DELIVERY_OK;
    }
    return CommitKind::OTHER;
}

}  // namespace stdrun
