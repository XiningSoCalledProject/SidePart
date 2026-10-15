//
// DePart_TPCC_Driver.h — wires the DePart T1–T8 workload into the proxy:
//
//   generator request (txn_type, params)
//     → push_txn()                      (QueriesReceiving intake)
//     → runEpoch_RBR_W()                (MVTSO, R read batches, write batch)
//     → TxnEngine stage functions here  (synopsis + ObjectKey-addressed ops)
//     → executeOramOps()                (pos_map: ObjectKey → server, block_id)
//
// Created 9/22/26.
//
// Pieces:
//   DePartSynopsis     : interface to the query proxy's DP synopses.  Plug your
//                        DP_PRICE_ITEM / DP_STOCK synopsis in here.
//   DePartRecordFormat : where S_QUANTITY sits in a STOCK tuple, tuple width,
//                        and how T8 builds ORDERS / NEW_ORDER / ORDER_LINE rows.
//   Router             : ObjectKey → server_id (existing rows: the pos_map;
//                        new rows from T8: a placement policy).
//   registerDePartTxnTypes(...) : the proxy-side state machines for T1..T8.
//   runDePartBenchmark(...)     : epoch loop — pushes requests, runs epochs,
//                        retries aborted txns, collects stats / CSV.
//
// Wire params per txn type (all int64):
//   T1 [price_lo, price_hi, (privileged)]
//   T2 [price_lo, price_hi, k, (privileged)]
//   T3 [price_lo, price_hi, k, (privileged)]
//   T4 [price_lo, price_hi, theta, max_fanout, (privileged)]
//   T5 [w, item_lo, item_hi, max_fanout, (privileged)]
//   privileged = 1: the EXACT synopsis (DePartContext::exact) is used.
//     T1/T2 : answered locally, 0 ORAM ops — the servers observe nothing.
//     T3-T5 : exact candidates are read from ORAM, and the txn additionally
//             contacts EVERY shard whose (public, DP) partition range
//             overlaps the query range (DePartContext::cover), sending a
//             padding read to each shard that has no real candidate.  The
//             contacted set then depends only on the query parameters and the
//             public boundaries, never on which rows the true counts picked.
//   Without the flag (or without `exact`) everything uses the DP synopsis.
//   T3 [price_lo, price_hi, k]
//   T4 [price_lo, price_hi, theta, max_fanout]
//   T5 [w, item_lo, item_hi, max_fanout]
//   T6 [w, amount, i_1, ..., i_n]
//   T7 [w_lo, w_hi, k_per_w, amount]
//   T8 [w, d, o, c, ol_cnt, (i, supply_w, qty) × ol_cnt]
//
#ifndef DEPART_TPCC_DRIVER_H
#define DEPART_TPCC_DRIVER_H

#include <algorithm>
#include <cstdlib>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "ObjectKey.h"
#include "Queries_Receiving.h"

namespace depart {

// ── Txn type ids (0 is reserved for assembled txns) ─────────────────────────
enum DePartTxn : TxnTypeId { T1 = 1, T2, T3, T4, T5, T6, T7, T8 };
inline const char* txnName(TxnTypeId t) {
    static const char* n[] = {"ASSEMBLED", "T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8"};
    return t <= 8 ? n[t] : "?";
}

// ── Query-proxy synopses (DP; answered locally, never touch ORAM) ──────────
struct DePartSynopsis {
    virtual ~DePartSynopsis() = default;
    virtual double units_in_price_range(int lo, int hi) const = 0;                          // T1
    virtual std::vector<uint32_t> top_k_items(int lo, int hi, int k) const = 0;              // T2, T3
    virtual std::vector<uint32_t> items_with_units_at_least(int lo, int hi, int theta) const = 0;  // T4
    virtual std::vector<uint32_t> low_stock_items(uint32_t w, uint32_t i_lo, uint32_t i_hi) const = 0; // T5
    virtual std::vector<std::pair<uint32_t, uint32_t>>                                      // T7: (w, i)
        low_stock_top_k_per_warehouse(uint32_t w_lo, uint32_t w_hi, int k) const = 0;
};

// ── Record layout ───────────────────────────────────────────────────────────
struct DePartRecordFormat {
    char     sep                 = ',';
    int      stock_quantity_col  = 2;   // column of S_QUANTITY in a STOCK tuple
    uint32_t tuple_width         = 0;   // >0: pad/truncate written tuples to this width

    // A READ result comes in one of two layouts:
    //   from ORAM               : bID(4) + tuple + '\0' padding
    //   from the MVTSO version  : the raw tuple (another txn's uncommitted
    //   cache (BUFFERED_FROM_     write, i.e. exactly the data_value that
    //   WRITE, newer version)     txn's stage produced; no block id yet)
    // (Fixed 10/8/26: the old code always dropped 4 bytes, so a read that
    //  observed an earlier txn's write in the same epoch lost the first 4
    //  characters of the row — e.g. a second T6/T7 on the same STOCK row.)
    // A block id < 2^29 always has a non-printable byte among its 4 bytes;
    // a CSV tuple's first 4 bytes are printable.
    static bool hasBlockIdPrefix(const std::string& r) {
        if (r.size() < 4) return false;
        for (int i = 0; i < 4; ++i) {
            const unsigned char ch = static_cast<unsigned char>(r[size_t(i)]);
            if (ch < 0x20 || ch > 0x7e) return true;
        }
        return false;
    }
    static std::string tupleOf(const std::string& result) {
        std::string t = hasBlockIdPrefix(result) ? result.substr(4) : result;
        const size_t end = t.find('\0');
        if (end != std::string::npos) t.resize(end);
        return t;
    }
    std::string fit(std::string tuple) const {
        if (tuple_width > 0) tuple.resize(tuple_width, '\0');
        return tuple;
    }
    bool stockQuantity(const std::string& tuple, int64_t& qty) const {
        size_t pos = 0;
        for (int c = 0; c < stock_quantity_col; ++c) {
            pos = tuple.find(sep, pos);
            if (pos == std::string::npos) return false;
            ++pos;
        }
        size_t end = tuple.find(sep, pos);
        try { qty = std::stoll(tuple.substr(pos, end == std::string::npos ? std::string::npos : end - pos)); }
        catch (...) { return false; }
        return true;
    }
    std::string withStockQuantity(const std::string& tuple, int64_t qty) const {
        size_t pos = 0;
        for (int c = 0; c < stock_quantity_col; ++c) pos = tuple.find(sep, pos) + 1;
        size_t end = tuple.find(sep, pos);
        return tuple.substr(0, pos) + std::to_string(qty) +
               (end == std::string::npos ? "" : tuple.substr(end));
    }
    // TPC-C column order.  Override these if your loader uses another layout.
    std::function<std::string(uint32_t w, uint32_t d, uint32_t o, uint32_t c, uint32_t ol_cnt, bool all_local)>
        orders_row = [](uint32_t w, uint32_t d, uint32_t o, uint32_t c, uint32_t ol_cnt, bool all_local) {
            return std::to_string(o) + "," + std::to_string(d) + "," + std::to_string(w) + "," +
                   std::to_string(c) + ",0,0," + std::to_string(ol_cnt) + "," + (all_local ? "1" : "0");
        };
    std::function<std::string(uint32_t w, uint32_t d, uint32_t o)>
        new_order_row = [](uint32_t w, uint32_t d, uint32_t o) {
            return std::to_string(o) + "," + std::to_string(d) + "," + std::to_string(w);
        };
    std::function<std::string(uint32_t w, uint32_t d, uint32_t o, uint32_t n, uint32_t i, uint32_t supply_w, uint32_t qty)>
        order_line_row = [](uint32_t w, uint32_t d, uint32_t o, uint32_t n, uint32_t i, uint32_t sw, uint32_t qty) {
            return std::to_string(o) + "," + std::to_string(d) + "," + std::to_string(w) + "," +
                   std::to_string(n) + "," + std::to_string(i) + "," + std::to_string(sw) + ",0," +
                   std::to_string(qty) + ",0,dist-info";
        };
};

// ObjectKey → server_id.  Must return the row's home shard for existing rows.
using Router = std::function<int(const ObjectKey&)>;

// Default router: pos_map for existing rows; new rows (T8 inserts) go to
// `insert_server(key)` — normally DePart placement of the new row's attribute
// bin (mrs.getServerIdForBinKey).  The row then takes a block from that
// partition's public insert reserve (setInsertReservePerPartition).
inline Router makeRouter(const MultiRingORAM_Servers& mrs,
                         std::function<int(const ObjectKey&)> insert_server) {
    return [&mrs, insert_server](const ObjectKey& k) {
        const int s = mrs.serverOfKey(k);
        return s >= 0 ? s : insert_server(k);
    };
}

// ── Stage functions ─────────────────────────────────────────────────────────
struct DePartContext {
    std::shared_ptr<const DePartSynopsis> synopsis;   // DP (public-safe)
    std::shared_ptr<const DePartSynopsis> exact;      // true counts; trusted proxy only
                                                      // (nullptr = no privileged answers)
    // Privileged T3-T5: shards whose PUBLIC partition range overlaps the query
    // range (T3/T4: ITEM by price, T5: STOCK by item id), and one row per shard
    // to read as padding so that shard is contacted.  Empty = no range cover.
    std::function<std::vector<int>(TxnTypeId, const std::vector<int64_t>&)> cover;
    std::vector<ObjectKey>                                                  touch_key_of_shard;
    Router                                route;
    DePartRecordFormat                    fmt;
};

inline Operat makeOp(const DePartContext& cx, OpType type, const ObjectKey& k,
                     std::string value = {}) {
    Operat op;
    op.type = type;
    setObjectKey(op, k);
    op.data_primary_key = static_cast<uint32_t>(k.logical_id);   // diagnostics only
    op.server_id        = cx.route(k);
    op.data_value       = std::move(value);
    return op;
}

inline void registerDePartTxnTypes(TxnEngine& engine, std::shared_ptr<DePartContext> cx) {
    using Prior = std::vector<std::vector<std::string>>;
    using P     = std::vector<int64_t>;

    // T1 / T2: synopsis only (0 ORAM ops).  The proxy answers from the DP synopsis.
    // Privileged T1/T2 read the exact synopsis; everyone else the DP one.
    auto pick = [cx](bool privileged) -> const DePartSynopsis& {
        return (privileged && cx->exact) ? *cx->exact : *cx->synopsis;
    };
    engine.registerType(T1, "Price-Range Sales Count", [pick](uint32_t, const P& p, const Prior&) {
        (void)pick(p.size() > 2 && p[2] == 1).units_in_price_range((int)p[0], (int)p[1]);
        return TxnStage{{}, true};
    });
    engine.registerType(T2, "Price-Range Top-K", [pick](uint32_t, const P& p, const Prior&) {
        (void)pick(p.size() > 3 && p[3] == 1).top_k_items((int)p[0], (int)p[1], (int)p[2]);
        return TxnStage{{}, true};
    });
    // T3: synopsis Top-K (proxy-local) → exact ITEM reads.
    // Privileged drill-downs: contact every shard of the public range cover,
    // padding with one read on each shard that has no real candidate.
    auto padToCover = [cx](TxnStage& s, TxnTypeId ty, const P& p) {
        if (!cx->cover) return;
        std::set<int> have;
        for (const auto& op : s.ops) have.insert(op.server_id);
        for (int sh : cx->cover(ty, p)) {
            if (have.count(sh) || sh < 0 || size_t(sh) >= cx->touch_key_of_shard.size()) continue;
            s.ops.push_back(makeOp(*cx, OpType::READ, cx->touch_key_of_shard[size_t(sh)]));
            have.insert(sh);
        }
    };
    engine.registerType(T3, "Top-K + Exact Drill-Down", [cx, pick, padToCover](uint32_t, const P& p, const Prior&) {
        const bool priv = p.size() > 3 && p[3] == 1 && cx->exact;
        TxnStage s;
        for (uint32_t i : pick(priv).top_k_items((int)p[0], (int)p[1], (int)p[2]))
            s.ops.push_back(makeOp(*cx, OpType::READ, tpcc_key::item(i)));
        if (priv) padToCover(s, T3, p);
        return s;
    });
    // T4: synopsis filter (units ≥ θ) → exact ITEM reads, capped at max_fanout.
    engine.registerType(T4, "Exact Price-Range Drill-Down", [cx, pick, padToCover](uint32_t, const P& p, const Prior&) {
        const bool priv = p.size() > 4 && p[4] == 1 && cx->exact;
        TxnStage s;
        auto c = pick(priv).items_with_units_at_least((int)p[0], (int)p[1], (int)p[2]);
        if (c.size() > (size_t)p[3]) c.resize((size_t)p[3]);
        for (uint32_t i : c) s.ops.push_back(makeOp(*cx, OpType::READ, tpcc_key::item(i)));
        if (priv) padToCover(s, T4, p);
        return s;
    });
    // T5: low-stock synopsis → exact STOCK reads.
    engine.registerType(T5, "Warehouse Low-Stock Range", [cx, pick, padToCover](uint32_t, const P& p, const Prior&) {
        const bool priv = p.size() > 4 && p[4] == 1 && cx->exact;
        TxnStage s;
        const uint32_t w = (uint32_t)p[0];
        auto c = pick(priv).low_stock_items(w, (uint32_t)p[1], (uint32_t)p[2]);
        if (c.size() > (size_t)p[3]) c.resize((size_t)p[3]);
        for (uint32_t i : c) s.ops.push_back(makeOp(*cx, OpType::READ, tpcc_key::stock(w, i)));
        if (priv) padToCover(s, T5, p);
        return s;
    });
    // Stage 1 of T6 / T7: S_QUANTITY += amount for every row read in stage 0.
    auto replenish = [cx](const std::vector<Operat>& reads, const std::vector<std::string>& results,
                          int64_t amount) {
        TxnStage s;
        for (size_t j = 0; j < reads.size() && j < results.size(); ++j) {
            std::string tuple = DePartRecordFormat::tupleOf(results[j]);
            int64_t qty = 0;
            if (tuple.empty() || !cx->fmt.stockQuantity(tuple, qty)) continue;   // row absent
            s.ops.push_back(makeOp(*cx, OpType::UPDATE, objectKeyOf(reads[j]),
                                   cx->fmt.fit(cx->fmt.withStockQuantity(tuple, qty + amount))));
        }
        return s;
    };
    // T6: READ selected STOCK rows (warehouse-local) → UPDATE.
    engine.registerType(T6, "Warehouse Inventory Update", [cx, replenish](uint32_t st, const P& p, const Prior& prior) {
        const uint32_t w = (uint32_t)p[0];
        std::vector<Operat> reads;
        for (size_t j = 2; j < p.size(); ++j)
            reads.push_back(makeOp(*cx, OpType::READ, tpcc_key::stock(w, (uint32_t)p[j])));
        if (st == 0) return TxnStage{reads, false};
        return replenish(reads, prior[0], p[1]);
    });
    // T7: grouped Top-k low-stock per warehouse → READ → UPDATE (grouped by w).
    engine.registerType(T7, "Cross-Warehouse Parallel Update", [cx, replenish](uint32_t st, const P& p, const Prior& prior) {
        auto c = cx->synopsis->low_stock_top_k_per_warehouse((uint32_t)p[0], (uint32_t)p[1], (int)p[2]);
        std::stable_sort(c.begin(), c.end(), [](auto& a, auto& b) { return a.first < b.first; });
        std::vector<Operat> reads;
        for (auto [w, i] : c) reads.push_back(makeOp(*cx, OpType::READ, tpcc_key::stock(w, i)));
        if (st == 0) return TxnStage{reads, false};
        return replenish(reads, prior[0], p[3]);
    });
    // T8: INSERT ORDERS + NEW_ORDER + O_OL_CNT ORDER_LINE rows.
    engine.registerType(T8, "Growing Order Insert", [cx](uint32_t, const P& p, const Prior&) {
        TxnStage s;
        const uint32_t w = (uint32_t)p[0], d = (uint32_t)p[1], o = (uint32_t)p[2], c = (uint32_t)p[3];
        const uint32_t n = (uint32_t)p[4];
        bool all_local = true;
        for (uint32_t k = 0; k < n; ++k) all_local &= ((uint32_t)p[5 + 3 * k + 1] == w);
        s.ops.push_back(makeOp(*cx, OpType::INSERT, tpcc_key::orders(w, d, o),
                               cx->fmt.fit(cx->fmt.orders_row(w, d, o, c, n, all_local))));
        s.ops.push_back(makeOp(*cx, OpType::INSERT, tpcc_key::newOrder(w, d, o),
                               cx->fmt.fit(cx->fmt.new_order_row(w, d, o))));
        for (uint32_t k = 0; k < n; ++k) {
            const uint32_t i = (uint32_t)p[5 + 3 * k], sw = (uint32_t)p[6 + 3 * k], q = (uint32_t)p[7 + 3 * k];
            s.ops.push_back(makeOp(*cx, OpType::INSERT, tpcc_key::orderLine(w, d, o, k + 1),
                                   cx->fmt.fit(cx->fmt.order_line_row(w, d, o, k + 1, i, sw, q))));
        }
        return s;
    });
}

// ── Epoch driver ────────────────────────────────────────────────────────────
// Admission-control knobs are read from the environment so that TPCC_Main does
// not need new arguments (and old command lines keep their old behaviour):
//   DEPART_MAX_ADMIT     max txns (new + retries) admitted into ONE epoch.
//                        0 / unset = unlimited (behaviour before 9/27/26).
//   DEPART_MAX_ADMIT_PER_SHARD  used only when DEPART_MAX_ADMIT is unset:
//                        cap = this x n_shards, so runs whose shard count is
//                        decided at run time (depart_cap) scale the cap with
//                        their batch capacity.  400 x 10 shards = 4000.
//   DEPART_MAX_INFLIGHT  max txns arrived but not yet resolved (waiting or
//                        admitted); an arrival beyond it is DROPPED, exactly
//                        like OasisDB tpccReplay's -maxinflight.  0 = unlimited.
inline uint32_t envU32(const char* name, uint32_t dflt) {
    const char* v = std::getenv(name);
    if (!v || !*v) return dflt;
    char* end = nullptr;
    const unsigned long x = std::strtoul(v, &end, 10);
    return (end && *end == '\0') ? static_cast<uint32_t>(x) : dflt;
}

struct DriverOptions {
    uint32_t    txns_per_epoch = 50;    // legacy mode: requests pushed per epoch
    uint32_t    max_retries    = 3;     // resubmit aborted txns (client behaviour)
    uint32_t    max_epochs     = 1000;  // safety bound
    std::string results_csv;            // per-attempt CSV ("" = none)
    // Open-loop arrival RATE in txn/s, paced in wall-clock time exactly as in
    // RQ2/RQ3/RQ4.  0 = legacy mode (txns_per_epoch pushed as fast as intake
    // allows at the start of every epoch, i.e. a burst into sub-epoch 1).
    double      offered_txn_s  = 0.0;
    uint32_t    n_shards       = 0;     // for slot accounting under all-shards padding
    // Admission control (paced mode only; see envU32 above).  Without a cap,
    // an overloaded run admits the WHOLE backlog into one epoch; proxy work
    // per epoch grows faster than the batches can commit, epochs get longer,
    // the backlog grows further, and throughput collapses (W=5, 9/26/26:
    // 65k attempts in one 396 s epoch).  Capping admissions keeps each epoch
    // near its service capacity; the backlog waits (or is dropped) instead.
    uint32_t    max_admit_per_epoch = envU32("DEPART_MAX_ADMIT", 0);
    uint32_t    max_inflight        = envU32("DEPART_MAX_INFLIGHT", 0);
    // Called once per finished ATTEMPT (after the epoch that ran it), on the
    // epoch thread, before the next epoch starts.  Used by the standard TPC-C
    // driver to update its proxy-side indexes only for COMMITTED txns.
    // Empty = no callback (T1-T8 runs are unchanged).
    std::function<void(const txn&, bool /*committed*/)> on_result;
};

struct DePartRunStats {
    uint64_t epochs = 0, attempts = 0, committed = 0, gave_up = 0;
    uint64_t pushes_outside = 0;        // paced: txns that had to wait for intake to reopen
    uint64_t dropped = 0;               // paced: arrivals dropped (max_inflight reached)
    uint64_t admit_capped_epochs = 0;   // epochs in which max_admit_per_epoch was reached
    double   elapsed_ms = 0;
    std::map<TxnTypeId, uint64_t>   committed_by_type, aborted_by_type;
    std::map<AbortReason, uint64_t> aborts_by_reason;
    std::vector<double>             epoch_ms;
    // Client-perceived latency of every COMMITTED txn: first accepted push →
    // end of the epoch that committed it (includes retries after aborts).
    std::map<TxnTypeId, std::vector<double>> latency_ms_by_type;
    // End-to-end latency (paced mode): SCHEDULED ARRIVAL -> end of the commit
    // epoch.  Unlike latency_ms_by_type it includes the time a txn waited
    // while intake was closed, i.e. what a client actually observes.
    std::map<TxnTypeId, std::vector<double>> e2e_latency_ms_by_type;
    // Logical ORAM ops of committed txns (result entries), per type.
    std::map<TxnTypeId, uint64_t> ops_by_type;
    // Placement cost, from the batches actually dispatched (EpochStats).
    // A shard is CONTACTED in a round iff it has >= 1 real op; under
    // contacted-only padding it then receives one bread (read) / bwrite
    // (write) sized batch, so slots_sent counts real + padding slots on the wire.
    uint64_t read_rounds = 0, read_contacted = 0, read_real = 0;
    uint64_t write_rounds = 0, write_contacted = 0, write_real = 0;
    uint64_t slots_sent = 0;
    // Per-shard load of every read round (10/9/26).  real[i] = real slots of
    // shard i in the round's batch (<= bread), rejected[i] = slots requested
    // by stages refused with READ_BATCH_FULL; real + rejected = demand.
    struct RoundLoad {
        uint64_t epoch = 0; uint32_t round = 0;
        std::vector<uint32_t> real, rejected;
    };
    std::vector<RoundLoad> read_rounds_load;
    // Committed txns by #distinct shards their ops touched (index = #shards).
    std::vector<uint64_t> txn_fanout_hist;
    void addEpoch(const EpochStats& es, bool pad_all, uint32_t n_shards) {
        for (size_t r = 0; r < es.read_real_per_bucket.size(); ++r) {
            RoundLoad rl;
            rl.epoch = epochs + 1; rl.round = uint32_t(r + 1);
            rl.real.assign(std::max<uint32_t>(n_shards, 1), 0);
            rl.rejected.assign(rl.real.size(), 0);
            auto slot = [&](int b) {   // bucket -1 (global capacity) -> index 0
                const size_t i = b < 0 ? 0 : size_t(b);
                if (i >= rl.real.size()) { rl.real.resize(i + 1, 0); rl.rejected.resize(i + 1, 0); }
                return i;
            };
            for (const auto& [b, n] : es.read_real_per_bucket[r]) { const size_t i = slot(b); rl.real[i] += n; }
            if (r < es.read_rejected_per_bucket.size())
                for (const auto& [b, n] : es.read_rejected_per_bucket[r]) { const size_t i = slot(b); rl.rejected[i] += n; }
            read_rounds_load.push_back(std::move(rl));
        }
        for (const auto& m : es.read_real_per_bucket) {
            uint32_t c = 0; uint64_t real = 0;
            for (const auto& [b, n] : m) if (n > 0) { ++c; real += n; }
            if (c == 0 && !pad_all) continue;
            const uint32_t sent = pad_all ? n_shards : c;
            ++read_rounds; read_contacted += sent; read_real += real;
            slots_sent += uint64_t(sent) * es.bread;
        }
        uint32_t c = 0; uint64_t real = 0;
        for (const auto& [b, n] : es.write_real_per_bucket) if (n > 0) { ++c; real += n; }
        if (c > 0 || pad_all) {
            const uint32_t sent = pad_all ? n_shards : c;
            ++write_rounds; write_contacted += sent; write_real += real;
            slots_sent += uint64_t(sent) * es.bwrite;
        }
    }
    double throughput_tps() const { return elapsed_ms > 0 ? committed * 1000.0 / elapsed_ms : 0; }
};

// Whether a request carries the privileged flag (see the wire-param table).
inline bool isPrivileged(const txn& t) {
    const auto& p = t.params;
    switch (t.txn_type) {
        case T1: return p.size() > 2 && p[2] == 1;
        case T2: case T3: return p.size() > 3 && p[3] == 1;
        case T4: case T5: return p.size() > 4 && p[4] == 1;
        default: return false;
    }
}

inline double quantileOf(std::vector<double> v, double q) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const double pos = q * (v.size() - 1);
    const size_t lo = static_cast<size_t>(pos), hi = std::min(lo + 1, v.size() - 1);
    return v[lo] + (pos - lo) * (v[hi] - v[lo]);
}

// Requests carry client_txn_id = index into `requests` (set here).
inline DePartRunStats runDePartBenchmark(QueriesReceiving& qr, MultiRingORAM_Servers& mrs,
                                         const EpochConfig& cfg, const TxnEngine& engine,
                                         std::vector<txn> requests, const DriverOptions& opt) {
    using clk = std::chrono::steady_clock;
    DePartRunStats st;
    const size_t N = requests.size();
    std::vector<uint32_t>         attempts(N, 0);
    std::vector<clk::time_point>  first_push(N);
    std::vector<char>             pushed_once(N, 0);
    std::vector<clk::time_point>  arrival(N);       // paced: scheduled arrival time
    std::vector<char>             arrived(N, 0);
    std::deque<uint64_t>          fresh;            // paced: not yet arrived
    std::mutex                    mu;               // guards pending, first_push, pushed_once
    std::deque<uint64_t>          pending;
    for (uint64_t i = 0; i < N; ++i) { requests[i].client_txn_id = i; pending.push_back(i); }

    std::ofstream csv;
    if (!opt.results_csv.empty()) {
        csv.open(opt.results_csv);
        csv << "epoch,client_txn_id,txn_type,attempt,committed,abort_reason,num_ops,latency_ms,privileged\n";
    }
    // Stamp the first-push time BEFORE pushing (an epoch may close and report
    // the txn before the producer could stamp it afterwards); undo on reject.
    auto tryPush = [&](uint64_t id) -> bool {
        bool was;
        {
            std::lock_guard<std::mutex> lk(mu);
            was = pushed_once[id];
            if (!was) { pushed_once[id] = 1; first_push[id] = clk::now(); }
        }
        if (qr.push_txn(requests[id])) return true;
        if (!was) { std::lock_guard<std::mutex> lk(mu); pushed_once[id] = 0; }
        return false;
    };
    // Handle one epoch's results.  Returns nothing; updates stats / pending.
    std::atomic<int64_t> outstanding{0};             // popped but not yet resolved
    auto absorb = [&](const std::vector<AssembledTxnResult>& results) {
        const auto t_end = clk::now();
        for (const auto& r : results) {
            const uint64_t id = r.client_txn_id;
            const TxnTypeId ty = requests[id].txn_type;
            ++st.attempts;
            ++attempts[id];
            if (opt.on_result) opt.on_result(requests[id], r.committed);
            double lat = 0.0;
            {
                std::lock_guard<std::mutex> lk(mu);
                lat = std::chrono::duration<double, std::milli>(t_end - first_push[id]).count();
            }
            if (csv) csv << st.epochs << ',' << id << ',' << txnName(ty) << ',' << attempts[id] << ','
                         << r.committed << ',' << toString(r.abort_reason) << ','
                         << r.per_op_result.size() << ',' << (r.committed ? lat : 0.0) << ','
                         << (isPrivileged(requests[id]) ? 1 : 0) << '\n';
            if (r.committed) {
                ++st.committed;
                ++st.committed_by_type[ty];
                st.latency_ms_by_type[ty].push_back(lat);
                {
                    std::lock_guard<std::mutex> lk(mu);
                    const auto from = arrived[id] ? arrival[id] : first_push[id];
                    st.e2e_latency_ms_by_type[ty].push_back(
                        std::chrono::duration<double, std::milli>(t_end - from).count());
                }
                st.ops_by_type[ty] += r.per_op_result.size();
                if (st.txn_fanout_hist.size() <= r.shards_touched) st.txn_fanout_hist.resize(r.shards_touched + 1, 0);
                ++st.txn_fanout_hist[r.shards_touched];
                --outstanding;
            } else {
                ++st.aborted_by_type[ty];
                ++st.aborts_by_reason[r.abort_reason];
                if (attempts[id] <= opt.max_retries) {
                    // Retry FIRST, like a client that resubmits right away.
                    // (Appending to the back made an aborted txn wait behind the
                    // whole remaining workload, so its latency measured the
                    // queue length, not the system: T7 p95 was 53 s.)
                    std::lock_guard<std::mutex> lk(mu);
                    pending.push_front(id);
                } else { ++st.gave_up; }
                --outstanding;
            }
        }
    };

    const auto t0 = clk::now();

    if (opt.offered_txn_s <= 0.0) {
        // ── Legacy: per-epoch batch pushed as fast as intake allows ─────────
        while (!pending.empty() && st.epochs < opt.max_epochs) {
            std::vector<uint64_t> batch;
            while (!pending.empty() && batch.size() < opt.txns_per_epoch) {
                batch.push_back(pending.front());
                pending.pop_front();
            }
            outstanding += static_cast<int64_t>(batch.size());
            std::atomic<bool>   epoch_over{false};
            std::atomic<size_t> accepted{0};
            std::thread producer([&] {
                for (size_t j = 0; j < batch.size(); ++j) {
                    while (!tryPush(batch[j])) {
                        if (epoch_over) return;
                        std::this_thread::sleep_for(std::chrono::microseconds(100));
                    }
                    accepted = j + 1;
                }
            });
            const auto e0 = clk::now();
            std::vector<AssembledTxnResult> results;
            try {
                EpochStats es;
                results = runEpoch_RBR_W(qr, mrs, cfg, engine, &es);
                st.addEpoch(es, cfg.pad_uncontacted_shards, opt.n_shards);
            } catch (...) { epoch_over = true; producer.join(); throw; }
            st.epoch_ms.push_back(std::chrono::duration<double, std::milli>(clk::now() - e0).count());
            epoch_over = true;
            producer.join();
            ++st.epochs;
            for (size_t j = batch.size(); j-- > accepted;) { pending.push_front(batch[j]); --outstanding; }
            absorb(results);
        }
    } else {
        // ── Paced open loop ─────────────────────────────────────────────────
        // Arrivals are released on a fixed schedule (1/rate) regardless of
        // the system: a txn that arrives while intake is closed WAITS in the
        // backlog and is admitted as soon as intake reopens (the whole backlog
        // is offered, not one txn per tick).  So the admitted rate is not
        // capped by the intake duty cycle, and end-to-end latency (arrival ->
        // commit) includes the wait -- the same semantics as the OasisDB
        // replay.  Aborted txns are retried first (pending.push_front).
        std::atomic<bool> done{false};
        std::atomic<uint64_t> outside{0};
        std::atomic<uint64_t> dropped{0};
        std::atomic<uint64_t> admitted_epoch{0};   // reset by the epoch loop
        std::atomic<bool>     capped_epoch{false};
        std::vector<char> waited(N, 0);
        const uint32_t admit_cap = opt.max_admit_per_epoch
            ? opt.max_admit_per_epoch
            : envU32("DEPART_MAX_ADMIT_PER_SHARD", 0) * opt.n_shards;
        std::cout << "[driver] admission control: max_admit_per_epoch="
                  << (admit_cap ? std::to_string(admit_cap) : "unlimited")
                  << "  max_inflight="
                  << (opt.max_inflight ? std::to_string(opt.max_inflight) : "unlimited") << "\n";
        {
            std::lock_guard<std::mutex> lk(mu);
            fresh.swap(pending);            // everything starts "not yet arrived"
        }
        std::thread producer([&] {
            const auto interval = std::chrono::nanoseconds(
                static_cast<int64_t>(1e9 / opt.offered_txn_s));
            auto next = clk::now();
            while (!done) {
                const auto now = clk::now();
                {   // 1. release every arrival that is due
                    std::lock_guard<std::mutex> lk(mu);
                    while (!fresh.empty() && next <= now) {
                        const uint64_t id = fresh.front(); fresh.pop_front();
                        next += interval;
                        if (opt.max_inflight &&
                            pending.size() + static_cast<uint64_t>(std::max<int64_t>(outstanding.load(), 0))
                                >= opt.max_inflight) {
                            ++dropped;      // never admitted, never counted as arrived
                            continue;
                        }
                        arrival[id] = next - interval; arrived[id] = 1;
                        pending.push_back(id);
                    }
                }
                // 2. admit as many waiting txns as intake accepts
                for (;;) {
                    if (admit_cap && admitted_epoch.load() >= admit_cap) {
                        capped_epoch = true;
                        break;              // this epoch is full: wait for the next one
                    }
                    uint64_t id;
                    {
                        std::lock_guard<std::mutex> lk(mu);
                        if (pending.empty()) break;
                        id = pending.front();
                        pending.pop_front();
                        ++outstanding;      // under mu: see the epoch loop's exit check
                    }
                    if (!tryPush(id)) {
                        std::lock_guard<std::mutex> lk(mu);
                        if (!waited[id]) { waited[id] = 1; ++outside; }
                        pending.push_front(id);
                        --outstanding;
                        break;              // intake closed: poll again shortly
                    }
                    ++admitted_epoch;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        });
        try {
            while (st.epochs < opt.max_epochs) {
                {
                    std::lock_guard<std::mutex> lk(mu);
                    if (pending.empty() && fresh.empty() && outstanding.load() <= 0) break;
                }
                admitted_epoch = 0;             // intake is closed here: new epoch, new budget
                capped_epoch = false;
                const auto e0 = clk::now();
                EpochStats es;
                auto results = runEpoch_RBR_W(qr, mrs, cfg, engine, &es);
                if (capped_epoch) ++st.admit_capped_epochs;
                st.addEpoch(es, cfg.pad_uncontacted_shards, opt.n_shards);
                st.epoch_ms.push_back(std::chrono::duration<double, std::milli>(clk::now() - e0).count());
                ++st.epochs;
                absorb(results);
            }
        } catch (...) { done = true; producer.join(); throw; }
        done = true;
        producer.join();
        st.pushes_outside = outside;
        st.dropped        = dropped;
    }
    st.elapsed_ms = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
    return st;
}

inline void printStats(const DePartRunStats& st, std::ostream& os = std::cout) {
    os << "[DePart] epochs=" << st.epochs << "  attempts=" << st.attempts
       << "  committed=" << st.committed << "  gave_up=" << st.gave_up
       << "  pushes_outside=" << st.pushes_outside
       << "  dropped=" << st.dropped << "  admit_capped_epochs=" << st.admit_capped_epochs
       << "  elapsed_ms=" << st.elapsed_ms << "  committed_tps=" << st.throughput_tps() << "\n";
    for (TxnTypeId t = T1; t <= T8; ++t) {
        auto c = st.committed_by_type.count(t) ? st.committed_by_type.at(t) : 0;
        auto a = st.aborted_by_type.count(t) ? st.aborted_by_type.at(t) : 0;
        std::vector<double> lat;
        if (st.latency_ms_by_type.count(t)) lat = st.latency_ms_by_type.at(t);
        os << "  " << txnName(t) << ": committed=" << c << " aborted_attempts=" << a
           << "  latency_ms p50=" << quantileOf(lat, .5) << " p95=" << quantileOf(lat, .95);
        if (st.e2e_latency_ms_by_type.count(t)) {
            const auto& e = st.e2e_latency_ms_by_type.at(t);
            os << "  e2e_ms p50=" << quantileOf(e, .5) << " p95=" << quantileOf(e, .95);
        }
        os << "\n";
    }
    os << "  abort reasons:";
    if (st.aborts_by_reason.empty()) os << " none";
    for (auto& [r, n] : st.aborts_by_reason) os << " " << toString(r) << "=" << n;
    os << "\n";
}

// Per-type summary CSV (one row per txn type) for the paper figures.
inline void writeStatsCsv(const DePartRunStats& st, const std::string& path,
                          const std::string& system_label, double offered_txn_s) {
    std::ofstream f(path);
    f << "system,offered_txn_s,txn_type,committed,aborted_attempts,share_of_commits,"
         "latency_p50_ms,latency_p95_ms,latency_mean_ms,run_committed_tps,run_elapsed_ms,epochs,"
         "e2e_p50_ms,e2e_p95_ms,dropped,admit_capped_epochs\n";
    for (TxnTypeId t = T1; t <= T8; ++t) {
        const uint64_t c = st.committed_by_type.count(t) ? st.committed_by_type.at(t) : 0;
        const uint64_t a = st.aborted_by_type.count(t) ? st.aborted_by_type.at(t) : 0;
        std::vector<double> lat;
        if (st.latency_ms_by_type.count(t)) lat = st.latency_ms_by_type.at(t);
        double mean = 0; for (double x : lat) mean += x; if (!lat.empty()) mean /= lat.size();
        f << system_label << ',' << offered_txn_s << ',' << txnName(t) << ',' << c << ',' << a << ','
          << (st.committed ? double(c) / st.committed : 0.0) << ','
          << quantileOf(lat, .5) << ',' << quantileOf(lat, .95) << ',' << mean << ','
          << st.throughput_tps() << ',' << st.elapsed_ms << ',' << st.epochs << ',';
        std::vector<double> e2e;
        if (st.e2e_latency_ms_by_type.count(t)) e2e = st.e2e_latency_ms_by_type.at(t);
        f << quantileOf(e2e, .5) << ',' << quantileOf(e2e, .95) << ','
          << st.dropped << ',' << st.admit_capped_epochs << '\n';
    }
}

}  // namespace depart

#endif  // DEPART_TPCC_DRIVER_H