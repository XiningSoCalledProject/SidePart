// ============================================================================
// tpcc_std_mvtso_test.cpp — proxy indexes vs MVTSO serialization.
//
// Runs the REAL epoch engine (src/Queries_Receiving.cpp: EpochRunner, MVTSO,
// cascading aborts, R read sub-epochs + write batch) with the REAL stage
// functions (tpcc_std_txns.h).  Only MultiRingORAM_Servers::executeOramOps
// is replaced by an in-memory store with the same rules as the real one:
//   READ of an unknown row -> ""      UPDATE of an unknown row -> dispatch fails
//   INSERT of a new row    -> created
//
// Scenarios (same epoch unless stated; "A->B" = A has the smaller timestamp):
//   1 NewOrder -> OrderStatus      OS sees the new order
//   2 OrderStatus -> NewOrder      OS sees the old order, both commit
//   3 NewOrder | OrderStatus       next epoch: OS sees it through the index
//   4 NewOrder -> StockLevel       SL reads the new order's lines and items
//   5 StockLevel -> NewOrder       SL does not see it
//   6 NewOrder -> Delivery         empty district: DL delivers the new order
//   7 Delivery -> NewOrder         DL skips; next epoch DL delivers it
//   8 Delivery -> Delivery         same district: two different orders, never one twice
//   9 NewOrder(rollback) -> OS     no effect, OS sees the old order
//  10 random mix, many epochs, retries: TPC-C consistency conditions 1-4
//     (§3.3.2), "delivered exactly once", and index == database;
//     sum of committed Payment amounts == change of W_YTD == change of
//     sum(C_YTD_PAYMENT)
//  11 Payment escrow (W_YTD / D_YTD as FieldDeltas):
//     a many Payments of one warehouse commit in ONE epoch
//     b Payment and NewOrder on one district, both orders: both commit and
//       the single DISTRICT write carries D_NEXT_O_ID and D_YTD
//     c an aborted Payment's increment is dropped
//     d escrow off: at most one Payment per warehouse per epoch (old behaviour)
//     e FieldDelta on a row nobody read -> DELTA_NO_BASE, no write
//  12 Stock-Level DP synopsis: released once, 0 ORAM ops; with negligible noise
//     it equals the exact ORAM Stock-Level at release; unchanged after updates
//     until repartition(); the measurement mirror tracks the DB
//
// Build: see CMakeLists (target tpcc_std_mvtso_test) or
//   g++ -std=c++17 -Iinclude -Isrc tests/tpcc_std_mvtso_test.cpp src/Queries_Receiving.cpp -pthread
// (plus whatever headers MultiRingORAM_Servers.h pulls in).  No network, no ORAM.
// ============================================================================
#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <thread>

#include "tpcc_std_txns.h"

using namespace stdrun;

// ── Fake storage layer ──────────────────────────────────────────────────────
static std::map<ObjectKey, std::string> g_db;   // committed rows (tuple text)
static uint64_t g_dispatch_failures = 0;

ExecutionResult MultiRingORAM_Servers::executeOramOps(const std::vector<Operat>& ops, uint32_t,
                                                      bool, bool) {
    ExecutionResult er;
    er.per_slot_results.assign(ops.size(), std::string{});
    for (size_t i = 0; i < ops.size(); ++i) {           // validate first (real code fails atomically)
        const Operat& op = ops[i];
        if (op.type == OpType::UPDATE && !g_db.count(objectKeyOf(op))) {
            er.success = false;
            er.error_message = "UPDATE of unknown row " + objectKeyOf(op).str();
            ++g_dispatch_failures;
            return er;
        }
    }
    for (size_t i = 0; i < ops.size(); ++i) {
        const Operat& op = ops[i];
        const ObjectKey k = objectKeyOf(op);
        if (op.type == OpType::READ) {
            auto it = g_db.find(k);
            if (it != g_db.end()) er.per_slot_results[i] = std::string(4, '\0') + it->second;
        } else {
            std::string v = op.data_value;
            const size_t z = v.find('\0');
            if (z != std::string::npos) v.resize(z);
            g_db[k] = v;
        }
    }
    er.success = true;
    return er;
}

// ── Test database ───────────────────────────────────────────────────────────
constexpr int W = 2, D = 10, C = 30, NI = 50;
static bool emptyDistrict(int w, int d) { return w == 2 && d >= 9; }   // nothing pending

struct Harness {
    std::shared_ptr<StdCtx> s = std::make_shared<StdCtx>();
    TxnEngine        engine;
    QueriesReceiving qr;
    MultiRingORAM_Servers mrs;
    EpochConfig      cfg;
    std::map<TxnTypeId, int> committed, aborted;
    std::map<AbortReason, int> reasons;
    uint64_t next_tag = 0;
    int64_t  paid_cents = 0;        // sum of h over committed Payments
    std::vector<std::string> ol_rows, stock_rows;   // for the Stock-Level synopsis

    explicit Harness(bool escrow = true) {
        g_db.clear();
        std::vector<std::string> cust, ord, no;
        auto put = [](const ObjectKey& k, const std::string& v) { g_db[k] = v; };
        for (int w = 1; w <= W; ++w) {
            put(key::warehouse(w), std::to_string(w) + ",WN" + std::to_string(w) + ",s1,s2,ct,ST,123456789,0.1000,300000.00");
            for (int d = 1; d <= D; ++d) {
                put(key::district(w, d), std::to_string(d) + "," + std::to_string(w) +
                    ",DN,s1,s2,ct,ST,123456789,0.0500,30000.00," + std::to_string(C + 1));
                for (int c = 1; c <= C; ++c) {
                    const std::string cr = std::to_string(c) + "," + std::to_string(d) + "," + std::to_string(w) +
                        ",F" + std::to_string(100 + (c * 37) % 50) + ",OE," + tpcc_std::lastName(c - 1) +
                        ",s1,s2,ct,ST,123456789,5551234,0," + (c % 10 ? "GC" : "BC") +
                        ",50000.00,0.1000,-10.00,10.00,1,0,cdata" + std::to_string(c);
                    put(key::customer(w, d, c), cr); cust.push_back(cr);
                    const int o = c, n = 5 + o % 3;
                    const bool pending = !emptyDistrict(w, d) && o > 20;
                    const std::string orow = std::to_string(o) + "," + std::to_string(d) + "," + std::to_string(w) +
                        "," + std::to_string(c) + ",0," + (pending ? "null" : "3") + "," + std::to_string(n) + ",1";
                    put(tpcc_key::orders(w, d, o), orow); ord.push_back(orow);
                    for (int j = 1; j <= n; ++j) {
                        const std::string olr = std::to_string(o) + "," + std::to_string(d) + "," +
                            std::to_string(w) + "," + std::to_string(j) + "," + std::to_string(1 + (o * 7 + j) % 30) +
                            "," + std::to_string(w) + "," + (pending ? "null" : "123") + ",5,10.00,distinfo";
                        put(tpcc_key::orderLine(w, d, o, j), olr);
                        ol_rows.push_back(olr);
                    }
                    if (pending) {
                        const std::string nr = std::to_string(o) + "," + std::to_string(d) + "," + std::to_string(w);
                        put(tpcc_key::newOrder(w, d, o), nr); no.push_back(nr);
                    }
                }
            }
            for (int i = 1; i <= NI; ++i) {
                std::string r = std::to_string(i) + "," + std::to_string(w) + "," + std::to_string(15 + i % 80);
                for (int x = 1; x <= 10; ++x) r += ",dist" + std::to_string(x);
                put(tpcc_key::stock(w, i), r + ",0,0,0,sdata");
                stock_rows.push_back(r + ",0,0,0,sdata");
            }
        }
        for (int i = 1; i <= NI; ++i) put(tpcc_key::item(i), std::to_string(i) + ",7,IN" + std::to_string(i) + "," + money(1 + i % 90) + ",idata");

        s->idx.build(cust, ord, no);
        s->items = NI;
        s->hist_base = 1;
        s->resetAssigned(100000);
        s->cx = std::make_shared<depart::DePartContext>();
        s->cx->route = [](const ObjectKey& k) { return k.w_id <= 1 ? 0 : 1; };   // 2 shards
        s->escrow = escrow;
        registerStdTypes(engine, s);
        cfg.R = 4; cfg.bread = 100000; cfg.bwrite = 100000; cfg.sub_epoch_ms = 60;
        cfg.pad_batches = false; cfg.pad_uncontacted_shards = false;
        cfg.per_server_capacity = true; cfg.enforce_batch_capacity = true;
    }

    txn make(StdTxn ty, std::vector<int64_t> p) {
        txn t; t.txn_type = tid(ty); p.push_back(int64_t(next_tag++)); t.params = std::move(p);
        return t;
    }

    // One epoch.  arrive[i] = read sub-epoch (0-based) in which txn i arrives;
    // default all 0.  Timestamp order = arrival order.
    std::vector<AssembledTxnResult> epoch(std::vector<txn> batch, std::vector<int> arrive = {}) {
        for (size_t i = 0; i < batch.size(); ++i) batch[i].client_txn_id = i;
        arrive.resize(batch.size(), 0);
        std::thread pusher([&] {
            using clk = std::chrono::steady_clock;
            clk::time_point t0;
            bool opened = false;
            for (int sub = 0; sub < int(cfg.R); ++sub) {
                for (size_t i = 0; i < batch.size(); ++i) {
                    if (arrive[i] != sub) continue;
                    if (opened) std::this_thread::sleep_until(t0 + std::chrono::milliseconds(
                                    sub * cfg.sub_epoch_ms + cfg.sub_epoch_ms / 2 + 5 * sub));
                    while (!qr.push_txn(batch[i])) std::this_thread::sleep_for(std::chrono::microseconds(200));
                    if (!opened) { opened = true; t0 = clk::now(); }
                }
            }
        });
        std::streambuf* old = std::cout.rdbuf();
        std::ostringstream sink;
        std::cout.rdbuf(sink.rdbuf());                      // engine is chatty
        auto res = runEpoch_RBR_W(qr, mrs, cfg, engine);
        std::cout.rdbuf(old);
        pusher.join();
        for (const auto& r : res) {
            const txn& t = batch[r.client_txn_id];
            if (r.committed) {
                onCommitted(*s, t); committed[t.txn_type]++;
                if (t.txn_type == tid(StdTxn::PAYMENT)) paid_cents += t.params[6];
            }
            else { aborted[t.txn_type]++; reasons[r.abort_reason]++; }
        }
        return res;
    }
    // Run until every txn committed (aborted ones retry next epoch, first).
    void runToCompletion(std::vector<txn> all, size_t per_epoch, int max_epochs = 400) {
        std::vector<txn> retry;
        size_t pos = 0;
        for (int e = 0; e < max_epochs && (pos < all.size() || !retry.empty()); ++e) {
            std::vector<txn> batch = std::move(retry); retry.clear();
            while (batch.size() < per_epoch && pos < all.size()) batch.push_back(all[pos++]);
            auto res = epoch(batch);
            for (const auto& r : res) if (!r.committed) retry.push_back(batch[r.client_txn_id]);
        }
        if (!retry.empty() || pos < all.size()) std::cerr << "  (runToCompletion: work left over)\n";
    }
};

// ── Helpers ─────────────────────────────────────────────────────────────────
static int g_fail = 0, g_pass = 0;
static void check(bool ok, const std::string& what) {
    (ok ? g_pass : g_fail)++;
    std::cout << (ok ? "  PASS  " : "  FAIL  ") << what << "\n";
}
static Row dbRow(const ObjectKey& k) {
    auto it = g_db.find(k);
    return it == g_db.end() ? Row{} : Row::parse(std::string(4, '\0') + it->second);
}
// O_IDs of the ORDER_LINE rows an attempt read.
static std::set<int64_t> olOrders(const AssembledTxnResult& r) {
    std::set<int64_t> out;
    for (const auto& x : r.per_op_result) { Row L = Row::parse(x); if (L.f.size() == 10) out.insert(L.inum(0)); }
    return out;
}
static std::set<int64_t> stockItems(const AssembledTxnResult& r) {
    std::set<int64_t> out;
    for (const auto& x : r.per_op_result) { Row S = Row::parse(x); if (S.f.size() == 17) out.insert(S.inum(0)); }
    return out;
}
static std::vector<int64_t> newOrderParams(int w, int d, int c, std::vector<int> items, bool rbk = false) {
    std::vector<int64_t> p = {w, d, c, int64_t(items.size()), rbk ? 1 : 0};
    for (size_t j = 0; j < items.size(); ++j) {
        const int i = (rbk && j + 1 == items.size()) ? NI + 1 : items[j];
        p.insert(p.end(), {i, w, 3});
    }
    return p;
}
static int dNext(int w, int d) { return int(dbRow(key::district(w, d)).inum(col::D_NEXT_O_ID)); }

// ── Scenarios ───────────────────────────────────────────────────────────────
// Same sub-epoch: both read DISTRICT in read round 1, so the OLDER New-Order's
// later write would invalidate what the younger reader saw -> MVTSO aborts the
// New-Order (WRITE_TOO_LATE) and the reader correctly sees the old state.
// Never allowed: both commit while the reader misses the order.
static void expectSerializable(const AssembledTxnResult& no, bool reader_saw_new, const std::string& what) {
    check(no.committed ? reader_saw_new : !reader_saw_new,
          what + (no.committed ? " (NewOrder committed, reader saw it)" : " (NewOrder aborted, reader saw old state)"));
}
static void testNewOrderThenOrderStatus() {
    std::cout << "[1] NewOrder -> OrderStatus\n";
    Harness h;
    auto r = h.epoch({h.make(StdTxn::NEW_ORDER, newOrderParams(1, 1, 5, {41, 42, 43})),
                      h.make(StdTxn::ORDER_STATUS, {1, 1, 0, 5})}, {0, 1});
    check(r[0].committed && r[1].committed, "reader one sub-epoch later: both commit");
    check(olOrders(r[1]) == std::set<int64_t>{31}, "OrderStatus returns the NEW in-epoch order 31");
    check(dNext(1, 1) == 32, "D_NEXT_O_ID = 32");
    Harness h2;
    auto q = h2.epoch({h2.make(StdTxn::NEW_ORDER, newOrderParams(1, 1, 5, {41, 42, 43})),
                       h2.make(StdTxn::ORDER_STATUS, {1, 1, 0, 5})});
    check(q[1].committed, "same sub-epoch: OrderStatus commits");
    expectSerializable(q[0], olOrders(q[1]).count(31) > 0, "same sub-epoch serializable");
}
static void testOrderStatusThenNewOrder() {
    std::cout << "[2] OrderStatus -> NewOrder (same epoch), [3] OrderStatus next epoch\n";
    Harness h;
    auto r = h.epoch({h.make(StdTxn::ORDER_STATUS, {1, 2, 0, 6}),
                      h.make(StdTxn::NEW_ORDER, newOrderParams(1, 2, 6, {11, 12}))});
    check(r[0].committed && r[1].committed, "both commit");
    check(olOrders(r[0]) == std::set<int64_t>{6}, "OrderStatus (older ts) returns the OLD order 6");
    auto r2 = h.epoch({h.make(StdTxn::ORDER_STATUS, {1, 2, 0, 6})});
    check(r2[0].committed && olOrders(r2[0]) == std::set<int64_t>{31}, "next epoch: index gives order 31");
}
static void testNewOrderStockLevel() {
    std::cout << "[4] NewOrder -> StockLevel, [5] StockLevel -> NewOrder\n";
    Harness h;
    auto r = h.epoch({h.make(StdTxn::NEW_ORDER, newOrderParams(1, 3, 2, {44, 45, 46, 47, 48})),
                      h.make(StdTxn::STOCK_LEVEL, {1, 3, 20})}, {0, 1});
    check(r[0].committed && r[1].committed, "reader one sub-epoch later: both commit");
    const auto ols = olOrders(r[1]);
    check(ols.count(31) == 1 && ols.size() == 20, "StockLevel window = orders 12..31, includes new order 31");
    const auto st = stockItems(r[1]);
    check(st.count(44) && st.count(48), "StockLevel reads STOCK of the new order's items (44..48)");

    Harness h3;
    auto q = h3.epoch({h3.make(StdTxn::NEW_ORDER, newOrderParams(1, 3, 2, {44, 45, 46, 47, 48})),
                       h3.make(StdTxn::STOCK_LEVEL, {1, 3, 20})});
    check(q[1].committed, "same sub-epoch: StockLevel commits");
    expectSerializable(q[0], olOrders(q[1]).count(31) > 0, "same sub-epoch serializable");

    auto r2 = h.epoch({h.make(StdTxn::STOCK_LEVEL, {1, 4, 20}),
                       h.make(StdTxn::NEW_ORDER, newOrderParams(1, 4, 2, {44, 45, 46, 47, 48}))});
    check(r2[0].committed && r2[1].committed, "StockLevel older: both commit");
    check(olOrders(r2[0]).count(31) == 0 && !stockItems(r2[0]).count(44),
          "StockLevel (older ts) does not see order 31");
}
static void testNewOrderDelivery() {
    std::cout << "[6] NewOrder -> Delivery, [7] Delivery -> NewOrder (empty districts)\n";
    Harness h;
    auto r = h.epoch({h.make(StdTxn::NEW_ORDER, newOrderParams(2, 10, 3, {1, 2, 3})),
                      h.make(StdTxn::DELIVERY, {2, 7, 10, 0})}, {0, 1});
    check(r[0].committed && r[1].committed, "Delivery one sub-epoch later: both commit");
    check(dbRow(tpcc_key::orders(2, 10, 31)).raw(col::O_CARRIER_ID) == "7", "Delivery delivered the in-epoch order 31");
    check(g_db[tpcc_key::newOrder(2, 10, 31)] == "DELETED", "NEW_ORDER(31) removed");
    check(h.s->idx.oldestNewOrder(2, 10) == -1, "index: district has nothing pending");

    Harness h4;
    auto q = h4.epoch({h4.make(StdTxn::NEW_ORDER, newOrderParams(2, 10, 3, {1, 2, 3})),
                       h4.make(StdTxn::DELIVERY, {2, 7, 10, 0})});
    check(q[1].committed, "same sub-epoch: Delivery commits");
    const bool delivered = g_db.count(tpcc_key::orders(2, 10, 31)) && !undelivered(dbRow(tpcc_key::orders(2, 10, 31)));
    expectSerializable(q[0], delivered, "same sub-epoch serializable");

    auto r2 = h.epoch({h.make(StdTxn::DELIVERY, {2, 7, 9, 0}),
                       h.make(StdTxn::NEW_ORDER, newOrderParams(2, 9, 4, {1, 2, 3}))});
    check(r2[0].committed && r2[1].committed, "Delivery older: both commit");
    check(undelivered(dbRow(tpcc_key::orders(2, 9, 31))), "Delivery (older ts) skipped: order 31 still undelivered");
    auto r3 = h.epoch({h.make(StdTxn::DELIVERY, {2, 7, 9, 0})});
    check(r3[0].committed && dbRow(tpcc_key::orders(2, 9, 31)).raw(col::O_CARRIER_ID) == "7",
          "next epoch: Delivery delivers order 31");
}
static void testDeliveryDelivery() {
    std::cout << "[8] Delivery -> Delivery (same district, same epoch)\n";
    Harness h;
    const int64_t before = dbRow(key::customer(1, 4, 21)).inum(col::C_DELIVERY_CNT) +
                           dbRow(key::customer(1, 4, 22)).inum(col::C_DELIVERY_CNT);
    h.runToCompletion({h.make(StdTxn::DELIVERY, {1, 5, 4, 0}), h.make(StdTxn::DELIVERY, {1, 6, 4, 1})}, 2);
    check(h.committed[tid(StdTxn::DELIVERY)] == 2, "both deliveries eventually commit (aborted: " +
          std::to_string(h.aborted[tid(StdTxn::DELIVERY)]) + ")");
    check(!undelivered(dbRow(tpcc_key::orders(1, 4, 21))) && !undelivered(dbRow(tpcc_key::orders(1, 4, 22))) &&
          undelivered(dbRow(tpcc_key::orders(1, 4, 23))), "orders 21 and 22 delivered, 23 not");
    const int64_t after = dbRow(key::customer(1, 4, 21)).inum(col::C_DELIVERY_CNT) +
                          dbRow(key::customer(1, 4, 22)).inum(col::C_DELIVERY_CNT);
    check(after - before == 2, "each order delivered exactly once (C_DELIVERY_CNT +1 each)");
    check(h.s->idx.oldestNewOrder(1, 4) == 23, "index: oldest pending is 23");
}
static void testRollbackOrderStatus() {
    std::cout << "[9] NewOrder(rollback) -> OrderStatus\n";
    Harness h;
    auto r = h.epoch({h.make(StdTxn::NEW_ORDER, newOrderParams(1, 5, 7, {1, 2, 3}, /*rbk=*/true)),
                      h.make(StdTxn::ORDER_STATUS, {1, 5, 0, 7})});
    check(r[1].committed && olOrders(r[1]) == std::set<int64_t>{7}, "OrderStatus returns the old order 7");
    check(dNext(1, 5) == 31 && !g_db.count(tpcc_key::orders(1, 5, 31)), "rolled-back order left no trace");
    check(h.s->idx.latestOrder(1, 5, 7) == 7, "index unchanged");
}

// ── Randomized + TPC-C consistency conditions ──────────────────────────────
static void testRandomConsistency(uint64_t seed, size_t per_epoch) {
    std::cout << "[10] random mix, seed " << seed << ", " << per_epoch << " txns/epoch\n";
    Harness h;
    std::map<std::pair<int, int>, int64_t> ytd0;
    tpcc_std::Config gc;
    gc.warehouses = W; gc.districts_per_w = D; gc.customers_per_d = C; gc.items = NI;
    tpcc_std::Generator gen(gc, seed);
    const auto reqs = gen.generate(400);
    std::vector<txn> all;
    for (const auto& q : reqs) all.push_back(h.make(q.type, q.params));
    int64_t dcnt0 = 0;
    for (const auto& [k, v] : g_db)
        if (k.table_id == uint16_t(TpccTable::CUSTOMER)) dcnt0 += Row::parse(std::string(4, '\0') + v).inum(col::C_DELIVERY_CNT);
    auto centsOf = [](const Row& r, int c) { return std::llround(r.num(c) * 100); };
    int64_t wytd0 = 0, cpay0 = 0;
    for (int w = 1; w <= W; ++w) wytd0 += centsOf(dbRow(key::warehouse(w)), col::W_YTD);
    for (const auto& [k, v] : g_db)
        if (k.table_id == uint16_t(TpccTable::CUSTOMER)) cpay0 += centsOf(Row::parse(std::string(4, '\0') + v), col::C_YTD_PAYMENT);
    int delivered0 = 0;
    for (const auto& [k, v] : g_db)
        if (k.table_id == uint16_t(TpccTable::ORDERS) && !undelivered(Row::parse(std::string(4, '\0') + v))) ++delivered0;

    h.runToCompletion(all, per_epoch);
    int c_all = 0, a_all = 0;
    for (auto& [t, n] : h.committed) c_all += n;
    for (auto& [t, n] : h.aborted) a_all += n;
    std::cout << "        committed " << c_all << " / " << all.size() << ", aborted attempts " << a_all << ":";
    for (auto& [r, n] : h.reasons) std::cout << " " << toString(r) << "=" << n;
    std::cout << "\n        aborted attempts by type:";
    for (auto& [t, n] : h.aborted) std::cout << " " << tpcc_std::txnName(StdTxn(t - 1)) << "=" << n;
    std::cout << "\n";
    check(size_t(c_all) == all.size(), "every txn committed eventually");
    check(g_dispatch_failures == 0, "no dispatch failure (no UPDATE of a missing row)");
    check(h.s->bad_rows == 0, "no expected row was missing");

    auto cents = [](const Row& r, int c) { return std::llround(r.num(c) * 100); };
    bool c1 = true, c2 = true, c3 = true, c4 = true, cidx = true, c_no = true;
    for (int w = 1; w <= W; ++w) {
        int64_t sum = 0;
        for (int d = 1; d <= D; ++d) sum += cents(dbRow(key::district(w, d)), col::D_YTD);
        c1 &= cents(dbRow(key::warehouse(w)), col::W_YTD) == sum;
        for (int d = 1; d <= D; ++d) {
            const int nx = dNext(w, d);
            int max_o = 0; int64_t ol_cnt_sum = 0, ol_rows = 0;
            std::vector<int64_t> live_no;
            std::map<int, int> latest;   // customer -> max O_ID
            for (auto it = g_db.lower_bound(ObjectKey{uint16_t(TpccTable::ORDERS), uint32_t(w), uint32_t(d), 0});
                 it != g_db.end() && it->first.table_id == uint16_t(TpccTable::ORDERS) && it->first.w_id == uint32_t(w) &&
                 it->first.d_id == uint32_t(d); ++it) {
                Row O = Row::parse(std::string(4, '\0') + it->second);
                const int o = int(O.inum(col::O_ID));
                max_o = std::max(max_o, o);
                ol_cnt_sum += O.inum(col::O_OL_CNT);
                auto& l = latest[int(O.inum(col::O_C_ID))]; l = std::max(l, o);
                const bool has_no = g_db.count(tpcc_key::newOrder(w, d, o)) && g_db[tpcc_key::newOrder(w, d, o)] != "DELETED";
                c_no &= (has_no == undelivered(O));      // carrier null <=> NEW_ORDER row exists
                if (has_no) live_no.push_back(o);
            }
            for (auto it = g_db.lower_bound(ObjectKey{uint16_t(TpccTable::ORDER_LINE), uint32_t(w), uint32_t(d), 0});
                 it != g_db.end() && it->first.table_id == uint16_t(TpccTable::ORDER_LINE) && it->first.w_id == uint32_t(w) &&
                 it->first.d_id == uint32_t(d); ++it) ++ol_rows;
            std::sort(live_no.begin(), live_no.end());
            c2 &= (nx - 1 == max_o);
            if (!live_no.empty()) c2 &= live_no.back() <= nx - 1;
            c3 &= live_no.empty() || int64_t(live_no.size()) == live_no.back() - live_no.front() + 1;
            c4 &= ol_cnt_sum == ol_rows;
            const auto idx_no = h.s->idx.pendingFirst(w, d, 100000);
            cidx &= std::vector<int64_t>(idx_no.begin(), idx_no.end()) == live_no;
            cidx &= int(h.s->idx.committedNext(w, d)) == nx;
            for (auto& [c, o] : latest) cidx &= h.s->idx.latestOrder(w, d, c) == o;
        }
    }
    check(c1, "C1: W_YTD = sum(D_YTD)");
    int64_t wytd1 = 0, cpay1 = 0;
    for (int w = 1; w <= W; ++w) wytd1 += centsOf(dbRow(key::warehouse(w)), col::W_YTD);
    for (const auto& [k, v] : g_db)
        if (k.table_id == uint16_t(TpccTable::CUSTOMER)) cpay1 += centsOf(Row::parse(std::string(4, '\0') + v), col::C_YTD_PAYMENT);
    check(wytd1 - wytd0 == h.paid_cents && cpay1 - cpay0 == h.paid_cents,
          "sum(committed Payment h) = delta W_YTD = delta sum(C_YTD_PAYMENT) (" + std::to_string(h.paid_cents) + " cents)");
    check(c2, "C2: D_NEXT_O_ID - 1 = max(O_ID) >= max(NO_O_ID)");
    check(c3, "C3: pending NEW_ORDER ids are contiguous (oldest always delivered first)");
    check(c4, "C4: sum(O_OL_CNT) = #ORDER_LINE rows");
    check(c_no, "O_CARRIER_ID unset <=> NEW_ORDER row exists");
    int64_t dcnt1 = 0; int delivered1 = 0;
    for (const auto& [k, v] : g_db) {
        if (k.table_id == uint16_t(TpccTable::CUSTOMER)) dcnt1 += Row::parse(std::string(4, '\0') + v).inum(col::C_DELIVERY_CNT);
        if (k.table_id == uint16_t(TpccTable::ORDERS) && !undelivered(Row::parse(std::string(4, '\0') + v))) ++delivered1;
    }
    check(dcnt1 - dcnt0 == delivered1 - delivered0, "every delivered order counted exactly once (" +
          std::to_string(delivered1 - delivered0) + " deliveries)");
    check(cidx, "proxy indexes == database (pending, committed next O_ID, latest order per customer)");
}

// ── Payment escrow ─────────────────────────────────────────────────────────
static int64_t ytdCents(const ObjectKey& k, int c) { return std::llround(dbRow(k).num(c) * 100); }
static std::vector<int64_t> payParams(int w, int d, int c, int64_t cents) { return {w, d, w, d, 0, c, cents}; }

static void testEscrow() {
    std::cout << "[11a] 40 Payments, one warehouse, one epoch\n";
    {
        Harness h;
        const int64_t w0 = ytdCents(key::warehouse(1), col::W_YTD);
        std::map<int, int64_t> d0, want;
        for (int d = 1; d <= D; ++d) d0[d] = ytdCents(key::district(1, d), col::D_YTD);
        std::vector<txn> b;
        int64_t total = 0;
        for (int i = 0; i < 40; ++i) {
            const int d = 1 + i % D, c = 1 + i / D;      // distinct customers
            const int64_t cents = 100 + 37 * i;
            b.push_back(h.make(StdTxn::PAYMENT, payParams(1, d, c, cents)));
            want[d] += cents; total += cents;
        }
        std::vector<int> arr(b.size());
        for (size_t i = 0; i < b.size(); ++i) arr[i] = int(i % 3);   // spread over sub-epochs
        auto r = h.epoch(b, arr);
        int ok = 0; for (auto& x : r) ok += x.committed;
        check(ok == 40, "all 40 commit in one epoch (" + std::to_string(ok) + ")");
        check(ytdCents(key::warehouse(1), col::W_YTD) - w0 == total, "W_YTD += sum of all 40 amounts");
        bool dok = true;
        for (int d = 1; d <= D; ++d) dok &= ytdCents(key::district(1, d), col::D_YTD) - d0[d] == want[d];
        check(dok, "every D_YTD += its district's sum");
    }
    std::cout << "[11b] Payment and NewOrder on one district, both orders\n";
    for (int order = 0; order < 2; ++order) {
        Harness h;
        const int64_t d0 = ytdCents(key::district(1, 6), col::D_YTD), w0 = ytdCents(key::warehouse(1), col::W_YTD);
        txn pay = h.make(StdTxn::PAYMENT, payParams(1, 6, 5, 1234));
        txn no  = h.make(StdTxn::NEW_ORDER, newOrderParams(1, 6, 9, {1, 2, 3}));
        auto r = order == 0 ? h.epoch({pay, no}) : h.epoch({no, pay});
        check(r[0].committed && r[1].committed,
              std::string(order == 0 ? "Payment older" : "NewOrder older") + ": both commit in the same sub-epoch");
        check(dNext(1, 6) == 32 && ytdCents(key::district(1, 6), col::D_YTD) - d0 == 1234 &&
              ytdCents(key::warehouse(1), col::W_YTD) - w0 == 1234,
              "DISTRICT has D_NEXT_O_ID + 1 AND D_YTD + 12.34; W_YTD + 12.34");
    }
    std::cout << "[11c] aborted Payment: its increment is dropped\n";
    {
        Harness h;
        const int64_t w0 = ytdCents(key::warehouse(2), col::W_YTD), d0 = ytdCents(key::district(2, 3), col::D_YTD);
        const int64_t c0 = ytdCents(key::customer(2, 3, 7), col::C_YTD_PAYMENT);
        // Same customer, same sub-epoch: the younger one reads CUSTOMER before
        // the older one writes it -> the older one aborts WRITE_TOO_LATE.
        auto r = h.epoch({h.make(StdTxn::PAYMENT, payParams(2, 3, 7, 500)),
                          h.make(StdTxn::PAYMENT, payParams(2, 3, 7, 900))});
        int64_t got = 0; int ok = 0;
        if (r[0].committed) { got += 500; ++ok; }
        if (r[1].committed) { got += 900; ++ok; }
        check(ok == 1, "exactly one of the two commits");
        check(ytdCents(key::warehouse(2), col::W_YTD) - w0 == got && ytdCents(key::district(2, 3), col::D_YTD) - d0 == got &&
              ytdCents(key::customer(2, 3, 7), col::C_YTD_PAYMENT) - c0 == got,
              "W_YTD, D_YTD and C_YTD_PAYMENT grow by the committed amount only (" + std::to_string(got) + ")");
    }
    std::cout << "[11d] escrow off (old behaviour), 10 Payments of one warehouse\n";
    {
        Harness h(/*escrow=*/false);
        std::vector<txn> b;
        for (int i = 0; i < 10; ++i) b.push_back(h.make(StdTxn::PAYMENT, payParams(1, 1 + i, 1 + i, 100)));
        auto r = h.epoch(b);
        int ok = 0; for (auto& x : r) ok += x.committed;
        check(ok == 1, "only " + std::to_string(ok) + " commits (WAREHOUSE row is a hot key)");
    }
    std::cout << "[11e] FieldDelta on a row nobody read\n";
    {
        Harness h;
        auto s = h.s;
        constexpr TxnTypeId kBlind = 99;
        h.engine.registerType(kBlind, "BlindDelta", [s](uint32_t, const P&, const Prior&) {
            TxnStage t{{}, true};
            t.deltas.push_back(FieldDelta{R(*s, key::warehouse(2)), uint32_t(col::W_YTD), 100});
            return t;
        });
        const int64_t w0 = ytdCents(key::warehouse(2), col::W_YTD);
        txn t; t.txn_type = kBlind; t.params = {int64_t(h.next_tag++)};
        auto r = h.epoch({t});
        check(!r[0].committed && r[0].abort_reason == AbortReason::DELTA_NO_BASE, "aborts with DELTA_NO_BASE");
        check(ytdCents(key::warehouse(2), col::W_YTD) == w0 && g_dispatch_failures == 0, "nothing written");
    }
}

// ── Stock-Level DP synopsis ────────────────────────────────────────────────
static int64_t lowStockFromOram(const AssembledTxnResult& r, int64_t threshold) {
    std::set<int64_t> low;
    for (const auto& x : r.per_op_result) {
        Row S = Row::parse(x);
        if (S.f.size() == 17 && S.inum(col::S_QUANTITY) < threshold) low.insert(S.inum(0));
    }
    return int64_t(low.size());
}
static void testStockLevelSynopsis() {
    std::cout << "[12] Stock-Level from the DP synopsis (released once, reused unchanged)\n";
    Harness h;
    // Low stock for warehouse 1 (S_QUANTITY 5..19) so the counts are not all 0.
    for (auto& r : h.stock_rows) {
        if (tpcc_std::csvU(r, 1) != 1) continue;
        const uint32_t i = tpcc_std::csvU(r, 0);
        Row S = Row::parse(std::string(4, '\0') + r);
        S.set(col::S_QUANTITY, std::to_string(5 + i % 15));
        r = S.str();
        g_db[tpcc_key::stock(1, i)] = r;
    }
    auto syn = std::make_shared<tpcc_std::StockLevelSynopsis>(1e12, 7, 20.0);   // noise ~ 1e-11
    syn->build(h.stock_rows, h.ol_rows);                                         // the release
    h.s->sl_synopsis = syn;
    const std::vector<int> ds = {3, 4, 5};
    const std::vector<int64_t> ths = {10, 12, 15, 18, 20};
    std::map<std::pair<int, int64_t>, double> at_release;
    bool eq0 = true, zero_ops = true;
    for (int d : ds)
        for (int64_t th : ths) {
            auto r1 = h.epoch({h.make(StdTxn::STOCK_LEVEL, {1, d, th})});
            zero_ops &= r1[0].committed && r1[0].per_op_result.empty();
            at_release[{d, th}] = syn->answer(1, uint32_t(d), th);
            h.s->sl_synopsis = nullptr;                                   // exact path through ORAM
            auto r2 = h.epoch({h.make(StdTxn::STOCK_LEVEL, {1, d, th})});
            h.s->sl_synopsis = syn;
            eq0 &= r2[0].committed && std::llround(at_release[{d, th}]) == lowStockFromOram(r2[0], th);
        }
    check(zero_ops, "synopsis Stock-Level commits with 0 ORAM ops");
    double nz = 0; for (auto& kv : at_release) nz += kv.second;
    check(eq0 && nz > 0, "noise -> 0, before any update: answer == exact ORAM Stock-Level (3 districts x 5 thresholds, "
                         "sum of answers " + std::to_string(std::llround(nz)) + ")");

    // New-Orders change stock and the order windows ...
    std::vector<txn> nos;
    for (int k = 0; k < 8; ++k)
        nos.push_back(h.make(StdTxn::NEW_ORDER, newOrderParams(1, 3 + (k % 2), 10 + k, {3, 5, 7, 9, 11, 13})));
    h.runToCompletion(nos, 1);
    check(h.committed[tid(StdTxn::NEW_ORDER)] == 8, "8 New-Orders committed");
    bool mirror = true;
    for (int i : {3, 5, 7, 9, 11, 13})
        mirror &= syn->quantity(1, uint32_t(i)) == dbRow(tpcc_key::stock(1, i)).inum(col::S_QUANTITY);
    check(mirror, "measurement mirror == database after the New-Orders (incl. +91 wrap)");
    // ... but the published synopsis does not change until the next repartitioning.
    bool unchanged = true, stale_somewhere = false;
    for (int d : ds)
        for (int64_t th : ths) {
            unchanged &= syn->answer(1, uint32_t(d), th) == at_release[{d, th}];
            stale_somewhere |= std::llround(at_release[{d, th}]) != syn->exact(1, uint32_t(d), th);
        }
    check(unchanged && syn->releases() == 1, "answers unchanged after updates, still 1 release");
    check(stale_somewhere, "the data did change (stale answers exist) -> the test is meaningful");
    syn->repartition();                                                      // next repartitioning
    bool fresh = true;
    for (int d : ds)
        for (int64_t th : ths) fresh &= std::llround(syn->answer(1, uint32_t(d), th)) == syn->exact(1, uint32_t(d), th);
    check(fresh && syn->releases() == 2, "after repartition(): new release reflects the current data (2 releases)");

    auto noisy = std::make_shared<tpcc_std::StockLevelSynopsis>(1.0, 9, 20.0);
    noisy->build(h.stock_rows, h.ol_rows);
    const double first = noisy->answer(1, 3, 20);
    for (int k = 0; k < 400; ++k) (void)noisy->answer(1 + k % 2, 1 + k % 10, 10 + k % 11);
    check(noisy->answer(1, 3, 20) == first && noisy->releases() == 1, "eps = 1: 401 answers, 1 release, same value");
    std::cout << "        eps=1 sens=20: mean |error| (noise) = " << noisy->meanAbsErrorNoise()
              << ", mean exact = " << noisy->meanExact() << "\n";
}

int main() {
    testNewOrderThenOrderStatus();
    testOrderStatusThenNewOrder();
    testNewOrderStockLevel();
    testNewOrderDelivery();
    testDeliveryDelivery();
    testRollbackOrderStatus();
    testRandomConsistency(7, 40);
    testRandomConsistency(11, 120);
    testEscrow();
    testStockLevelSynopsis();
    std::cout << "\n" << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
