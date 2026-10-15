// ============================================================================
// TPCC_Std_Main.cpp — STANDARD TPC-C (New-Order, Payment, Order-Status,
// Delivery, Stock-Level) on the ORAM cluster: SidePart vs Obladi.
//
//   ./TPCC_Std run <data_dir> [k=4] [business_txns=2000] [offered_txn_s=400]
//                  [placement=depart|random|exact_oracle|obladi] [eps_part=0.5] [base_port=8881]
//                  [out_prefix=tpccstd] [seed=42] [warehouses=4]
//                  [bread=256] [bwrite=512] [insert_reserve=0(auto)] [R=4]
//   ./TPCC_Std gen [business_txns=2000] [seed=42] [warehouses=4]   (mix only)
//   ./TPCC_Std plan <data_dir> <W> <m> [eps_part=0.5]   (elastic K: print the
//                  partitions Algorithm 4 gives for threshold m, no shards)
//
// ELASTIC K (10/14/26): env TPCC_WH_PER_PART=m (1 <= m <= W) switches `depart`
// from "exactly k partitions" to a FIXED PUBLIC THRESHOLD per table,
//     T_table = (m - 0.5) x rows_per_warehouse(table)        (TPC-C schema constant)
// so Algorithm 4 closes a partition after m warehouses and the number of
// partitions K = ceil(W / m) is decided by the threshold, not passed in.  The
// k argument is then ignored (K replaces it; W % K need not be 0: m = 3, W = 8
// gives 3 / 3 / 2 warehouses).  Random / Obladi get K from the caller.
//
// Data: ./build/bin/tpcc-generator <W> <data_dir>   (all 9 tables, unchanged)
// Network: sudo ./net_emulate.sh on <base_port> <k> shared   (1 Gbps, 10 ms RTT)
//
// placement
//   depart       : SidePart, DP data-aware.  DP range partitioning
//                  (Repartition_Main, eps_part > 0; warehouse-keyed tables use
//                  Algorithm 4 with a public schema-derived threshold, ITEM
//                  Algorithm 3 with fixed k; W % k == 0) of every table on
//                  its WAREHOUSE column (ITEM: I_PRICE), union model: partition
//                  i of every table -> shard i.  Needs k <= W.  WAREHOUSE and
//                  DISTRICT (1 / 10 rows per warehouse, fixed by the schema, so
//                  their counts are public) follow STOCK's DP warehouse
//                  boundaries instead of being DP-partitioned themselves.
//   random       : SidePart with eps = 0 = zero information: every row on a
//                  uniformly random shard (data-independent), k shards.
//                  `depart` with eps_part <= 0 runs this.  (old name: index)
//   exact_oracle : NOT a privacy setting.  The same range partitioning as
//                  `depart`, cut on EXACT counts (no noise, no dummy blocks):
//                  a non-private, upper-bound-locality oracle that measures
//                  what DP noise costs relative to ideal data-aware placement.
//                  Never report it as eps = 0.  (old name: depart_exact)
//   obladi       : one oblivious store (k forced to 1), no partitioning, no DP.
//                  Give it the same total batch capacity: bread = k x 256.
// All placements use the SAME proxy-side secondary indexes (tpcc_proxy_index.h):
// last-name -> C_ID, latest order per customer, oldest NEW_ORDER per
// district, O_OL_CNT per order.  Indexes only find keys; rows go through ORAM.
//
// Stage functions and their MVTSO handling of the proxy indexes:
// tpcc_std_txns.h.  Correctness tests: tests/tpcc_std_mvtso_test.cpp.
//
// Txn type ids on the wire: 1 NewOrder, 2 Payment, 3 OrderStatus, 4 Delivery,
// 5 StockLevel.  (<prefix>_attempts.csv from the driver prints them as T1..T5.)
// Every request carries one extra trailing param = its request index ("tag").
// ============================================================================
#include <cstdlib>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "DePart_TPCC_Driver.h"
#include "MultiRingORAM_Servers.h"
#include "Repartition.h"
#include "RQ_Preflight.h"
#include "TpccSchema.h"
#include "tpcc_proxy_index.h"
#include "tpcc_std_txns.h"
#include "tpcc_std_workload.h"

namespace stdrun {

// ── Loading ─────────────────────────────────────────────────────────────────
static std::vector<std::string> readRows(const std::string& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open " + path + " (run tpcc-generator first)");
    std::vector<std::string> rows;
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!line.empty()) rows.push_back(std::move(line));
    }
    return rows;
}
static size_t numCols(const std::string& r) { return size_t(std::count(r.begin(), r.end(), ',')) + 1; }
using tpcc_std::csvU;

struct Table {
    TpccTable id;
    std::string name, file, schema;
    int attr_col;          // depart partitioning attribute
    size_t ncols;          // expected column count (sanity check)
    std::function<ObjectKey(uint32_t, const std::string&)> key;
    std::vector<std::string> rows;
    std::unique_ptr<Repartition> rp;
    BinInfo rr_bins;
    std::vector<Partition> parts;
    const BinInfo& bins() const { return rp ? rp->getBinInfo() : rr_bins; }
};

// exact_oracle (NON-private): the same attribute range partitioning as Repartition
// (one bin per distinct attribute value, bins in value order, k contiguous
// ranges), but cut on EXACT counts: no noise, no dummy blocks.  A bin goes to
// the partition holding the middle of its rows (W = k: one warehouse per
// partition; W > k: contiguous groups of warehouses).
static void exactRangePartition(Table& t, uint32_t k) {
    std::map<std::string, std::vector<uint32_t>> by_val;
    for (uint32_t r = 0; r < t.rows.size(); ++r) by_val[tpcc_std::csvField(t.rows[r], t.attr_col)].push_back(r);
    std::vector<std::pair<double, std::string>> order;
    for (const auto& kv : by_val) {
        double v = 0; try { v = std::stod(kv.first); } catch (...) {}
        order.push_back({v, kv.first});
    }
    std::sort(order.begin(), order.end());
    t.parts.assign(k, Partition{});
    const double total = double(t.rows.size());
    double cum = 0;
    for (const auto& [v, b] : order) {
        (void)v;
        const auto& idx = by_val[b];
        const uint32_t part = std::min<uint32_t>(k - 1, uint32_t((cum + idx.size() / 2.0) * k / total));
        cum += idx.size();
        t.rr_bins.bin_key_to_data_indices[b] = idx;
        t.rr_bins.synopsis[b] = uint32_t(idx.size());
        t.rr_bins.noised_synopsis[b] = uint32_t(idx.size());
        t.parts[part].index.push_back(b);
    }
    for (auto& pt : t.parts) {
        uint32_t n = 0;
        for (const auto& b : pt.index) n += uint32_t(by_val[b].size());
        pt.synopsis = pt.noisy_synopsis = std::max<uint32_t>(1, n);
        pt.dummy_num = 0; pt.noise_stash_reserve = 0; pt.stash_overflow_count = 0;
    }
}

// depart, warehouse-keyed tables: Algorithm 4 (fixed public threshold).
// Every bin is one warehouse and all warehouses have (about) the same size, so
// Algorithm 3's threshold ceil(total/k) sits exactly on a bin boundary and the
// DP noise decides whether two bins merge (seen: W=k=4 gave 3 partitions).
// Threshold T = (W/k - 0.5) * rows-per-warehouse, with rows-per-warehouse the
// TPC-C SCHEMA constant (public, not data): a partition closes after W/k
// warehouses with half a warehouse of slack against the noise -> exactly k
// partitions, and the same cut points in every table.  Needs W % k == 0.
static double rowsPerWarehouse(TpccTable t) {
    switch (t) {
        case TpccTable::WAREHOUSE:  return 1;
        case TpccTable::DISTRICT:   return 10;
        case TpccTable::CUSTOMER:   return 30000;
        case TpccTable::HISTORY:    return 30000;
        case TpccTable::ORDERS:     return 30000;
        case TpccTable::NEW_ORDER:  return 9000;
        case TpccTable::ORDER_LINE: return 300000;   // 10 lines per order on average
        case TpccTable::STOCK:      return 100000;
        default:                    return 0;
    }
}
static double warehouseThreshold(TpccTable t, int W, uint32_t k) {
    return (double(W) / double(k) - 0.5) * rowsPerWarehouse(t);
}
// Elastic K: m warehouses per partition (public), see the header.
static double elasticThreshold(TpccTable t, int m) { return (double(m) - 0.5) * rowsPerWarehouse(t); }
static int elasticM() {
    const char* e = std::getenv("TPCC_WH_PER_PART");
    return (e && *e) ? std::max(0, std::atoi(e)) : 0;
}

// depart, for tables keyed by warehouse whose DP partitioning cannot give k
// partitions (WAREHOUSE: 1 row per warehouse, DISTRICT: 10): place every
// bin on the shard that the reference table's DP partitioning (STOCK) gave
// to the same warehouse.  Their per-warehouse cardinalities are fixed by
// the TPC-C schema (1 and 10, never inserted), so per-partition counts are
// public and need no noise; the warehouse -> shard boundaries are the DP
// output of the reference table (post-processing, no extra budget).
static bool mapPartition(Table& t, uint32_t k, const std::map<long, uint32_t>& wmap) {
    std::map<std::string, std::vector<uint32_t>> by_val;
    for (uint32_t r = 0; r < t.rows.size(); ++r) by_val[tpcc_std::csvField(t.rows[r], t.attr_col)].push_back(r);
    t.parts.assign(k, Partition{});
    t.rr_bins = BinInfo{};
    for (const auto& [b, idx] : by_val) {
        long v = -1; try { v = std::lround(std::stod(b)); } catch (...) {}
        auto it = wmap.find(v);
        if (it == wmap.end()) { std::cerr << t.name << ": warehouse " << b << " not in reference map\n"; return false; }
        t.rr_bins.bin_key_to_data_indices[b] = idx;
        t.rr_bins.synopsis[b] = uint32_t(idx.size());
        t.rr_bins.noised_synopsis[b] = uint32_t(idx.size());
        t.parts[it->second].index.push_back(b);
    }
    for (auto& pt : t.parts) {
        uint32_t n = 0;
        for (const auto& b : pt.index) n += uint32_t(by_val[b].size());
        pt.synopsis = pt.noisy_synopsis = std::max<uint32_t>(1, n);
        pt.dummy_num = 0; pt.noise_stash_reserve = 0; pt.stash_overflow_count = 0;
    }
    return true;
}

static void writeTypesCsv(const depart::DePartRunStats& st, const std::string& path,
                          const std::string& sys, double offered) {
    std::ofstream f(path);
    f << "system,offered_txn_s,txn_type,committed,aborted_attempts,latency_p50_ms,latency_p95_ms,"
         "e2e_p50_ms,e2e_p95_ms,ops_per_committed\n";
    std::cout << "\n  type          committed  aborted  p50_ms   p95_ms   e2e_p95_ms\n";
    for (int t = 0; t < tpcc_std::kNumStdTxn; ++t) {
        const TxnTypeId id = tid(StdTxn(t));
        auto get = [&](const auto& m) { auto it = m.find(id); return it == m.end() ? 0ull : (unsigned long long)it->second; };
        const auto c = get(st.committed_by_type), a = get(st.aborted_by_type), ops = get(st.ops_by_type);
        std::vector<double> lat, e2e;
        if (st.latency_ms_by_type.count(id)) lat = st.latency_ms_by_type.at(id);
        if (st.e2e_latency_ms_by_type.count(id)) e2e = st.e2e_latency_ms_by_type.at(id);
        const char* nm = tpcc_std::txnName(StdTxn(t));
        f << sys << ',' << offered << ',' << nm << ',' << c << ',' << a << ','
          << depart::quantileOf(lat, .5) << ',' << depart::quantileOf(lat, .95) << ','
          << depart::quantileOf(e2e, .5) << ',' << depart::quantileOf(e2e, .95) << ','
          << (c ? double(ops) / c : 0.0) << '\n';
        std::printf("  %-12s %10llu %8llu %8.1f %8.1f %10.1f\n", nm, c, a,
                    depart::quantileOf(lat, .5), depart::quantileOf(lat, .95), depart::quantileOf(e2e, .95));
    }
}

static int runMain(int argc, char** argv) {
    const std::string dir   = argc > 1 ? argv[1] : "my_tpcc_input";
    uint32_t k              = argc > 2 ? std::stoul(argv[2]) : 4;
    const int n_business    = argc > 3 ? std::stoi(argv[3]) : 2000;
    const double offered    = argc > 4 ? std::stod(argv[4]) : 400.0;
    const std::string place = argc > 5 ? argv[5] : "depart";
    const double eps_part   = argc > 6 ? std::stod(argv[6]) : 0.5;
    const int base_port     = argc > 7 ? std::stoi(argv[7]) : 8881;
    const std::string pre   = argc > 8 ? argv[8] : "tpccstd";
    const uint64_t seed     = argc > 9 ? std::stoull(argv[9]) : 42;
    const int W             = argc > 10 ? std::max(1, std::stoi(argv[10])) : 4;
    const uint32_t bread    = argc > 11 ? std::stoul(argv[11]) : 256;
    const uint32_t bwrite   = argc > 12 ? std::stoul(argv[12]) : 512;
    uint32_t reserve        = argc > 13 ? std::stoul(argv[13]) : 0;
    // Read sub-epochs per epoch.  A txn must finish its read stages inside
    // ONE epoch, starting from the sub-epoch it arrives in, so a 3-round
    // Stock-Level that arrives in sub-epoch 2 of R=3 aborts (UNFINISHED) and
    // retries.  R=4 lets 3-round txns arriving in the first 2 sub-epochs and
    // 2-round txns arriving in the first 3 finish.  Same R for both systems.
    const uint32_t R_sub    = argc > 14 ? std::max(1ul, std::stoul(argv[14])) : 4;

    // Names: "index" is the old name of "random"; "depart_exact" of "exact_oracle".
    // SidePart with eps = 0 releases NOTHING about the data, i.e. data-
    // independent random placement — so depart + eps_part <= 0 runs "random".
    std::string place_eff = place;
    if (place_eff == "index") place_eff = "random";
    if (place_eff == "depart_exact") place_eff = "exact_oracle";
    if (place_eff == "depart" && eps_part <= 0.0) place_eff = "random";
    const bool obladi = place_eff == "obladi", random_place = place_eff == "random" || obladi;
    const bool exact  = place_eff == "exact_oracle";
    if (place_eff != "depart" && !exact && !random_place) {
        std::cerr << "placement: depart | random | exact_oracle | obladi\n"; return 1;
    }
    if (obladi && k != 1) { std::cout << "obladi: forcing k=1 (was " << k << ")\n"; k = 1; }
    const int m_elastic = (place_eff == "depart") ? elasticM() : 0;     // 0 = fixed k (default)
    if (m_elastic > 0 && m_elastic > W) {
        std::cerr << "TPCC_WH_PER_PART=" << m_elastic << " > W=" << W << "\n"; return 1;
    }
    if (m_elastic > 0) k = uint32_t((W + m_elastic - 1) / m_elastic);   // expected K; checked below
    if (!random_place && (k < 1 || int(k) > W)) {
        std::cerr << "depart partitions on the warehouse column: need 1 <= k <= W (use W = k)\n"; return 1;
    }
    if (place_eff == "depart" && m_elastic == 0 && W % int(k) != 0) {
        std::cerr << "depart: W must be a multiple of k (each shard gets W/k whole warehouses)\n"; return 1;
    }
    if (!random_place && W % int(k) != 0)
        std::cout << "note: W=" << W << " is not a multiple of k=" << k
                  << "; shards will hold unequal numbers of warehouses\n";

    std::cout << "Standard TPC-C   data=" << dir << "  W=" << W << "  k=" << k << "  placement=" << place_eff
              << (exact ? " (NON-PRIVATE oracle: exact data-aware partitioning)"
                  : place_eff == "random" ? " (SidePart eps=0: data-independent random placement)"
                  : place_eff == "depart" ? " (SidePart DP data-aware, eps_part=" + std::to_string(eps_part) + ")"
                  : "")
              << "  business_txns=" << n_business << "  offered=" << offered << " txn/s  bread=" << bread
              << "  bwrite=" << bwrite << "  R=" << R_sub << "\n\n";

    // ── 1. load all 9 tables ───────────────────────────────────────────────
    std::vector<Table> T;
    auto add = [&](TpccTable id, const char* nm, const char* file, const std::string& sch, int attr, size_t nc,
                   std::function<ObjectKey(uint32_t, const std::string&)> kf) {
        Table t; t.id = id; t.name = nm; t.file = file; t.schema = sch; t.attr_col = attr; t.ncols = nc;
        t.key = std::move(kf); T.push_back(std::move(t));
    };
    add(TpccTable::WAREHOUSE, "WAREHOUSE", "warehouse.csv", Tpcc::schema_str_warehouse, 0, 9,
        [](uint32_t, const std::string& r) { return key::warehouse(csvU(r, 0)); });
    add(TpccTable::DISTRICT, "DISTRICT", "district.csv", Tpcc::schema_str_district, 1, 11,
        [](uint32_t, const std::string& r) { return key::district(csvU(r, 1), csvU(r, 0)); });
    add(TpccTable::CUSTOMER, "CUSTOMER", "customer.csv", Tpcc::schema_str_customers, 2, 21,
        [](uint32_t, const std::string& r) { return key::customer(csvU(r, 2), csvU(r, 1), csvU(r, 0)); });
    add(TpccTable::HISTORY, "HISTORY", "history.csv", Tpcc::schema_str_history, 4, 8,
        [](uint32_t i, const std::string& r) { return key::history(csvU(r, 4), csvU(r, 3), uint64_t(i) + 1); });
    add(TpccTable::NEW_ORDER, "NEW_ORDER", "new_order.csv", Tpcc::schema_str_new_orders, 2, 3,
        [](uint32_t, const std::string& r) { return tpcc_key::newOrder(csvU(r, 2), csvU(r, 1), csvU(r, 0)); });
    add(TpccTable::ORDERS, "ORDERS", "order.csv", Tpcc::schema_str_orders, 2, 8,
        [](uint32_t, const std::string& r) { return tpcc_key::orders(csvU(r, 2), csvU(r, 1), csvU(r, 0)); });
    add(TpccTable::ORDER_LINE, "ORDER_LINE", "order_line.csv", Tpcc::schema_str_order_line, 2, 10,
        [](uint32_t, const std::string& r) { return tpcc_key::orderLine(csvU(r, 2), csvU(r, 1), csvU(r, 0), csvU(r, 3)); });
    add(TpccTable::ITEM, "ITEM", "item.csv", Tpcc::schema_str_item, 3, 5,
        [](uint32_t, const std::string& r) { return tpcc_key::item(csvU(r, 0)); });
    add(TpccTable::STOCK, "STOCK", "stock.csv", Tpcc::schema_str_stock, 1, 17,
        [](uint32_t, const std::string& r) { return tpcc_key::stock(csvU(r, 1), csvU(r, 0)); });

    uint32_t max_width = 0;
    for (auto& t : T) {
        t.rows = readRows(dir + "/" + t.file);
        if (t.rows.empty() || numCols(t.rows[0]) != t.ncols) {
            std::cerr << t.file << ": expected " << t.ncols << " columns, got "
                      << (t.rows.empty() ? 0 : numCols(t.rows[0])) << " — check the col:: constants\n";
            return 1;
        }
        max_width = std::max(max_width, tupleWidthBytesFromSchema(t.schema));
        std::cout << "  loaded " << std::left << std::setw(11) << t.name << t.rows.size() << " rows\n";
    }
    {
        uint32_t wmax = 0;
        for (const auto& r : T[0].rows) wmax = std::max(wmax, csvU(r, 0));
        if (int(wmax) != W) { std::cerr << "data has " << wmax << " warehouses but W=" << W << "\n"; return 1; }
    }
    std::cout << "  tuple width (widest schema) = " << max_width << " B\n";

    // ── 2. proxy indexes + workload ────────────────────────────────────────
    auto s = std::make_shared<StdCtx>();
    s->idx.build(T[2].rows, T[5].rows, T[4].rows);
    s->items = uint32_t(T[7].rows.size());
    s->hist_base = T[3].rows.size() + 1;
    // Payment escrow of W_YTD / D_YTD (tpcc_std_txns.h); TPCC_ESCROW=0 turns it off.
    if (const char* e = std::getenv("TPCC_ESCROW")) s->escrow = std::string(e) != "0";
    std::cout << "  Payment escrow (W_YTD/D_YTD): " << (s->escrow ? "on" : "off") << "\n";
    // Stock-Level from the DP synopsis (tpcc_stock_synopsis.h); TPCC_SL_SYNOPSIS=1 turns it on.
    // Released ONCE here (= at partitioning) and reused unchanged for the whole run.
    // TPCC_SL_EPS = epsilon of the release (default 1.0), TPCC_SL_SENS = L1 sensitivity (default 20).
    if (const char* e = std::getenv("TPCC_SL_SYNOPSIS"); e && std::string(e) == "1") {
        double sl_eps = 1.0, sl_sens = 20.0;
        if (const char* x = std::getenv("TPCC_SL_EPS")) sl_eps = std::stod(x);
        if (const char* x = std::getenv("TPCC_SL_SENS")) sl_sens = std::stod(x);
        s->sl_synopsis = std::make_shared<tpcc_std::StockLevelSynopsis>(sl_eps, seed ^ 0x51ULL, sl_sens);
        s->sl_synopsis->build(T[8].rows, T[6].rows);
        std::cout << "  Stock-Level: DP synopsis released once at partitioning, eps=" << sl_eps
                  << " sens=" << sl_sens << " (0 ORAM ops, approximate, not refreshed)\n";
    } else {
        std::cout << "  Stock-Level: exact, through ORAM\n";
    }

    tpcc_std::Config gc;
    gc.warehouses = W;
    gc.items = int(s->items);
    tpcc_std::Generator gen(gc, seed);
    const auto reqs = gen.generate(n_business);
    const auto counts = gen.counts(n_business);
    s->resetAssigned(reqs.size());

    std::vector<txn> requests;
    requests.reserve(reqs.size());
    for (size_t i = 0; i < reqs.size(); ++i) {
        txn t;
        t.txn_type = tid(reqs[i].type);
        t.params = reqs[i].params;
        t.params.push_back(int64_t(i));                       // tag
        requests.push_back(std::move(t));
    }
    std::cout << "  " << n_business << " business txns -> " << requests.size() << " DB txns (NewOrder "
              << counts[0] << ", Payment " << counts[1] << ", OrderStatus " << counts[2] << ", Delivery "
              << counts[3] << " x10, StockLevel " << counts[4] << ")\n";

    // ── 3. placement + union pos_map ───────────────────────────────────────
    std::map<long, uint32_t> wmap;
    Table* ref = nullptr;
    // Threshold of a warehouse-keyed table: fixed k (default) or elastic m.
    auto thr = [&](TpccTable id) {
        return m_elastic > 0 ? elasticThreshold(id, m_elastic) : warehouseThreshold(id, W, k);
    };
    // depart: DP-partition the reference table (STOCK, by S_W_ID) first; its
    // warehouse -> shard map places the tiny, fixed-size warehouse tables.
    if (place_eff == "depart") {
        for (auto& t : T) if (t.id == TpccTable::STOCK) ref = &t;
        ref->rp = std::make_unique<Repartition>();
        ref->parts = ref->rp->Repartition_Main(ref->rows, ref->schema, {ref->attr_col}, eps_part, 0.0,
                                               thr(ref->id), false, 0, 0, 0);
        if (m_elastic > 0) {
            // Elastic K: the threshold decides the number of partitions.
            std::cout << "  [elastic] m=" << m_elastic << " warehouses/partition, T(STOCK)=" << thr(ref->id)
                      << " -> K=" << ref->parts.size() << " partitions (expected " << k << ")\n";
            if (ref->parts.empty() || int(ref->parts.size()) > W) { std::cerr << "STOCK: bad K\n"; return 1; }
            k = uint32_t(ref->parts.size());
        } else if (ref->parts.size() != k) {
            std::cerr << "STOCK: DP partitioning gave " << ref->parts.size() << " partitions, want " << k << "\n";
            return 1;
        }
        for (uint32_t j = 0; j < k; ++j)
            for (const auto& b : ref->parts[j].index) {
                try { wmap[std::lround(std::stod(b))] = j; } catch (...) {}
            }
        std::cout << "  [depart] warehouse -> partition (from STOCK's DP partitioning):";
        for (auto& [w, j] : wmap) std::cout << " w" << w << "->" << j;
        std::cout << "\n";
    }


    // The reserve is PER SHARD and shared by all tables (pos_map free blocks).
    // Inserts per committed txn: New-Order <= 17 (ORDERS + NEW_ORDER + 15 OL),
    // Payment 1 (HISTORY).  Only committed writes allocate, so x2 is ample.
    if (reserve == 0) {
        const double per_shard = (17.0 * counts[0] + 1.0 * counts[1]) / double(k);
        reserve = std::max<uint32_t>(1024, uint32_t(std::ceil(2.0 * per_shard)));
    }
    std::cout << "  insert reserve per shard = " << reserve << "\n";

    std::vector<ServerInfo> servers(k);
    for (uint32_t i = 0; i < k; ++i) { servers[i].server_id = int(i); servers[i].port = base_port + int(i); servers[i].partition_id = i; }
    MultiRingORAM_Servers mrs;
    mrs.setInsertReservePerPartition(reserve);
    mrs.beginPositionMapBuild();
    std::mt19937_64 place_rng(seed ^ 0x9E3779B97F4A7C15ULL);

    for (auto& t : T) {
        if (&t == ref) {
            // already DP-partitioned above
        } else if (random_place) {
            t.parts.assign(k, Partition{});
            for (uint32_t r = 0; r < t.rows.size(); ++r) {
                const std::string b = std::to_string(r);
                t.rr_bins.bin_key_to_data_indices[b] = {r};
                t.rr_bins.synopsis[b] = 1; t.rr_bins.noised_synopsis[b] = 1;
                t.parts[k == 1 ? 0 : place_rng() % k].index.push_back(b);
            }
            for (auto& pt : t.parts) {
                pt.synopsis = pt.noisy_synopsis = static_cast<uint32_t>(std::max<size_t>(1, pt.index.size()));
                pt.dummy_num = 0; pt.noise_stash_reserve = 0; pt.stash_overflow_count = 0;
            }
        } else if (exact) {
            exactRangePartition(t, k);
        } else if (t.id == TpccTable::WAREHOUSE || t.id == TpccTable::DISTRICT) {
            if (!mapPartition(t, k, wmap)) return 1;
            std::cout << "  " << t.name << ": fixed-size table, placed by STOCK's warehouse boundaries\n";
        } else {
            t.rp = std::make_unique<Repartition>();
            if (t.id == TpccTable::ITEM)       // fine-grained price bins: Algorithm 3 (fixed k)
                t.parts = t.rp->Repartition_Main(t.rows, t.schema, {t.attr_col}, eps_part, 0.0, 0.0, true, k, 0, k);
            else                               // warehouse bins: Algorithm 4, public threshold
                t.parts = t.rp->Repartition_Main(t.rows, t.schema, {t.attr_col}, eps_part, 0.0,
                                                 thr(t.id), false, 0, 0, 0);
            if (t.parts.size() != k) {
                const bool by_w = t.id != TpccTable::ITEM;
                std::cerr << "  WARNING " << t.name << ": DP partitioning gave " << t.parts.size()
                          << " partitions, want " << k << "; "
                          << (by_w ? "using STOCK's warehouse boundaries\n" : "giving up\n");
                if (!by_w) return 1;
                t.rp.reset();
                if (!mapPartition(t, k, wmap)) return 1;
            }
        }
        auto kf = t.key;
        mrs.setObjectKeyMapper([kf](uint32_t i, const std::string& r) { return kf(i, r); });
        mrs.addTablePartitions(t.id, t.bins(), t.parts, servers, t.rows);
        std::cout << "  " << std::left << std::setw(11) << t.name << "rows/partition:";
        for (const auto& pt : t.parts) std::cout << " " << pt.index.size();
        std::cout << " (bins)\n";
    }
    mrs.finishPositionMapBuild();

    // Locality check (depart): which shard holds each warehouse's rows.
    if (!random_place) {
        std::cout << "  warehouse -> shard (WAREHOUSE / DISTRICT / CUSTOMER / STOCK):";
        for (int w = 1; w <= W; ++w)
            std::cout << "  w" << w << "=" << mrs.serverOfKey(key::warehouse(w)) << "/"
                      << mrs.serverOfKey(key::district(w, 1)) << "/"
                      << mrs.serverOfKey(key::customer(w, 1, 1)) << "/"
                      << mrs.serverOfKey(tpcc_key::stock(w, 1));
        std::cout << "\n";
    }

    // ── 4. shards ──────────────────────────────────────────────────────────
    requireListeners("127.0.0.1", base_port, k, "TPCC_Std");
    const uint32_t block_size = max_width + 16 + 2 * sizeof(uint32_t);
    for (uint32_t i = 0; i < k; ++i) {
        std::vector<TableSlice> slices;
        for (auto& t : T) {
            TableSlice sl;
            sl.table_id = static_cast<uint16_t>(t.id); sl.bin_info = &t.bins();
            sl.part = &t.parts[i]; sl.data = &t.rows; sl.schema_str = t.schema;
            slices.push_back(sl);
        }
        mrs.ServerInitializationBulkUnion(servers[i], slices, 8, "TPCCSTD_S" + std::to_string(i),
                                          block_size, "127.0.0.1", 4);
    }
    std::cerr << "  [init] forking " << k << " shards, waiting...\n";
    mrs.waitForAllServersReady();
    std::cerr << "  [init] all shards ready\n";

    // ── 5. run ─────────────────────────────────────────────────────────────
    EpochConfig cfgE;
    cfgE.R = R_sub; cfgE.bread = bread; cfgE.bwrite = bwrite; cfgE.sub_epoch_ms = 60;
    cfgE.pad_batches = true;
    cfgE.pad_uncontacted_shards = false;
    cfgE.per_server_capacity = true;
    cfgE.enforce_batch_capacity = true;

    std::function<int(const ObjectKey&)> insert_server;
    if (random_place)
        insert_server = [k](const ObjectKey& kk) { return int(ObjectKeyHash{}(kk) % k); };
    else   // new ORDERS / NEW_ORDER / ORDER_LINE / HISTORY rows: their warehouse's partition
        insert_server = [&mrs](const ObjectKey& kk) {
            const int sh = mrs.getServerIdForBinKey(kk.table_id, std::to_string(kk.w_id));
            return sh >= 0 ? sh : 0;
        };
    s->cx = std::make_shared<depart::DePartContext>();
    s->cx->route = depart::makeRouter(mrs, insert_server);
    s->cx->fmt.tuple_width = max_width;

    TxnEngine engine;
    registerStdTypes(engine, s);

    std::atomic<uint64_t> no_rollback{0}, no_ok{0}, dl_skipped{0};
    depart::DriverOptions opt;
    opt.offered_txn_s = offered;
    opt.max_retries   = 3;
    opt.max_epochs    = 1000000;
    opt.results_csv   = pre + "_attempts.csv";
    opt.n_shards      = k;
    opt.on_result = [s, &no_rollback, &no_ok, &dl_skipped](const txn& t, bool committed) {
        if (!committed) return;
        switch (onCommitted(*s, t)) {
            case CommitKind::NEWORDER_OK:       ++no_ok; break;
            case CommitKind::NEWORDER_ROLLBACK: ++no_rollback; break;
            case CommitKind::DELIVERY_SKIPPED:  ++dl_skipped; break;
            default: break;
        }
    };

    QueriesReceiving qr;
    auto stats = depart::runDePartBenchmark(qr, mrs, cfgE, engine, requests, opt);

    // ── 6. report ──────────────────────────────────────────────────────────
    const double min = stats.elapsed_ms / 60000.0;
    const double tpmC = min > 0 ? double(no_ok + no_rollback) / min : 0;
    std::cout << "\n[" << place_eff << "] epochs=" << stats.epochs << "  committed=" << stats.committed
              << "  gave_up=" << stats.gave_up << "  dropped=" << stats.dropped
              << "  elapsed_s=" << stats.elapsed_ms / 1000 << "  committed_tps=" << stats.throughput_tps() << "\n"
              << "  tpmC=" << tpmC << "  (NewOrder ok=" << no_ok << ", rolled back=" << no_rollback
              << ")  delivery districts skipped=" << dl_skipped << "  empty reads=" << s->bad_rows << "\n";
    std::cout << "  abort reasons:";
    for (auto& [r, n] : stats.aborts_by_reason) std::cout << " " << toString(r) << "=" << n;
    std::cout << "\n";
    writeTypesCsv(stats, pre + "_types.csv", place_eff, offered);

    // ── per-shard read load (locality vs load balance) ─────────────────────
    // demand_i = real_i + rejected_i of a read round; a round is ACTIVE if any
    // shard has demand.  hot = max_i real_i, imbalance = max_i demand_i /
    // mean_i demand_i (mean over ALL k shards; 1 = perfectly balanced, k = all
    // demand on one shard).
    double hot_sum = 0, imb_sum = 0; uint64_t active = 0, rounds_rej = 0, rej_slots = 0;
    {
        std::ofstream f(pre + "_rounds.csv");
        f << "epoch,round,shard,real,rejected\n";
        for (const auto& rl : stats.read_rounds_load) {
            uint64_t tot = 0, rej = 0; uint32_t hot = 0, dmax = 0;
            for (size_t i = 0; i < rl.real.size(); ++i) {
                f << rl.epoch << ',' << rl.round << ',' << i << ',' << rl.real[i] << ',' << rl.rejected[i] << '\n';
                const uint32_t d = rl.real[i] + rl.rejected[i];
                tot += d; rej += rl.rejected[i];
                hot = std::max(hot, rl.real[i]); dmax = std::max(dmax, d);
            }
            if (tot == 0) continue;
            ++active; hot_sum += hot;
            imb_sum += double(dmax) / (double(tot) / double(rl.real.size()));
            if (rej > 0) ++rounds_rej;
            rej_slots += rej;
        }
    }
    const double hot_mean = active ? hot_sum / active : 0, imb_mean = active ? imb_sum / active : 0;
    const double rej_frac = active ? double(rounds_rej) / active : 0;
    uint64_t fan_n = 0, fan_sum = 0, fan_one = 0;
    for (size_t i = 0; i < stats.txn_fanout_hist.size(); ++i) {
        fan_n += stats.txn_fanout_hist[i]; fan_sum += i * stats.txn_fanout_hist[i];
        if (i == 1) fan_one = stats.txn_fanout_hist[i];
    }
    const double fan_mean = fan_n ? double(fan_sum) / fan_n : 0, fan_single = fan_n ? double(fan_one) / fan_n : 0;
    std::cout << std::fixed << std::setprecision(2)
              << "  read load: hot shard " << hot_mean << " / " << bread << " slots, imbalance (max/mean demand) "
              << imb_mean << ", rounds with READ_BATCH_FULL " << 100 * rej_frac << "%, refused slots " << rej_slots
              << (s->sl_synopsis ? "\n  Stock-Level synopsis: " + std::to_string(s->sl_synopsis->queries()) +
                                   " answers from " + std::to_string(s->sl_synopsis->releases()) + " release(s), mean |error| " +
                                   std::to_string(s->sl_synopsis->meanAbsError()) + " (noise only " +
                                   std::to_string(s->sl_synopsis->meanAbsErrorNoise()) + ")" +
                                   " (mean exact count " + std::to_string(s->sl_synopsis->meanExact()) + ")"
                                 : std::string())
              << "\n  txn fan-out: " << fan_mean << " shards/committed txn, single-shard " << 100 * fan_single << "%\n";

    const double rr = stats.read_rounds ? double(stats.read_contacted) / stats.read_rounds : 0;
    const double wr = stats.write_rounds ? double(stats.write_contacted) / stats.write_rounds : 0;
    {
        std::ofstream f(pre + "_summary.csv");
        f << "system,k,W,offered_txn_s,bread,bwrite,elapsed_ms,epochs,committed,committed_tps,tpmC,"
             "neworder_ok,neworder_rollback,delivery_skipped,gave_up,dropped,shards_per_read_round,"
             "shards_per_write_round,slots_sent,slots_per_committed_txn,empty_reads,escrow,"
             "read_hot_shard_slots,read_imbalance,read_rounds_rejected_frac,read_rejected_slots,"
             "txn_shards_mean,txn_single_shard_frac,sl_synopsis,sl_eps,sl_queries,sl_releases,sl_mean_abs_err,sl_mean_exact,"
             "sl_sens,sl_mean_abs_err_noise,wh_per_part\n";
        f << place_eff << ',' << k << ',' << W << ',' << offered << ',' << bread << ',' << bwrite << ','
          << stats.elapsed_ms << ',' << stats.epochs << ',' << stats.committed << ',' << stats.throughput_tps() << ','
          << tpmC << ',' << no_ok << ',' << no_rollback << ',' << dl_skipped << ',' << stats.gave_up << ','
          << stats.dropped << ',' << rr << ',' << wr << ',' << stats.slots_sent << ','
          << (stats.committed ? double(stats.slots_sent) / stats.committed : 0) << ',' << s->bad_rows << ','
          << (s->escrow ? 1 : 0) << ',' << hot_mean << ',' << imb_mean << ',' << rej_frac << ','
          << rej_slots << ',' << fan_mean << ',' << fan_single << ','
          << (s->sl_synopsis ? 1 : 0) << ',' << (s->sl_synopsis ? s->sl_synopsis->eps() : 0.0) << ','
          << (s->sl_synopsis ? s->sl_synopsis->queries() : 0) << ',' << (s->sl_synopsis ? s->sl_synopsis->releases() : 0) << ','
          << (s->sl_synopsis ? s->sl_synopsis->meanAbsError() : 0.0) << ','
          << (s->sl_synopsis ? s->sl_synopsis->meanExact() : 0.0) << ','
          << (s->sl_synopsis ? s->sl_synopsis->sensitivity() : 0.0) << ','
          << (s->sl_synopsis ? s->sl_synopsis->meanAbsErrorNoise() : 0.0) << ',' << m_elastic << '\n';
    }
    {
        std::ofstream f(pre + "_epochs.csv");
        f << "epoch,epoch_ms\n";
        for (size_t e = 0; e < stats.epoch_ms.size(); ++e) f << (e + 1) << ',' << stats.epoch_ms[e] << '\n';
    }
    std::cout << "\n  " << std::fixed << std::setprecision(2) << rr << " shards/read round, " << wr
              << " shards/write round\nWrote " << pre << "_{summary,types,attempts,epochs,rounds}.csv\n";
    mrs.shutdownAllServers();
    return 0;
}

static int genMain(int argc, char** argv) {
    const int n = argc > 1 ? std::stoi(argv[1]) : 2000;
    tpcc_std::Config c;
    c.warehouses = argc > 3 ? std::stoi(argv[3]) : 4;
    tpcc_std::Generator g(c, argc > 2 ? std::stoull(argv[2]) : 42);
    const auto r = g.generate(n);
    std::map<int, int> cnt;
    size_t rbk = 0;
    for (const auto& x : r) {
        ++cnt[int(x.type)];
        if (x.type == StdTxn::NEW_ORDER && x.params[4] == 1) ++rbk;
    }
    std::cout << n << " business txns -> " << r.size() << " DB txns\n";
    for (auto& [t, k] : cnt) std::cout << "  " << tpcc_std::txnName(StdTxn(t)) << " " << k << "\n";
    std::cout << "  NewOrder rollbacks " << rbk << "\n";
    return 0;
}

// Elastic K dry run: load STOCK only, partition it with threshold m, print K.
static int planMain(int argc, char** argv) {
    if (argc < 4) { std::cerr << "plan <data_dir> <W> <m> [eps_part=0.5]\n"; return 1; }
    const std::string dir = argv[1];
    const int W = std::stoi(argv[2]), m = std::stoi(argv[3]);
    const double eps = argc > 4 ? std::stod(argv[4]) : 0.5;
    auto rows = readRows(dir + "/stock.csv");
    Repartition rp;
    auto parts = rp.Repartition_Main(rows, Tpcc::schema_str_stock, {1}, eps, 0.0,
                                     elasticThreshold(TpccTable::STOCK, m), false, 0, 0, 0);
    std::cout << "W=" << W << " m=" << m << " T=" << elasticThreshold(TpccTable::STOCK, m)
              << " eps=" << eps << " -> K=" << parts.size() << " (expected " << (W + m - 1) / m << ")\n";
    for (size_t j = 0; j < parts.size(); ++j) {
        std::cout << "  partition " << j << ": warehouses";
        for (const auto& b : parts[j].index) std::cout << " " << b;
        std::cout << "  (" << parts[j].synopsis << " rows)\n";
    }
    std::cout << "K=" << parts.size() << "\n";
    return 0;
}

}  // namespace stdrun

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "run") return stdrun::runMain(argc - 1, argv + 1);
    if (argc > 1 && std::string(argv[1]) == "gen") return stdrun::genMain(argc - 1, argv + 1);
    if (argc > 1 && std::string(argv[1]) == "plan") return stdrun::planMain(argc - 1, argv + 1);
    std::cerr << "usage: " << argv[0] << " run <data_dir> [k] [business_txns] [offered_txn_s] "
                 "[depart|random|exact_oracle|obladi] [eps_part] [base_port] [out_prefix] [seed] [W] [bread] [bwrite] [reserve] [R]\n"
              << "       " << argv[0] << " gen [business_txns] [seed] [W]\n";
    return 1;
}
