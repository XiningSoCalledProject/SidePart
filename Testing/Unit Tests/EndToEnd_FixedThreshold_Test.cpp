//
// EndToEnd_FixedThreshold_Test.cpp  —  Fixed-Threshold (Algorithm 2)
// Post-processing: Fixed-Threshold (use_fixed_partition_count = false)
//   The DP-noised threshold T determines partition boundaries.
//   Number of partitions is decided by the algorithm, NOT forced to k.
//   This should produce more balanced partitions than Fixed-Partition-Count.
//
// Goal: Test whether throughput scales closer to k× linear with better balance.
//
// Throughput measurement:
//   Clock starts  → batches ready, proxy sends to untrusted servers
//   Clock ends    → all servers returned to proxy
//   Throughput    = committed_ops / batch_execution_time
//
// IMPORTANT: Before running, start enough server processes.
//   The test will pre-scan all configs and tell you the max # needed.
//   Then start that many: ./bin/Servers_MultiRingORAM 8881
//                          ./bin/Servers_MultiRingORAM 8882
//                          ... etc.
//

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <cassert>
#include <random>
#include <atomic>
#include <map>
#include <set>
#include <algorithm>
#include <numeric>
#include <cmath>

#include "MultiRingORAM_Servers.h"
#include "Queries_Receiving.h"
#include "DataPath.h"


// ============================================================================
// Helpers
// ============================================================================

static std::vector<std::string> readCSV(const std::string& filename) {
    std::vector<std::string> data;
    std::ifstream file(filename);
    if (!file.is_open()) throw std::runtime_error("Cannot open: " + filename);
    std::string line;
    std::getline(file, line);
    while (std::getline(file, line))
        if (!line.empty()) data.push_back(line);
    return data;
}

static void push_txn_to_queue(QueriesReceiving& qr, const txn& t) {
    // Intake is open only between collect_epoch()'s start and its drain.
    // Retry briefly, then give up (the epoch closed) instead of losing the txn
    // silently the way the old extern-global push did.
    for (int i = 0; i < 2000; ++i) {
        if (qr.push_txn(t)) return;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

static Operat make_op(OpType type, uint32_t key, const std::string& val,
                       int server_id, bool last = false) {
    Operat op;
    op.type = type;
    op.data_primary_key = key;
    op.data_value = val;
    op.server_id = server_id;
    op.last_one = last;
    op.read_marker = false;
    op.txn_timestamp_id = 0;
    return op;
}

static void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "  " << title << "\n";
    std::cout << std::string(70, '=') << "\n\n";
}

// ============================================================================
// Per-server diagnostic
// ============================================================================
struct ServerDiag {
    int      server_id;
    uint32_t oram_tree_size;
    uint32_t read_ops;
    uint32_t update_ops;
    double   read_phase_ms;
    double   update_phase_ms;
    double   total_ms;
    double   read_per_op_ms;
    double   update_per_op_ms;
};

// ============================================================================
// Test result
// ============================================================================
struct DiagResult {
    double   epsilon;
    uint32_t num_servers_hint;  // what we requested
    uint32_t num_partitions;    // what DP threshold actually produced
    std::string post_processing;
    std::vector<uint32_t> partition_real_counts;
    std::vector<uint32_t> partition_dummy_counts;

    uint32_t total_txns;
    uint32_t committed_txns;
    uint32_t aborted_txns;
    uint32_t committed_ops;

    double   repartition_ms;
    double   oram_init_ms;
    double   epoch_collect_ms;
    double   mvcc_ms;
    double   batch_build_ms;

    double   batch_exec_ms;
    double   read_phase_wall_ms;
    double   update_phase_wall_ms;
    double   phases_sum_ms;            // read_wall + update_wall (pure server time)
    double   throughput_ops_sec;       // uses phases_sum_ms

    std::vector<ServerDiag> server_diags;
    double   read_max_ms, read_min_ms, read_straggler_ratio;
    double   update_max_ms, update_min_ms, update_straggler_ratio;

    // Balance metric
    double   partition_size_cv;   // coefficient of variation — lower = better
    double   partition_max_min;   // max/min ratio
};

// ============================================================================
// Run one test: Fixed-Threshold (Algorithm 2)
//   Key difference: Repartition_Main(..., false, ...)
//   Partition count is determined by DP threshold, not forced.
// ============================================================================
static DiagResult run_fixed_threshold_test(
    const std::vector<std::string>& data,
    const std::string& schema,
    const std::vector<int>& attr,
    double epsilon_1_ratio,
    double epsilon_2_ratio,
    double total_epsilon,
    uint32_t num_servers_hint,
    int primary_key_col,
    int NUM_CLIENTS,
    int TXNS_PER_CLIENT,
    int EPOCH_MS)
{
    DiagResult diag;
    diag.epsilon           = total_epsilon;
    diag.num_servers_hint  = num_servers_hint;
    diag.post_processing   = "FixedThreshold";

    using clk = std::chrono::high_resolution_clock;
    auto ms_between = [](auto a, auto b) {
        return std::chrono::duration<double, std::milli>(b - a).count();
    };

    // ================================================================
    // Step 1b: Repartition — Fixed-Threshold (Algorithm 2)
    //   false = use threshold-based partitioning, NOT forced k
    // ================================================================
    auto t0 = clk::now();

    Repartition repartitioner;
    auto partitions = repartitioner.Repartition_Main(
        data, schema, attr,
        epsilon_1_ratio, epsilon_2_ratio, total_epsilon,
        false,              // ← FIXED-THRESHOLD (Algorithm 2)
        num_servers_hint, primary_key_col
    );
    const BinInfo& bin_info = repartitioner.getBinInfo();

    auto t1 = clk::now();
    diag.repartition_ms = ms_between(t0, t1);
    diag.num_partitions = static_cast<uint32_t>(partitions.size());

    for (size_t i = 0; i < partitions.size(); ++i) {
        diag.partition_real_counts.push_back(
            static_cast<uint32_t>(partitions[i].synopsis));
        diag.partition_dummy_counts.push_back(
            static_cast<uint32_t>(partitions[i].dummy_num));
    }

    // Compute partition balance metrics
    {
        double sum = 0, sumsq = 0;
        uint32_t mn = UINT32_MAX, mx = 0;
        for (auto c : diag.partition_real_counts) {
            sum += c; sumsq += (double)c * c;
            mn = std::min(mn, c); mx = std::max(mx, c);
        }
        double n = diag.partition_real_counts.size();
        double mean = sum / n;
        double var = sumsq / n - mean * mean;
        diag.partition_size_cv = (mean > 0) ? (std::sqrt(std::max(var, 0.0)) / mean) : 0.0;
        diag.partition_max_min = (mn > 0) ? ((double)mx / mn) : 0.0;
    }

    std::cout << "  [Repartition] FixedThreshold: eps=" << total_epsilon
              << " hint=" << num_servers_hint
              << " → " << partitions.size() << " actual partitions"
              << "  (CV=" << std::fixed << std::setprecision(3) << diag.partition_size_cv
              << " max/min=" << std::setprecision(1) << diag.partition_max_min << "x)\n";
    std::cout << "    Sizes: ";
    for (size_t i = 0; i < partitions.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << partitions[i].synopsis << "r+" << partitions[i].dummy_num << "d";
    }
    std::cout << "\n";

    // ================================================================
    // Step 1c-d: ServerInfo + ORAM init
    // ================================================================
    int base_port = 8881;
    std::vector<ServerInfo> servers(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        servers[i].server_id    = static_cast<int>(i);
        servers[i].port         = base_port + static_cast<int>(i);
        servers[i].pid          = 0;
        servers[i].partition_id = i;
        servers[i].assigned_bins = partitions[i].index;
    }

    MultiRingORAM_Servers multi_oram;
    multi_oram.distributeDataToPartitions(bin_info, partitions, servers);

    uint32_t tuple_width = tupleWidthBytesFromSchema(schema);
    uint32_t block_size  = tuple_width + AES::BLOCKSIZE + 2 * sizeof(uint32_t);
    uint32_t bucket_size = 8;
    uint32_t S = 4;

    // Fork one process per server; children init their RingORAMs in parallel.
    auto t2 = clk::now();
    for (size_t i = 0; i < partitions.size(); ++i) {
        multi_oram.ServerInitialization(
            servers[i], partitions[i], bin_info, data, schema,
            bucket_size,
            "RingORAM_S" + std::to_string(i),
            block_size, "127.0.0.1", S);
    }
    multi_oram.waitForAllServersReady();
    auto t3 = clk::now();
    diag.oram_init_ms = ms_between(t2, t3);

    std::vector<uint32_t> blocks_per_server(servers.size());
    for (size_t i = 0; i < servers.size(); ++i)
        blocks_per_server[i] = static_cast<uint32_t>(servers[i].assigned_data_indices.size());

    diag.server_diags.resize(servers.size());
    for (size_t i = 0; i < servers.size(); ++i) {
        diag.server_diags[i].server_id = servers[i].server_id;
        diag.server_diags[i].oram_tree_size = blocks_per_server[i];
    }

    // ================================================================
    // Step 2: Epoch collection
    // ================================================================
    // (collect_epoch() opens, drains and closes the intake itself)

    QueriesReceiving qr;
    std::vector<std::thread> client_threads;
    auto t4 = clk::now();

    for (int c = 0; c < NUM_CLIENTS; c++) {
        client_threads.emplace_back([&, c]() {
            std::mt19937 rng(c * 7777 + 42);
            for (int t_idx = 0; t_idx < TXNS_PER_CLIENT; t_idx++) {
                txn t;
                int nops = 2 + (rng() % 3);
                for (int q = 0; q < nops; q++) {
                    int sid = rng() % servers.size();
                    // Partition-local BLOCK ID in [0, blocks_per_server[sid]).
                    // Correct for this path: these benchmarks dispatch with
                    // sendBatchToServer(), which sends data_primary_key straight
                    // to the child as the block id — the pos_map translation
                    // (ObjectKey → block_id) happens only inside
                    // executeOramOps().  Feeding a global data index here would
                    // address blocks that do not exist on that shard.
                    uint32_t block_id = rng() % blocks_per_server[sid];
                    OpType type = (rng() % 2 == 0) ? OpType::READ : OpType::UPDATE;
                    std::string val = "";
                    if (type == OpType::UPDATE)
                        val = "c" + std::to_string(c) + "_upd_" + std::to_string(block_id);
                    t.operations.push_back(
                        make_op(type, block_id, val, sid, (q == nops - 1)));
                }
                push_txn_to_queue(qr, t);
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }

    auto epoch_txns = qr.collect_epoch(EPOCH_MS);
    for (auto& t : client_threads) t.join();

    auto t5 = clk::now();
    diag.epoch_collect_ms = ms_between(t4, t5);
    diag.total_txns = static_cast<uint32_t>(epoch_txns.size());

    // ================================================================
    // Step 3: MVCC
    // ================================================================
    auto t6 = clk::now();
    qr.AssignAbort(epoch_txns);
    auto t7 = clk::now();
    diag.mvcc_ms = ms_between(t6, t7);

    diag.committed_txns = 0;
    diag.aborted_txns = 0;
    for (auto& tx : epoch_txns) {
        if (tx.is_committed) diag.committed_txns++;
        else diag.aborted_txns++;
    }

    // ================================================================
    // Step 4: Batch building
    // ================================================================
    auto t8 = clk::now();

    const auto& vcs = qr.getVersionChains();
    std::map<int, BatchInfo> read_batches;
    std::map<int, BatchInfo> update_batches;

    for (auto& s : servers) {
        read_batches[s.server_id].server_id   = s.server_id;
        read_batches[s.server_id].batch_type   = OpType::READ;
        update_batches[s.server_id].server_id  = s.server_id;
        update_batches[s.server_id].batch_type = OpType::UPDATE;
    }

    uint32_t total_read_ops = 0, total_update_ops = 0;
    // ── Note (4/27/26): vcs key is std::pair<int, uint32_t>(server_id, pk),
    // not just uint32_t.  We don't actually need either component here —
    // the loop only routes by op.server_id — so we use _vc_key as a
    // placeholder name to avoid confusion.
    for (auto& [_vc_key, ops] : vcs) {
        (void)_vc_key;
        for (auto& op : ops) {
            if (op.type == OpType::READ) {
                read_batches[op.server_id].operations.push_back(op);
                total_read_ops++;
            } else if (op.type == OpType::UPDATE) {
                update_batches[op.server_id].operations.push_back(op);
                total_update_ops++;
            }
        }
    }

    auto t9 = clk::now();
    diag.batch_build_ms = ms_between(t8, t9);
    diag.committed_ops = total_read_ops + total_update_ops;

    for (size_t i = 0; i < servers.size(); ++i) {
        int sid = servers[i].server_id;
        diag.server_diags[i].read_ops =
            static_cast<uint32_t>(read_batches[sid].operations.size());
        diag.server_diags[i].update_ops =
            static_cast<uint32_t>(update_batches[sid].operations.size());
    }

    std::cout << "  [Ops] " << total_read_ops << " R + " << total_update_ops
              << " W = " << diag.committed_ops << " committed"
              << "  (txns: " << diag.committed_txns << "/" << diag.total_txns << ")\n";

    // ================================================================
    // Step 5: Batch execution — THROUGHPUT MEASUREMENT
    //   Clock starts: proxy sends batches to untrusted servers
    //   Clock ends:   all servers return results to proxy
    // ================================================================

    auto batch_clock_start = clk::now();

    // Phase A: READs
    auto read_phase_start = clk::now();
    {
        std::vector<double> per_server_read_ms(servers.size(), 0.0);
        std::vector<TransmissionResult> read_results(servers.size());
        std::vector<std::thread> read_threads;
        for (size_t i = 0; i < servers.size(); ++i) {
            read_threads.emplace_back([&, i]() {
                auto s_start = clk::now();
                if (!read_batches[servers[i].server_id].operations.empty()) {
                    read_results[i] = multi_oram.sendBatchToServer(
                        servers[i], read_batches[servers[i].server_id]);
                } else {
                    read_results[i].server_id = servers[i].server_id;
                    read_results[i].success = true;
                    read_results[i].records_sent = 0;
                    read_results[i].elapsed_time_ms = 0;
                }
                auto s_end = clk::now();
                per_server_read_ms[i] = ms_between(s_start, s_end);
            });
        }
        for (auto& t : read_threads) t.join();
        for (auto& r : read_results) assert(r.success && "READ batch failed");
        for (size_t i = 0; i < servers.size(); ++i)
            diag.server_diags[i].read_phase_ms = per_server_read_ms[i];
    }
    auto read_phase_end = clk::now();
    diag.read_phase_wall_ms = ms_between(read_phase_start, read_phase_end);

    // Phase B: UPDATEs
    auto update_phase_start = clk::now();
    {
        std::vector<double> per_server_update_ms(servers.size(), 0.0);
        std::vector<TransmissionResult> update_results(servers.size());
        std::vector<std::thread> update_threads;
        for (size_t i = 0; i < servers.size(); ++i) {
            update_threads.emplace_back([&, i]() {
                auto s_start = clk::now();
                if (!update_batches[servers[i].server_id].operations.empty()) {
                    update_results[i] = multi_oram.sendBatchToServer(
                        servers[i], update_batches[servers[i].server_id]);
                } else {
                    update_results[i].server_id = servers[i].server_id;
                    update_results[i].success = true;
                    update_results[i].records_sent = 0;
                    update_results[i].elapsed_time_ms = 0;
                }
                auto s_end = clk::now();
                per_server_update_ms[i] = ms_between(s_start, s_end);
            });
        }
        for (auto& t : update_threads) t.join();
        for (auto& r : update_results) assert(r.success && "UPDATE batch failed");
        for (size_t i = 0; i < servers.size(); ++i)
            diag.server_diags[i].update_phase_ms = per_server_update_ms[i];
    }
    auto update_phase_end = clk::now();
    diag.update_phase_wall_ms = ms_between(update_phase_start, update_phase_end);

    auto batch_clock_end = clk::now();
    diag.batch_exec_ms = ms_between(batch_clock_start, batch_clock_end);

    // Throughput uses sum of phase wall times (pure server execution, no proxy gap)
    diag.phases_sum_ms = diag.read_phase_wall_ms + diag.update_phase_wall_ms;
    diag.throughput_ops_sec = (diag.phases_sum_ms > 0)
        ? (diag.committed_ops * 1000.0 / diag.phases_sum_ms) : 0.0;

    double gap_ms = diag.batch_exec_ms - diag.phases_sum_ms;
    std::cout << "  [Throughput] R=" << std::fixed << std::setprecision(2)
              << diag.read_phase_wall_ms << " + W="
              << diag.update_phase_wall_ms << " = "
              << diag.phases_sum_ms << " ms (gap=" << gap_ms << " ms)"
              << " → " << std::setprecision(1) << diag.throughput_ops_sec << " ops/sec\n";

    // ---- Derived metrics ----
    for (auto& sd : diag.server_diags) {
        sd.total_ms = sd.read_phase_ms + sd.update_phase_ms;
        sd.read_per_op_ms = (sd.read_ops > 0) ? (sd.read_phase_ms / sd.read_ops) : 0.0;
        sd.update_per_op_ms = (sd.update_ops > 0) ? (sd.update_phase_ms / sd.update_ops) : 0.0;
    }

    auto compute_straggler = [](const std::vector<ServerDiag>& sds,
                                 auto get_ops, auto get_ms,
                                 double& out_max, double& out_min, double& out_ratio) {
        std::vector<double> times;
        for (auto& sd : sds)
            if (get_ops(sd) > 0) times.push_back(get_ms(sd));
        if (!times.empty()) {
            out_max = *std::max_element(times.begin(), times.end());
            out_min = *std::min_element(times.begin(), times.end());
            out_ratio = (out_min > 0) ? (out_max / out_min) : 0.0;
        } else {
            out_max = out_min = out_ratio = 0;
        }
    };
    compute_straggler(diag.server_diags,
        [](const ServerDiag& s){ return s.read_ops; },
        [](const ServerDiag& s){ return s.read_phase_ms; },
        diag.read_max_ms, diag.read_min_ms, diag.read_straggler_ratio);
    compute_straggler(diag.server_diags,
        [](const ServerDiag& s){ return s.update_ops; },
        [](const ServerDiag& s){ return s.update_phase_ms; },
        diag.update_max_ms, diag.update_min_ms, diag.update_straggler_ratio);

    // Per-server table
    std::cout << "  Per-Server:\n";
    std::cout << "    " << std::left
              << std::setw(5) << "Srv"  << std::setw(8) << "Tree"
              << std::setw(6) << "R_op" << std::setw(9) << "R_ms"
              << std::setw(9) << "R/op" << std::setw(6) << "W_op"
              << std::setw(9) << "W_ms" << std::setw(9) << "W/op"
              << std::setw(9) << "Total" << "\n";
    std::cout << "    " << std::string(70, '-') << "\n";
    for (auto& sd : diag.server_diags) {
        std::cout << "    "
                  << std::setw(5) << sd.server_id
                  << std::setw(8) << sd.oram_tree_size
                  << std::setw(6) << sd.read_ops
                  << std::setw(9) << std::setprecision(2) << sd.read_phase_ms
                  << std::setw(9) << std::setprecision(3) << sd.read_per_op_ms
                  << std::setw(6) << sd.update_ops
                  << std::setw(9) << std::setprecision(2) << sd.update_phase_ms
                  << std::setw(9) << std::setprecision(3) << sd.update_per_op_ms
                  << std::setw(9) << std::setprecision(2) << sd.total_ms << "\n";
    }

    double max_straggler = std::max(diag.read_straggler_ratio, diag.update_straggler_ratio);
    std::cout << "  Straggler: R=" << std::setprecision(2) << diag.read_straggler_ratio
              << "x  W=" << diag.update_straggler_ratio << "x";
    if (max_straggler > 2.0) std::cout << "  ⚠️ LOAD_IMBAL";
    std::cout << "\n";

    // Cleanup
    multi_oram.shutdownAllServers();
    return diag;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  Test 2: Fixed-Threshold (Algorithm 2)                     ║\n";
    std::cout << "║  Goal: Better partition balance → closer to k× scaling     ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n\n";

    // ========================================================================
    // Configuration
    // ========================================================================
    std::vector<double>   epsilon_values    = {1.0, 5.0, 10.0, 15.0, 50.0};
    std::vector<uint32_t> server_hints      = {1, 2, 3, 4, 5};

    double epsilon_1_ratio  = 0.7;
    double epsilon_2_ratio  = 0.3;
    int    primary_key_col  = 0;
    int    NUM_CLIENTS      = 4;
    int    TXNS_PER_CLIENT  = 5;
    int    EPOCH_MS         = 500;

    // 500-record dataset
    std::string csv_path_in = departDataPath("Testing/Unit Tests/test_data_500.csv");

    std::string schema = "customer_id:int32,region:int32,order_amount:int32,timestamp:int32";
    std::vector<int> attr = {0};  // partition by customer_id (500 bins of 1 → fine-grained)

    // ========================================================================
    // Load data
    // ========================================================================
    std::vector<std::string> data = readCSV(csv_path_in);
    std::cout << "Loaded " << data.size() << " records from: " << csv_path_in << "\n\n";

    // ========================================================================
    // PRE-SCAN: Determine how many servers are needed
    //   Run repartition (dry run) for each config to find max partition count
    // ========================================================================
    print_separator("Pre-Scan: Determining Max Server Count Needed");

    uint32_t max_partitions = 0;
    for (double eps : epsilon_values) {
        for (uint32_t ns : server_hints) {
            Repartition repartitioner;
            auto partitions = repartitioner.Repartition_Main(
                data, schema, attr,
                epsilon_1_ratio, epsilon_2_ratio, eps,
                false, ns, primary_key_col
            );
            uint32_t np = static_cast<uint32_t>(partitions.size());
            max_partitions = std::max(max_partitions, np);

            std::cout << "  eps=" << std::fixed << std::setprecision(1) << eps
                      << " hint=" << ns
                      << " → " << np << " partitions: ";
            for (size_t i = 0; i < partitions.size(); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << partitions[i].synopsis;
            }
            std::cout << "\n";
        }
    }

    std::cout << "\n  ╔══════════════════════════════════════════════════════╗\n";
    std::cout << "  ║  MAX SERVERS NEEDED: " << std::setw(3) << max_partitions
              << "                             ║\n";
    std::cout << "  ║  Start servers on ports 8881-"
              << (8880 + max_partitions) << "                   ║\n";
    std::cout << "  ╚══════════════════════════════════════════════════════╝\n\n";

    std::cout << "  Press ENTER when all " << max_partitions
              << " servers are running...";
    std::cin.get();

    // ========================================================================
    // Run tests
    // ========================================================================
    std::vector<DiagResult> all_results;

    for (double eps : epsilon_values) {
        for (uint32_t ns : server_hints) {
            print_separator("FixedThreshold: eps=" + std::to_string(eps)
                          + "  hint_servers=" + std::to_string(ns));
            try {
                auto result = run_fixed_threshold_test(
                    data, schema, attr,
                    epsilon_1_ratio, epsilon_2_ratio, eps,
                    ns, primary_key_col,
                    NUM_CLIENTS, TXNS_PER_CLIENT, EPOCH_MS);
                all_results.push_back(result);
                std::cout << "  ✅ PASSED\n";
            } catch (const std::exception& e) {
                std::cerr << "  ❌ FAILED: " << e.what() << "\n";
            }
        }
    }

    // ========================================================================
    // Write CSV
    // ========================================================================
    std::string csv_out = "fixed_threshold_results.csv";
    std::ofstream csv(csv_out);
    if (!csv.is_open()) {
        std::cerr << "Cannot open: " << csv_out << "\n";
        return 1;
    }

    csv << "post_processing,epsilon,hint_servers,actual_partitions,"
        << "partition_real_counts,partition_dummy_counts,"
        << "partition_balance_cv,partition_max_min,"
        << "committed_ops,phases_sum_ms,batch_exec_ms,throughput_ops_sec,"
        << "read_phase_wall_ms,update_phase_wall_ms,"
        << "read_straggler_ratio,update_straggler_ratio,"
        << "per_server_tree_sizes,"
        << "per_server_read_ms_per_op,per_server_update_ms_per_op\n";

    for (auto& d : all_results) {
        csv << std::fixed << d.post_processing << ","
            << std::setprecision(1) << d.epsilon << ","
            << d.num_servers_hint << "," << d.num_partitions << ",\"";
        for (size_t i = 0; i < d.partition_real_counts.size(); ++i) {
            if (i > 0) csv << ";";
            csv << d.partition_real_counts[i];
        }
        csv << "\",\"";
        for (size_t i = 0; i < d.partition_dummy_counts.size(); ++i) {
            if (i > 0) csv << ";";
            csv << d.partition_dummy_counts[i];
        }
        csv << "\","
            << std::setprecision(3) << d.partition_size_cv << ","
            << std::setprecision(2) << d.partition_max_min << ","
            << d.committed_ops << ","
            << std::setprecision(2) << d.phases_sum_ms << ","
            << std::setprecision(2) << d.batch_exec_ms << ","
            << std::setprecision(1) << d.throughput_ops_sec << ","
            << std::setprecision(2) << d.read_phase_wall_ms << ","
            << d.update_phase_wall_ms << ","
            << d.read_straggler_ratio << ","
            << d.update_straggler_ratio << ",\"";
        for (size_t i = 0; i < d.server_diags.size(); ++i) {
            if (i > 0) csv << ";";
            csv << d.server_diags[i].oram_tree_size;
        }
        csv << "\",\"";
        for (size_t i = 0; i < d.server_diags.size(); ++i) {
            if (i > 0) csv << ";";
            csv << std::setprecision(3) << d.server_diags[i].read_per_op_ms;
        }
        csv << "\",\"";
        for (size_t i = 0; i < d.server_diags.size(); ++i) {
            if (i > 0) csv << ";";
            csv << std::setprecision(3) << d.server_diags[i].update_per_op_ms;
        }
        csv << "\"\n";
    }
    csv.close();

    // ========================================================================
    // Scaling Analysis: compare to theoretical k× linear
    // ========================================================================
    print_separator("Scaling Analysis: FixedThreshold");

    std::cout << "  Theoretical: throughput(k partitions) ≈ k × throughput(1 partition)\n\n";

    for (double eps : epsilon_values) {
        std::cout << "  ── Epsilon = " << std::fixed << std::setprecision(1) << eps << " ──\n\n";

        // Find single-partition baseline
        double baseline_tput = 0;
        for (auto& d : all_results) {
            if (std::abs(d.epsilon - eps) < 0.01 && d.num_partitions == 1) {
                baseline_tput = d.throughput_ops_sec;
                break;
            }
        }

        std::cout << "  " << std::left
                  << std::setw(6) << "Hint"
                  << std::setw(6) << "Part"
                  << std::setw(26) << "Partition Sizes"
                  << std::setw(8) << "CV"
                  << std::setw(6) << "Ops"
                  << std::setw(9) << "R_ms"
                  << std::setw(9) << "W_ms"
                  << std::setw(10) << "Sum_ms"
                  << std::setw(10) << "Tput"
                  << std::setw(8) << "Ideal"
                  << std::setw(8) << "Actual"
                  << std::setw(10) << "Effic%"
                  << "\n";
        std::cout << "  " << std::string(106, '-') << "\n";

        for (auto& d : all_results) {
            if (std::abs(d.epsilon - eps) > 0.01) continue;

            std::ostringstream sizes;
            for (size_t i = 0; i < d.partition_real_counts.size(); ++i) {
                if (i > 0) sizes << ",";
                sizes << d.partition_real_counts[i];
            }

            double ideal_x = static_cast<double>(d.num_partitions);
            double actual_x = (baseline_tput > 0) ? (d.throughput_ops_sec / baseline_tput) : 0.0;
            double efficiency = (ideal_x > 0) ? (actual_x / ideal_x * 100.0) : 0.0;

            std::cout << "  "
                      << std::setw(6) << d.num_servers_hint
                      << std::setw(6) << d.num_partitions
                      << std::setw(26) << sizes.str()
                      << std::setw(8) << std::setprecision(3) << d.partition_size_cv
                      << std::setw(6) << d.committed_ops
                      << std::setw(9) << std::setprecision(2) << d.read_phase_wall_ms
                      << std::setw(9) << std::setprecision(2) << d.update_phase_wall_ms
                      << std::setw(10) << std::setprecision(2) << d.phases_sum_ms
                      << std::setw(10) << std::setprecision(1) << d.throughput_ops_sec
                      << std::setw(7) << std::setprecision(1) << ideal_x << "x "
                      << std::setw(7) << std::setprecision(2) << actual_x << "x "
                      << std::setw(9) << std::setprecision(1) << efficiency << "%"
                      << "\n";
        }
        std::cout << "\n";
    }

    // ========================================================================
    // Compare balance: FixedThreshold vs FixedPartitionCount (from Test 1)
    // ========================================================================
    print_separator("Balance Comparison (FixedThreshold results)");

    std::cout << "  Binning by customer_id: 500 bins of 1 record each\n";
    std::cout << "  → Algorithm can freely group bins into k balanced partitions\n\n";

    std::cout << "  FixedThreshold (this test):\n";
    // Group by actual partition count and show CV
    std::set<uint32_t> seen_parts;
    for (auto& d : all_results) {
        if (seen_parts.count(d.num_partitions)) continue;
        seen_parts.insert(d.num_partitions);

        std::ostringstream sizes;
        for (size_t i = 0; i < d.partition_real_counts.size(); ++i) {
            if (i > 0) sizes << ",";
            sizes << d.partition_real_counts[i];
        }
        std::cout << "    " << d.num_partitions << " part → ["
                  << sizes.str() << "]"
                  << "  CV=" << std::setprecision(3) << d.partition_size_cv
                  << "  max/min=" << std::setprecision(1) << d.partition_max_min << "x\n";
    }

    std::cout << "\n  CSV: " << csv_out << "\n";
    std::cout << "\n✅ Fixed-Threshold scaling test complete.\n\n";
    return 0;
}