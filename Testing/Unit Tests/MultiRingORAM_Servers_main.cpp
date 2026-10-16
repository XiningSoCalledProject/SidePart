//
// Created by Xining Yuan on 2/23/26.
// Test: MultiRingORAM — parallel servers, serial ops within each batch
//
// Architecture:
//   Server A, B, C: initialized in PARALLEL (one thread each)
//   Batch sending across servers: PARALLEL (one thread each)
//   Ops within one batch: SERIAL (op[0] → op[1] → op[2] → ...)
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

#include "MultiRingORAM_Servers.h"
#include "DataPath.h"

// ============================================
// CSV Reader (raw lines, skip header)
// ============================================
static std::vector<std::string> readCSV(const std::string& filename) {
    std::vector<std::string> data;
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }
    std::string line;
    std::getline(file, line); // skip header
    while (std::getline(file, line)) {
        if (!line.empty()) data.push_back(line);
    }
    return data;
}

// ============================================
// Main Test
// ============================================
int main() {
    std::cout << "\n============================================================\n";
    std::cout << "  MultiRingORAM Parallel Server Test\n";
    std::cout << "============================================================\n\n";

    try {
        // ============================================================
        // Step 1: Load CSV data & Repartition
        // ============================================================
        std::cout << "[Step 1] Load data & Repartition\n";
        std::cout << std::string(60, '-') << "\n";

        std::string csv_path = departDataPath("Testing/Unit Tests/test_data.csv");
        std::vector<std::string> data = readCSV(csv_path);
        std::cout << "  Loaded " << data.size() << " records\n";

        Repartition repartitioner;
        std::string schema = "customer_id:int32,region:int32,order_amount:int32,timestamp:int32";
        std::vector<int> attr = {1};  // partition by region (column 1)
        uint32_t num_servers = 3;

        auto partitions = repartitioner.Repartition_Main(
            data, schema, attr,
            0.7,          // epsilon_max
            0.3,          // epsilon_threshold (unused for Algorithm 3)
            15.0,         // threshold_pub (unused for Algorithm 3)
            true,         // fixed_number_part → Algorithm 3
            num_servers,  // k = 3 servers
            0             // primary_key_col = 0 (customer_id)
        );

        const BinInfo& bin_info = repartitioner.getBinInfo();

        std::cout << "  Created " << partitions.size() << " partitions:\n";
        for (size_t i = 0; i < partitions.size(); ++i) {
            const auto& p = partitions[i];
            std::cout << "    Partition " << i << ": bins=[";
            for (size_t j = 0; j < p.index.size(); ++j) {
                if (j > 0) std::cout << ",";
                std::cout << p.index[j];
            }
            std::cout << "] real=" << p.synopsis
                      << " noisy=" << p.noisy_synopsis
                      << " dummy=" << p.dummy_num << "\n";
        }

        // ============================================================
        // Step 2: Create ServerInfo & build all mappings
        // ============================================================
        std::cout << "\n[Step 2] Create servers & build mappings\n";
        std::cout << std::string(60, '-') << "\n";

        int base_port = 8881;
        std::vector<ServerInfo> servers(partitions.size());
        for (size_t i = 0; i < partitions.size(); ++i) {
            servers[i].server_id = static_cast<int>(i);
            servers[i].port = base_port + static_cast<int>(i);
            servers[i].pid = 0;
            servers[i].partition_id = i;
            servers[i].assigned_bins = partitions[i].index;
        }

        MultiRingORAM_Servers multi_oram;
        multi_oram.distributeDataToPartitions(bin_info, partitions, servers);

        // Print server assignments
        for (const auto& s : servers) {
            std::cout << "  Server " << s.server_id
                      << " (partition " << s.partition_id << "): "
                      << s.assigned_data_indices.size() << " records, bins=[";
            for (size_t j = 0; j < s.assigned_bins.size(); ++j) {
                if (j > 0) std::cout << ",";
                std::cout << s.assigned_bins[j];
            }
            std::cout << "]\n";
        }

        // Sanity check: every data row is assigned to exactly one server
        uint32_t total_assigned = 0;
        for (const auto& s : servers) total_assigned += s.assigned_data_indices.size();
        std::cout << "  Total assigned: " << total_assigned << " / " << data.size() << "\n";
        assert(total_assigned == data.size());

        // ============================================================
        // Step 3: PARALLEL server initialization
        //   One thread per server: create RingORAM, insert real + dummy data
        // ============================================================
        std::cout << "\n[Step 3] Parallel server initialization\n";
        std::cout << std::string(60, '-') << "\n";

        // RingORAM parameters
        uint32_t tuple_width_bytes = tupleWidthBytesFromSchema(schema);
        uint32_t block_size = tuple_width_bytes + AES::BLOCKSIZE + 2 * sizeof(uint32_t);
        uint32_t bucket_size = 8;
        uint32_t S = 4;

        // Fork one process per server; all children init in parallel.
        auto init_start = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < partitions.size(); ++i) {
            std::string oram_name = "RingORAM_S" + std::to_string(servers[i].server_id);
            multi_oram.ServerInitialization(
                servers[i], partitions[i], bin_info, data, schema,
                bucket_size, oram_name, block_size, "127.0.0.1", S
            );
        }
        // Block until every child process signals it is ready.
        multi_oram.waitForAllServersReady();

        auto init_end = std::chrono::high_resolution_clock::now();
        double init_ms = std::chrono::duration<double, std::milli>(init_end - init_start).count();
        std::cout << "  All " << partitions.size() << " server processes initialized in "
                  << std::fixed << std::setprecision(2) << init_ms << " ms (parallel)\n";

        // ============================================================
        // Step 4: Build READ batches (one batch per server)
        //   Each batch reads back all records that were inserted
        // ============================================================
        std::cout << "\n[Step 4] Build READ batches\n";
        std::cout << std::string(60, '-') << "\n";

        std::vector<BatchInfo> read_batches(servers.size());
        for (size_t i = 0; i < servers.size(); ++i) {
            read_batches[i].server_id = servers[i].server_id;
            read_batches[i].batch_type = OpType::READ;

            // Records were inserted with block_id = 0, 1, 2, ... within each server
            for (uint32_t block_id = 0; block_id < servers[i].assigned_data_indices.size(); ++block_id) {
                Operat op;
                op.type = OpType::READ;
                op.data_primary_key = block_id;  // block_id in this server's ORAM
                op.data_value = "";               // empty for reads
                op.server_id = servers[i].server_id;
                read_batches[i].operations.push_back(op);
            }

            std::cout << "  Server " << servers[i].server_id
                      << ": " << read_batches[i].operations.size() << " READ ops\n";
        }

        // ============================================================
        // Step 5: PARALLEL batch execution
        //   - Different servers run in PARALLEL (one thread per server)
        //   - Within each thread, ops execute SERIALLY (op[0] → op[1] → ...)
        // ============================================================
        std::cout << "\n[Step 5] Parallel batch send (serial ops within each batch)\n";
        std::cout << std::string(60, '-') << "\n";

        std::vector<TransmissionResult> results(servers.size());

        auto batch_start = std::chrono::high_resolution_clock::now();

        // *** PARALLEL: one thread per server ***
        std::vector<std::thread> batch_threads;
        for (size_t i = 0; i < servers.size(); ++i) {
            batch_threads.emplace_back([&, i]() {
                // sendBatchToServer loops through ops SERIALLY inside
                results[i] = multi_oram.sendBatchToServer(servers[i], read_batches[i]);
            });
        }

        // Wait for ALL batches to complete
        for (auto& t : batch_threads) t.join();

        auto batch_end = std::chrono::high_resolution_clock::now();
        double batch_ms = std::chrono::duration<double, std::milli>(batch_end - batch_start).count();

        // ============================================================
        // Step 6: Print results
        // ============================================================
        std::cout << "\n[Step 6] Results\n";
        std::cout << std::string(60, '-') << "\n";

        size_t total_ops = 0;
        bool all_ok = true;
        for (const auto& r : results) {
            std::cout << "  Server " << r.server_id << ": ";
            if (r.success) {
                std::cout << r.records_sent << " ops in "
                          << std::fixed << std::setprecision(2)
                          << r.elapsed_time_ms << " ms\n";
                total_ops += r.records_sent;
            } else {
                std::cout << "FAILED - " << r.error_message << "\n";
                all_ok = false;
            }
        }

        std::cout << "\n  Total ops: " << total_ops << "\n";
        std::cout << "  Parallel batch time: "
                  << std::fixed << std::setprecision(2) << batch_ms << " ms\n";
        if (batch_ms > 0) {
            std::cout << "  Throughput: "
                      << std::fixed << std::setprecision(0)
                      << (total_ops * 1000.0 / batch_ms) << " ops/sec\n";
        }

        // ============================================================
        // Step 7: End-to-end mapping verification
        //   primary_key → data_index → bin_key → server_id
        // ============================================================
        std::cout << "\n[Step 7] Mapping verification\n";
        std::cout << std::string(60, '-') << "\n";

        // Sample: one from each region
        std::vector<std::string> sample_pks = {
            "1001",  // region 1
            "1003",  // region 2
            "1005",  // region 3
            "1041",  // region 4
            "1046"   // region 5
        };
        for (const auto& pk : sample_pks) {
            auto it = bin_info.primary_key_to_data_index.find(pk);
            if (it != bin_info.primary_key_to_data_index.end()) {
                uint32_t didx = it->second;
                const std::string& bk = bin_info.data_index_to_bin_key[didx];
                int sid = multi_oram.getServerIdForBinKey(bk);
                std::cout << "  PK=" << pk
                          << " -> data_idx=" << didx
                          << " -> bin=\"" << bk
                          << "\" -> server " << sid << "\n";
            } else {
                std::cout << "  PK=" << pk << " -> NOT FOUND\n";
            }
        }

        // ============================================================
        // Cleanup
        // ============================================================
        multi_oram.shutdownAllServers();

        std::cout << "\n";
        assert(all_ok);
        std::cout << "All tests PASSED.\n\n";
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "\nERROR: " << e.what() << std::endl;
        return 1;
    }
}