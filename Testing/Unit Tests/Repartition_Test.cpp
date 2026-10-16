#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <set>
#include <iomanip>
#include <algorithm>
#include <random>
#include <cmath>
#include <stdexcept>
#include "Repartition.h"
#include "DataPath.h"

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

void printPartitionResults(const std::vector<Partition>& partitions, const std::string& test_name) {
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "TEST: " << test_name << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    std::cout << "\n📊 Partition Results:" << std::endl;
    std::cout << "Total Partitions: " << partitions.size() << std::endl;

    uint32_t total_real = 0;
    uint32_t total_noisy = 0;
    uint32_t total_dummy = 0;

    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto& p = partitions[i];
        total_real += p.synopsis;
        total_noisy += p.noisy_synopsis;
        total_dummy += p.dummy_num;

        std::cout << "\n  Partition " << i << ":" << std::endl;
        std::cout << "    ├─ Bin keys: [";
        for (size_t j = 0; j < p.index.size(); ++j) {
            std::cout << "\"" << p.index[j] << "\"";  // Print string keys with quotes
            if (j < p.index.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
        std::cout << "    ├─ Real data (synopsis): " << p.synopsis << std::endl;
        std::cout << "    ├─ Noisy synopsis: " << p.noisy_synopsis << std::endl;
        std::cout << "    └─ Dummy count: " << p.dummy_num << std::endl;
    }

    std::cout << "\n📈 Summary:" << std::endl;
    std::cout << "  ├─ Total real: " << total_real << std::endl;
    std::cout << "  ├─ Total noisy: " << total_noisy << std::endl;
    std::cout << "  ├─ Total dummy: " << total_dummy << std::endl;
    std::cout << "  └─ Overhead: " << std::fixed << std::setprecision(2)
              << (total_real > 0 ? 100.0 * total_dummy / total_real : 0.0) << "%" << std::endl;
}

int main() {
    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║         Repartition Standalone Test                       ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n\n";

    try {
        std::cout << "📂 Loading test data..." << std::endl;
        std::vector<std::string> data = readCSV(departDataPath("Testing/Unit Tests/test_data.csv"));
        std::cout << "✅ Loaded " << data.size() << " records\n" << std::endl;

        Repartition repartitioner;

        std::string schema = "customer_id:uint32,region:uint32,order_amount:uint32,timestamp:uint32";
        std::vector<int> attr = {1};  // Partition by region (index 1)

        // ============================================
        // Test 1: Fixed Number of Partitions (Algorithm 3)
        // ============================================
        std::cout << "\n" << std::string(60, '-') << std::endl;
        std::cout << "🧪 Test 1: Fixed Number of Partitions (Algorithm 3)" << std::endl;
        std::cout << std::string(60, '-') << std::endl;

        auto parts1 = repartitioner.Repartition_Main(
            data, schema, attr,
            0.7,    // epsilon_max: privacy budget for adding noise to bins
            0.3,    // epsilon_threshold: not used in Algorithm 3
            15.0,   // threshold_pub: not used in Algorithm 3
            true,   // fixed_number_part = true → use Algorithm 3
            3,      // fixed_part_num = 3 (force exactly 3 partitions)
            0       // primary_key_col = 0 (customer_id)
        );
        printPartitionResults(parts1, "Algorithm 3: Fixed Number (k=3)");

        // Verify mapping: primary_key -> data_index -> bin_key -> partition
        const BinInfo& bi = repartitioner.getBinInfo();
        std::cout << "\n🔗 Mapping Verification (sample):\n";
        std::vector<std::string> sample_keys = {"1001", "1003", "1005"};
        for (const auto& pk : sample_keys) {
            auto it = bi.primary_key_to_data_index.find(pk);
            if (it != bi.primary_key_to_data_index.end()) {
                uint32_t didx = it->second;
                const std::string& bk = bi.data_index_to_bin_key[didx];
                // Find which partition this bin_key belongs to
                int part_id = -1;
                for (size_t p = 0; p < parts1.size(); ++p) {
                    for (const auto& key : parts1[p].index) {
                        if (key == bk) { part_id = p; break; }
                    }
                    if (part_id >= 0) break;
                }
                std::cout << "  PK=" << pk << " → data_idx=" << didx
                          << " → bin=\"" << bk << "\" → partition " << part_id << "\n";
            }
        }

        // ============================================
        // Test 2: Fixed Threshold (Algorithm 4)
        // ============================================
        std::cout << "\n" << std::string(60, '-') << std::endl;
        std::cout << "🧪 Test 2: Fixed Threshold (Algorithm 4)" << std::endl;
        std::cout << std::string(60, '-') << std::endl;

        auto parts2 = repartitioner.Repartition_Main(
            data, schema, attr,
            0.7,    // epsilon_max: privacy budget for adding noise to bins
            0.3,    // epsilon_threshold: privacy budget for noising the threshold
            20.0,   // threshold_pub: public threshold T (will be noised)
            false,  // fixed_number_part = false → use Algorithm 4
            0,      // fixed_part_num: not used in Algorithm 4
            0       // primary_key_col = 0 (customer_id)
        );
        printPartitionResults(parts2, "Algorithm 4: Fixed Threshold (T≈20)");

        // ============================================
        // Test 3: Different Parameters
        // ============================================
        std::cout << "\n" << std::string(60, '-') << std::endl;
        std::cout << "🧪 Test 3: Fixed Number with k=2" << std::endl;
        std::cout << std::string(60, '-') << std::endl;

        auto parts3 = repartitioner.Repartition_Main(
            data, schema, attr,
            1.0,    // epsilon_max: higher privacy budget (less noise)
            0.0,    // epsilon_threshold: not used
            0.0,    // threshold_pub: not used
            true,   // fixed_number_part = true
            2,      // fixed_part_num = 2 partitions
            0       // primary_key_col = 0 (customer_id)
        );
        printPartitionResults(parts3, "Algorithm 3: Fixed Number (k=2)");

        std::cout << "\n✅ All tests completed successfully!\n" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "\n❌ ERROR: " << e.what() << std::endl;
        return 1;
    }
}