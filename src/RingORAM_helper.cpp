#include "RingORAM.h"
#include "RingORAM_helper.h"
#include "NetIOConnector.h"
#include "csv_reader.h"
#include "generate_data.h"
#include "Config.h"
#include "TpccSchema.h"
#include <cryptopp/osrng.h>
#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <ctime>

using namespace std;
using namespace CryptoPP;

void test_ringoram(uint32_t N, std::vector<std::string> &tuple_entries, uint32_t tuple_width_bytes, RingORAM* oram, size_t roundNum) {
    // Put phase
    for (uint32_t i = 0; i < N; ++i) {
        std::cout << "[TEST] Inserting block " << i << std::endl;
        std::string value = tuple_entries[i];
        value.resize(tuple_width_bytes);
        int32_t blockID = i;
        std::string bID = std::string((const char *)(&blockID), sizeof(uint32_t));
        value = bID + value;
        oram -> access(i, OpType::INSERT, value);
        std::cout << "================== Has already put " << i << " in the database =======================" << std::endl;
    }

    // Verify get correctness
    for (int i = 0; i < 3; ++i) {
        int key = rand() % N;
        std::cout << "[TEST] Getting block " << key << std::endl;
        std::string block = oram -> get(key);
        std::cout << "The Block is " << std::endl;
        std::cout << block << ", and the size of the block is " << block.size() << std::endl;
        std::string expected = tuple_entries[key];
        expected.resize(tuple_width_bytes);
        int32_t blockID = key;
        std::string bID = std::string((const char *)(&blockID), sizeof(uint32_t));
        expected = bID + expected;
        std::cout << "The expected is " << std::endl;
        std::cout << expected  << ", and the size of the expected block is "
        << expected.size() << std::endl;
        for (size_t i = 0; i < expected.size(); ++i) {
            if (expected[i] != block[i]) {
                std::cout << "Mismatch at index " << i << ": "
                          << "expected[" << i << "] = " << (int)expected[i]
                          << ", block[" << i << "] = " << (int)block[i] << std::endl;
            }
            assert(expected[i] == block[i]);
        }
        //assert(expected == block);
        std::cout << "================== The expected value is equal to the block. ======================" << std::endl;
    }

    for (size_t r = 0; r < roundNum; r++) {
        for (size_t i = 0; i < N; i++) {
            std::string block = oram -> get(i);
            block.resize(block.size());
            std::cout << "The Block is " << std::endl;
            std::cout << block << ", and the size of the block is " << block.length() << std::endl;
            std::string expected = tuple_entries[i];
            expected.resize(tuple_width_bytes);
            int32_t blockID = i;
            std::string bID = std::string((const char *)(&blockID), sizeof(uint32_t));
            expected = bID + expected;
            std::cout << "The expected is " << std::endl;
            std::cout << expected  << std::endl;
            std::cout << ", and the size of the expected block is "
                      << expected.length() << std::endl;
            for (size_t i = 0; i < expected.size(); ++i) {
                if (expected[i] != block[i]) {
                    std::cout << "Mismatch at index " << i << ": "
                              << "expected[" << i << "] = " << (int)expected[i]
                              << ", block[" << i << "] = " << (int)block[i] << std::endl;
                }
                assert(expected[i] == block[i]);
            }
            //assert(expected == block);
        }
        printf("\n=================== ROUND %zu PASSED ===================\n", r + 1);
    }
}

void testTable(string fname, string schema, NetIOConnector *conn) {
    cout << "Testing table: " << fname << endl;
    std::cout << "The schema of the table is " << schema << std::endl;
    std::vector<std::string> tuple_entries = readCSVFileWithSchema(fname, schema);
    std::cout << "I can successfully find the table." << std::endl;
    uint32_t N = tuple_entries.size();
    uint32_t tuple_width_bytes = tupleWidthBytesFromSchema(schema);
    uint32_t block_size = tuple_width_bytes + AES::BLOCKSIZE + 2 * sizeof(uint32_t);

    uint32_t S = 4;
    std::cout << "I can run here successfully." << std::endl;
    RingORAM* oram = new RingORAM(N, 8, "RingORAM", block_size, conn, S);
    std::cout << "Construct the Ring ORAM." << std::endl;
    test_ringoram(N, tuple_entries, tuple_width_bytes, oram, 1);
    std::cout << "[testTable] Testing table with N = " << N << ", tuple width = " << tuple_width_bytes << std::endl;
    delete oram;
    cout << "Passed test on " << fname << "! :)" << endl;
}