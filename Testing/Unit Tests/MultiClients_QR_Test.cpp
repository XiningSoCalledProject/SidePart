//
// MultiClients_QR_Test.cpp
//
// Integration test: multiple client threads concurrently send transactions
// into the shared queue, then collect_epoch() + AssignAbort() processes them.
//
// Build (in CMake): target MultiClients_QR_Test links oram_core
// Run:  ./bin/MultiClients_QR_Test
//

#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <random>
#include <chrono>
#include <cassert>
#include <atomic>
#include <iomanip>
#include "Queries_Receiving.h"

// ============================================================================
// Access the shared queue globals defined in Queries_Receiving.cpp
// (These are what real TCP client handlers would push into)
// ============================================================================

// ============================================================================
// Helpers
// ============================================================================

std::string op_type_str(OpType t) {
    switch (t) {
        case OpType::READ:   return "READ";
        case OpType::UPDATE: return "WRITE";
        case OpType::INSERT: return "INSERT";
        default:             return "???";
    }
}

Operat make_op(OpType type, uint32_t key, const std::string& val,
               int server_id = 0, bool last = false) {
    Operat op;
    op.type = type;
    op.data_primary_key = key;
    op.data_value = val;
    op.server_id = server_id;
    op.last_one = last;
    return op;
}

// Simulate a client: push a pre-built txn through the public intake API.
// (The queue used to be file-scope globals; it is now private state of
// QueriesReceiving, which is what fixed the lost-txn race.)
void push_txn(QueriesReceiving& qr, const txn& t) {
    for (int i = 0; i < 2000; ++i) {                 // intake opens inside collect_epoch()
        if (qr.push_txn(t)) return;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

void print_txns(const std::vector<txn>& txns) {
    for (auto& t : txns) {
        std::cout << "  txn ts=" << t.timestamp_id
                  << "  committed=" << t.is_committed
                  << "  ops=" << t.operations.size() << " [";
        for (size_t i = 0; i < t.operations.size(); i++) {
            if (i > 0) std::cout << ", ";
            std::cout << op_type_str(t.operations[i].type)
                      << "(k=" << t.operations[i].data_primary_key << ")";
        }
        std::cout << "]\n";
    }
}

void print_version_chains(
    const std::map<ObjectKey, std::vector<Operat>>& vc) {
    std::cout << "\n===== Final Version Chains =====\n";
    if (vc.empty()) {
        std::cout << "  (empty)\n";
        return;
    }
    // ── FIX (9/22/26): the VC key is now the logical ObjectKey ──────────
    // (table_id, w_id, d_id, logical_id) — the same identity the ORAM
    // pos_map uses.  server_id is placement, not identity, so it is no
    // longer part of the key.
    for (auto& [vc_key, ops] : vc) {
        std::cout << "\n  Key = " << vc_key.str()
                  << " (" << ops.size() << " ops):\n";
        for (size_t i = 0; i < ops.size(); i++) {
            auto& op = ops[i];
            std::cout << "    [" << i << "] "
                      << op_type_str(op.type)
                      << "  txn_ts=" << op.txn_timestamp_id
                      << "  server=" << op.server_id
                      << "  value=\"" << op.data_value << "\""
                      << "  last_one=" << op.last_one
                      << "\n";
        }
    }
    std::cout << "================================\n";
}

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(60, '#') << "\n";
    std::cout << "  " << title << "\n";
    std::cout << std::string(60, '#') << "\n";
}

// ============================================================================
// Test 1: 4 clients, no conflicts (different keys)
//
// Client 0: READ  key=10
// Client 1: WRITE key=20
// Client 2: READ  key=30
// Client 3: WRITE key=40
// ============================================================================

void test_multi_client_no_conflict() {
    print_separator("Test 1: 4 Clients, No Conflict");

    QueriesReceiving qr;

    // Pre-build txns for each client
    std::vector<txn> client_txns(4);
    client_txns[0].operations = { make_op(OpType::READ,   10, "",       0, true) };
    client_txns[1].operations = { make_op(OpType::UPDATE, 20, "w20",    1, true) };
    client_txns[2].operations = { make_op(OpType::READ,   30, "",       2, true) };
    client_txns[3].operations = { make_op(OpType::UPDATE, 40, "w40",    3, true) };

    // Spawn 4 client threads that push concurrently during epoch window
    std::vector<std::thread> clients;
    for (int c = 0; c < 4; c++) {
        clients.emplace_back([&, c]() {
            // Wait until window opens
            push_txn(qr, client_txns[c]);
        });
    }

    // Collect epoch (200ms window)
    auto epoch_txns = qr.collect_epoch(200);
    for (auto& t : clients) t.join();

    std::cout << "\nCollected " << epoch_txns.size() << " txns\n";

    // Process
    qr.AssignAbort(epoch_txns);

    std::cout << "\n--- After AssignAbort ---\n";
    print_txns(epoch_txns);
    print_version_chains(qr.getVersionChains());

    // All should be committed (no shared keys)
    for (auto& t : epoch_txns) assert(t.is_committed == true);
    std::cout << "✅ Test 1 passed\n";
}

// ============================================================================
// Test 2: 3 clients with conflict on key=100
//
// Client 0: WRITE key=100  (will get some ts)
// Client 1: READ  key=100  (will get some ts)
// Client 2: WRITE key=200  (no conflict)
//
// Depending on arrival order:
//   If WRITE(100) gets ts < READ(100)'s ts → WRITE aborted (Rule A)
//   If WRITE(100) gets ts > READ(100)'s ts → no conflict
// We force ordering with small delays to ensure WRITE arrives first.
// ============================================================================

void test_multi_client_conflict() {
    print_separator("Test 2: 3 Clients, Conflict on Key=100");

    QueriesReceiving qr;

    // Client threads: client 0 sends WRITE first, client 1 sends READ slightly later
    std::vector<std::thread> clients;

    // Client 0: WRITE key=100 (arrives first → smaller ts)
    clients.emplace_back([&]() {
        // Push immediately when window opens
        txn t;
        t.operations = { make_op(OpType::UPDATE, 100, "write_100", 0, true) };
        push_txn(qr, t);
    });

    // Client 1: READ key=100 (arrives second → larger ts)
    clients.emplace_back([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // small delay
        txn t;
        t.operations = { make_op(OpType::READ, 100, "", 1, true) };
        push_txn(qr, t);
    });

    // Client 2: WRITE key=200 (no conflict)
    clients.emplace_back([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        txn t;
        t.operations = { make_op(OpType::UPDATE, 200, "write_200", 2, true) };
        push_txn(qr, t);
    });

    auto epoch_txns = qr.collect_epoch(200);
    for (auto& t : clients) t.join();

    std::cout << "\nCollected " << epoch_txns.size() << " txns\n";
    std::cout << "\n--- Before AssignAbort ---\n";
    print_txns(epoch_txns);

    qr.AssignAbort(epoch_txns);

    std::cout << "\n--- After AssignAbort ---\n";
    print_txns(epoch_txns);
    print_version_chains(qr.getVersionChains());

    // Verify: the WRITE(key=100) txn with smaller ts should be aborted
    // The READ(key=100) txn should be committed
    // The WRITE(key=200) txn should be committed
    bool found_aborted_writer = false;
    bool reader_committed = false;
    for (auto& t : epoch_txns) {
        for (auto& op : t.operations) {
            if (op.data_primary_key == 100 && op.type == OpType::UPDATE && !t.is_committed)
                found_aborted_writer = true;
            if (op.data_primary_key == 100 && op.type == OpType::READ && t.is_committed)
                reader_committed = true;
        }
    }
    assert(found_aborted_writer);
    assert(reader_committed);
    std::cout << "✅ Test 2 passed\n";
}

// ============================================================================
// Test 3: 4 clients, some send incomplete txns
//
// Client 0: READ(k=50), WRITE(k=50) with last_one=false → incomplete
// Client 1: READ(k=60) complete
// Client 2: WRITE(k=70), READ(k=80) complete
// Client 3: READ(k=90) with last_one=false → incomplete
// ============================================================================

void test_multi_client_incomplete() {
    print_separator("Test 3: 4 Clients, Some Incomplete Txns");

    QueriesReceiving qr;

    std::vector<std::thread> clients;

    // Client 0: incomplete txn
    clients.emplace_back([&]() {
        txn t;
        t.operations = {
            make_op(OpType::READ,   50, "",    0, false),
            make_op(OpType::UPDATE, 50, "v50", 0, false)  // last_one=false!
        };
        push_txn(qr, t);
    });

    // Client 1: complete
    clients.emplace_back([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        txn t;
        t.operations = { make_op(OpType::READ, 60, "", 1, true) };
        push_txn(qr, t);
    });

    // Client 2: complete, multi-op
    clients.emplace_back([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(8));
        txn t;
        t.operations = {
            make_op(OpType::UPDATE, 70, "v70", 2, false),
            make_op(OpType::READ,   80, "",    2, true)
        };
        push_txn(qr, t);
    });

    // Client 3: incomplete
    clients.emplace_back([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
        txn t;
        t.operations = { make_op(OpType::READ, 90, "", 3, false) };  // last_one=false!
        push_txn(qr, t);
    });

    auto epoch_txns = qr.collect_epoch(200);
    for (auto& t : clients) t.join();

    std::cout << "\nCollected " << epoch_txns.size() << " txns\n";
    std::cout << "\n--- Before AssignAbort ---\n";
    print_txns(epoch_txns);

    qr.AssignAbort(epoch_txns);

    std::cout << "\n--- After AssignAbort ---\n";
    print_txns(epoch_txns);
    print_version_chains(qr.getVersionChains());

    // Count committed vs aborted
    int committed = 0, aborted = 0;
    for (auto& t : epoch_txns) {
        if (t.is_committed) committed++; else aborted++;
    }
    std::cout << "\nCommitted: " << committed << ", Aborted: " << aborted << "\n";
    assert(aborted >= 2);  // at least 2 incomplete txns
    std::cout << "✅ Test 3 passed\n";
}

// ============================================================================
// Test 4: 8 clients, many txns per client, random workload
//
// Each client sends 5 txns with random READ/WRITE on keys 1-20
// during a 500ms window. Then we run MVCC and print results.
// ============================================================================

void test_multi_client_stress() {
    print_separator("Test 4: 8 Clients x 5 Txns, Random Workload");

    QueriesReceiving qr;
    const int NUM_CLIENTS = 8;
    const int TXNS_PER_CLIENT = 5;
    const int KEY_RANGE = 20;

    std::vector<std::thread> clients;

    for (int c = 0; c < NUM_CLIENTS; c++) {
        clients.emplace_back([&, c]() {
            std::mt19937 rng(c * 1111 + 42);
            std::uniform_int_distribution<uint32_t> key_dist(1, KEY_RANGE);
            std::uniform_real_distribution<double> coin(0.0, 1.0);

            // Wait for window

            for (int i = 0; i < TXNS_PER_CLIENT; i++) {
                txn t;

                // 2-4 queries per txn
                int nops = 2 + (rng() % 3);
                for (int q = 0; q < nops; q++) {
                    OpType type = (coin(rng) < 0.5) ? OpType::READ : OpType::UPDATE;
                    uint32_t key = key_dist(rng);
                    std::string val = (type == OpType::UPDATE)
                        ? "c" + std::to_string(c) + "_k" + std::to_string(key)
                        : "";
                    t.operations.push_back(make_op(type, key, val, c, (q == nops - 1)));
                }

                push_txn(qr, t);
                // Small delay between txns
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
    }

    auto epoch_txns = qr.collect_epoch(500);
    for (auto& t : clients) t.join();

    std::cout << "\nCollected " << epoch_txns.size() << " txns (expected ~"
              << NUM_CLIENTS * TXNS_PER_CLIENT << ")\n";

    std::cout << "\n--- Before AssignAbort ---\n";
    print_txns(epoch_txns);

    qr.AssignAbort(epoch_txns);

    std::cout << "\n--- After AssignAbort ---\n";
    print_txns(epoch_txns);
    print_version_chains(qr.getVersionChains());

    // Summary
    int committed = 0, aborted = 0;
    uint64_t total_ops = 0;
    for (auto& t : epoch_txns) {
        if (t.is_committed) committed++; else aborted++;
        total_ops += t.operations.size();
    }
    std::cout << "\n--- Summary ---\n";
    std::cout << "  Total txns:    " << epoch_txns.size() << "\n";
    std::cout << "  Total ops:     " << total_ops << "\n";
    std::cout << "  Committed:     " << committed << "\n";
    std::cout << "  Aborted:       " << aborted << "\n";
    std::cout << "  Abort rate:    " << std::fixed << std::setprecision(1)
              << (epoch_txns.size() > 0 ? 100.0 * aborted / epoch_txns.size() : 0)
              << "%\n";
    std::cout << "  VC keys:       " << qr.getVersionChains().size() << "\n";

    assert(epoch_txns.size() > 0);
    std::cout << "✅ Test 4 passed\n";
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "========================================\n";
    std::cout << " Multi-Client QueriesReceiving Test\n";
    std::cout << "========================================\n";

    test_multi_client_no_conflict();
    test_multi_client_conflict();
    test_multi_client_incomplete();
    test_multi_client_stress();

    std::cout << "\n========================================\n";
    std::cout << " All tests passed! ✅\n";
    std::cout << "========================================\n";
    return 0;
}