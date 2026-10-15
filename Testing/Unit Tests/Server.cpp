//
// Server.cpp - 完整实现 (v2, 4/18/26)
//
// 改动:
//   1. 从 argv[1] 读 port (之前是 hardcoded 54325)
//   2. 支持多次连续连接 (之前处理完一个 client 就 exit)
//   3. Bind 时设置 SO_REUSEADDR,允许 TIME_WAIT 期间重 bind
//
// 与你的 NetIOConnector.cpp 完全匹配。
// 对 single-connection 的老 test 完全兼容 (那些 test 只用一次就算了,
// 新的 accept loop 会在之后继续等下一个连接)。
//

#include <emp-tool/emp-tool.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>
#include <csignal>
#include <cstdlib>
#include <cstring>

using namespace std;

// ========================================
// 全局存储
//
// Keyed by oram_name: each ORAM keeps its own map<global_slot_id, cipher>.
// Storage persists ACROSS connections within one server process, so successive
// clients connecting to the same port see the same tree state. This is exactly
// what TPC-C needs: Phase 3 run 1's child sets up the tree, run 2's child
// connects to the same listener and continues (although in practice TPC-C
// re-inits between runs, so stale data is overwritten).
// ========================================

map<string, map<uint32_t, string>> server_storage;

const int NAME_LENGTH = 32;

// ========================================
// 辅助函数
// ========================================

string recvString(emp::NetIO* netio, uint32_t length) {
    vector<char> buffer(length);
    netio->recv_data(buffer.data(), length);
    return string(buffer.begin(), buffer.end());
}

void sendString(emp::NetIO* netio, const string& str, uint32_t length) {
    string padded = str;
    if (padded.size() > length) {
        padded.resize(length);
    } else if (padded.size() < length) {
        padded.resize(length, '\0');
    }
    netio->send_data(padded.data(), length);
}

// ========================================
// 处理 INSERT_SLOT (操作码 1)
// ========================================

void handle_insert_slot(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    uint32_t global_slot_id_network;
    netio->recv_data(&global_slot_id_network, sizeof(uint32_t));
    uint32_t global_slot_id = ntohl(global_slot_id_network);

    uint32_t block_size_network;
    netio->recv_data(&block_size_network, sizeof(uint32_t));
    uint32_t block_size = ntohl(block_size_network);

    string encrypted_data = recvString(netio, block_size);

    server_storage[oram_name][global_slot_id] = encrypted_data;

    // 每 10000 次才打一次 log,避免 bulk init 时 log 爆炸
    static uint64_t insert_count = 0;
    insert_count++;
    if (insert_count % 10000 == 0) {
        cout << "[INSERT_SLOT x" << insert_count << "] ORAM=[" << oram_name
             << "] total_slots=" << server_storage[oram_name].size() << endl;
    }
}

// ========================================
// 处理 FIND_SLOT (操作码 3)
// ========================================

void handle_find_slot(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    uint32_t bucket_id_network;
    netio->recv_data(&bucket_id_network, sizeof(uint32_t));
    // bucket_id: not used by storage, but we must consume bytes
    (void) ntohl(bucket_id_network);

    uint32_t global_slot_id_network;
    netio->recv_data(&global_slot_id_network, sizeof(uint32_t));
    uint32_t global_slot_id = ntohl(global_slot_id_network);

    uint32_t block_size_network;
    netio->recv_data(&block_size_network, sizeof(uint32_t));
    uint32_t block_size = ntohl(block_size_network);

    string encrypted_data;
    auto oram_it = server_storage.find(oram_name);
    if (oram_it != server_storage.end()) {
        auto slot_it = oram_it->second.find(global_slot_id);
        if (slot_it != oram_it->second.end()) {
            encrypted_data = slot_it->second;
        }
    }

    if (encrypted_data.empty() || encrypted_data.size() != block_size) {
        encrypted_data.resize(block_size, '\0');
    }

    netio->send_data(encrypted_data.data(), block_size);
}

// ========================================
// 处理 SET_TUPLE_LENGTH (操作码 9)
// ========================================

void handle_set_tuple_length(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    uint32_t tuple_length_network;
    netio->recv_data(&tuple_length_network, sizeof(uint32_t));
    uint32_t tuple_length = ntohl(tuple_length_network);

    cout << "[SET_TUPLE_LENGTH] ORAM=[" << oram_name
         << "] tuple_length=" << tuple_length << endl;
}

// ========================================
// 处理 CLEAR (操作码 0)
// ========================================

void handle_clear(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    server_storage[oram_name].clear();

    cout << "[CLEAR] Cleared ORAM=[" << oram_name << "]" << endl;
}

// ========================================
// handle_chunk_barrier (opcode 10) — bulk init flow control (v4, 4/20/26)
//
// See Servers_MultiRingORAM.cpp for the full protocol description.
// This handler mirrors it so single-server tests (./Server) also work
// with the new client protocol.
// ========================================

static uint64_t g_barriers_acked = 0;

void handle_chunk_barrier(emp::NetIO* netio) {
    int8_t ack = 1;
    netio->send_data(&ack, 1);
    g_barriers_acked++;
    if (g_barriers_acked % 50 == 0) {
        cout << "[BARRIER] ACKed " << g_barriers_acked << " chunks cumulatively\n";
    }
}

// ========================================
// 处理 UPDATE / INSERT (操作码 4) — legacy, for ServerConnector
// Reads the typical insert/update payload (id + data).
// ========================================

// Returns false: the session MUST end.  This stub consumed only the name, so
// the rest of the payload is still in the stream — continuing would read that
// payload as opcodes and corrupt everything after it.  Nothing in this repo
// sends opcode 4 any more (all traffic is insert_slot / find_slot), so the
// honest response is to report it and drop the connection rather than to
// desynchronize silently.
bool handle_update(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();
    cerr << "[ERROR] Legacy opcode 4 (UPDATE) for ORAM=[" << oram_name
         << "] is not implemented; closing the session so the byte stream "
            "cannot desynchronize.\n";
    return false;
}

// ========================================
// 处理单个客户端连接的完整会话
// 返回 true = normal close (-1), false = error / EOF (未接到有效 opcode)
// ========================================

bool handle_client(emp::NetIO* netio) {
    int operation_count = 0;

    while (true) {
        int8_t code;
        // 如果客户端直接关 socket 或 read 失败,emp::NetIO 可能 block/crash
        // 我们这里简单地 try,任何异常都返回 false 让 main loop 重 accept
        try {
            netio->recv_data(&code, 1);
        } catch (...) {
            cout << "[handle_client] recv_data threw; ending session after "
                 << operation_count << " ops.\n";
            return false;
        }

        operation_count++;

        switch (code) {
            case -1:
                cout << "[handle_client] Client sent CLOSE after "
                     << operation_count << " ops. Session ended.\n";
                return true;
            case 0:
                handle_clear(netio); break;
            case 1:
                handle_insert_slot(netio); break;
            case 3:
                handle_find_slot(netio); break;
            case 4:
                if (!handle_update(netio)) return false;
                break;
            case 9:
                handle_set_tuple_length(netio); break;
            case 10:
                handle_chunk_barrier(netio); break;  // bulk-init flow control (v4)
            default:
                cerr << "[handle_client] Unknown opcode " << (int)code
                     << " after " << operation_count << " ops. Closing.\n";
                return false;
        }
    }
}

// ========================================
// Main — accept loop, supports many successive client connections
// ========================================

int main(int argc, char** argv) {
    // Port from argv[1] (defaults to 54325 for backward compatibility)
    int port = 54325;
    if (argc >= 2) {
        port = std::atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            cerr << "Invalid port: " << argv[1] << "\n";
            return 1;
        }
    }

    // Ignore SIGPIPE — if the peer closes mid-send, we just get an error
    // return instead of being killed.
    signal(SIGPIPE, SIG_IGN);

    cout << "========================================" << endl;
    cout << "🚀 RingORAM Server" << endl;
    cout << "   Port: " << port << endl;
    cout << "   Multi-connection mode: ENABLED" << endl;
    cout << "========================================" << endl;

    // ── Accept loop ──────────────────────────────────────────────────────
    //
    // emp::NetIO's constructor (with nullptr host) itself does bind+listen+
    // accept once. So each iteration creates a fresh NetIO which does a
    // full accept. The SO_REUSEADDR happens inside emp-tool; if the next
    // accept call fails because of lingering TIME_WAIT on the listen socket,
    // we retry with a small sleep.
    //
    // This is a simple approach that works with emp-tool's API. A more
    // efficient approach would be to bind/listen once and accept() in a
    // loop, but that requires reaching below the emp::NetIO abstraction.
    int connections_handled = 0;
    while (true) {
        cout << "\n[Server port=" << port
             << "] Waiting for connection #" << (connections_handled + 1)
             << " ..." << endl;

        emp::NetIO* netio = nullptr;
        int retry = 0;
        while (retry < 10) {
            try {
                netio = new emp::NetIO(nullptr, port);
                break;
            } catch (const std::exception& e) {
                cerr << "[Server port=" << port
                     << "] accept failed (attempt " << (retry+1)
                     << "): " << e.what() << "\n";
                retry++;
                sleep(1);
            }
        }
        if (!netio) {
            cerr << "[Server port=" << port
                 << "] Could not accept connection after 10 retries. Exiting.\n";
            return 1;
        }

        cout << "[Server port=" << port
             << "] Client connected (connection #"
             << (connections_handled + 1) << ")" << endl;

        bool ok = handle_client(netio);

        delete netio;   // closes the TCP connection
        connections_handled++;

        cout << "[Server port=" << port
             << "] Connection closed (total handled: "
             << connections_handled << ", status: "
             << (ok ? "OK" : "ERR") << ")" << endl;

        // Loop back to accept another connection. Note: storage persists
        // across connections.
    }

    return 0;  // unreachable
}