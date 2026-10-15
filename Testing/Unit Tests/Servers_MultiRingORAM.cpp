//
// Servers_MultiRingORAM.cpp - 完整实现 (v3, 4/19/26; opcode 11 FIND_SLOTS 10/8/26)
//
// 这是 TPC-C 和其他 tests 实际使用的 server listener
// (编译出 ./Servers_MultiRingORAM 可执行文件)。
//
// 改动 (v3):
//   1. argv[1] 读 port (已有)
//   2. ★ 支持多次连续连接 (accept loop) — v3 新增
//      之前是 handle_client 返回后 main() 退出, listener 进程死。
//      这导致 TPC-C Phase 1 每次 tree-size sweep 后 listener 死掉,
//      以及 Phase 3 Run 2 开始时连不上 server。
//   3. 忽略 SIGPIPE — client 突然关 socket 不会杀死 server
//   4. 减少 INSERT_SLOT 的 log (每 10000 次才打一次), 避免 bulk init
//      时 log 爆炸
//
// 语义:
//   Server_storage 在进程生命周期内持久化,所以连续 connection 的
//   client 能看到前一个 connection 留下的 tree。TPC-C 每次 init
//   会 re-upload 整个 tree, 所以旧状态会被覆盖, 没问题。
//
// 对其他 (single-connection) test 完全兼容: 那些 test 只用一次连接,
// 新的 accept loop 只在"之后"继续等下一个连接, 不影响已完成的 test。
//

#include <emp-tool/emp-tool.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <arpa/inet.h>
#include <csignal>
#include <unistd.h>
#include <cstdlib>

using namespace std;

// ========================================
// 全局存储
// oram_name -> global_slot_id -> encrypted_data
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
// Handlers (log 已经精简,避免刷屏)
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

    // Every 10000 inserts, print progress (keeps log readable during bulk init)
    static uint64_t insert_count = 0;
    insert_count++;
    if (insert_count % 10000 == 0) {
        cout << "[INSERT x" << insert_count << "] ORAM=[" << oram_name
             << "] slots=" << server_storage[oram_name].size() << endl;
    }
}

void handle_find_slot(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    uint32_t bucket_id_network;
    netio->recv_data(&bucket_id_network, sizeof(uint32_t));
    (void)ntohl(bucket_id_network);  // not used for lookup

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
        // ── BAND-AID FALLBACK ────────────────────────────────────────────
        // Slot not present (race: client requested before tree finished
        // uploading), or size mismatch (oram name collision, wrong
        // block_size).  Return zeros so client doesn't crash on recv.
        // Log at low frequency so the issue is visible but doesn't flood
        // logs during cold-start races.
        static uint64_t miss_count = 0;
        miss_count++;
        if (miss_count == 1 || miss_count % 1000 == 0) {
            cerr << "[FIND_SLOT] WARNING: returning zeros for "
                 << "ORAM=[" << oram_name << "] slot=" << global_slot_id
                 << " (miss #" << miss_count << ")"
                 << "  — likely tree not fully uploaded yet, "
                 << "or stale state from previous run\n";
        }
        encrypted_data.resize(block_size, '\0');
    }

    netio->send_data(encrypted_data.data(), block_size);
}

// ========================================
// handle_find_slots (opcode 11, 10/8/26) — batched FIND_SLOT
//   client -> [11][name 32][count u32][block_size u32][count x (bucket u32, slot u32)]
//   server -> [count x block_size]
// The WHOLE request is read before anything is written.  emp::NetIO uses one
// FILE* for both directions; if replies were written while later requests
// still sat unread in the stdio buffer, switching the stream to write mode
// would discard them.  That is why pipelining opcode 3 does not work.
// ========================================
static string lookup_slot(const string& oram_name, uint32_t global_slot_id, uint32_t block_size) {
    auto oram_it = server_storage.find(oram_name);
    if (oram_it != server_storage.end()) {
        auto slot_it = oram_it->second.find(global_slot_id);
        if (slot_it != oram_it->second.end() && slot_it->second.size() == block_size)
            return slot_it->second;
    }
    static uint64_t miss_count = 0;
    if (++miss_count == 1 || miss_count % 1000 == 0)
        cerr << "[FIND_SLOTS] WARNING: returning zeros for ORAM=[" << oram_name
             << "] slot=" << global_slot_id << " (miss #" << miss_count << ")\n";
    return string(block_size, '\0');
}

void handle_find_slots(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();
    uint32_t n_net = 0, bs_net = 0;
    netio->recv_data(&n_net, sizeof(uint32_t));
    netio->recv_data(&bs_net, sizeof(uint32_t));
    const uint32_t n = ntohl(n_net), block_size = ntohl(bs_net);

    vector<uint32_t> ids(2 * static_cast<size_t>(n));            // (bucket, slot) pairs
    if (n > 0) netio->recv_data(ids.data(), ids.size() * sizeof(uint32_t));

    string reply;
    reply.reserve(static_cast<size_t>(n) * block_size);
    for (uint32_t i = 0; i < n; ++i)
        reply += lookup_slot(oram_name, ntohl(ids[2 * i + 1]), block_size);
    if (!reply.empty()) netio->send_data(reply.data(), reply.size());
}

void handle_set_tuple_length(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    uint32_t tuple_length_network;
    netio->recv_data(&tuple_length_network, sizeof(uint32_t));
    uint32_t tuple_length = ntohl(tuple_length_network);

    cout << "[SET_TUPLE_LENGTH] ORAM=[" << oram_name
         << "] tuple_length=" << tuple_length << endl;
}

void handle_clear(emp::NetIO* netio) {
    string oram_name = recvString(netio, NAME_LENGTH);
    oram_name = oram_name.c_str();

    server_storage[oram_name].clear();

    cout << "[CLEAR] Cleared ORAM=[" << oram_name << "]" << endl;
}

// ========================================
// handle_chunk_barrier (opcode 10) — bulk init flow control (v4, 4/20/26)
//
// The bulk_init_tree{_stream} client sends slot data in 10 MB chunks
// and terminates each chunk with this barrier byte. We send a single
// ACK byte back, which the client blocks on before sending the next
// chunk. This caps the client's in-flight data at one chunk worth,
// preventing TCP send-buffer deadlock on multi-GB uploads.
//
// Protocol:
//   client → [slot][slot]...[slot][opcode=10]  (end of chunk)
//   server → [0x01]                             (ack — proceed)
//
// There's no payload after opcode 10; we just reply. A future version
// could include a server-side "slots committed so far" count, but for
// now a single ack byte is enough.
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
// 单次会话的 handler
// 返回 true = 正常关闭 (收到 opcode -1)
// 返回 false = 错误或 EOF (应该结束整个进程还是重新 accept?
//              我们选择重新 accept, 更 robust)
// ========================================

bool handle_client(emp::NetIO* netio, int port, int connection_num) {
    int operation_count = 0;

    while (true) {
        int8_t code;
        try {
            netio->recv_data(&code, 1);
        } catch (...) {
            cout << "[Server port=" << port << " conn#" << connection_num
                 << "] recv threw after " << operation_count
                 << " ops; ending session\n";
            return false;
        }

        operation_count++;

        switch (code) {
            case -1:  // Client 请求关闭
                cout << "[Server port=" << port << " conn#" << connection_num
                     << "] CLOSE after " << operation_count
                     << " ops\n";
                return true;

            case 0:  handle_clear(netio); break;
            case 1:  handle_insert_slot(netio); break;
            case 3:  handle_find_slot(netio); break;
            case 9:  handle_set_tuple_length(netio); break;
            case 10: handle_chunk_barrier(netio); break;  // bulk-init flow control (v4)
            case 11: handle_find_slots(netio); break;     // batched FIND_SLOT (10/8/26)

            default:
                cerr << "[Server port=" << port << " conn#" << connection_num
                     << "] Unknown opcode " << (int)code
                     << " after " << operation_count << " ops; closing\n";
                return false;
        }
    }
}

// ========================================
// Main — accept loop (v3)
//
// 之前是单连接: main() → new NetIO → handle_client → return → process dies
// 现在是多连接: main() → loop { new NetIO → handle_client → delete NetIO }
//
// 每个 emp::NetIO 对象在 destruction 时关闭 TCP socket。下一次迭代
// new 一个新的 NetIO 会重新 bind+listen+accept。由于 SO_REUSEADDR
// (emp-tool 内部已设置) + 短暂 sleep, TIME_WAIT 不会 block 我们太久。
// ========================================

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 54325;
    if (port <= 0 || port > 65535) {
        cerr << "Invalid port: " << argv[1] << "\n";
        return 1;
    }

    // 客户端突然关 socket 时, 不要让 SIGPIPE 杀死我们
    signal(SIGPIPE, SIG_IGN);

    cout << "========================================" << endl;
    cout << "🚀 RingORAM Server (v3, multi-connection)" << endl;
    cout << "   Port: " << port << endl;
    cout << "========================================" << endl;

    int connections_handled = 0;

    while (true) {
        cout << "\n[Server port=" << port
             << "] Waiting for connection #" << (connections_handled + 1)
             << "..." << endl;

        emp::NetIO* netio = nullptr;
        int retry = 0;
        const int MAX_RETRY = 120;   // 总共 ~120 秒等 TIME_WAIT

        while (retry < MAX_RETRY) {
            try {
                netio = new emp::NetIO(nullptr, port);
                break;
            } catch (const std::exception& e) {
                retry++;
                if (retry == 1 || retry % 15 == 0) {
                    cerr << "[Server port=" << port
                         << "] accept retry " << retry
                         << ": " << e.what() << "\n";
                }
                sleep(1);
            } catch (...) {
                retry++;
                sleep(1);
            }
        }

        if (!netio) {
            cerr << "[Server port=" << port
                 << "] Could not accept after " << MAX_RETRY
                 << "s. Exiting.\n";
            return 1;
        }

        connections_handled++;
        cout << "[Server port=" << port
             << "] ✅ Client connected (conn #"
             << connections_handled << ")" << endl;

        bool ok = handle_client(netio, port, connections_handled);

        delete netio;

        cout << "[Server port=" << port
             << "] 🔌 Connection #" << connections_handled
             << " closed (" << (ok ? "OK" : "ERR") << ")" << endl;

        // 让 kernel 有机会释放 socket, 再 bind 下一次
        usleep(200000);  // 200ms
    }

    return 0;
}