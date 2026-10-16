//
// Modified: per-server process model + fixed batch size with padding
//
// Architecture
// ─────────────
//  Parent (benchmark) ─── pipe ──► Child process i
//                                      owns NetIOConnector_i
//                                      owns RingORAM_i
//                      ◄── pipe ─── sends results back
//
// IPC protocol (parent → child, to_child pipe)
//   [uint32_t num_ops]          0 → shutdown
//   per op (num_ops times):
//     [uint8_t  type]           OpType cast to uint8
//     [uint32_t key]
//     [uint32_t val_len]
//     [char...  val]            val_len bytes
//     [uint8_t  is_dummy]       1 = padding dummy
//
// IPC protocol (child → parent, from_child pipe)
//   [uint32_t real_ops_done]    non-dummy ops processed
//   [double   elapsed_ms]       wall-clock inside child
//   [uint8_t  ok]               1 = success
//
// Fixed-batch guarantee
//   Every sub-batch sent to a child is exactly FIXED_BATCH_SIZE ops.
//   If a real batch has > FIXED_BATCH_SIZE ops it is split into multiple
//   sub-batches.  The last sub-batch is padded with dummy READ ops.
//

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <chrono>
#include <cstring>
#include <cassert>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifdef __linux__
  #include <sys/prctl.h>   // PR_SET_PDEATHSIG — auto-kill child on parent death
#endif
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <iomanip>
#include <unordered_map>
#include <set>
#include <map>
#include <algorithm>
#include <atomic>
#include <stdexcept>
#include "MultiRingORAM_Servers.h"
#include "NetEmu.h"          // batch-level network emulation (off unless DEPART_NET_* is set)

#define BUFFER_SIZE    4096
#define DEFAULT_STARTING_PORT 8881

// ============================================================================
// Globals
// ============================================================================
std::vector<pid_t> server_pids;
std::mutex         cout_mutex;

void safe_print(const std::string& msg) {
    std::lock_guard<std::mutex> lk(cout_mutex);
    std::cout << msg << std::flush;
}

void cleanup_servers(int signum) {
    std::cout << "\n\n🛑 Shutting down servers..." << std::endl;
    for (pid_t pid : server_pids) {
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
    }
    exit(signum);
}

// ============================================================================
// CSV Reader
// ============================================================================
std::vector<std::string> readCSV(const std::string& filename) {
    std::vector<std::string> data;
    std::ifstream file(filename);
    if (!file.is_open())
        throw std::runtime_error("Failed to open file: " + filename);
    std::string line;
    std::getline(file, line); // skip header
    while (std::getline(file, line))
        if (!line.empty()) data.push_back(line);
    return data;
}

// ============================================================================
// Server Management (NetIO storage servers — unchanged)
// ============================================================================
pid_t startServer(int port, const std::string& server_exec) {
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "❌ Failed to fork server process for port " << port << std::endl;
        return -1;
    }
    if (pid == 0) {
        // ── Zombie prevention ────────────────────────────────────────────
        // If parent (TPCC_Main) crashes / is OOM-killed / exits without a
        // clean shutdown, the kernel sends SIGTERM to this child immediately.
        // Without this, orphaned server processes accumulate across failed
        // runs and monopolize ports 8800-9500, causing the next run to
        // connect to stale ORAMs and hang. PR_SET_PDEATHSIG survives the
        // execl() below (assuming the server binary is not setuid).
        // Linux-only — macOS has no equivalent; not a problem since the
        // EC2 benchmark host is Linux. Local macOS dev builds skip it.
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

        std::string log_file = "server_" + std::to_string(port) + ".log";
        freopen(log_file.c_str(), "w", stdout);
        freopen(log_file.c_str(), "w", stderr);
        std::string port_str = std::to_string(port);
        execl(server_exec.c_str(), server_exec.c_str(), port_str.c_str(), nullptr);
        std::cerr << "❌ Failed to execute server: " << server_exec << std::endl;
        exit(1);
    }
    return pid;
}

bool waitForServer(int port, int max_attempts) {
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) continue;
        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (connect(sock_fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            close(sock_fd);
            return true;
        }
        close(sock_fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

// ============================================================================
// Pipe helpers
// ============================================================================
bool MultiRingORAM_Servers::write_all(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w <= 0) return false;
        p += w; n -= static_cast<size_t>(w);
    }
    return true;
}

bool MultiRingORAM_Servers::read_all(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r <= 0) return false;
        p += r; n -= static_cast<size_t>(r);
    }
    return true;
}

// ============================================================================
// Data Distribution + pos_map (revised 9/22/26)
// ============================================================================
//
// pos_map:  ObjectKey → (server_id, block_id)   (PartitionPosMap)
//
// Previously pk_to_blockid_[server_id][uint32 pk]: CUSTOMER(C=1) and
// ORDERS(O=1) on one server collided (the later one overwrote the earlier),
// and all ORDER_LINE rows of an order collapsed to one entry.  The map is
// now keyed by the SAME ObjectKey the proxy's MVTSO layer uses.
//
// ── MULTI-TABLE LIFECYCLE (union across tables) ─────────────────────────────
// A TPC-C-derived workload partitions EACH TABLE separately and then unions
// the i-th partition of every table into one shard:
//
//     P_i  =  P_CUSTOMER,i  ∪  P_ORDERS,i  ∪  P_ORDER_LINE,i  ∪  …   →  S_i
//
// So one ORAM instance (one server_id) holds records of MANY tables, and the
// pos_map must accumulate them.  The build is therefore explicit:
//
//     mrs.beginPositionMapBuild();                                  // once
//     mrs.addTablePartitions(TpccTable::CUSTOMER,   bi_c, parts_c, servers, data_c);
//     mrs.addTablePartitions(TpccTable::ORDERS,     bi_o, parts_o, servers, data_o);
//     mrs.addTablePartitions(TpccTable::ORDER_LINE, bi_l, parts_l, servers, data_l);
//     mrs.finishPositionMapBuild();                                 // once
//
// Every addTablePartitions() APPENDS; only beginPositionMapBuild() resets.
// Calling distributeDataToPartitions() once per table would have left only
// the LAST table in the pos_map.
//
// The SAME `servers` vector is normally passed for every table: table T's
// partition i and table U's partition i land on the same server_id, which is
// exactly the union above.
//
// Consequences handled here:
//   * BLOCK IDS CONTINUE ACROSS TABLES on a server.  Table T's slice occupies
//     [0, n_T), table U's [n_T, n_T + n_U), … and the filler blocks
//     (Σ dummy_num + insert_reserve) come last — so a server must be
//     initialised ONCE with all its slices, in the same order they were added
//     here (ServerInitializationUnion / ServerInitializationBulkUnion).
//   * partition ids are made GLOBAL (a running counter across tables); the
//     caller's per-table indices 0..k-1 are not used as map keys;
//   * bin keys are qualified with the table id, because bin keys are raw
//     attribute values and repeat across tables.
//
// TERMINOLOGY: `server_id` identifies an ORAM INSTANCE (one child process,
// one RingORAM), not a physical machine.  Several instances may be hosted by
// one machine — record that with setPhysicalServer() if you report it.
//
// distributeDataToPartitions() stays as the single-table shorthand
// (begin + add + finish), so existing single-table benchmarks are unchanged.
//
// Block ids replay the EXACT order ServerInitialization /
// ServerInitializationBulk insert records: part.index →
// bin_info.bin_key_to_data_indices[bin_key], real records first
// (block 0 .. R-1), then part.dummy_num dummy fillers (R .. R+dummy-1).
//
// Filler blocks (dummy_num DP padding + insert_reserve_per_partition_) are
// registered as free blocks: a later INSERT of a new row takes one over, so
// the ORAM tree size never changes.  NOTE: dummy_num = noisy − real is only
// noise-sized (≈0, and 0 in Case B), so the public insert reserve is what
// actually provides T8 capacity.  setInsertReservePerPartition() must be
// called BEFORE the build and before ServerInitialization*.
//
// Key extraction, in order of preference:
//   1. key_mapper_ (setObjectKeyMapper): ObjectKey from (data_idx, record).
//      Required for TPC-C (composite keys, many tables).
//   2. pk_mapper_ (setPrimaryKeyMapper, legacy): ObjectKey{0,0,0,pk}.
// `record` is data[data_idx] when data is supplied, else "".
//
// A duplicate ObjectKey (two records → same logical row) throws
// std::logic_error: that is a key-mapper bug and must not be papered over.
// ============================================================================
// Verify the caller hands a server exactly the slices the pos_map registered
// for it, in the same order — otherwise block ids would not line up and every
// row on that server would read back the wrong record.
void MultiRingORAM_Servers::checkSliceOrder(int server_id,
                                            const std::vector<TableSlice>& slices) const {
    const auto& layout = sliceLayout(server_id);
    if (layout.empty()) return;          // single-table / legacy build: nothing to check
    if (layout.size() != slices.size())
        throw std::logic_error("[ServerInit] server " + std::to_string(server_id) +
                               " was registered with " + std::to_string(layout.size()) +
                               " table slice(s) but is being initialised with " +
                               std::to_string(slices.size()));
    for (size_t i = 0; i < slices.size(); ++i) {
        if (layout[i].table_id != slices[i].table_id)
            throw std::logic_error(
                "[ServerInit] server " + std::to_string(server_id) + " slice " +
                std::to_string(i) + ": pos_map has table " +
                std::to_string(layout[i].table_id) + " but init got table " +
                std::to_string(slices[i].table_id) +
                " (slices must be passed in addTablePartitions order)");
    }
}

std::string MultiRingORAM_Servers::qualifiedBinKey(uint16_t table_id,
                                                   const std::string& bin_key) {
    return std::to_string(table_id) + "|" + bin_key;
}

void MultiRingORAM_Servers::beginPositionMapBuild() {
    pos_map_.clear();
    bin_key_to_partition_id_.clear();
    partition_id_to_server_id_.clear();
    server_id_to_partition_id_.clear();
    server_next_block_.clear();
    server_dummy_total_.clear();
    server_slices_.clear();
    next_global_partition_id_ = 0;
    building_pos_map_         = true;
    std::cout << "\n📦 Position-map build started.\n";
}

void MultiRingORAM_Servers::addTablePartitions(
    uint16_t table_id,
    const BinInfo& bin_info,
    const std::vector<Partition>& partitions,
    std::vector<ServerInfo>& servers,
    const std::vector<std::string>& data)
{
    if (!building_pos_map_)
        throw std::logic_error("[pos_map] addTablePartitions() called outside "
                               "beginPositionMapBuild()/finishPositionMapBuild()");
    if (servers.size() < partitions.size())
        throw std::logic_error("[pos_map] fewer ServerInfo entries (" +
                               std::to_string(servers.size()) + ") than partitions (" +
                               std::to_string(partitions.size()) + ") for table " +
                               std::to_string(table_id));

    static const std::string kEmpty;
    auto keyOf = [&](uint32_t data_idx) -> ObjectKey {
        const std::string& record = data_idx < data.size() ? data[data_idx] : kEmpty;
        if (key_mapper_) return key_mapper_(data_idx, record);
        return ObjectKey{0, 0, 0, static_cast<uint64_t>(pk_mapper_(data_idx, record))};
    };

    for (uint32_t local_id = 0; local_id < partitions.size(); ++local_id) {
        const Partition& part = partitions[local_id];
        ServerInfo& server    = servers[local_id];
        const int sid         = server.server_id;

        const uint32_t part_id = next_global_partition_id_++;
        partition_id_to_server_id_[part_id] = sid;
        server_id_to_partition_id_[sid]     = part_id;   // last slice wins (diagnostics only)
        pos_map_.ensureServer(sid);

        // Block ids continue where the previous table's slice ended on this
        // server; ServerInitialization*Union replays exactly this order.
        uint32_t block_counter = server_next_block_[sid];
        const uint32_t first_block = block_counter;
        for (const auto& bin_key : part.index) {
            bin_key_to_partition_id_[qualifiedBinKey(table_id, bin_key)] = part_id;
            auto it = bin_info.bin_key_to_data_indices.find(bin_key);
            if (it == bin_info.bin_key_to_data_indices.end()) continue;
            for (uint32_t data_idx : it->second) {
                server.assigned_data_indices.push_back(data_idx);
                pos_map_.addRecord(sid, keyOf(data_idx), block_counter);
                ++block_counter;
            }
        }
        server_next_block_[sid] = block_counter;
        // Fillers (Σ dummy_num + one insert reserve per server) are appended
        // after ALL tables' records, so they are registered in finish().
        server_dummy_total_[sid] += part.dummy_num;
        server_slices_[sid].push_back(TableSliceInfo{table_id, local_id,
                                                     first_block, block_counter - first_block});

        std::cout << "  Table " << table_id << " → Server " << sid
                  << " (Partition " << part_id << "): "
                  << part.index.size() << " bins, "
                  << (block_counter - first_block) << " records at blocks ["
                  << first_block << ", " << block_counter << ")\n";
    }
}

void MultiRingORAM_Servers::finishPositionMapBuild() {
    if (!building_pos_map_)
        throw std::logic_error("[pos_map] finishPositionMapBuild() without "
                               "beginPositionMapBuild()");
    // Filler blocks sit after every table's records on that server:
    //   [0, R_total)                       real rows of all slices
    //   [R_total, R_total + Σdummy + Rins) dummy fillers / insert reserve
    for (const auto& [sid, real_total] : server_next_block_) {
        pos_map_.addFreeBlocks(sid, real_total,
                               server_dummy_total_[sid] + insert_reserve_per_partition_);
        std::cout << "  Server " << sid << ": " << server_slices_[sid].size()
                  << " table slice(s), " << real_total << " rows, "
                  << pos_map_.freeBlocks(sid) << " insert slots\n";
    }
    building_pos_map_ = false;
    std::cout << "✅ Position-map build complete: "
              << next_global_partition_id_ << " partitions across "
              << server_next_block_.size() << " ORAM instances, "
              << pos_map_.totalKeys() << " rows.\n";
}

const std::vector<TableSliceInfo>& MultiRingORAM_Servers::sliceLayout(int server_id) const {
    static const std::vector<TableSliceInfo> kEmpty;
    auto it = server_slices_.find(server_id);
    return it == server_slices_.end() ? kEmpty : it->second;
}

// Single-table shorthand (begin + add + finish), table_id = 0.
void MultiRingORAM_Servers::distributeDataToPartitions(
    const BinInfo& bin_info,
    const std::vector<Partition>& partitions,
    std::vector<ServerInfo>& servers)
{
    static const std::vector<std::string> kNoData;
    distributeDataToPartitions(bin_info, partitions, servers, kNoData);
}

void MultiRingORAM_Servers::distributeDataToPartitions(
    const BinInfo& bin_info,
    const std::vector<Partition>& partitions,
    std::vector<ServerInfo>& servers,
    const std::vector<std::string>& data)
{
    std::cout << "\n📦 Step 3: Distributing data to partitions (single table)...\n";
    beginPositionMapBuild();
    addTablePartitions(/*table_id=*/0, bin_info, partitions, servers, data);
    finishPositionMapBuild();
}

// ============================================================================
// pos_map lookup
// ============================================================================
uint32_t MultiRingORAM_Servers::lookupBlockId(int server_id, const ObjectKey& key) const {
    return pos_map_.lookup(server_id, key);
}

// Legacy: untyped single-table benchmarks, ObjectKey = (0, 0, 0, pk).
uint32_t MultiRingORAM_Servers::lookupBlockId(int server_id, uint32_t primary_key) const {
    return pos_map_.lookup(server_id, ObjectKey{0, 0, 0, primary_key});
}

// Tuple width each server's records were loaded with (parent-side copy).
// ServerInitialization* may run concurrently for several servers.
static std::mutex g_tuple_width_mu;

void MultiRingORAM_Servers::recordTupleWidth(int server_id, uint32_t width) {
    std::lock_guard<std::mutex> lk(g_tuple_width_mu);
    tuple_width_by_server_[server_id] = width;
}

uint32_t MultiRingORAM_Servers::tupleWidthOf(int server_id) const {
    std::lock_guard<std::mutex> lk(g_tuple_width_mu);
    auto it = tuple_width_by_server_.find(server_id);
    return it == tuple_width_by_server_.end() ? 0 : it->second;
}

void MultiRingORAM_Servers::printPkMapStats() const {
    pos_map_.printStats(std::cout);
}

// ============================================================================
// Server Process Initialization
// ============================================================================
// Child event loop (runs inside forked child process).
// oram is already initialized; child reads batches and executes them.
// conn: the child's connection to its storage listener.  RingORAM's destructor
// does NOT delete its connector, so the child must do it: ~NetIOConnector sends
// the close opcode (-1) and flushes before the socket closes.  Without it the
// listener hits EOF mid-read, and emp-tool's NetIO (net_io_channel.h: fread ==
// 0 -> error("net_recv_data") -> exit(1)) kills the listener, so the NEXT run
// on that port hangs forever in connect.  Older emp-tool builds tolerated the
// EOF, which is why this only showed up on some machines.
static void child_event_loop(RingORAM* oram, NetIOConnector* conn,
                             int read_fd, int write_fd) {
    using clk = std::chrono::high_resolution_clock;

    // Reusable helper for var-length writes (handles partial-write loop).
    auto write_all_local = [](int fd, const void* buf, size_t n) -> bool {
        const char* p = static_cast<const char*>(buf);
        size_t sent = 0;
        while (sent < n) {
            ssize_t w = write(fd, p + sent, n - sent);
            if (w <= 0) return false;
            sent += static_cast<size_t>(w);
        }
        return true;
    };

    while (true) {
        // ── Read num_ops (0 = shutdown) ──────────────────────────────────
        uint32_t num_ops = 0;
        ssize_t r = read(read_fd, &num_ops, sizeof(uint32_t));
        if (r <= 0 || num_ops == 0) break;   // shutdown

        // ── Execute ops ──────────────────────────────────────────────────
        auto t0 = clk::now();
        uint32_t real_ops = 0;

        // ── FIX (4/26/26): capture each access() return value ─────────────
        // For READ ops, access() returns the decrypted plaintext.  Previously
        // this was discarded, which made TPC-C transactions impossible (proxy
        // had no way to read row data through the ORAM).  We now collect each
        // result and stream them back after the summary header.
        std::vector<std::string> op_results;
        std::vector<RingORAM::BatchOp> batch_ops;
        batch_ops.reserve(num_ops);

        // 1) Read the WHOLE batch from the pipe first (10/8/26) ...
        for (uint32_t j = 0; j < num_ops; ++j) {
            uint8_t  type_byte;
            uint32_t key;
            uint32_t val_len;
            uint8_t  is_dummy;

            // Read fields
            if (read(read_fd, &type_byte, 1) != 1)          goto child_error;
            if (read(read_fd, &key,       4) != 4)          goto child_error;
            if (read(read_fd, &val_len,   4) != 4)          goto child_error;
            {
                std::string val(val_len, '\0');
                if (val_len > 0) {
                    size_t offset = 0;
                    while (offset < val_len) {
                        ssize_t got = read(read_fd, &val[offset], val_len - offset);
                        if (got <= 0) goto child_error;
                        offset += static_cast<size_t>(got);
                    }
                }
                if (read(read_fd, &is_dummy, 1) != 1)       goto child_error;

                OpType op_type = static_cast<OpType>(type_byte);

                // ── FIX (4/26/26): UPDATE val must carry bID(4) prefix ────
                if (op_type == OpType::UPDATE && val_len > 0) {
                    static uint64_t bad_update = 0;
                    static uint64_t bad_warned = 0;
                    if (val_len < sizeof(uint32_t)) {
                        if (bad_warned == 0) {
                            std::cerr << "[child_event_loop] WARNING: UPDATE "
                                      << "val_len=" << val_len
                                      << " < 4 bytes; expected bID(4)+data."
                                      << "  Check proxy prepends block_id.\n";
                            bad_warned++;
                        }
                    } else {
                        int32_t embedded_bid = 0;
                        memcpy(&embedded_bid, val.data(), sizeof(int32_t));
                        if (static_cast<uint32_t>(embedded_bid) != key) {
                            bad_update++;
                            if (bad_update == 1 || bad_update % 1000 == 0) {
                                std::cerr << "[child_event_loop] WARNING: "
                                          << "UPDATE val bID(" << embedded_bid
                                          << ") != key(" << key
                                          << ").  Proxy may have forgotten "
                                          << "to prepend block_id; tally="
                                          << bad_update << "\n";
                            }
                        }
                    }
                }
                batch_ops.push_back({key, op_type, std::move(val)});
                if (!is_dummy) ++real_ops;
            }
        }

        // 2) ... then execute it as ONE ORAM batch: one network round trip
        //    for all slot reads + one streamed write burst (RingORAM::
        //    access_batch), instead of ~2 round trips per access.  Results:
        //    READ -> plaintext, UPDATE/INSERT -> "" (same as access()).
        //    Set DEPART_ORAM_SEQUENTIAL=1 to get the old per-access path.
        {
            static const bool sequential = [] {
                const char* e = std::getenv("DEPART_ORAM_SEQUENTIAL");
                return e && std::string(e) == "1";
            }();
            if (sequential) {
                op_results.reserve(batch_ops.size());
                for (const auto& op : batch_ops)
                    op_results.push_back(oram->access(op.block_id, op.op, op.data));
            } else {
                op_results = oram->access_batch(batch_ops);
            }
        }

        {
            auto t1 = clk::now();
            double elapsed_ms =
                std::chrono::duration<double, std::milli>(t1 - t0).count();
            uint8_t ok = 1;

            // ── Summary header (existing protocol) ───────────────────────
            if (!write_all_local(write_fd, &real_ops,   sizeof(uint32_t))) goto child_error;
            if (!write_all_local(write_fd, &elapsed_ms, sizeof(double)))    goto child_error;
            if (!write_all_local(write_fd, &ok,         sizeof(uint8_t)))   goto child_error;

            // ── Per-op results (NEW, only sent when ok==1) ───────────────
            // Layout per op: [u32 result_len] [result_len bytes plaintext]
            // Order matches input batch order.  result_len==0 for UPDATE/INSERT.
            for (const auto& res : op_results) {
                uint32_t result_len = static_cast<uint32_t>(res.size());
                if (!write_all_local(write_fd, &result_len, sizeof(uint32_t))) goto child_error;
                if (result_len > 0 &&
                    !write_all_local(write_fd, res.data(), result_len))
                                                                              goto child_error;
            }
        }
        continue;

    child_error:
        {
            uint32_t z = 0; double d = 0.0; uint8_t fail = 0;
            // Best-effort error response: matches old protocol so parent
            // sees ok=0 and aborts.  No per-op results follow.
            (void)write(write_fd, &z,    sizeof(uint32_t));
            (void)write(write_fd, &d,    sizeof(double));
            (void)write(write_fd, &fail, sizeof(uint8_t));
        }
        break;
    }

    delete oram;
    delete conn;          // sends close opcode (-1) -> listener waits for next connection
    std::cout.flush();
    close(read_fd);
    close(write_fd);
    exit(0);
}

// ── Union helpers (9/22/26) ─────────────────────────────────────────────────
// One ORAM instance holds the i-th partition of EVERY table (P_i = ∪_T P_T,i).
// The slices must be passed in the same order they were added to the pos_map,
// because block ids were handed out in that order.
static uint32_t maxTupleWidth(const std::vector<TableSlice>& slices) {
    uint32_t w = 0;
    for (const auto& sl : slices) w = std::max(w, tupleWidthBytesFromSchema(sl.schema_str));
    return w;   // all tables in one ORAM share one block layout → widest wins
}
static uint32_t sumNoisy(const std::vector<TableSlice>& slices) {
    uint32_t n = 0; for (const auto& sl : slices) n += sl.part->noisy_synopsis; return n;
}
static uint32_t sumDummy(const std::vector<TableSlice>& slices) {
    uint32_t n = 0; for (const auto& sl : slices) n += sl.part->dummy_num; return n;
}
static uint32_t sumNoiseStashReserve(const std::vector<TableSlice>& slices) {
    uint32_t n = 0; for (const auto& sl : slices) n += sl.part->noise_stash_reserve; return n;
}

// Single-table shorthand: one slice.
void MultiRingORAM_Servers::ServerInitialization(
    const ServerInfo& server,
    const Partition&  part,
    const BinInfo&    bin_info,
    const std::vector<std::string>& data,
    const std::string& schema_str,
    uint32_t bucket_size,
    std::string oram_name,
    uint32_t block_length,
    const std::string& host,
    uint32_t S_input)
{
    std::vector<TableSlice> slices{TableSlice{0, &bin_info, &part, &data, schema_str}};
    ServerInitializationUnion(server, slices, bucket_size, std::move(oram_name),
                              block_length, host, S_input);
}

void MultiRingORAM_Servers::ServerInitializationUnion(
    const ServerInfo& server,
    const std::vector<TableSlice>& slices,
    uint32_t bucket_size,
    std::string oram_name,
    uint32_t block_length,
    const std::string& host,
    uint32_t S_input)
{
    if (slices.empty())
        throw std::logic_error("[ServerInit] no table slices for server " +
                               std::to_string(server.server_id));
    checkSliceOrder(server.server_id, slices);
    recordTupleWidth(server.server_id, maxTupleWidth(slices));

    // ── Create pipe pairs ─────────────────────────────────────────────────
    int to_child_fds[2], from_child_fds[2];
    if (pipe(to_child_fds) < 0 || pipe(from_child_fds) < 0) {
        throw std::runtime_error("pipe() failed for server " +
                                 std::to_string(server.server_id));
    }

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed for server " +
                                 std::to_string(server.server_id));
    }

    if (pid == 0) {
        // ── CHILD ────────────────────────────────────────────────────────
        // Zombie prevention: if parent dies unexpectedly (OOM, crash, SIGINT),
        // kernel sends SIGTERM to us immediately so we don't become an orphan
        // holding onto a TCP port and ORAM state indefinitely.
        // Linux-only; macOS builds skip it harmlessly.
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

        close(to_child_fds[1]);    // child doesn't write to to_child
        close(from_child_fds[0]);  // child doesn't read from from_child
        int read_fd  = to_child_fds[0];
        int write_fd = from_child_fds[1];

        // ── Close ALL pipe fds inherited from previously-forked siblings ──
        // When parent already created pipes for servers 0..N-1 and then
        // forked us (server N), we inherited those fds. Close them so EOF
        // detection and pipe lifecycle work correctly.
        for (auto& [sid, sp] : server_processes_) {
            if (sp.to_child   != -1) close(sp.to_child);
            if (sp.from_child != -1) close(sp.from_child);
        }

        // ── Wrap all child work in try-catch so parent is always notified ──
        try {
            NetIOConnector* conn = new NetIOConnector(
                host.c_str(), server.port, oram_name);

            // ── FIX (4/26/26): clear server-side stash for this ORAM ────
            // server_storage is process-global on the server side; without
            // this clear() any leftover slots from a previous run on this
            // (oram_name, port) combination would persist past the new
            // tree's slot range — invisible bytes that bloat memory across
            // benchmark sweeps.  Especially matters when noisy_synopsis
            // changes between runs (different epsilon → different tree size).
            conn->clear(oram_name);

            // One block layout for the whole instance: every table's tuples
            // are padded to the widest schema among this server's slices.
            uint32_t tuple_width_bytes = maxTupleWidth(slices);

            // A tuple must at least hold the 4-byte block id that every record
            // and every dummy carries.  Width 0 means the schema parser did not
            // recognize the column types: csv_reader.cpp's fieldWidthBytes()
            // knows int8/int16/int32/float/double/date/char(N)/varchar(N)/
            // numeric(a-b) and returns 0 for anything else (uint32, bigint,
            // text, ...).  Without this check the failure surfaces far away, as
            // std::out_of_range("basic_string") from the dummy-filler substr.
            if (tuple_width_bytes < sizeof(uint32_t)) {
                std::string schemas;
                for (const auto& sl : slices)
                    schemas += (schemas.empty() ? "" : " | ") + sl.schema_str;
                throw std::runtime_error(
                    "[ServerInitialization] tuple width is " +
                    std::to_string(tuple_width_bytes) + " bytes, need at least " +
                    std::to_string(sizeof(uint32_t)) + " for the block id.  The column "
                    "types in this schema are probably not ones csv_reader.cpp knows "
                    "(int8/int16/int32/float/double/date/char(N)/varchar(N)/numeric(a-b)): " +
                    schemas);
            }

            // ── ORAM capacity ─────────────────────────────────────────────
            // Privacy invariant: ORAM tree is ALWAYS sized to noisy_synopsis.
            //
            //   Case A (noisy >= real): noisy_synopsis = real + dummy_num
            //     → tree holds real + dummy records (unchanged behaviour)
            //
            //   Case B (noisy < real):  noisy_synopsis < real
            //     → tree is smaller than the real record count.
            //     → noise_stash_reserve = real - noisy extra stash CAPACITY is
            //       reserved (RingORAM stash_limit is pre-enlarged).  Which
            //       records sit in the stash at any moment is not fixed — they
            //       move in and out with normal eviction.
            //     → server sees the same noisy-sized tree regardless of noise
            //       sign, closing the tree-height side channel.
            // Tree = noisy_synopsis + insert_reserve.  insert_reserve is a
            // PUBLIC per-partition constant (independent of the data), so the
            // tree height still reveals only DP-noised counts.  The reserve is
            // pre-filled with dummy blocks exactly like dummy_num padding and
            // later overwritten by INSERTs of new rows (see pos_map).
            // Summed over the slices this instance holds (P_i = ∪_T P_T,i).
            const uint32_t n_filler       = sumDummy(slices) + insert_reserve_per_partition_;
            uint32_t       total_data_num = sumNoisy(slices) + insert_reserve_per_partition_;

            RingORAM* oram = new RingORAM(
                total_data_num, bucket_size, oram_name, block_length, conn, S_input,
                sumNoiseStashReserve(slices));   // extra stash CAPACITY for Case B

            // ── Insert real records ───────────────────────────────────────
            // Insert ALL real records unconditionally.
            //
            // Case A: dummy_num > 0; tree has room for all real records.
            // Case B: noise_stash_reserve > 0; tree is sized to noisy < real.
            //   All real records are inserted; the tree fills to its noisy
            //   capacity and the remainder (≈ real − noisy) stays in the stash.
            //   Which records those are changes with every eviction; only the
            //   COUNT is bounded, by noise_stash_reserve.  RingORAM.stash_limit
            //   has been enlarged by that reserve.
            uint32_t block_counter = 0;

            // Slice order == the order addTablePartitions() registered them.
            for (const auto& sl : slices) {
                for (const auto& bin_key : sl.part->index) {
                    auto bit = sl.bin_info->bin_key_to_data_indices.find(bin_key);
                    if (bit == sl.bin_info->bin_key_to_data_indices.end()) continue;
                    for (uint32_t data_idx : bit->second) {
                        std::string value = (*sl.data)[data_idx];
                        value.resize(tuple_width_bytes);
                        int32_t blockID = block_counter;
                        std::string bID((const char*)(&blockID), sizeof(uint32_t));
                        value = bID + value;
                        oram->access(block_counter, OpType::INSERT, value);
                        block_counter++;
                    }
                }
            }

            // Insert dummy padding (Case A only; Case B: dummy_num == 0 → no-op)
            for (uint32_t d = 0; d < n_filler; d++) {
                std::string dummy_value(block_length, '\0');
                dummy_value.resize(tuple_width_bytes);
                int32_t blockID = block_counter;
                std::string bID((const char*)(&blockID), sizeof(uint32_t));
                dummy_value = bID + dummy_value;
                oram->access(block_counter, OpType::INSERT, dummy_value);
                block_counter++;
            }

            std::cout << "[ServerInit] server " << server.server_id << ": inserted "
                      << (block_counter - n_filler) << " real (" << slices.size()
                      << " table slice(s)) + " << sumDummy(slices) << " dummy + "
                      << insert_reserve_per_partition_ << " insert-reserve records."
                      << "  noise_stash_reserve=" << sumNoiseStashReserve(slices)
                      << " (stash capacity headroom, not pinned records)\n";

            // ── Post-init stash flush ─────────────────────────────────────
            // Drain stash entries pushed in by Phase 2 (and normal INSERT eviction).
            // After this the stash is at most A entries — the normal operating level.
            oram->flush_stash_if_needed();

            // Signal parent: init succeeded
            uint8_t ready = 1;
            write(write_fd, &ready, sizeof(uint8_t));

            // Enter event loop (calls exit(0) internally)
            child_event_loop(oram, conn, read_fd, write_fd);

        } catch (const std::exception& e) {
            // Signal parent: init failed with 0 so it doesn't hang forever
            std::cerr << "[Server " << server.server_id << "] INIT ERROR: "
                      << e.what() << std::endl;
            uint8_t failed = 0;
            write(write_fd, &failed, sizeof(uint8_t));
            close(read_fd);
            close(write_fd);
            exit(1);
        }
        // child_event_loop calls exit(0) — we never reach here
    }

    // ── PARENT ───────────────────────────────────────────────────────────
    close(to_child_fds[0]);    // parent doesn't read from to_child
    close(from_child_fds[1]);  // parent doesn't write to from_child

    ServerProcess sp;
    sp.pid        = pid;
    sp.to_child   = to_child_fds[1];
    sp.from_child = from_child_fds[0];

    server_processes_[server.server_id] = sp;
}

// ============================================================================
//  ServerInitializationBulk (4/18/26)  —  fast-path TPC-C init
// ============================================================================
//
//  Purpose
//  -------
//  The original ServerInitialization initializes RingORAM by calling
//  insert_slot() millions of times (once per bucket×slot for dummy init,
//  plus once per record insert). On localhost this is still bottlenecked
//  by TCP round-trips and takes hours for warehouse=5.
//
//  This alternate path:
//    1. Uses silent mode on NetIOConnector so RingORAM's constructor
//       populates metadata locally but does NOT send any dummy slots.
//    2. Generates the same dummy content locally and uploads it all in
//       ONE bulk_init_tree() send_data call.
//    3. Then performs real-record access(INSERT) as usual (this part
//       is unchanged and still uses per-slot insert_slot, but its cost
//       is much smaller than the dummy init loop was).
//
//  On-server state after this function is byte-equivalent to what the
//  original ServerInitialization produces.
//
//  NOTE: The insert-only RingORAM scenario at runtime must continue to use
//  the ORIGINAL ServerInitialization path.
// ============================================================================
// Single-table shorthand: one slice.
void MultiRingORAM_Servers::ServerInitializationBulk(
    const ServerInfo& server,
    const Partition&  part,
    const BinInfo&    bin_info,
    const std::vector<std::string>& data,
    const std::string& schema_str,
    uint32_t bucket_size,
    std::string oram_name,
    uint32_t block_length,
    const std::string& host,
    uint32_t S_input)
{
    std::vector<TableSlice> slices{TableSlice{0, &bin_info, &part, &data, schema_str}};
    ServerInitializationBulkUnion(server, slices, bucket_size, std::move(oram_name),
                                  block_length, host, S_input);
}

void MultiRingORAM_Servers::ServerInitializationBulkUnion(
    const ServerInfo& server,
    const std::vector<TableSlice>& slices,
    uint32_t bucket_size,
    std::string oram_name,
    uint32_t block_length,
    const std::string& host,
    uint32_t S_input)
{
    if (slices.empty())
        throw std::logic_error("[BulkInit] no table slices for server " +
                               std::to_string(server.server_id));
    checkSliceOrder(server.server_id, slices);
    recordTupleWidth(server.server_id, maxTupleWidth(slices));

    // ── Pipe + fork (identical to ServerInitialization) ──────────────────
    int to_child_fds[2], from_child_fds[2];
    if (pipe(to_child_fds) < 0 || pipe(from_child_fds) < 0) {
        throw std::runtime_error("pipe() failed for server " +
                                 std::to_string(server.server_id));
    }

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed for server " +
                                 std::to_string(server.server_id));
    }

    if (pid == 0) {
        // ── CHILD PROCESS ────────────────────────────────────────────────
        // Zombie prevention: see ServerInitialization for rationale.
        // Linux-only; macOS builds skip it harmlessly.
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

        close(to_child_fds[1]);
        close(from_child_fds[0]);
        int read_fd  = to_child_fds[0];
        int write_fd = from_child_fds[1];

        for (auto& [sid, sp] : server_processes_) {
            if (sp.to_child   != -1) close(sp.to_child);
            if (sp.from_child != -1) close(sp.from_child);
        }

        try {
            NetIOConnector* conn = new NetIOConnector(
                host.c_str(), server.port, oram_name);

            // ── FIX (4/26/26): clear server-side stash for this ORAM ────
            // Same reason as ServerInitialization — server_storage is
            // process-global, stale slots from previous runs leak.  Doubly
            // important here because BulkInit is the path used for benchmark
            // sweeps over (epsilon × workload × variant) where tree sizes
            // legitimately change between runs.
            conn->clear(oram_name);

            uint32_t tuple_width_bytes = maxTupleWidth(slices);

            // A tuple must at least hold the 4-byte block id that every record
            // and every dummy carries.  Width 0 means the schema parser did not
            // recognize the column types: csv_reader.cpp's fieldWidthBytes()
            // knows int8/int16/int32/float/double/date/char(N)/varchar(N)/
            // numeric(a-b) and returns 0 for anything else (uint32, bigint,
            // text, ...).  Without this check the failure surfaces far away, as
            // std::out_of_range("basic_string") from the dummy-filler substr.
            if (tuple_width_bytes < sizeof(uint32_t)) {
                std::string schemas;
                for (const auto& sl : slices)
                    schemas += (schemas.empty() ? "" : " | ") + sl.schema_str;
                throw std::runtime_error(
                    "[ServerInitialization] tuple width is " +
                    std::to_string(tuple_width_bytes) + " bytes, need at least " +
                    std::to_string(sizeof(uint32_t)) + " for the block id.  The column "
                    "types in this schema are probably not ones csv_reader.cpp knows "
                    "(int8/int16/int32/float/double/date/char(N)/varchar(N)/numeric(a-b)): " +
                    schemas);
            }
            // Tree = Σ noisy_synopsis + insert_reserve.  insert_reserve is a
            // PUBLIC per-partition constant (independent of the data), so the
            // tree height still reveals only DP-noised counts.  The reserve is
            // pre-filled with dummy blocks exactly like dummy_num padding and
            // later overwritten by INSERTs of new rows (see pos_map).
            const uint32_t n_filler       = sumDummy(slices) + insert_reserve_per_partition_;
            uint32_t       total_data_num = sumNoisy(slices) + insert_reserve_per_partition_;

            // ── STEP 1: Silent-mode RingORAM construction ────────────────
            // RingORAM constructor runs normally (sets up height, stash,
            // bucket_blockdata, encryption keys, pos_map), but the per-slot
            // insert_slot loop is a no-op because conn is silent.
            conn->set_silent(true);

            RingORAM* oram = new RingORAM(
                total_data_num, bucket_size, oram_name, block_length, conn, S_input,
                sumNoiseStashReserve(slices));   // extra stash CAPACITY for Case B

            conn->set_silent(false);

            std::cout << "[BulkInit] Silent-mode ctor done "
                      << "(metadata ready, 0 bytes sent)\n";

            // ── STEP 2: Build the record list ────────────────────────────
            // All real records will go directly into the ORAM's stash, and
            // we'll upload a fresh dummy tree to the server. No per-record
            // access(INSERT), no eviction, no stash explosion.
            std::vector<std::pair<uint32_t, std::string>> records;
            records.reserve(total_data_num);

            uint32_t block_counter = 0;
            // Slice order == the order addTablePartitions() registered them.
            for (const auto& sl : slices) {
                for (const auto& bin_key : sl.part->index) {
                    auto bit = sl.bin_info->bin_key_to_data_indices.find(bin_key);
                    if (bit == sl.bin_info->bin_key_to_data_indices.end()) continue;
                    for (uint32_t data_idx : bit->second) {
                        std::string value = (*sl.data)[data_idx];
                        value.resize(tuple_width_bytes);
                        int32_t blockID = block_counter;
                        std::string bID((const char*)(&blockID), sizeof(uint32_t));
                        value = bID + value;   // prepend 4-byte block_id prefix
                        records.emplace_back(block_counter, std::move(value));
                        block_counter++;
                    }
                }
            }
            // Add dummy "records" — same as original ServerInitialization's
            // dummy_num loop. These are indistinguishable from real records
            // from the server's perspective (both in stash, stash-first reads
            // never look for them, they just pad access pattern distributions
            // that downstream privacy analyses rely on).
            for (uint32_t d = 0; d < n_filler; d++) {
                std::string dummy_value(tuple_width_bytes, '\0');
                int32_t blockID = block_counter;
                std::string bID((const char*)(&blockID), sizeof(uint32_t));
                dummy_value = bID + dummy_value.substr(sizeof(uint32_t));
                dummy_value.resize(tuple_width_bytes, '\0');
                records.emplace_back(block_counter, std::move(dummy_value));
                block_counter++;
            }

            std::cout << "[BulkInit] server " << server.server_id << ": built "
                      << records.size() << " records (" << (block_counter - n_filler)
                      << " real from " << slices.size() << " table slice(s) + "
                      << sumDummy(slices) << " dummy + "
                      << insert_reserve_per_partition_ << " insert-reserve)\n";

            // ── STEP 3: Direct stash load — no network, no eviction ──────
            // This populates ORAM's internal state (stash + pos_map) without
            // going through access(INSERT) / EvictPath / WriteBucket.
            oram->bulk_load_stash(records);

            // Free the records vector — we don't need it after bulk_load_stash
            // copied the data into the ORAM's stash. This matters because
            // records is ~200-500 MB for big tables, and we're about to stream
            // a multi-GB dummy tree that needs its own headroom.
            {
                std::vector<std::pair<uint32_t, std::string>> empty;
                records.swap(empty);
            }

            // ── STEP 4: STREAM dummy tree to server (v4, OOM-safe) ───────
            // Previously we generated the entire dummy tree into a vector
            // (2-5 GB for ORDER_LINE-sized tables), then bulk_init_tree
            // copied it into another 2-5 GB send buffer. Peak memory per
            // child process was 7-12 GB. With 8 tables init in parallel,
            // total memory easily exceeded the machine's 32 GB RAM → OOM
            // killer killed 3-6 children, causing the entire run to fail.
            //
            // New approach: generate one slot at a time via callback,
            // accumulate into a 10 MB network buffer, flush, repeat.
            // Peak memory per child: ~10 MB + ORAM internal state.
            // Total peak across 8 children: <1 GB for the dummy tree stage.
            uint32_t total_slots = oram->get_total_slot_count();
            std::cout << "[BulkInit] Streaming " << total_slots
                      << " dummy slots to server (memory-safe, constant ~10MB)\n";

            conn->bulk_init_tree_stream(
                oram_name, block_length, total_slots,
                [&oram](uint32_t i) -> SlotPayload {
                    return oram->generate_one_dummy_slot(i);
                });

            std::cout << "[BulkInit] Uploaded " << total_slots
                      << " dummy slots to server via streaming.\n";

            // ── Optional: drain stash to A entries before measurements ──
            //
            // After BulkInit, ALL records live in stash and tree has only
            // dummies.  Runtime queries will gradually evict records into
            // the tree via EarlyReshuffle / EvictPath — BUT during this
            // warm-up phase each WriteBucket scans the (huge) stash, adding
            // ~10ms × height ≈ 250ms per eviction trigger.  Throughput is
            // artificially low for the first ~12% × N queries.
            //
            // If your benchmark needs steady-state throughput from query 1
            // (e.g., comparing against papers that report post-warmup
            // numbers), define BULK_INIT_FLUSH_STASH at compile time to
            // pay the warmup cost once during init instead.  Cost: ~50s
            // per child for ORDER_LINE-sized tables, parallelizable.
            //
            // Default (undefined) preserves the original BulkInit semantics.
#ifdef BULK_INIT_FLUSH_STASH
            std::cout << "[BulkInit] BULK_INIT_FLUSH_STASH set — "
                      << "draining stash before handover...\n";
            auto flush_t0 = std::chrono::steady_clock::now();
            oram->flush_stash_if_needed();
            auto flush_t1 = std::chrono::steady_clock::now();
            double flush_secs = std::chrono::duration<double>(
                flush_t1 - flush_t0).count();
            std::cout << "[BulkInit] flush_stash done in " << flush_secs
                      << "s; stash now " << oram->get_stash_size() << " entries\n";
#else
            // No flush_stash_if_needed(): records are intentionally in stash.
            // Runtime access(READ) will evict them gradually via EarlyReshuffle
            // and EvictPath, which is fine — stash-first reads always work.
#endif

            std::cout << "[BulkInit] Complete for ORAM '" << oram_name
                      << "'. noise_stash_reserve=" << sumNoiseStashReserve(slices)
                      << " (stash capacity headroom, not pinned records)\n";

            uint8_t ready = 1;
            write(write_fd, &ready, sizeof(uint8_t));

            child_event_loop(oram, conn, read_fd, write_fd);

        } catch (const std::exception& e) {
            std::cerr << "[BulkInit Server " << server.server_id << "] ERROR: "
                      << e.what() << std::endl;
            uint8_t failed = 0;
            write(write_fd, &failed, sizeof(uint8_t));
            close(read_fd);
            close(write_fd);
            exit(1);
        }
    }

    // ── PARENT ───────────────────────────────────────────────────────────
    close(to_child_fds[0]);
    close(from_child_fds[1]);

    ServerProcess sp;
    sp.pid        = pid;
    sp.to_child   = to_child_fds[1];
    sp.from_child = from_child_fds[0];

    server_processes_[server.server_id] = sp;
}

void MultiRingORAM_Servers::waitForAllServersReady() {
    // Read the 1-byte ready signal from every child (1 = OK, 0 = failed).
    // All children initialize in parallel; this blocks until the slowest one.
    for (auto& [sid, sp] : server_processes_) {
        uint8_t ready = 0;
        if (!read_all(sp.from_child, &ready, 1)) {
            throw std::runtime_error(
                "Server process " + std::to_string(sid) +
                " closed its pipe before sending ready signal. "
                "Check that ./Server " + std::to_string(
                    // find the port from server_id_to_partition — just print sid
                    sid) + " is running.");
        }
        if (ready != 1) {
            throw std::runtime_error(
                "Server process " + std::to_string(sid) +
                " reported initialization failure (check stderr above).");
        }
        std::cout << "  [Server " << sid << "] ready ✓" << std::endl;
    }
}

void MultiRingORAM_Servers::shutdownAllServers() {
    for (auto& [sid, sp] : server_processes_) {
        // Send shutdown signal: num_ops = 0
        uint32_t shutdown = 0;
        write_all(sp.to_child, &shutdown, sizeof(uint32_t));
        close(sp.to_child);
        close(sp.from_child);
        waitpid(sp.pid, nullptr, 0);
    }
    server_processes_.clear();
}

// ============================================================================
// Batch Construction (unchanged)
// ============================================================================
// ── FIX (4/26/26): no longer silently drops ops when batch is full ──────
//
// Old behaviour: if `current_batch.operations.size() >= fixed_batch_size`,
// the function returned the unmodified batch — the new op was *thrown
// away*, with only a stdout log.  A caller who didn't pre-check size
// would silently lose ops.  This is a correctness bug for any batch
// builder that doesn't externally enforce size.
//
// New behaviour: always append.  If size now exceeds fixed_batch_size,
// log a stderr WARNING (loud, single-line, not flooding stdout).  The
// caller is still responsible for flushing-when-full; this is the
// minimum-disruption fix that turns silent data loss into a loud
// warning until the proper batch-construction layer (Step 4) is in
// place.
// ────────────────────────────────────────────────────────────────────────
BatchInfo MultiRingORAM_Servers::PutOpinBatch(
    BatchInfo current_batch, Operat current_op, uint32_t fixed_batch_size)
{
    if (current_op.type != current_batch.batch_type) {
        std::cerr << "[PutOpinBatch] WARNING: op type "
                  << static_cast<int>(current_op.type)
                  << " does not match batch type "
                  << static_cast<int>(current_batch.batch_type)
                  << "; op DROPPED.\n";
        return current_batch;
    }

    // Always append — never silently drop.
    current_batch.operations.push_back(current_op);

    if (current_batch.operations.size() > fixed_batch_size) {
        static uint64_t over_count = 0;
        over_count++;
        if (over_count == 1 || over_count % 100 == 0) {
            std::cerr << "[PutOpinBatch] WARNING: batch for server "
                      << current_batch.server_id
                      << " has " << current_batch.operations.size()
                      << " ops, exceeds FIXED_BATCH_SIZE=" << fixed_batch_size
                      << " (over-count #" << over_count << "). "
                      << "Caller must flush before adding (current call "
                      << "will not split). sendBatchToServer will still "
                      << "auto-chunk, so this is a perf/observability "
                      << "issue, not data loss.\n";
        }
    }
    return current_batch;
}

// ============================================================================
// Send Batch — exact real ops, no padding
//
// Timing model (matches proxy perspective):
//   wall_start → pipe write all ops → child processes → pipe read result → wall_end
//
// No dummy padding here: we want to measure the true cost of processing
// exactly the committed ops on each server.  Partition scalability
// (k↑ → smaller ORAM per partition → faster per op) is only visible if
// we don't inflate small batches with unnecessary dummy accesses.
//
// If FIXED_BATCH_SIZE padding is needed for security experiments, call
// sendBatchToServer_padded() instead (not used in throughput benchmarks).
// ============================================================================
TransmissionResult MultiRingORAM_Servers::sendBatchToServer(
    const ServerInfo& server, BatchInfo batch)
{
    TransmissionResult result;
    result.server_id    = server.server_id;
    result.success      = false;
    result.records_sent = 0;
    result.bytes_sent   = 0;

    // Empty batch: nothing to send — return immediately
    if (batch.operations.empty()) {
        result.success         = true;
        result.elapsed_time_ms = 0.0;
        return result;
    }

    auto it = server_processes_.find(server.server_id);
    if (it == server_processes_.end()) {
        result.error_message = "No process found for server " +
                               std::to_string(server.server_id);
        return result;
    }
    const ServerProcess& sp = it->second;

    // ── Proxy sends exactly the real ops — timing starts here ────────────
    auto wall_start = std::chrono::high_resolution_clock::now();

    const auto& ops  = batch.operations;
    size_t total_ops = ops.size();
    size_t offset    = 0;

    while (offset < total_ops) {
        // Chunk into at most FIXED_BATCH_SIZE real ops per round-trip.
        // No padding: real_count == num_to_send exactly.
        size_t real_count = std::min(
            static_cast<size_t>(FIXED_BATCH_SIZE),
            total_ops - offset);

        // Send header: exactly real_count ops (no padding)
        uint32_t num_to_send = static_cast<uint32_t>(real_count);
        if (!write_all(sp.to_child, &num_to_send, sizeof(uint32_t))) {
            result.error_message = "Pipe write failed (header)";
            return result;
        }

        // Send real ops only
        for (size_t j = 0; j < real_count; ++j) {
            const Operat& op = ops[offset + j];
            uint8_t  type_byte  = static_cast<uint8_t>(op.type);
            uint32_t key        = op.data_primary_key;
            uint32_t val_len    = static_cast<uint32_t>(op.data_value.size());
            // ── FIX (4/26/26): respect op.is_dummy ────────────────────────
            // Old code hardcoded dummy_flag = 0, defeating any padding the
            // batch builder might emit (Step 4).  Child counts is_dummy
            // ops separately for real_ops_done; padding only works if we
            // forward the flag.
            uint8_t  dummy_flag = op.is_dummy ? 1 : 0;

            if (!write_all(sp.to_child, &type_byte,  1))          goto pipe_err;
            if (!write_all(sp.to_child, &key,         4))          goto pipe_err;
            if (!write_all(sp.to_child, &val_len,     4))          goto pipe_err;
            if (val_len > 0 &&
                !write_all(sp.to_child, op.data_value.data(), val_len))
                                                                    goto pipe_err;
            if (!write_all(sp.to_child, &dummy_flag,  1))          goto pipe_err;
        }

        // ── Read result ───────────────────────────────────────────────────
        {
            uint32_t records_done = 0;
            double   elapsed_ms   = 0.0;
            uint8_t  ok           = 0;

            if (!read_all(sp.from_child, &records_done, sizeof(uint32_t))) goto pipe_err;
            if (!read_all(sp.from_child, &elapsed_ms,   sizeof(double)))   goto pipe_err;
            if (!read_all(sp.from_child, &ok,           sizeof(uint8_t)))  goto pipe_err;

            if (!ok) {
                result.error_message = "Child reported error for server " +
                                       std::to_string(server.server_id);
                return result;
            }
            result.records_sent += records_done;

            // ── FIX (4/26/26): receive per-op plaintext results ───────────
            // Child sends one [u32 len][len bytes] entry per op (in order).
            // For READ ops len > 0 (the decrypted plaintext), for UPDATE/
            // INSERT len == 0.  We accumulate into result.per_op_results
            // so the caller can pick out READ rows after the call returns.
            for (size_t j = 0; j < real_count; ++j) {
                uint32_t result_len = 0;
                if (!read_all(sp.from_child, &result_len, sizeof(uint32_t)))
                                                                          goto pipe_err;
                std::string op_result;
                if (result_len > 0) {
                    op_result.resize(result_len);
                    if (!read_all(sp.from_child, &op_result[0], result_len))
                                                                          goto pipe_err;
                }
                result.per_op_results.push_back(std::move(op_result));
            }
        }

        offset += real_count;
        continue;

    pipe_err:
        result.error_message = "Pipe I/O failed for server " +
                               std::to_string(server.server_id);
        return result;
    }

    auto wall_end = std::chrono::high_resolution_clock::now();
    result.elapsed_time_ms =
        std::chrono::duration<double, std::milli>(wall_end - wall_start).count();
    result.success = true;
    return result;
}

// ============================================================================
// Lookup helpers
// ============================================================================
int MultiRingORAM_Servers::getServerIdForBinKey(uint16_t table_id,
                                                const std::string& bin_key) const {
    return getServerIdForBinKey(qualifiedBinKey(table_id, bin_key));
}

// Legacy / already-qualified form.  Single-table builds use table_id 0, so
// getServerIdForBinKey("0|" + bin_key) is what they need; the raw-bin-key
// call below is kept working for them.
int MultiRingORAM_Servers::getServerIdForBinKey(const std::string& bin_key) const {
    auto it = bin_key_to_partition_id_.find(bin_key);
    if (it == bin_key_to_partition_id_.end())
        it = bin_key_to_partition_id_.find(qualifiedBinKey(0, bin_key));
    if (it == bin_key_to_partition_id_.end()) return -1;
    auto it2 = partition_id_to_server_id_.find(it->second);
    if (it2 == partition_id_to_server_id_.end()) return -1;
    return it2->second;
}

int MultiRingORAM_Servers::getServerIdForPartition(uint32_t partition_id) const {
    auto it = partition_id_to_server_id_.find(partition_id);
    if (it == partition_id_to_server_id_.end()) return -1;
    return it->second;
}

// ============================================================================
// Partition display (unchanged)
// ============================================================================
void printPartitionDetails(const std::vector<Partition>& partitions) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "📊 Partition Details\n";
    std::cout << std::string(70, '=') << "\n";

    uint32_t total_real = 0, total_noisy = 0, total_dummy = 0;
    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto& p = partitions[i];
        total_real  += p.synopsis;
        total_noisy += p.noisy_synopsis;
        total_dummy += p.dummy_num;

        std::cout << "\nPartition " << i << ":\n";
        std::cout << "  ├─ Bins: [";
        for (size_t j = 0; j < std::min(p.index.size(), size_t(5)); ++j) {
            std::cout << "\"" << p.index[j] << "\"";
            if (j < std::min(p.index.size(), size_t(5)) - 1) std::cout << ", ";
        }
        if (p.index.size() > 5) std::cout << ", ...";
        std::cout << "] (total: " << p.index.size() << " bins)\n";
        std::cout << "  ├─ Real data (synopsis): " << p.synopsis << "\n";
        std::cout << "  ├─ Noisy synopsis: "        << p.noisy_synopsis << "\n";
        std::cout << "  └─ Dummy count: "            << p.dummy_num << "\n";
    }

    std::cout << "\n" << std::string(70, '-') << "\n";
    std::cout << "Summary:\n";
    std::cout << "  ├─ Total partitions: " << partitions.size() << "\n";
    std::cout << "  ├─ Total real: "       << total_real  << "\n";
    std::cout << "  ├─ Total noisy: "      << total_noisy << "\n";
    std::cout << "  ├─ Total dummy: "      << total_dummy << "\n";
    std::cout << "  └─ Overhead: " << std::fixed << std::setprecision(2)
              << (total_real > 0 ? 100.0 * total_dummy / total_real : 0.0)
              << "%\n";
    std::cout << std::string(70, '=') << "\n";
}

// ============================================================================
// Step 4 (4/26/26): build batches from oram_bound_ops
// ============================================================================
//
// Algorithm:
//   1. Group ops by server_id (the partition each op is routed to).
//   2. For each server: sort its ops with READs first (stable, preserves
//      original order within READ group and within UPDATE/INSERT group).
//   3. Slice into chunks of `batch_size`.  Last chunk may be < batch_size.
//   4. If pad_last_batch, append dummy READ ops to the last chunk until
//      it reaches batch_size.  Dummies have is_dummy=true, which the
//      child's event loop and our Step 5 result placement both honor.
//
// Output preserves slot_id ↔ result mapping: per_batch_op_to_slot[b][i]
// gives the slot_id for op #i in batch #b (or UINT32_MAX for a dummy).
// ============================================================================

static constexpr uint32_t kSlotDummy = UINT32_MAX;

BatchPlan buildBatchesFromOramOps(
    const std::vector<Operat>& oram_bound_ops,
    uint32_t                   batch_size,
    bool                       pad_last_batch)
{
    BatchPlan bp;

    if (batch_size == 0) {
        std::cerr << "[buildBatchesFromOramOps] ERROR: batch_size=0; "
                     "returning empty plan.\n";
        return bp;
    }

    // ── 1. Group slot_ids by server ──────────────────────────────────────
    std::map<int, std::vector<uint32_t>> server_to_slots;
    for (uint32_t s = 0; s < oram_bound_ops.size(); ++s) {
        server_to_slots[oram_bound_ops[s].server_id].push_back(s);
    }

    // ── 2. Per server: sort RBW ──────────────────────────────────────────
    // Stable sort: key is (priority, original_slot_id).  Priority 0 = READ,
    // 1 = UPDATE/INSERT.  This both enforces RBW within a server's ops AND
    // preserves original order (= ts-then-op-position-in-txn) within each
    // group — important when we split into multiple batches because the
    // first batches will contain only reads, then mixed reads+writes only
    // at the read/write boundary, then writes-only.
    for (auto& [server_id, slots] : server_to_slots) {
        std::stable_sort(slots.begin(), slots.end(),
            [&](uint32_t a, uint32_t b) {
                int prio_a = (oram_bound_ops[a].type == OpType::READ) ? 0 : 1;
                int prio_b = (oram_bound_ops[b].type == OpType::READ) ? 0 : 1;
                return prio_a < prio_b;
            });

        // ── 3 + 4. Chunk + pad ──────────────────────────────────────────
        size_t cursor = 0;
        while (cursor < slots.size()) {
            size_t take = std::min<size_t>(batch_size, slots.size() - cursor);

            BatchInfo bi;
            bi.server_id  = server_id;
            // batch_type: descriptive only — the wire protocol cares about
            // each Operat's own .type field.  Use the FIRST op's type as
            // a "predominantly" tag (READ or UPDATE) for diagnostics.
            bi.batch_type = oram_bound_ops[slots[cursor]].type;

            std::vector<uint32_t> slot_map;
            slot_map.reserve(batch_size);

            for (size_t i = 0; i < take; ++i) {
                uint32_t slot = slots[cursor + i];
                bi.operations.push_back(oram_bound_ops[slot]);
                bi.operations.back().is_dummy = false;
                slot_map.push_back(slot);
            }

            // Pad with dummy READs if last chunk under-fills and caller wants
            if (pad_last_batch && take < batch_size) {
                size_t need = batch_size - take;
                for (size_t k = 0; k < need; ++k) {
                    Operat dummy;
                    dummy.type             = OpType::READ;
                    dummy.data_primary_key = 0;       // any block_id works; child returns "[unknown]"
                    dummy.data_value.clear();
                    dummy.server_id        = server_id;
                    dummy.is_dummy         = true;    // ← critical: child won't count it,
                                                      //   sendBatchToServer must forward this flag
                    dummy.global_op_id     = 0;       // not in any real plan
                    bi.operations.push_back(dummy);
                    slot_map.push_back(kSlotDummy);
                }
            }

            bp.batches.push_back(std::move(bi));
            bp.per_batch_op_to_slot.push_back(std::move(slot_map));

            cursor += take;
        }
    }

    return bp;
}

// ============================================================================
// Step 5: execute ORAM-bound ops (revised 9/22/26)
// ============================================================================
//
// executeOramOps is a GENERIC executor.  If a server receives more than
// batch_size real ops it splits them into several batch_size batches.
// The Obladi-style fixed-epoch guarantee — every server gets exactly ONE
// batch of bread (bwrite) ops per read (write) round — is enforced one
// layer up: runEpoch_RBR_W aborts txns so that N_server ≤ bread/bwrite
// before calling this function.  Do not describe this executor itself as
// overflow-free.
//
// pad_to_batch_size = true (security mode):
//   * the last batch of each server is padded with dummy READs, and
//   * EVERY server gets at least one full dummy batch, even with zero real
//     ops (incl. an empty op list) — otherwise which servers were contacted
//     in a round would be visible.
// pad_to_batch_size = false: throughput mode, only contacted servers.
//
// pad_uncontacted_servers (only meaningful with pad_to_batch_size = true):
//   true  (default) — the rule above: every server gets >= 1 full batch, so
//                     the set of contacted servers C is hidden.
//   false           — CONTACTED-ONLY mode: servers with real ops are padded to
//                     batch_size; servers with none get NO batch.  This LEAKS
//                     C for every round (which partitions / key ranges were
//                     accessed).  Weaker threat model — opt-in only.
//
// pos_map translation (single-threaded, before any worker starts):
//   ObjectKey = objectKeyOf(op)  →  block_id on bi.server_id
//   * routing check: the key's home server must equal the op's server_id;
//     otherwise the dispatch fails (partition-mapping bug).
//   * READ of an unknown row → demoted to a dummy; result "" (row absent).
//   * INSERT of a new row → takes a free dummy-filler block on that server
//     (PartitionPosMap::allocate) and is sent as UPDATE, because the block
//     already exists in the tree.  No free block → dispatch fails
//     ("needs IOO"); allocations are rolled back.
//   * INSERT of an existing row → sent as UPDATE (overwrite).
//   * UPDATE of an unknown row → dispatch fails (application bug; the old
//     code silently dropped it).
//   * UPDATE/INSERT values are normalized to bID(4) + tuple padded to the
//     server's load-time tuple width — the exact layout ServerInitialization
//     writes (only the proxy knows block ids; see auto_prefix_block_id_).
// The caller's oram_bound_ops is never modified.
//
// Parallel per-server dispatch (paper §3.3 / §5.2 / §6.4): one thread per
// contacted server; batches of one server stay sequential.
// ============================================================================
// Stored block = bID(4) + tuple padded to the server's load-time tuple width
// (exactly what ServerInitialization writes).  Keeping every write at that
// width matters: RingORAM::access(UPDATE) re-chunks the new ciphertext but
// does not drop chunk ids the old value used, so a SHORTER value would be
// read back with stale trailing chunks.  tuple_width == 0 → width unknown
// (not initialised through ServerInitialization*): prefix only.
static std::string normalizeWriteValue(uint32_t block_id, const std::string& value,
                                       uint32_t tuple_width) {
    int32_t bid = static_cast<int32_t>(block_id);
    const char* bid_bytes = reinterpret_cast<const char*>(&bid);
    std::string tuple;
    // Already prefixed with this block id (e.g. a READ result echoed back as
    // the new value of a read-modify-write)?  Strip it, then re-normalize.
    if (value.size() >= sizeof(int32_t) &&
        std::memcmp(value.data(), bid_bytes, sizeof(int32_t)) == 0 &&
        (tuple_width == 0 || value.size() == sizeof(int32_t) + tuple_width))
        tuple = value.substr(sizeof(int32_t));
    else
        tuple = value;
    if (tuple_width > 0) {
        if (tuple.size() > tuple_width) {
            static std::atomic<uint64_t> truncated{0};
            if (truncated++ == 0)
                std::cerr << "[executeOramOps] WARNING: write value (" << tuple.size()
                          << " B) longer than tuple width (" << tuple_width
                          << " B); truncating.  Further truncations not reported.\n";
        }
        tuple.resize(tuple_width, '\0');
    }
    return std::string(bid_bytes, sizeof(int32_t)) + tuple;
}

ExecutionResult MultiRingORAM_Servers::executeOramOps(
    const std::vector<Operat>& oram_bound_ops,
    uint32_t                   batch_size,
    bool                       pad_to_batch_size,
    bool                       pad_uncontacted_servers)
{
    ExecutionResult er;
    er.per_slot_results.assign(oram_bound_ops.size(), std::string{});

    if (batch_size == 0) {
        er.success       = false;
        er.error_message = "batch_size must be > 0";
        return er;
    }
    const bool pad_all_servers = pad_to_batch_size && pad_uncontacted_servers;
    if (oram_bound_ops.empty() && !pad_all_servers) {
        er.success = true;
        return er;
    }

    BatchPlan bp = buildBatchesFromOramOps(oram_bound_ops, batch_size, pad_to_batch_size);

    // ── Every server gets a batch in security mode (hides C) ───────────
    if (pad_all_servers) {
        std::set<int> contacted;
        for (const auto& bi : bp.batches) contacted.insert(bi.server_id);
        for (const auto& kv : partition_id_to_server_id_) {
            const int sid = kv.second;
            if (!contacted.insert(sid).second) continue;
            BatchInfo bi;
            bi.server_id  = sid;
            bi.batch_type = OpType::READ;
            std::vector<uint32_t> slot_map;
            for (uint32_t k = 0; k < batch_size; ++k) {
                Operat dummy;
                dummy.type             = OpType::READ;
                dummy.data_primary_key = 0;
                dummy.server_id        = sid;
                dummy.is_dummy         = true;
                dummy.global_op_id     = 0;
                bi.operations.push_back(dummy);
                slot_map.push_back(kSlotDummy);
            }
            bp.batches.push_back(std::move(bi));
            bp.per_batch_op_to_slot.push_back(std::move(slot_map));
        }
    }
    er.total_batches_sent = bp.batches.size();

    // ── pos_map translation (main thread) ──────────────────────────────
    std::vector<std::pair<int, ObjectKey>> allocated;   // for rollback
    std::vector<uint32_t>                  absent_slots; // READs of absent rows
    auto fail = [&](const std::string& msg) {
        for (const auto& [sid, k] : allocated) pos_map_.release(sid, k);
        er.success       = false;
        er.error_message = msg;
        std::cerr << "[executeOramOps] ERROR: " << msg << "\n";
        return er;
    };

    for (size_t b = 0; b < bp.batches.size(); ++b) {
        BatchInfo&                   bi       = bp.batches[b];
        const std::vector<uint32_t>& slot_map = bp.per_batch_op_to_slot[b];
        for (size_t i = 0; i < bi.operations.size(); ++i) {
            Operat& op = bi.operations[i];
            if (op.is_dummy) continue;

            const ObjectKey k    = objectKeyOf(op);
            const int       home = pos_map_.serverOf(k);
            if (home >= 0 && home != bi.server_id)
                return fail("routing: " + k.str() + " lives on server " +
                            std::to_string(home) + " but was routed to server " +
                            std::to_string(bi.server_id));

            uint32_t bid = pos_map_.lookup(bi.server_id, k);
            if (bid == PartitionPosMap::kNoBlock) {
                if (op.type == OpType::READ) {
                    op.is_dummy = true;                 // row does not exist
                    if (i < slot_map.size() && slot_map[i] != kSlotDummy)
                        absent_slots.push_back(slot_map[i]);
                    continue;
                }
                if (op.type != OpType::INSERT)
                    return fail("UPDATE of unknown row " + k.str() + " on server " +
                                std::to_string(bi.server_id));
                bid = pos_map_.allocate(bi.server_id, k);
                if (bid == PartitionPosMap::kNoBlock)
                    return fail("no free block on server " + std::to_string(bi.server_id) +
                                " for INSERT of " + k.str() +
                                " (insert reserve exhausted; reorganize/repartition "
                                "at the next epoch boundary or raise "
                                "setInsertReservePerPartition)");
                allocated.emplace_back(bi.server_id, k);
            }
            if (op.type == OpType::INSERT) op.type = OpType::UPDATE;   // block exists
            op.data_primary_key = bid;
            if (op.type != OpType::READ && auto_prefix_block_id_)
                op.data_value = normalizeWriteValue(bid, op.data_value,
                                                    tupleWidthOf(bi.server_id));
        }
    }

    // ── Group batch indices by server ───────────────────────────────────
    std::map<int, std::vector<size_t>> batches_by_server;
    for (size_t b = 0; b < bp.batches.size(); ++b)
        batches_by_server[bp.batches[b].server_id].push_back(b);

    struct ServerOutcome {
        bool        ok = true;
        std::string error_message;
        uint32_t    real_ops  = 0;
        uint32_t    dummy_ops = 0;
    };
    std::vector<std::pair<int, std::vector<size_t>>> server_jobs(
        batches_by_server.begin(), batches_by_server.end());
    std::vector<ServerOutcome> outcomes(server_jobs.size());

    // ── Network emulation (NetEmu.h): bytes each shard's padded batches put on the wire this round.
    //    Every worker sleeps RTT + bytes/BW before its batches, in parallel, so the round pays the
    //    largest delay once.  Off (zero cost) unless DEPART_NET_RTT_MS / DEPART_NET_BW_MBPS is set.
    std::vector<double> net_delay_ms(server_jobs.size(), 0.0);
    if (netemu::config().enabled()) {
        std::vector<uint64_t> job_bytes(server_jobs.size(), 0);
        uint64_t round_bytes = 0, nbatches = 0;
        for (size_t j = 0; j < server_jobs.size(); ++j) {
            const uint64_t per_slot = netemu::bytesPerSlot(server_jobs[j].first);
            for (size_t b : server_jobs[j].second) {
                job_bytes[j] += uint64_t(bp.batches[b].operations.size()) * per_slot;
                ++nbatches;
            }
            round_bytes += job_bytes[j];
        }
        double max_delay = 0.0;
        for (size_t j = 0; j < server_jobs.size(); ++j) {
            net_delay_ms[j] = netemu::delayMs(job_bytes[j], round_bytes);
            max_delay = std::max(max_delay, net_delay_ms[j]);
        }
        if (round_bytes > 0) {
            netemu::Counters& nc = netemu::counters();
            nc.bytes   += round_bytes;
            nc.batches += nbatches;
            nc.rounds  += 1;
            nc.round_delay_us += static_cast<uint64_t>(std::llround(max_delay * 1000.0));
        }
    }

    auto t_start = std::chrono::high_resolution_clock::now();

    // Worker: all batches of ONE server, sequentially.  Workers only read
    // shared state (translation already done), write disjoint slots of
    // er.per_slot_results, and talk to different children.
    auto run_server = [&](size_t job_idx) {
        ServerOutcome&             oc        = outcomes[job_idx];
        const std::vector<size_t>& batch_ids = server_jobs[job_idx].second;
        netemu::sleepMs(net_delay_ms[job_idx]);      // emulated proxy <-> shard network (0 if off)

        for (size_t b : batch_ids) {
            BatchInfo&                   bi       = bp.batches[b];
            const std::vector<uint32_t>& slot_map = bp.per_batch_op_to_slot[b];

            if (slot_map.size() != bi.operations.size()) {
                oc.ok            = false;
                oc.error_message = "internal: slot_map size mismatch";
                return;
            }

            ServerInfo stub;
            stub.server_id = bi.server_id;
            TransmissionResult tr = sendBatchToServer(stub, bi);

            if (!tr.success) {
                oc.ok            = false;
                oc.error_message = "Batch " + std::to_string(b) +
                                   " (server " + std::to_string(bi.server_id) +
                                   ") failed: " + tr.error_message;
                return;
            }

            oc.real_ops  += tr.records_sent;
            oc.dummy_ops += (bi.operations.size() - tr.records_sent);

            if (tr.per_op_results.size() != bi.operations.size()) {
                std::cerr << "[executeOramOps] WARNING: batch " << b
                          << " expected " << bi.operations.size()
                          << " per-op results, got " << tr.per_op_results.size() << "\n";
            }

            size_t n = std::min(slot_map.size(), tr.per_op_results.size());
            for (size_t i = 0; i < n; ++i) {
                uint32_t slot = slot_map[i];
                if (slot == kSlotDummy) continue;
                er.per_slot_results[slot] = std::move(tr.per_op_results[i]);
            }
        }
    };

    std::vector<std::thread> workers;
    workers.reserve(server_jobs.size());
    for (size_t j = 0; j < server_jobs.size(); ++j)
        workers.emplace_back(run_server, j);
    for (auto& w : workers) w.join();

    auto t_end = std::chrono::high_resolution_clock::now();
    er.total_elapsed_ms =
        std::chrono::duration<double, std::milli>(t_end - t_start).count();

    er.success = true;
    for (const auto& oc : outcomes) {
        er.total_real_ops_sent  += oc.real_ops;
        er.total_dummy_ops_sent += oc.dummy_ops;
        if (!oc.ok) {
            er.success = false;
            if (er.error_message.empty()) er.error_message = oc.error_message;
        }
    }
    if (!er.success) {
        for (const auto& [sid, k] : allocated) pos_map_.release(sid, k);
    }
    // READs of absent rows return "" (not the dummy block's content).
    for (uint32_t slot : absent_slots) er.per_slot_results[slot].clear();

    std::cout << "[executeOramOps]"
              << " batches="      << er.total_batches_sent
              << "  servers="     << server_jobs.size() << " (parallel)"
              << "  batch_size="  << batch_size
              << "  real_ops="    << er.total_real_ops_sent
              << "  dummy_ops="   << er.total_dummy_ops_sent
              << "  inserts_placed=" << allocated.size()
              << "  absent_reads=" << absent_slots.size()
              << "  elapsed_ms="  << er.total_elapsed_ms
              << std::endl;

    return er;
}