// Per-server process model + fixed batch size with padding.
//
// REBUILT 9/22/26 — the repo's copy had been overwritten by the patch text.
// Every declaration here is taken from the definitions in
// src/MultiRingORAM_Servers.cpp, so the two are guaranteed to match.
//
// Architecture
// ─────────────
//  Parent (proxy/benchmark) ─── pipe ──► Child process i
//                                          owns NetIOConnector_i
//                                          owns RingORAM_i
//                           ◄── pipe ─── sends results back
//
// One `server_id` == one child == one RingORAM == one ORAM INSTANCE.
// It is NOT a physical machine: with the union layout below, an instance
// holds partition i of EVERY table, and several instances may live on one
// machine (see setPhysicalServer / physicalServerOf).
//
#ifndef MULTIRINGORAM_SERVERS_H
#define MULTIRINGORAM_SERVERS_H

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <sys/types.h>          // pid_t
#include <unordered_map>
#include <vector>

#include "NetIOConnector.h"     // NetIOConnector, SlotPayload
#include "RingORAM.h"           // RingORAM, OpType
#include "Repartition.h"        // BinInfo, Partition
#include "TpccSchema.h"         // tupleWidthBytesFromSchema
#include "ObjectKey.h"          // ObjectKey, TpccTable, tpcc_key::
#include "PartitionPosMap.h"    // PartitionPosMap

// Max real ops per pipe round-trip inside sendBatchToServer.  This is a
// TRANSPORT chunk size (it prevents one huge write from deadlocking the pipe),
// not the Obladi batch size — that one is EpochConfig::bread / bwrite, passed
// to executeOramOps.  It lived in the header this file replaces; if you had a
// different value, set it back here.
// 10/8/26: raised from 500.  The child now reads a whole chunk from the pipe
// before executing it (no pipe deadlock), and each chunk is ONE ORAM batch =
// one network round trip, so a chunk must hold a full bread/bwrite batch
// (Obladi gets k x 512 write slots on its single ORAM).
constexpr uint32_t FIXED_BATCH_SIZE = 8192;

// ============================================================================
// One operation of a transaction, as it travels proxy → child.
// ============================================================================
struct Operat {
    OpType      type = OpType::READ;
    uint32_t    data_primary_key = 0;   // rewritten to block_id inside executeOramOps
    std::string data_value;
    int         server_id = 0;          // routing (ORAM instance)
    bool        last_one         = false;
    bool        read_marker      = false;
    uint32_t    txn_timestamp_id = 0;
    bool        is_dummy         = false;   // padding dummy, not counted as a real op
    uint32_t    global_op_id     = 0;       // unique within one epoch

    // ── Logical row identity (ObjectKey.h).  Defaults keep single-table
    // benchmarks working unchanged: ObjectKey = (0, 0, 0, data_primary_key).
    uint16_t    table_id   = 0;
    uint32_t    w_id       = 0;
    uint32_t    d_id       = 0;
    uint64_t    logical_id = UINT64_MAX;    // kLogicalIdFromPk → use data_primary_key
};

// ============================================================================
// Cluster bookkeeping
// ============================================================================
struct ServerInfo {
    int                      server_id    = 0;   // ORAM instance id
    int                      port         = 0;
    pid_t                    pid          = 0;   // child pid, if the caller tracks it
    uint32_t                 partition_id = 0;   // partition this instance holds
    std::vector<std::string> assigned_bins;
    std::vector<uint32_t>    assigned_data_indices;
};

struct ServerProcess {
    pid_t pid        = -1;
    int   to_child   = -1;
    int   from_child = -1;
};

struct BatchInfo {
    int                 server_id  = 0;
    OpType              batch_type = OpType::READ;
    std::vector<Operat> operations;
};

struct TransmissionResult {
    int         server_id       = -1;
    bool        success         = false;
    uint32_t    records_sent    = 0;
    size_t      bytes_sent      = 0;
    double      elapsed_time_ms = 0.0;
    std::string error_message;
    // One entry per op of the batch, same order.
    //   READ            → decrypted plaintext (bID(4) + tuple + zero padding)
    //   UPDATE / INSERT → empty string
    std::vector<std::string> per_op_results;
};

// ============================================================================
// Batch building / dispatch results
// ============================================================================
struct BatchPlan {
    std::vector<BatchInfo> batches;
    // per_batch_op_to_slot[b][i] = slot id of op i of batch b (UINT32_MAX = dummy)
    std::vector<std::vector<uint32_t>> per_batch_op_to_slot;
};

struct ExecutionResult {
    // Indexed by slot id = position in the oram_bound_ops vector passed in.
    std::vector<std::string> per_slot_results;
    bool        success = true;
    std::string error_message;
    double      total_elapsed_ms     = 0.0;
    size_t      total_real_ops_sent  = 0;
    size_t      total_dummy_ops_sent = 0;
    size_t      total_batches_sent   = 0;
};



// ============================================================================
// Union layout: each table is partitioned on its own, then partition i of
// EVERY table is unioned into shard i:
//     P_i = P_CUSTOMER,i ∪ P_ORDERS,i ∪ P_ORDER_LINE,i ∪ …  →  ORAM instance i
// ============================================================================
struct TableSlice {
    uint16_t                        table_id = 0;
    const BinInfo*                  bin_info = nullptr;
    const Partition*                part     = nullptr;
    const std::vector<std::string>* data     = nullptr;
    std::string                     schema_str;
};

// What the pos_map registered for one ORAM instance, in build order.
struct TableSliceInfo {
    uint16_t table_id              = 0;
    uint32_t local_partition_index = 0;
    uint32_t first_block           = 0;
    uint32_t num_rows              = 0;
};

// ============================================================================
// Free helpers (defined in MultiRingORAM_Servers.cpp)
// ============================================================================
extern std::vector<pid_t> server_pids;
extern std::mutex         cout_mutex;

void  safe_print(const std::string& msg);
void  cleanup_servers(int signum);
// NOTE: MultiRingORAM_Servers.cpp also defines a global readCSV(), but it is
// deliberately NOT declared here: several benchmarks define their own
// `static readCSV`, which would clash with a declaration in this header.
pid_t startServer(int port, const std::string& server_exec);
bool  waitForServer(int port, int max_attempts = 30);
void  printPartitionDetails(const std::vector<Partition>& partitions);

// Groups ops by server, sorts READ-before-WRITE, slices into batch_size
// chunks and (optionally) pads the last chunk with dummy READs.
BatchPlan buildBatchesFromOramOps(const std::vector<Operat>& oram_bound_ops,
                                  uint32_t                   batch_size,
                                  bool                       pad_last_batch);

// ============================================================================
class MultiRingORAM_Servers {
public:
    // ── Position-map construction ────────────────────────────────────────
    // Single-table shorthand (= begin + addTablePartitions(table 0) + finish).
    void distributeDataToPartitions(const BinInfo& bin_info,
                                    const std::vector<Partition>& partitions,
                                    std::vector<ServerInfo>& servers);
    // Data-aware overload: the ObjectKey mapper needs the record bytes.
    void distributeDataToPartitions(const BinInfo& bin_info,
                                    const std::vector<Partition>& partitions,
                                    std::vector<ServerInfo>& servers,
                                    const std::vector<std::string>& data);

    // Multi-table build.  begin() resets; every addTablePartitions() APPENDS;
    // finish() closes it.  Pass the SAME `servers` vector for every table:
    // partition i of each table lands on ORAM instance i.  Block ids continue
    // across tables on an instance, so each instance must then be initialised
    // ONCE, with all of its slices, in the order they were added here.
    void beginPositionMapBuild();
    void addTablePartitions(uint16_t table_id,
                            const BinInfo& bin_info,
                            const std::vector<Partition>& partitions,
                            std::vector<ServerInfo>& servers,
                            const std::vector<std::string>& data);
    void addTablePartitions(TpccTable t,
                            const BinInfo& bin_info,
                            const std::vector<Partition>& partitions,
                            std::vector<ServerInfo>& servers,
                            const std::vector<std::string>& data) {
        addTablePartitions(static_cast<uint16_t>(t), bin_info, partitions, servers, data);
    }
    void finishPositionMapBuild();

    // Slices registered for one ORAM instance, in build order.
    const std::vector<TableSliceInfo>& sliceLayout(int server_id) const;

    // ── Key mapping / lookup ─────────────────────────────────────────────
    // ObjectKey extractor used to build the pos_map (required for TPC-C:
    // composite keys and many tables).
    using ObjectKeyMapper =
        std::function<ObjectKey(uint32_t /*data_idx*/, const std::string& /*record*/)>;
    void setObjectKeyMapper(ObjectKeyMapper m) { key_mapper_ = std::move(m); }

    // Legacy fallback when no ObjectKeyMapper is set: ObjectKey = (0,0,0,pk).
    using PrimaryKeyMapper =
        std::function<uint32_t(uint32_t /*data_idx*/, const std::string& /*record*/)>;
    void setPrimaryKeyMapper(PrimaryKeyMapper m) { pk_mapper_ = std::move(m); }

    uint32_t lookupBlockId(int server_id, const ObjectKey& key) const;
    uint32_t lookupBlockId(int server_id, uint32_t primary_key) const;   // legacy

    int  serverOfKey(const ObjectKey& key) const { return pos_map_.serverOf(key); }
    const PartitionPosMap& posMap() const { return pos_map_; }
    void printPkMapStats() const;

    // Bin keys are raw attribute values and repeat across tables, so routing
    // is per (table, bin_key).
    int getServerIdForBinKey(uint16_t table_id, const std::string& bin_key) const;
    int getServerIdForBinKey(const std::string& bin_key) const;          // legacy / qualified
    int getServerIdForPartition(uint32_t partition_id) const;
    static std::string qualifiedBinKey(uint16_t table_id, const std::string& bin_key);

    // ── Configuration ────────────────────────────────────────────────────
    // Prepend bID(4) and pad to the load-time tuple width for UPDATE/INSERT
    // (default on).  Turn OFF only for legacy code that builds bID+tuple itself.
    void setAutoPrefixBlockId(bool on) { auto_prefix_block_id_ = on; }

    // PUBLIC per-partition insert capacity: blocks pre-filled as dummies and
    // taken over by INSERTs of new rows.  Independent of the data, so the tree
    // height still reveals only DP-noised counts.  Call BEFORE the pos_map
    // build and before ServerInitialization*.
    void setInsertReservePerPartition(uint32_t n) { insert_reserve_per_partition_ = n; }

    // Tuple width an instance was loaded with (0 = unknown).
    uint32_t tupleWidthOf(int server_id) const;

    // ORAM instance → physical machine (reporting only).
    void setPhysicalServer(int oram_instance_id, int physical_id) {
        physical_server_[oram_instance_id] = physical_id;
    }
    int physicalServerOf(int oram_instance_id) const {
        auto it = physical_server_.find(oram_instance_id);
        return it == physical_server_.end() ? -1 : it->second;
    }

    // ── Server (child) lifecycle ─────────────────────────────────────────
    // Single-table shorthands: forward to the *Union variants with one slice.
    void ServerInitialization(const ServerInfo& server,
                              const Partition&  part,
                              const BinInfo&    bin_info,
                              const std::vector<std::string>& data,
                              const std::string& schema_str,
                              uint32_t bucket_size,
                              std::string oram_name,
                              uint32_t block_length,
                              const std::string& host,
                              uint32_t S_input);
    void ServerInitializationBulk(const ServerInfo& server,
                                  const Partition&  part,
                                  const BinInfo&    bin_info,
                                  const std::vector<std::string>& data,
                                  const std::string& schema_str,
                                  uint32_t bucket_size,
                                  std::string oram_name,
                                  uint32_t block_length,
                                  const std::string& host,
                                  uint32_t S_input);

    // Union init: one ORAM instance, all of its table slices, in
    // addTablePartitions() order (checked).  Tuple width = widest schema among
    // the slices; capacity = Σ noisy_synopsis + insert reserve.
    void ServerInitializationUnion(const ServerInfo& server,
                                   const std::vector<TableSlice>& slices,
                                   uint32_t bucket_size,
                                   std::string oram_name,
                                   uint32_t block_length,
                                   const std::string& host,
                                   uint32_t S_input);
    void ServerInitializationBulkUnion(const ServerInfo& server,
                                       const std::vector<TableSlice>& slices,
                                       uint32_t bucket_size,
                                       std::string oram_name,
                                       uint32_t block_length,
                                       const std::string& host,
                                       uint32_t S_input);

    void waitForAllServersReady();
    void shutdownAllServers();

    // ── Execution ────────────────────────────────────────────────────────
    TransmissionResult sendBatchToServer(const ServerInfo& server, BatchInfo batch);
    BatchInfo PutOpinBatch(BatchInfo current_batch, Operat current_op,
                           uint32_t fixed_batch_size);

    // Generic executor: a server with more than batch_size real ops gets
    // several batches.  The Obladi-style fixed-epoch guarantee (one batch of
    // bread/bwrite per server per round) is enforced one layer up, by
    // runEpoch_RBR_W in Queries_Receiving.
    // pad_uncontacted_servers (only with pad_to_batch_size = true):
    //   true  — every server gets >= 1 padded batch per round (hides C)
    //   false — only servers with real ops get a batch, padded to batch_size;
    //           the contacted set C of each round is visible (paper setting)
    ExecutionResult executeOramOps(const std::vector<Operat>& oram_bound_ops,
                                   uint32_t                   batch_size,
                                   bool                       pad_to_batch_size = true,
                                   bool                       pad_uncontacted_servers = true);

private:
    // Pipe I/O helpers (handle partial reads/writes)
    static bool write_all(int fd, const void* buf, size_t n);
    static bool read_all(int fd, void* buf, size_t n);

    void recordTupleWidth(int server_id, uint32_t width);
    void checkSliceOrder(int server_id, const std::vector<TableSlice>& slices) const;

    // Routing maps.  partition ids are GLOBAL across tables; bin keys are
    // table-qualified (see qualifiedBinKey).
    std::map<std::string, uint32_t> bin_key_to_partition_id_;
    std::map<uint32_t, int>         partition_id_to_server_id_;
    std::map<int, uint32_t>         server_id_to_partition_id_;

    // Child processes, keyed by ORAM instance id.
    std::map<int, ServerProcess>    server_processes_;

    std::unordered_map<uint32_t, uint32_t> data_primary_key_to_attr_key;

    // Position map: ObjectKey → (server_id, block_id).  Replaces the old
    // pk_to_blockid_[server_id][uint32 pk], which collided across tables.
    PartitionPosMap          pos_map_;
    ObjectKeyMapper          key_mapper_;                  // empty → pk_mapper_
    PrimaryKeyMapper         pk_mapper_ =
        [](uint32_t data_idx, const std::string&) { return data_idx; };

    bool                     auto_prefix_block_id_ = true;
    uint32_t                 insert_reserve_per_partition_ = 0;
    std::map<int, uint32_t>  tuple_width_by_server_;       // guarded by a mutex in the .cpp

    // Position-map build state (union across tables).
    std::map<int, uint32_t>  server_next_block_;           // next free block id per instance
    std::map<int, uint32_t>  server_dummy_total_;          // Σ dummy_num per instance
    std::map<int, std::vector<TableSliceInfo>> server_slices_;
    std::map<int, int>       physical_server_;             // ORAM instance → machine
    uint32_t                 next_global_partition_id_ = 0;
    bool                     building_pos_map_ = false;
};

#endif // MULTIRINGORAM_SERVERS_H