//
// Created by Xining Yuan on 2/24/26.
//
// ============================================================================
// REVISION 9/22/26 — MVTSO / epoch-pipeline correctness fixes
// ----------------------------------------------------------------------------
//  (1) MVTSO write rule: a writer T_w aborts only if a *later* reader already
//      read the version T_w's write should have preceded (per-version read
//      marker).  Old rule "abort any UPDATE with ts < max read ts on the key"
//      wrongly aborted T1 in  T1:W(x) → T3:R(x)  (Obladi Fig. 5 is legal).
//  (2) Real write→read dependency tracking + cascading aborts (fixed point).
//  (3) VersionCache stores version metadata (value, writer_ts, read marker),
//      not just key → value.
//  (4) Concurrency-control identity = logical ObjectKey (table, w_id, d_id,
//      64-bit logical_id).  One logical row ⇔ one key; composite PKs such as
//      ORDER_LINE (W, D, O, OL_NUMBER) are packed collision-free by the
//      tpcc_key:: encoders.  server_id is routing only.
//  (5) Txn completeness: last_one must be on the final op only, and an
//      optional declared_op_count catches gaps (q2 missing).
//  (6) push_txn race: accepting_ is checked under queue_mu_; closing intake
//      and the final drain happen atomically under the same lock.
//  (7) Obladi bread / bwrite overflow: a txn whose reads (writes) do not fit
//      in the fixed-size per-server batch is aborted instead of spilling
//      into an extra batch.
//  (8) WRITE = {UPDATE, INSERT, (DELETE)} consistently everywhere.
//  (9) Dependent-query execution: a txn is a sequence of stages; stage k+1
//      is produced from the results of stages 0..k and runs in a later read
//      sub-epoch (execution cursor / ready-op model).
//
// REVISION 9/22/26 (b)
//  (10) ObjectKey.logical_id is 64-bit + per-table TPC-C encoders.
//  (11) Staged txns are data on the wire: (txn_type, params).  The stage
//       logic lives in a proxy-side TxnEngine registry (trusted proxy
//       transaction engine), not in a client-supplied closure.
//  (12) Placement check: within one epoch a logical object must stay on
//       one shard; a mismatching server_id is a fatal error (default) or
//       aborts the txn (PLACEMENT_MISMATCH).  Repartitioning must happen at
//       epoch boundaries.
//  (13) Write-overflow victim choice documented as an implementation
//       policy, not an Obladi requirement.
//
// REVISION 9/22/26 (c)
//  (14) ObjectKey moved to ObjectKey.h and shared with the storage layer:
//       the ORAM pos_map (PartitionPosMap) is keyed by the same ObjectKey,
//       so MVTSO identity == ORAM routing identity.
//
// REVISION 10/9/26 — commutative field increments ("escrow")
//  (15) A stage may emit FieldDelta{row, field, amount} instead of an UPDATE.
//       Deltas bypass MVTSO: they create no version and set no read marker.
//       The write phase sums the deltas of every COMMITTED txn per (row,
//       field) and applies them once, on top of the row's latest surviving
//       version (e.g. a New-Order's DISTRICT write) or, if no txn wrote the
//       row, on top of its base value fetched this epoch.  Deltas of aborted
//       (incl. cascaded) txns are dropped.  The row is written once, as
//       before, so the write batch and the access pattern do not change.
//       Requirement: a delta field is never read for a decision and never
//       written any other way (other writers carry it through unchanged).
//  (16) TxnStage::stable_reads: reads that use only columns no txn ever
//       writes.  They read the committed base and set no read marker, so
//       they never make an older writer of the row abort (WRITE_TOO_LATE).
//       Placement and batch accounting are unchanged.
// ============================================================================

#ifndef QUERIES_RECEIVING_H
#define QUERIES_RECEIVING_H

#include "MultiRingORAM_Servers.h"
#include "ObjectKey.h"
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// ObjectKey, objectKeyOf, setObjectKey, TpccTable and tpcc_key:: live in
// ObjectKey.h so that the storage layer's pos_map uses the SAME identity.

// Single definition of "write" used by every phase.  Anything that is not a
// READ creates a version (UPDATE, INSERT, and DELETE if you add it to OpType).
inline bool isWriteOp(OpType t) { return t != OpType::READ; }

// ============================================================================
// Abort reasons (diagnostics + reported back to the client)
// ============================================================================
enum class AbortReason : uint8_t {
    NONE = 0,
    INCOMPLETE,             // malformed/incomplete txn (no last_one, gap, ...)
    UNFINISHED,             // stages left when the read phase ended (R too small)
    WRITE_TOO_LATE,         // MVTSO: a later reader already read the prior version
    INTERMEDIATE_OBSERVED,  // txn rewrote a key after another txn read its first write
    READ_BATCH_FULL,        // Obladi: no read batch can hold the txn's reads
    WRITE_BATCH_FULL,       // write batch would overflow bwrite; this txn was
                            // chosen as the victim (see EpochRunner::writePhase)
    CASCADE,                // read an uncommitted version whose writer aborted
    DISPATCH_FAILED,        // ORAM dispatch failed; whole epoch aborted
    UNKNOWN_TXN_TYPE,       // staged txn_type not registered in the TxnEngine
    PLACEMENT_MISMATCH,     // same ObjectKey routed to two shards in one epoch
    DELTA_NO_BASE           // FieldDelta on a row with no value this epoch (row
                            // not read by anyone, or no delta applier set)
};
const char* toString(AbortReason r);

// ============================================================================
// Transactions (wire format) and the proxy-side transaction engine
// ============================================================================
// A txn arrives at the trusted proxy as plain data — no code crosses the
// network.  Two kinds:
//
//  (a) ASSEMBLED  (txn_type == kAssembledTxnType):
//      `operations` holds every op, the final op has last_one = true.
//      push_txn() receives the whole txn at once — it does NOT accept
//      query-by-query arrival.  Set declared_op_count (> 0) if the network
//      layer knows how many ops the client sent; the proxy then also rejects
//      txns with gaps.
//
//  (b) STAGED  (txn_type != kAssembledTxnType; dependent queries, e.g. TPC-C,
//      DePart T3/T4/T5/T7):
//      the client sends only (txn_type, params) and leaves `operations`
//      empty.  The proxy's TxnEngine — the application's transaction state
//      machine, hosted inside the trusted proxy — produces
//          stage 0             from (txn_type, params)
//          stage k (k ≥ 1)     from (txn_type, params, results of stages 0..k-1)
//      One stage runs per read sub-epoch (stages that need no new ORAM fetch
//      run immediately).  A txn not `done` after R read sub-epochs aborts
//      with UNFINISHED.  The proxy appends every executed stage to
//      `operations`, so AssembledTxnResult::per_op_result lines up with the
//      full op list.
// ============================================================================
using TxnTypeId = uint16_t;
constexpr TxnTypeId kAssembledTxnType = 0;

// Commutative increment of one numeric field of one row (see (15)).
struct FieldDelta {
    Operat   row;           // key + routing of the row (type / value ignored)
    uint32_t field  = 0;    // application-defined field id (e.g. column index)
    int64_t  amount = 0;    // application-defined units (e.g. cents)
};

struct TxnStage {
    std::vector<Operat> ops;
    bool done = true;       // true → this is the final stage
    // (15) commutative increments, applied in the write phase iff the txn commits
    std::vector<FieldDelta> deltas;
    // (16) indices into `ops` of READs that only use never-written columns
    std::vector<uint32_t>   stable_reads;
};

// Applies the summed deltas of one row.  `row` is the row's latest value
// (an uncommitted version, or the base value as fetched from ORAM);
// `sums` maps field id -> total amount.  Returns the new row value as it
// should be written (already formatted / padded), or "" if `row` cannot be
// parsed (the contributing txns then abort with DELTA_NO_BASE).
using DeltaApplyFn = std::function<std::string(
    const ObjectKey&                   key,
    const std::string&                 row,
    const std::map<uint32_t, int64_t>& sums)>;

// Stage function of one txn type.  Must be deterministic in its inputs.
//   stage_id      : 0, 1, 2, ...
//   params        : the txn's wire parameters
//   prior_results : prior_results[j][i] = result of op i of stage j
//                   (one string per op; "" for writes)
using StageFn = std::function<TxnStage(
    uint32_t                                     stage_id,
    const std::vector<int64_t>&                  params,
    const std::vector<std::vector<std::string>>& prior_results)>;

class TxnEngine {
public:
    // Register the state machine of one txn type (type != kAssembledTxnType).
    void registerType(TxnTypeId type, std::string name, StageFn fn);

    bool               knows(TxnTypeId type) const { return types_.count(type) > 0; }
    const std::string& name(TxnTypeId type) const;

    TxnStage stage(TxnTypeId type, uint32_t stage_id,
                   const std::vector<int64_t>& params,
                   const std::vector<std::vector<std::string>>& prior_results) const;

    // (15) Needed iff some stage emits FieldDeltas.
    void setDeltaApplier(DeltaApplyFn fn) { apply_ = std::move(fn); }
    const DeltaApplyFn& deltaApplier() const { return apply_; }

private:
    struct Entry { std::string name; StageFn fn; };
    std::map<TxnTypeId, Entry> types_;
    DeltaApplyFn               apply_;
    static const std::string   unknown_;
};

struct txn {
    std::vector<Operat> operations;
    uint32_t timestamp_id = 0;
    bool is_committed = true;
    // ── wire fields (appended so existing aggregate-inits keep compiling) ─
    TxnTypeId            txn_type = kAssembledTxnType;
    std::vector<int64_t> params;                 // staged txns only
    uint32_t             declared_op_count = 0;  // assembled txns only; 0 = not declared
    uint64_t             client_txn_id = 0;      // opaque; echoed in AssembledTxnResult
    // ── proxy output ─────────────────────────────────────────────────────
    AbortReason abort_reason = AbortReason::NONE;
};

// Completeness check for an assembled txn: non-empty, last op has last_one,
// no earlier op has last_one, and (if declared) no missing ops.
bool isAssembledTxnComplete(const txn& t);

// ============================================================================
// DedupPlan / OpDecision
// ============================================================================
struct OpDecision {
    enum class Kind {
        TO_ORAM,              // result = er.per_slot_results[slot_id]
        BUFFERED_FROM_WRITE,  // READ served from the version cache; result = buffered_value
                              // (either an uncommitted version, or a base value
                              //  already fetched in an earlier sub-epoch)
        COALESCED_READ,       // READ shares a base fetch issued in the same batch
        DROPPED               // WRITE superseded by a later-timestamp version
    };

    Kind kind = Kind::DROPPED;
    uint32_t slot_id = 0;
    std::string buffered_value;
    uint32_t leader_slot_id = 0;

    // Which version the READ observed: -1 = committed base (from ORAM),
    // otherwise the writer's timestamp_id (a write→read dependency).
    int64_t source_writer_ts = -1;
};

struct DedupPlan {
    std::vector<Operat> oram_bound_ops;               // slot i = index i
    std::map<uint32_t, OpDecision> per_op_decision;   // keyed by global_op_id
};

// ============================================================================
// AssembledTxnResult
// ============================================================================
struct AssembledTxnResult {
    uint32_t txn_timestamp_id = 0;
    bool     committed        = false;
    AbortReason abort_reason  = AbortReason::NONE;
    uint64_t client_txn_id    = 0;       // copied from txn::client_txn_id
    uint32_t shards_touched   = 0;       // committed: #distinct server_id over its ops
    // One entry per op in txn.operations (all stages); empty if aborted.
    std::vector<std::string> per_op_result;
};

// Legacy single-batch assembler (unchanged semantics).
std::vector<AssembledTxnResult> assembleResults(
    const std::vector<txn>& txns,
    const DedupPlan&        plan,
    const struct ExecutionResult& er);

// ============================================================================
// Epoch configuration — Obladi (R, bread, bwrite)
// ============================================================================
struct EpochConfig {
    uint32_t R              = 4;     // read sub-epochs per epoch
    uint32_t bread          = 500;   // per-read-batch size on the wire
    uint32_t bwrite         = 2000;  // write-batch size on the wire
    uint64_t sub_epoch_ms   = 25;    // intake window per read sub-epoch
    bool     pad_batches    = true;  // pad every batch to bread / bwrite
    // true  = every shard gets a padded batch every round (hides contacted set C)
    // false = only shards with real ops get a batch, padded to bread / bwrite;
    //         C is visible, as the paper's threat model allows
    bool     pad_uncontacted_shards = true;

    // Capacity enforcement (Obladi §6.2): real ops that do not fit in a
    // batch abort their txn instead of creating an extra batch.
    //   per_server_capacity = true : bread/bwrite is the per-server batch
    //                                size (matches executeOramOps, which
    //                                builds one batch per server).
    //   per_server_capacity = false: bread/bwrite bounds the total across
    //                                all servers.
    bool     enforce_batch_capacity = true;
    bool     per_server_capacity    = true;

    // Placement invariant: within one epoch every ObjectKey must be routed
    // to a single server (repartitioning happens only at epoch boundaries).
    //   true : a mismatch is a partition-mapping bug → throw std::logic_error
    //   false: the offending txn aborts with PLACEMENT_MISMATCH
    bool     fatal_on_placement_mismatch = true;

    // Safety bound on how many stages of one txn may run back-to-back
    // without an ORAM fetch (guards against a continuation that loops).
    uint32_t max_local_stages = 64;
};

struct EpochStats {
    uint32_t R = 0, bread = 0, bwrite = 0;
    bool     per_server_capacity = false;
    // txns drained from the intake queue at the start of each read sub-epoch
    std::vector<size_t> arrivals_per_sub_epoch;
    // per read sub-epoch: capacity bucket (server id, or -1 if global) ->
    // REAL ORAM slots in that batch; bread minus this is dummy padding
    std::vector<std::map<int, uint32_t>> read_real_per_bucket;
    // same for the single write batch (after WRITE_BATCH_FULL aborts)
    std::map<int, uint32_t> write_real_per_bucket;
    // per read sub-epoch: capacity bucket -> slots REQUESTED by stages that
    // were refused (READ_BATCH_FULL).  real + rejected = demand on the bucket.
    std::vector<std::map<int, uint32_t>> read_rejected_per_bucket;
};


// ============================================================================
// VersionCache — per-epoch MVTSO version store (Obladi §6.1 / §6.2)
// ============================================================================
// For every ObjectKey touched this epoch:
//   * base version  : the last committed value (lives in ORAM).  Fetched at
//                     most once per epoch; later reads are served locally.
//   * versions      : uncommitted versions created this epoch, one per
//                     writer txn, ordered by writer timestamp.
// Every version (including base) carries a read marker = max timestamp of
// any txn that read it.
//
// MVTSO rules implemented here:
//   READ  by T_r : returns T_r's own version if it wrote the key, else the
//                  version with max writer_ts < ts(T_r), else base.  Bumps
//                  that version's read marker.  Reading another txn's
//                  uncommitted version records T_r → T_w.
//   WRITE by T_w : let V = version with max writer_ts < ts(T_w) (or base).
//                  If V.read_marker > ts(T_w) → T_w aborts (WRITE_TOO_LATE).
//                  If T_w already has a version and someone else has read it
//                  → T_w aborts (INTERMEDIATE_OBSERVED).
//                  Otherwise T_w's version is created / overwritten.
//
// NOT thread-safe; used only on the proxy epoch thread.
// ============================================================================
struct CachedVersion {
    std::string value;
    uint32_t    writer_ts   = 0;
    int64_t     max_read_ts = -1;   // read marker (-1 = never read)
    Operat      last_write;          // op that produced `value` (sent in write batch)
};

struct KeyState {
    int         server_id   = -1;   // routing only; fixed for the whole epoch
    bool        base_fetched = false;
    bool        base_pending = false;   // fetch issued in the current read batch
    uint32_t    base_pending_slot = 0;
    std::string base_value;
    int64_t     base_max_read_ts = -1;
    std::map<uint32_t, CachedVersion> versions;   // writer_ts → version
};

class VersionCache {
public:
    enum class WriteStatus { OK, WRITE_TOO_LATE, INTERMEDIATE_OBSERVED };

    struct Visible {
        bool     is_base   = true;
        uint32_t writer_ts = 0;      // valid if !is_base
    };

    // Which version would T(ts) read?  Pure query (no side effects).
    Visible visible(const ObjectKey& k, uint32_t reader_ts) const;

    // True iff a READ by T(ts) on k would need a new ORAM fetch right now:
    // it would read base, and base is neither fetched nor already pending.
    bool needsOramFetch(const ObjectKey& k, uint32_t reader_ts) const;

    // MVTSO read (bumps read marker, records dependency).
    Visible read(const ObjectKey& k, uint32_t reader_ts, int server_id);

    // (16) Stable read: binds placement only; the caller serves the base
    // value.  No read marker, no dependency.
    void stableRead(const ObjectKey& k, int server_id);
    // Base needs a fetch (not fetched, not pending)?
    bool baseNeedsFetch(const ObjectKey& k) const;
    // (15) Bind a delta's row to its shard (no version, no marker).
    void bind(const ObjectKey& k, int server_id);

    // MVTSO write.  On OK the version is installed / overwritten.
    WriteStatus write(const Operat& op, uint32_t writer_ts);

    // Value of an uncommitted version (k must hold writer_ts).
    const std::string& versionValue(const ObjectKey& k, uint32_t writer_ts) const;

    // Shard this key is bound to in the current epoch (-1 = not yet bound).
    int boundServer(const ObjectKey& k) const;

    KeyState&       state(const ObjectKey& k) { return keys_[k]; }
    const KeyState* find(const ObjectKey& k) const;

    // Mark the base of k as being fetched in the current batch at `slot`.
    void markBasePending(const ObjectKey& k, uint32_t slot);

    // After a read batch returns: fill base values for every fetched slot.
    void absorb(const DedupPlan& plan, const struct ExecutionResult& er);

    // Remove every version written by `writer_ts` (used when it aborts).
    void removeVersionsOf(uint32_t writer_ts);

    // Transitive closure of `initial` over reverse dependencies: every txn
    // that (transitively) read an uncommitted version of an aborted txn.
    std::set<uint32_t> propagateAborts(const std::set<uint32_t>& initial) const;

    const std::set<uint32_t>& dependenciesOf(uint32_t reader_ts) const;
    const std::map<ObjectKey, KeyState>& keys() const { return keys_; }
    size_t size() const { return keys_.size(); }
    void clear();

private:
    std::map<ObjectKey, KeyState>            keys_;
    std::map<uint32_t, std::set<uint32_t>>   deps_;         // reader → writers
    std::map<uint32_t, std::set<uint32_t>>   rdeps_;        // writer → readers
    std::map<uint32_t, std::set<ObjectKey>>  written_by_;   // writer → keys
    static const std::string                 empty_;
    static const std::set<uint32_t>          empty_set_;
};

struct PhaseAReadResult;
struct PhaseBWriteResult;

// ============================================================================
// QueriesReceiving
// ============================================================================
class QueriesReceiving {
public:
    // ── Producer side ────────────────────────────────────────────────────
    // Thread-safe.  Returns false (txn not enqueued) when intake is closed.
    // accepting_ is checked under queue_mu_, so a txn is either enqueued
    // before the final drain of an epoch or rejected — never lost.
    bool push_txn(txn t);

    // Placement-mismatch policy for the legacy path (runEpoch_RBR_W uses
    // EpochConfig::fatal_on_placement_mismatch instead).
    void setFatalOnPlacementMismatch(bool fatal) { fatal_on_placement_mismatch_ = fatal; }

    // ── Legacy single-batch path ─────────────────────────────────────────
    std::vector<txn> collect_epoch(uint64_t interval_ms);
    void      AssignAbort(std::vector<txn>& received_txns);
    DedupPlan Dedup();
    void VCConstruct(const txn& current_txn);
    // MVTSO replay of every version chain in push (= execution) order.
    // Inserts writers that violate the MVTSO write rule into aborted_ids and
    // records write→read dependencies (used by AssignAbort for cascading).
    void VCAbort(std::set<uint32_t>& aborted_ids,
                 std::map<uint32_t, AbortReason>* reasons = nullptr);
    void VCDedup(const std::set<uint32_t>& aborted_ids);
    void clearVersionChains() {
        version_chain_vector.clear(); vc_deps_.clear(); vc_abort_events_.clear();
    }
    const std::map<ObjectKey, std::vector<Operat>>& getVersionChains() const {
        return version_chain_vector;
    }

    // ── Paper-aligned (R, bread, bwrite) path — driven by runEpoch_RBR_W ─
    void start_epoch();
    // Sleep sub_epoch_ms, then drain the queue.  If close_intake_after is
    // true, intake is closed in the SAME critical section as the drain
    // (no txn can slip in between the last drain and the close).
    std::vector<txn> collect_sub_epoch(uint64_t sub_epoch_ms,
                                       bool close_intake_after = false);
    // Close intake + drain atomically.  Returns anything still queued
    // (always empty if the last collect_sub_epoch closed intake).
    std::vector<txn> end_intake();

    // Tag ops of a later stage with the txn's timestamp + fresh global ids.
    void assignOpIds(std::vector<Operat>& ops, uint32_t txn_ts);

private:
    void tagNewTxns(std::vector<txn>& drained);

    // Intake queue (was file-scope globals).  All three guarded by queue_mu_.
    std::mutex       queue_mu_;
    std::vector<txn> incoming_queue_;
    bool             accepting_ = false;

    // Legacy path state.
    std::map<ObjectKey, std::vector<Operat>> version_chain_vector;
    std::map<uint32_t, std::set<uint32_t>>   vc_deps_;   // reader → writers
    std::map<ObjectKey, int>                 vc_abort_events_;  // diagnostics
    bool                                     fatal_on_placement_mismatch_ = true;

    // Epoch-wide id allocators for the (R, bread, bwrite) path.
    uint32_t epoch_next_ts_   = 0;
    uint32_t epoch_next_goid_ = 0;
};

// ============================================================================
// Paper-aligned epoch driver
// ============================================================================
//   start_epoch
//   for r in 1..R:                               (read phase)
//       collect new txns (intake closes atomically at r == R)
//       run every ready stage through MVTSO; reads of un-fetched base
//       versions get a slot in this sub-epoch's read batch (≤ bread)
//       executeOramOps(read batch) → absorb base values
//       feed each stage's results to its continuation → next stage
//   abort unfinished txns; cascade to fixed point
//   write batch = latest committed version per key (≤ bwrite; on overflow
//   one contributing txn aborts and the batch is rebuilt — see writePhase)
//   executeOramOps(write batch); assemble per-txn results
// ============================================================================
struct PhaseAReadResult {
    DedupPlan        plan;
    struct ExecutionResult er;
};

struct PhaseBWriteResult {
    DedupPlan        plan;
    struct ExecutionResult er;
};

std::vector<AssembledTxnResult> assembleResultsRBRW(
    const std::vector<txn>&              txns,
    const std::vector<PhaseAReadResult>& read_results,
    const PhaseBWriteResult&             write_result);

// `engine` hosts the stage logic of every staged txn type.  Staged txns
// whose type is not registered abort with UNKNOWN_TXN_TYPE.
std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg,
    const TxnEngine&         engine);

// Convenience overload for workloads with assembled txns only.
std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg);

// Same epoch plus per-sub-epoch batch occupancy (RQ3 instrumentation).
// Pure observation: stats == nullptr gives identical behaviour.
std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg,
    const TxnEngine&         engine,
    EpochStats*              stats);

#endif // QUERIES_RECEIVING_H