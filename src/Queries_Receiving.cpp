// Revised 9/22/26 — see the revision block at the top of Queries_Receiving.h.
//

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#include "Queries_Receiving.h"

// ════════════════════════════════════════════════════════════════════════════
// Small helpers
// ════════════════════════════════════════════════════════════════════════════
const char* toString(AbortReason r) {
    switch (r) {
        case AbortReason::NONE:                  return "NONE";
        case AbortReason::INCOMPLETE:            return "INCOMPLETE";
        case AbortReason::UNFINISHED:            return "UNFINISHED";
        case AbortReason::WRITE_TOO_LATE:        return "WRITE_TOO_LATE";
        case AbortReason::INTERMEDIATE_OBSERVED: return "INTERMEDIATE_OBSERVED";
        case AbortReason::READ_BATCH_FULL:       return "READ_BATCH_FULL";
        case AbortReason::WRITE_BATCH_FULL:      return "WRITE_BATCH_FULL";
        case AbortReason::CASCADE:               return "CASCADE";
        case AbortReason::DISPATCH_FAILED:       return "DISPATCH_FAILED";
        case AbortReason::UNKNOWN_TXN_TYPE:      return "UNKNOWN_TXN_TYPE";
        case AbortReason::PLACEMENT_MISMATCH:    return "PLACEMENT_MISMATCH";
        case AbortReason::DELTA_NO_BASE:         return "DELTA_NO_BASE";
    }
    return "?";
}

static const char* opName(OpType t) {
    if (t == OpType::READ)   return "READ";
    if (t == OpType::UPDATE) return "UPDATE";
    if (t == OpType::INSERT) return "INSERT";
    return "WRITE";
}

bool isAssembledTxnComplete(const txn& t) {
    if (t.operations.empty()) return false;
    if (!t.operations.back().last_one) return false;
    // last_one anywhere but the final op means the tail arrived out of order
    // or the txn was spliced — either way it is not a well-formed txn.
    for (size_t i = 0; i + 1 < t.operations.size(); ++i)
        if (t.operations[i].last_one) return false;
    // Gap check (q1, _, q3, q4+last): only possible if the sender declared
    // how many ops it sent.
    if (t.declared_op_count > 0 && t.operations.size() != t.declared_op_count)
        return false;
    return true;
}

static std::string keyStr(const ObjectKey& k) { return k.str(); }

// Report a placement mismatch.  fatal → throws std::logic_error;
// otherwise logs and returns (caller aborts the txn).
static void reportPlacementMismatch(bool fatal, const ObjectKey& k, int bound,
                                    int got, uint32_t ts) {
    const std::string msg =
        "[placement] same ObjectKey routed to different shards within one epoch: " +
        keyStr(k) + " bound to server " + std::to_string(bound) + ", txn ts=" +
        std::to_string(ts) + " routed it to server " + std::to_string(got) +
        ".  Repartitioning must happen at epoch boundaries.";
    if (fatal) throw std::logic_error(msg);
    std::cerr << msg << "\n";
}

// ════════════════════════════════════════════════════════════════════════════
// TxnEngine — proxy-side transaction state machines
// ════════════════════════════════════════════════════════════════════════════
const std::string TxnEngine::unknown_ = "<unknown>";

void TxnEngine::registerType(TxnTypeId type, std::string name, StageFn fn) {
    if (type == kAssembledTxnType) {
        throw std::logic_error("[TxnEngine] txn_type 0 is reserved for assembled txns");
    }
    types_[type] = Entry{std::move(name), std::move(fn)};
}

const std::string& TxnEngine::name(TxnTypeId type) const {
    auto it = types_.find(type);
    return it == types_.end() ? unknown_ : it->second.name;
}

TxnStage TxnEngine::stage(TxnTypeId type, uint32_t stage_id,
                          const std::vector<int64_t>& params,
                          const std::vector<std::vector<std::string>>& prior) const {
    return types_.at(type).fn(stage_id, params, prior);
}

// ════════════════════════════════════════════════════════════════════════════
// VersionCache — MVTSO version store
// ════════════════════════════════════════════════════════════════════════════
const std::string        VersionCache::empty_;
const std::set<uint32_t> VersionCache::empty_set_;

void VersionCache::clear() {
    keys_.clear();
    deps_.clear();
    rdeps_.clear();
    written_by_.clear();
}

// Bind k to a shard on first touch; afterwards the route must not change.
// EpochRunner checks placement before touching the store, so a mismatch here
// is an internal invariant violation.
static void bindPlacement(KeyState& ks, const ObjectKey& k, int server_id) {
    if (ks.server_id < 0) {
        ks.server_id = server_id;
    } else if (ks.server_id != server_id) {
        throw std::logic_error(
            "[VersionCache] same ObjectKey routed to different shards within one epoch: " +
            keyStr(k) + " bound to server " + std::to_string(ks.server_id) +
            ", op routed to server " + std::to_string(server_id));
    }
}

int VersionCache::boundServer(const ObjectKey& k) const {
    const KeyState* ks = find(k);
    return ks ? ks->server_id : -1;
}

const KeyState* VersionCache::find(const ObjectKey& k) const {
    auto it = keys_.find(k);
    return it == keys_.end() ? nullptr : &it->second;
}

VersionCache::Visible VersionCache::visible(const ObjectKey& k,
                                            uint32_t reader_ts) const {
    Visible v;                                   // default: base
    auto it = keys_.find(k);
    if (it == keys_.end()) return v;
    const auto& vs = it->second.versions;
    if (vs.count(reader_ts)) {                   // read-your-own-write
        v.is_base = false; v.writer_ts = reader_ts;
        return v;
    }
    auto lb = vs.lower_bound(reader_ts);         // first version with ts > reader
    if (lb == vs.begin()) return v;              // nothing older → base
    --lb;                                        // max writer_ts < reader_ts
    v.is_base = false; v.writer_ts = lb->first;
    return v;
}

void VersionCache::stableRead(const ObjectKey& k, int server_id) {
    bindPlacement(keys_[k], k, server_id);
}

void VersionCache::bind(const ObjectKey& k, int server_id) {
    bindPlacement(keys_[k], k, server_id);
}

bool VersionCache::baseNeedsFetch(const ObjectKey& k) const {
    const KeyState* ks = find(k);
    return ks == nullptr || (!ks->base_fetched && !ks->base_pending);
}

bool VersionCache::needsOramFetch(const ObjectKey& k, uint32_t reader_ts) const {
    if (!visible(k, reader_ts).is_base) return false;
    const KeyState* ks = find(k);
    return ks == nullptr || (!ks->base_fetched && !ks->base_pending);
}

VersionCache::Visible VersionCache::read(const ObjectKey& k, uint32_t reader_ts,
                                         int server_id) {
    KeyState& ks = keys_[k];
    bindPlacement(ks, k, server_id);

    Visible v = visible(k, reader_ts);
    const int64_t ts = reader_ts;
    if (v.is_base) {
        ks.base_max_read_ts = std::max(ks.base_max_read_ts, ts);
    } else {
        CachedVersion& cv = ks.versions.at(v.writer_ts);
        cv.max_read_ts = std::max(cv.max_read_ts, ts);
        if (v.writer_ts != reader_ts) {          // observed an uncommitted write
            deps_[reader_ts].insert(v.writer_ts);
            rdeps_[v.writer_ts].insert(reader_ts);
        }
    }
    return v;
}

VersionCache::WriteStatus VersionCache::write(const Operat& op, uint32_t writer_ts) {
    const ObjectKey k = objectKeyOf(op);
    KeyState& ks = keys_[k];
    bindPlacement(ks, k, op.server_id);
    auto& vs = ks.versions;
    const int64_t ts = writer_ts;

    auto own = vs.find(writer_ts);
    if (own != vs.end()) {
        // Rewriting our own version.  If a younger txn already read the
        // previous value it observed an intermediate state → abort.
        if (own->second.max_read_ts > ts) return WriteStatus::INTERMEDIATE_OBSERVED;
        own->second.value      = op.data_value;
        own->second.last_write = op;
        return WriteStatus::OK;
    }

    // The version this write would be inserted directly after.
    auto lb = vs.lower_bound(writer_ts);
    const int64_t prev_read_ts = (lb == vs.begin())
        ? ks.base_max_read_ts
        : std::prev(lb)->second.max_read_ts;

    // MVTSO: a txn younger than us already read the version that our write
    // should have superseded → we are too late.
    if (prev_read_ts > ts) return WriteStatus::WRITE_TOO_LATE;

    CachedVersion cv;
    cv.value      = op.data_value;
    cv.writer_ts  = writer_ts;
    cv.last_write = op;
    vs.emplace(writer_ts, std::move(cv));
    written_by_[writer_ts].insert(k);
    return WriteStatus::OK;
}

const std::string& VersionCache::versionValue(const ObjectKey& k,
                                              uint32_t writer_ts) const {
    const KeyState* ks = find(k);
    if (!ks) return empty_;
    auto it = ks->versions.find(writer_ts);
    return it == ks->versions.end() ? empty_ : it->second.value;
}

void VersionCache::markBasePending(const ObjectKey& k, uint32_t slot) {
    KeyState& ks = keys_[k];
    ks.base_pending      = true;
    ks.base_pending_slot = slot;
}

void VersionCache::absorb(const DedupPlan& plan, const ExecutionResult& er) {
    if (!er.success) return;   // never cache results of a failed dispatch
    const size_t n = std::min(plan.oram_bound_ops.size(), er.per_slot_results.size());
    for (size_t slot = 0; slot < n; ++slot) {
        const Operat& op = plan.oram_bound_ops[slot];
        if (op.type != OpType::READ) continue;
        KeyState& ks = keys_[objectKeyOf(op)];
        ks.base_value   = er.per_slot_results[slot];
        ks.base_fetched = true;
        ks.base_pending = false;
    }
}

void VersionCache::removeVersionsOf(uint32_t writer_ts) {
    auto it = written_by_.find(writer_ts);
    if (it == written_by_.end()) return;
    for (const auto& k : it->second) {
        auto kit = keys_.find(k);
        if (kit != keys_.end()) kit->second.versions.erase(writer_ts);
    }
    written_by_.erase(it);
    // Dependency edges are kept on purpose: propagateAborts still needs
    // writer → readers after the versions are gone.
}

std::set<uint32_t> VersionCache::propagateAborts(const std::set<uint32_t>& initial) const {
    std::set<uint32_t> out(initial.begin(), initial.end());
    std::vector<uint32_t> stack(initial.begin(), initial.end());
    while (!stack.empty()) {
        uint32_t w = stack.back(); stack.pop_back();
        auto it = rdeps_.find(w);
        if (it == rdeps_.end()) continue;
        for (uint32_t reader : it->second)
            if (out.insert(reader).second) stack.push_back(reader);
    }
    return out;
}

const std::set<uint32_t>& VersionCache::dependenciesOf(uint32_t reader_ts) const {
    auto it = deps_.find(reader_ts);
    return it == deps_.end() ? empty_set_ : it->second;
}

// ════════════════════════════════════════════════════════════════════════════
// Intake (thread-safe producer / consumer)
// ════════════════════════════════════════════════════════════════════════════
// Invariant: accepting_ only changes while holding queue_mu_, and closing
// intake always drains the queue in the same critical section.  Therefore a
// push either lands before the final drain (processed this epoch) or sees
// accepting_ == false and is rejected.  No txn can be silently lost.
// ════════════════════════════════════════════════════════════════════════════
bool QueriesReceiving::push_txn(txn t) {
    std::lock_guard<std::mutex> lk(queue_mu_);
    if (!accepting_) return false;
    incoming_queue_.push_back(std::move(t));
    return true;
}

void QueriesReceiving::assignOpIds(std::vector<Operat>& ops, uint32_t txn_ts) {
    for (auto& op : ops) {
        op.txn_timestamp_id = txn_ts;
        op.global_op_id     = epoch_next_goid_++;
    }
}

void QueriesReceiving::tagNewTxns(std::vector<txn>& drained) {
    for (auto& t : drained) {
        t.timestamp_id = epoch_next_ts_++;
        t.is_committed = true;
        t.abort_reason = AbortReason::NONE;
        assignOpIds(t.operations, t.timestamp_id);
    }
}

// Legacy single-window collection.
std::vector<txn> QueriesReceiving::collect_epoch(uint64_t interval_ms) {
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        if (!incoming_queue_.empty())
            std::cerr << "[collect_epoch] WARNING: " << incoming_queue_.size()
                      << " txns were queued while intake was closed; dropping.\n";
        incoming_queue_.clear();
        accepting_ = true;
    }
    epoch_next_ts_   = 0;
    epoch_next_goid_ = 0;

    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

    std::vector<txn> result;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        accepting_ = false;                 // close + final drain: atomic
        result.swap(incoming_queue_);
    }
    tagNewTxns(result);
    return result;
}

void QueriesReceiving::start_epoch() {
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        if (!incoming_queue_.empty())
            std::cerr << "[start_epoch] WARNING: " << incoming_queue_.size()
                      << " txns were queued while intake was closed; dropping.\n";
        incoming_queue_.clear();
        accepting_ = true;
    }
    epoch_next_ts_   = 0;
    epoch_next_goid_ = 0;
    clearVersionChains();
    std::cout << "[start_epoch] intake opened.\n";
}

std::vector<txn> QueriesReceiving::collect_sub_epoch(uint64_t sub_epoch_ms,
                                                     bool close_intake_after) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sub_epoch_ms));

    std::vector<txn> drained;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        drained.swap(incoming_queue_);
        if (close_intake_after) accepting_ = false;   // same critical section
    }
    tagNewTxns(drained);

    std::cout << "[collect_sub_epoch] window=" << sub_epoch_ms << "ms"
              << "  txns=" << drained.size()
              << (close_intake_after ? "  (intake closed)" : "")
              << "  next_ts=" << epoch_next_ts_
              << "  next_goid=" << epoch_next_goid_ << std::endl;
    return drained;
}

std::vector<txn> QueriesReceiving::end_intake() {
    std::vector<txn> drained;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        accepting_ = false;
        drained.swap(incoming_queue_);
    }
    tagNewTxns(drained);
    return drained;
}

// ════════════════════════════════════════════════════════════════════════════
// Legacy single-batch path
// ════════════════════════════════════════════════════════════════════════════
void QueriesReceiving::VCConstruct(const txn& current_txn) {
    for (const auto& op : current_txn.operations)
        version_chain_vector[objectKeyOf(op)].push_back(op);
}

// MVTSO replay of each version chain in push order (= execution order).
// Per chain we keep only the read markers of the base version and of each
// writer's version — the same rules as VersionCache::read / write.
void QueriesReceiving::VCAbort(std::set<uint32_t>& aborted_ids,
                               std::map<uint32_t, AbortReason>* reasons) {
    for (const auto& [key, vc] : version_chain_vector) {
        int64_t base_rts = -1;
        std::map<uint32_t, int64_t> ver_rts;   // writer_ts → read marker

        for (const auto& op : vc) {
            const uint32_t ts = op.txn_timestamp_id;
            if (aborted_ids.count(ts)) continue;

            if (op.type == OpType::READ) {
                auto own = ver_rts.find(ts);
                if (own != ver_rts.end()) {
                    own->second = std::max<int64_t>(own->second, ts);
                    continue;
                }
                auto lb = ver_rts.lower_bound(ts);
                if (lb == ver_rts.begin()) {
                    base_rts = std::max<int64_t>(base_rts, ts);
                } else {
                    --lb;
                    lb->second = std::max<int64_t>(lb->second, ts);
                    vc_deps_[ts].insert(lb->first);          // ts read lb->first's write
                }
                continue;
            }

            // WRITE (UPDATE / INSERT / DELETE)
            AbortReason why = AbortReason::NONE;
            auto own = ver_rts.find(ts);
            if (own != ver_rts.end()) {
                if (own->second > ts) why = AbortReason::INTERMEDIATE_OBSERVED;
            } else {
                auto lb = ver_rts.lower_bound(ts);
                int64_t prev = (lb == ver_rts.begin()) ? base_rts : std::prev(lb)->second;
                if (prev > ts) why = AbortReason::WRITE_TOO_LATE;
                else           ver_rts.emplace(ts, -1);
            }
            if (why != AbortReason::NONE) {
                aborted_ids.insert(ts);
                if (reasons) (*reasons)[ts] = why;
                vc_abort_events_[key]++;
            }
        }
    }
}

void QueriesReceiving::VCDedup(const std::set<uint32_t>& aborted_ids) {
    for (auto it = version_chain_vector.begin(); it != version_chain_vector.end(); ) {
        auto& vc = it->second;
        vc.erase(std::remove_if(vc.begin(), vc.end(),
                     [&](const Operat& op) { return aborted_ids.count(op.txn_timestamp_id) > 0; }),
                 vc.end());
        it = vc.empty() ? version_chain_vector.erase(it) : std::next(it);
    }
}

void QueriesReceiving::AssignAbort(std::vector<txn>& received_txns) {
    version_chain_vector.clear();
    vc_deps_.clear();
    vc_abort_events_.clear();

    std::set<uint32_t>              aborted;
    std::map<uint32_t, AbortReason> reasons;
    std::map<ObjectKey, int>        route;     // ObjectKey → shard this epoch

    // ── 1. Completeness + placement ────────────────────────────────────────────────
    for (auto& t : received_txns) {
        t.is_committed = true;
        t.abort_reason = AbortReason::NONE;
        bool ok = true;
        if (t.txn_type != kAssembledTxnType) {
            // Staged (dependent-query) txns can only run on the R-sub-epoch
            // path (runEpoch_RBR_W); the single-batch path cannot feed
            // results back to the client logic.
            std::cerr << "[AssignAbort] WARNING: staged txn ts=" << t.timestamp_id
                      << " on legacy single-batch path; aborting.\n";
            ok = false;
        } else if (!isAssembledTxnComplete(t)) {
            ok = false;
        }
        if (!ok) {
            aborted.insert(t.timestamp_id);
            reasons[t.timestamp_id] = AbortReason::INCOMPLETE;
            continue;
        }
        // Placement: every ObjectKey keeps one shard for the whole epoch.
        bool placed = true;
        std::map<ObjectKey, int> mine;
        for (const auto& op : t.operations) {
            const ObjectKey k = objectKeyOf(op);
            auto it = route.find(k);
            int bound = (it != route.end()) ? it->second
                      : (mine.count(k) ? mine[k] : -1);
            if (bound >= 0 && bound != op.server_id) {
                reportPlacementMismatch(fatal_on_placement_mismatch_, k, bound,
                                        op.server_id, t.timestamp_id);
                placed = false;
                break;
            }
            mine[k] = op.server_id;
        }
        if (!placed) {
            aborted.insert(t.timestamp_id);
            reasons[t.timestamp_id] = AbortReason::PLACEMENT_MISMATCH;
            continue;
        }
        route.insert(mine.begin(), mine.end());
        VCConstruct(t);
    }
    const size_t n_incomplete = aborted.size();

    // ── 2. MVTSO write rule ────────────────────────────────────────────
    std::set<uint32_t> mvtso;
    VCAbort(mvtso, &reasons);
    aborted.insert(mvtso.begin(), mvtso.end());

    // ── 3. Cascading aborts to a fixed point ───────────────────────────
    size_t n_cascade = 0;
    for (bool changed = true; changed; ) {
        changed = false;
        for (const auto& [reader, writers] : vc_deps_) {
            if (aborted.count(reader)) continue;
            for (uint32_t w : writers) {
                if (aborted.count(w)) {
                    aborted.insert(reader);
                    reasons[reader] = AbortReason::CASCADE;
                    ++n_cascade; changed = true;
                    break;
                }
            }
        }
    }

    for (auto& t : received_txns) {
        if (aborted.count(t.timestamp_id)) {
            t.is_committed = false;
            t.abort_reason = reasons[t.timestamp_id];
        }
    }
    VCDedup(aborted);

    // ── Diagnostics ────────────────────────────────────────────────────
    size_t committed = 0;
    for (const auto& t : received_txns) committed += t.is_committed ? 1 : 0;
    std::cout << "[AssignAbort] total=" << received_txns.size()
              << "  committed=" << committed
              << "  incomplete_or_misplaced=" << n_incomplete
              << "  mvtso=" << mvtso.size()
              << "  cascade=" << n_cascade << std::endl;

    if (!vc_abort_events_.empty()) {
        std::vector<std::pair<ObjectKey, int>> hot(vc_abort_events_.begin(),
                                                   vc_abort_events_.end());
        std::sort(hot.begin(), hot.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });
        std::cout << "  [MVTSO hot keys] ";
        for (size_t i = 0; i < std::min<size_t>(3, hot.size()); ++i)
            std::cout << keyStr(hot[i].first) << "x" << hot[i].second << " ";
        std::cout << std::endl;
    }
    for (const auto& t : received_txns) {
        if (t.abort_reason != AbortReason::INCOMPLETE) continue;
        std::cout << "  [incomplete example] ts=" << t.timestamp_id
                  << " ops=" << t.operations.size()
                  << " declared=" << t.declared_op_count;
        if (!t.operations.empty())
            std::cout << " last.type=" << opName(t.operations.back().type)
                      << " last.last_one=" << (t.operations.back().last_one ? "true" : "false");
        std::cout << std::endl;
        break;
    }
}

// Legacy dedup over version chains (push order = ts order on this path).
DedupPlan QueriesReceiving::Dedup() {
    DedupPlan plan;

    for (auto& [key, vc] : version_chain_vector) {
        (void)key;
        // Last write = max (txn_timestamp_id, global_op_id), lexicographic.
        bool     has_write = false;
        uint32_t last_ts = 0, last_goid = 0;
        for (const auto& op : vc) {
            if (!isWriteOp(op.type)) continue;
            if (!has_write ||
                std::make_pair(op.txn_timestamp_id, op.global_op_id) >
                std::make_pair(last_ts, last_goid)) {
                last_ts = op.txn_timestamp_id; last_goid = op.global_op_id;
                has_write = true;
            }
        }

        std::string buf_value;
        int64_t     buf_writer = -1;
        bool        has_buffer = false;
        bool        leader_assigned = false;
        uint32_t    leader_slot = 0;

        for (const auto& op : vc) {
            OpDecision d;
            if (op.type == OpType::READ) {
                if (has_buffer) {
                    d.kind             = OpDecision::Kind::BUFFERED_FROM_WRITE;
                    d.buffered_value   = buf_value;
                    d.source_writer_ts = buf_writer;
                } else if (!leader_assigned) {
                    d.kind    = OpDecision::Kind::TO_ORAM;
                    d.slot_id = static_cast<uint32_t>(plan.oram_bound_ops.size());
                    plan.oram_bound_ops.push_back(op);
                    leader_slot = d.slot_id; leader_assigned = true;
                } else {
                    d.kind           = OpDecision::Kind::COALESCED_READ;
                    d.leader_slot_id = leader_slot;
                }
            } else {
                buf_value  = op.data_value;
                buf_writer = op.txn_timestamp_id;
                has_buffer = true;
                if (op.global_op_id == last_goid) {
                    d.kind    = OpDecision::Kind::TO_ORAM;
                    d.slot_id = static_cast<uint32_t>(plan.oram_bound_ops.size());
                    plan.oram_bound_ops.push_back(op);
                } else {
                    d.kind = OpDecision::Kind::DROPPED;
                }
            }
            plan.per_op_decision[op.global_op_id] = std::move(d);
        }
    }

    size_t to_oram = 0, buffered = 0, coalesced = 0, dropped = 0;
    for (const auto& kv : plan.per_op_decision) {
        switch (kv.second.kind) {
            case OpDecision::Kind::TO_ORAM:             ++to_oram;   break;
            case OpDecision::Kind::BUFFERED_FROM_WRITE: ++buffered;  break;
            case OpDecision::Kind::COALESCED_READ:      ++coalesced; break;
            case OpDecision::Kind::DROPPED:             ++dropped;   break;
        }
    }
    std::cout << "[Dedup] total_ops=" << plan.per_op_decision.size()
              << "  to_oram=" << to_oram << "  buffered=" << buffered
              << "  coalesced=" << coalesced << "  dropped=" << dropped << std::endl;
    return plan;
}

// ════════════════════════════════════════════════════════════════════════════
// Result assembly
// ════════════════════════════════════════════════════════════════════════════
static std::string resolveDecision(const OpDecision& d, const ExecutionResult* er) {
    switch (d.kind) {
        case OpDecision::Kind::TO_ORAM:
            if (er && d.slot_id < er->per_slot_results.size())
                return er->per_slot_results[d.slot_id];
            return {};
        case OpDecision::Kind::COALESCED_READ:
            if (er && d.leader_slot_id < er->per_slot_results.size())
                return er->per_slot_results[d.leader_slot_id];
            return {};
        case OpDecision::Kind::BUFFERED_FROM_WRITE:
            return d.buffered_value;
        case OpDecision::Kind::DROPPED:
            return {};
    }
    return {};
}

std::vector<AssembledTxnResult> assembleResults(
    const std::vector<txn>& txns,
    const DedupPlan&        plan,
    const ExecutionResult&  er)
{
    std::vector<AssembledTxnResult> out;
    out.reserve(txns.size());
    const bool global_failure = !er.success;
    if (global_failure)
        std::cerr << "[assembleResults] WARNING: dispatch failed (" << er.error_message
                  << "); forcing all txns to ABORT.\n";

    size_t commits = 0, aborts = 0;
    for (const auto& t : txns) {
        AssembledTxnResult r;
        r.txn_timestamp_id = t.timestamp_id;
        r.client_txn_id    = t.client_txn_id;
        if (!t.is_committed || global_failure) {
            r.committed    = false;
            r.abort_reason = global_failure ? AbortReason::DISPATCH_FAILED : t.abort_reason;
            ++aborts;
            out.push_back(std::move(r));
            continue;
        }
        r.committed = true;
        for (const auto& op : t.operations) {
            auto it = plan.per_op_decision.find(op.global_op_id);
            if (it == plan.per_op_decision.end()) {
                std::cerr << "[assembleResults] WARNING: no decision for goid="
                          << op.global_op_id << " (txn ts=" << t.timestamp_id << ")\n";
                r.per_op_result.emplace_back();
                continue;
            }
            r.per_op_result.push_back(resolveDecision(it->second, &er));
        }
        ++commits;
        out.push_back(std::move(r));
    }
    std::cout << "[assembleResults] txns=" << txns.size()
              << "  committed=" << commits << "  aborted=" << aborts << std::endl;
    return out;
}

std::vector<AssembledTxnResult> assembleResultsRBRW(
    const std::vector<txn>&              txns,
    const std::vector<PhaseAReadResult>& read_results,
    const PhaseBWriteResult&             write_result)
{
    std::vector<AssembledTxnResult> out;
    out.reserve(txns.size());

    bool global_failure = !write_result.er.success;
    for (const auto& rr : read_results) global_failure |= !rr.er.success;
    if (global_failure)
        std::cerr << "[assembleResultsRBRW] WARNING: a dispatch failed; "
                     "forcing all txns to ABORT.\n";

    struct Ref { const ExecutionResult* er; const OpDecision* d; };
    std::map<uint32_t, Ref> by_goid;
    for (const auto& rr : read_results)
        for (const auto& [goid, d] : rr.plan.per_op_decision) by_goid[goid] = {&rr.er, &d};
    for (const auto& [goid, d] : write_result.plan.per_op_decision)
        by_goid[goid] = {&write_result.er, &d};

    size_t commits = 0, aborts = 0, missing = 0;
    for (const auto& t : txns) {
        AssembledTxnResult r;
        r.txn_timestamp_id = t.timestamp_id;
        r.client_txn_id    = t.client_txn_id;
        if (!t.is_committed || global_failure) {
            r.committed    = false;
            r.abort_reason = global_failure ? AbortReason::DISPATCH_FAILED : t.abort_reason;
            ++aborts;
            out.push_back(std::move(r));
            continue;
        }
        r.committed = true;
        {
            std::set<int> sv;
            for (const auto& op : t.operations) sv.insert(op.server_id);
            r.shards_touched = static_cast<uint32_t>(sv.size());
        }
        r.per_op_result.reserve(t.operations.size());
        for (const auto& op : t.operations) {
            auto it = by_goid.find(op.global_op_id);
            if (it == by_goid.end()) {
                std::cerr << "[assembleResultsRBRW] WARNING: no decision for goid="
                          << op.global_op_id << " (txn ts=" << t.timestamp_id << ")\n";
                r.per_op_result.emplace_back();
                ++missing;
                continue;
            }
            r.per_op_result.push_back(resolveDecision(*it->second.d, it->second.er));
        }
        ++commits;
        out.push_back(std::move(r));
    }
    std::cout << "[assembleResultsRBRW] txns=" << txns.size()
              << "  committed=" << commits << "  aborted=" << aborts
              << "  missing_dec=" << missing << std::endl;
    return out;
}

// ════════════════════════════════════════════════════════════════════════════
// Paper-aligned (R, bread, bwrite) epoch — execution-cursor model
// ════════════════════════════════════════════════════════════════════════════
namespace {

struct TxnRuntime {
    size_t stage_begin = 0;     // first op of the current stage in txn.operations
    bool   final_stage = true;  // current stage is the last one
    bool   ready       = false; // stage parked; runs in the next read sub-epoch
    int    waiting_on  = -1;    // sub-epoch whose read batch this stage waits on
    bool   finished    = false;
    bool   aborted     = false;
    // Staged txns: next stage id + results of all completed stages.
    uint32_t                               next_stage_id = 1;
    std::vector<std::vector<std::string>>  history;
    // (15)/(16) commutative increments of all stages so far; the current
    // stage's are deltas[delta_begin..].  stable = goids of stable reads.
    std::vector<FieldDelta>                deltas;
    size_t                                 delta_begin = 0;
    std::set<uint32_t>                     stable;
};

enum class StageStatus {
    RESOLVED,     // every result is already known (no pending ORAM fetch)
    WAITING,      // needs results of the current read batch
    NEEDS_BATCH,  // needs an ORAM fetch but no batch is open (runs next sub-epoch)
    ABORTED
};

class EpochRunner {
public:
    EpochRunner(QueriesReceiving& qr, MultiRingORAM_Servers& mrs,
                const EpochConfig& cfg, const TxnEngine& engine,
                EpochStats* stats = nullptr)
        : qr_(qr), mrs_(mrs), cfg_(cfg), engine_(engine), stats_(stats) {}

    std::vector<AssembledTxnResult> run();

private:
    QueriesReceiving&        qr_;
    MultiRingORAM_Servers&   mrs_;
    const EpochConfig&       cfg_;
    const TxnEngine&         engine_;
    EpochStats*              stats_ = nullptr;   // optional instrumentation (RQ3)

    VersionCache                  store_;
    std::vector<txn>              txns_;     // index == timestamp_id
    std::vector<TxnRuntime>       rt_;
    std::vector<PhaseAReadResult> reads_;    // one per read sub-epoch
    std::map<int, uint32_t>       batch_load_;   // capacity bucket → real slots used
    std::map<int, uint32_t>       rejected_load_; // capacity bucket → slots refused (READ_BATCH_FULL)
    bool                          dispatch_failed_ = false;
    std::map<AbortReason, size_t> abort_counts_;

    int bucket(int server_id) const { return cfg_.per_server_capacity ? server_id : -1; }

    void admit(std::vector<txn>&& incoming);
    void install(uint32_t ts, TxnStage&& st);   // append a stage to txn ts
    void abortTxn(uint32_t ts, AbortReason why);
    StageStatus runStage(uint32_t ts, uint32_t r, bool batch_open);
    std::vector<std::string> stageResults(uint32_t ts, uint32_t r) const;
    void drive(uint32_t ts, uint32_t r, bool batch_open);
    void advance(uint32_t ts, uint32_t r, bool batch_open, std::vector<std::string> results);
    PhaseBWriteResult writePhase();
};

void EpochRunner::admit(std::vector<txn>&& incoming) {
    for (auto& t : incoming) {
        if (t.timestamp_id != txns_.size()) {
            // collect_sub_epoch hands out consecutive ids starting at 0.
            throw std::logic_error("[runEpoch] non-contiguous timestamp " +
                                   std::to_string(t.timestamp_id) + " (expected " +
                                   std::to_string(txns_.size()) + ")");
        }
        const uint32_t ts     = t.timestamp_id;
        const bool     staged = (t.txn_type != kAssembledTxnType);
        AbortReason    bad    = AbortReason::NONE;
        TxnRuntime     rt;
        rt.ready = true;

        if (!staged) {
            rt.final_stage = true;
            if (!isAssembledTxnComplete(t)) bad = AbortReason::INCOMPLETE;
        } else if (!engine_.knows(t.txn_type)) {
            bad = AbortReason::UNKNOWN_TXN_TYPE;
        } else if (!t.operations.empty()) {
            // Staged txns carry (type, params) only; ops come from the engine.
            bad = AbortReason::INCOMPLETE;
        }

        txns_.push_back(std::move(t));
        rt_.push_back(std::move(rt));
        if (bad == AbortReason::NONE && staged) {
            install(ts, engine_.stage(txns_[ts].txn_type, 0, txns_[ts].params, rt_[ts].history));
        }
        if (bad != AbortReason::NONE) abortTxn(ts, bad);
    }
}

void EpochRunner::install(uint32_t ts, TxnStage&& st) {
    txn&        t  = txns_[ts];
    TxnRuntime& rt = rt_[ts];
    qr_.assignOpIds(st.ops, ts);
    for (uint32_t i : st.stable_reads)
        if (i < st.ops.size() && st.ops[i].type == OpType::READ)
            rt.stable.insert(st.ops[i].global_op_id);
    rt.delta_begin = rt.deltas.size();
    for (auto& d : st.deltas) rt.deltas.push_back(std::move(d));
    rt.stage_begin = t.operations.size();
    rt.final_stage = st.done;
    for (auto& op : st.ops) t.operations.push_back(std::move(op));
}

void EpochRunner::abortTxn(uint32_t ts, AbortReason why) {
    if (ts >= rt_.size() || rt_[ts].aborted) return;
    // The victim plus everyone who (transitively) read its uncommitted writes.
    for (uint32_t v : store_.propagateAborts({ts})) {
        if (v >= rt_.size() || rt_[v].aborted) continue;
        TxnRuntime& rt = rt_[v];
        rt.aborted = true; rt.ready = false; rt.waiting_on = -1;
        txns_[v].is_committed = false;
        txns_[v].abort_reason = (v == ts) ? why : AbortReason::CASCADE;
        abort_counts_[txns_[v].abort_reason]++;
        // Its versions disappear; later readers fall back to older versions.
        // (Read markers it set are kept — conservative, never unsafe.)
        store_.removeVersionsOf(v);
    }
}

StageStatus EpochRunner::runStage(uint32_t ts, uint32_t r, bool batch_open) {
    txn&        t  = txns_[ts];
    TxnRuntime& rt = rt_[ts];
    rt.ready = false;
    DedupPlan&  plan = reads_[r].plan;

    // ── 1. Admission: how many NEW ORAM slots does this stage need? ────
    // Obladi: if a txn's reads cannot fit in the read batch, it aborts
    // (instead of the batch growing or spilling into an extra batch).
    std::map<int, uint32_t>  need;
    std::set<ObjectKey>      planned, written_here;
    std::map<ObjectKey, int> stage_route;
    for (size_t i = rt.stage_begin; i < t.operations.size(); ++i) {
        const Operat& op = t.operations[i];
        const ObjectKey k = objectKeyOf(op);

        // Placement: the object must stay on the shard it was bound to this
        // epoch (checked before any side effect on the version store).
        int bound = store_.boundServer(k);
        if (bound < 0) {
            auto it = stage_route.find(k);
            if (it != stage_route.end()) bound = it->second;
        }
        if (bound >= 0 && bound != op.server_id) {
            reportPlacementMismatch(cfg_.fatal_on_placement_mismatch, k, bound,
                                    op.server_id, ts);
            abortTxn(ts, AbortReason::PLACEMENT_MISMATCH);
            return StageStatus::ABORTED;
        }
        stage_route[k] = op.server_id;

        if (isWriteOp(op.type)) { written_here.insert(k); continue; }
        if (written_here.count(k) || planned.count(k)) continue;
        const bool fetch = rt.stable.count(op.global_op_id) ? store_.baseNeedsFetch(k)
                                                            : store_.needsOramFetch(k, ts);
        if (fetch) { planned.insert(k); need[bucket(op.server_id)]++; }
    }
    for (size_t i = rt.delta_begin; i < rt.deltas.size(); ++i) {   // (15) placement
        const Operat&   row = rt.deltas[i].row;
        const ObjectKey k   = objectKeyOf(row);
        int bound = store_.boundServer(k);
        if (bound < 0) {
            auto it = stage_route.find(k);
            if (it != stage_route.end()) bound = it->second;
        }
        if (bound >= 0 && bound != row.server_id) {
            reportPlacementMismatch(cfg_.fatal_on_placement_mismatch, k, bound,
                                    row.server_id, ts);
            abortTxn(ts, AbortReason::PLACEMENT_MISMATCH);
            return StageStatus::ABORTED;
        }
        stage_route[k] = row.server_id;
    }
    if (!need.empty()) {
        if (!batch_open) return StageStatus::NEEDS_BATCH;
        if (cfg_.enforce_batch_capacity) {
            for (const auto& [b, n] : need) {
                if (batch_load_[b] + n > cfg_.bread) {
                    for (const auto& [b2, n2] : need) rejected_load_[b2] += n2;
                    abortTxn(ts, AbortReason::READ_BATCH_FULL);
                    return StageStatus::ABORTED;
                }
            }
        }
    }

    // ── 2. Execute the stage through MVTSO, op by op ───────────────────
    bool pending = false;
    for (size_t i = rt.stage_begin; i < t.operations.size(); ++i) {
        const Operat& op = t.operations[i];
        const ObjectKey k = objectKeyOf(op);

        if (isWriteOp(op.type)) {
            auto st = store_.write(op, ts);
            if (st != VersionCache::WriteStatus::OK) {
                abortTxn(ts, st == VersionCache::WriteStatus::WRITE_TOO_LATE
                                 ? AbortReason::WRITE_TOO_LATE
                                 : AbortReason::INTERMEDIATE_OBSERVED);
                return StageStatus::ABORTED;
            }
            continue;   // write decisions are made in the write phase
        }

        // (16) stable read: committed base, no read marker, no dependency
        const bool stable = rt.stable.count(op.global_op_id) > 0;
        VersionCache::Visible vis;
        if (stable) store_.stableRead(k, op.server_id);
        else        vis = store_.read(k, ts, op.server_id);
        OpDecision d;
        if (!vis.is_base) {
            d.kind             = OpDecision::Kind::BUFFERED_FROM_WRITE;
            d.buffered_value   = store_.versionValue(k, vis.writer_ts);
            d.source_writer_ts = vis.writer_ts;
        } else {
            KeyState& ks = store_.state(k);
            if (ks.base_fetched) {
                d.kind           = OpDecision::Kind::BUFFERED_FROM_WRITE;
                d.buffered_value = ks.base_value;
            } else if (ks.base_pending) {
                d.kind           = OpDecision::Kind::COALESCED_READ;
                d.leader_slot_id = ks.base_pending_slot;
                pending = true;
            } else {
                d.kind    = OpDecision::Kind::TO_ORAM;
                d.slot_id = static_cast<uint32_t>(plan.oram_bound_ops.size());
                plan.oram_bound_ops.push_back(op);
                store_.markBasePending(k, d.slot_id);
                batch_load_[bucket(op.server_id)]++;
                pending = true;
            }
        }
        plan.per_op_decision[op.global_op_id] = std::move(d);
    }
    for (size_t i = rt.delta_begin; i < rt.deltas.size(); ++i)   // (15)
        store_.bind(objectKeyOf(rt.deltas[i].row), rt.deltas[i].row.server_id);
    rt.delta_begin = rt.deltas.size();
    return pending ? StageStatus::WAITING : StageStatus::RESOLVED;
}

std::vector<std::string> EpochRunner::stageResults(uint32_t ts, uint32_t r) const {
    const txn&        t  = txns_[ts];
    const TxnRuntime& rt = rt_[ts];
    const auto&       rr = reads_[r];
    std::vector<std::string> out;
    out.reserve(t.operations.size() - rt.stage_begin);
    for (size_t i = rt.stage_begin; i < t.operations.size(); ++i) {
        const Operat& op = t.operations[i];
        if (isWriteOp(op.type)) { out.emplace_back(); continue; }
        auto it = rr.plan.per_op_decision.find(op.global_op_id);
        out.push_back(it == rr.plan.per_op_decision.end()
                          ? std::string{} : resolveDecision(it->second, &rr.er));
    }
    return out;
}

void EpochRunner::drive(uint32_t ts, uint32_t r, bool batch_open) {
    switch (runStage(ts, r, batch_open)) {
        case StageStatus::RESOLVED:    advance(ts, r, batch_open, stageResults(ts, r)); break;
        case StageStatus::WAITING:     rt_[ts].waiting_on = static_cast<int>(r);       break;
        case StageStatus::NEEDS_BATCH: rt_[ts].ready = true;                            break;
        case StageStatus::ABORTED:                                                      break;
    }
}

// Feed a finished stage's results to the client logic and run the next
// stage(s).  Stages that need no new ORAM fetch execute immediately; a
// stage that needs a fetch joins the current batch if it is still open,
// otherwise it is parked for the next read sub-epoch.
void EpochRunner::advance(uint32_t ts, uint32_t r, bool batch_open,
                          std::vector<std::string> results) {
    for (uint32_t iter = 0; ; ++iter) {
        TxnRuntime& rt = rt_[ts];
        txn&        t  = txns_[ts];
        if (rt.aborted) return;
        if (rt.final_stage) { rt.finished = true; return; }
        if (iter >= cfg_.max_local_stages) { abortTxn(ts, AbortReason::UNFINISHED); return; }

        rt.history.push_back(std::move(results));
        install(ts, engine_.stage(t.txn_type, rt.next_stage_id++, t.params, rt.history));

        switch (runStage(ts, r, batch_open)) {
            case StageStatus::RESOLVED:    results = stageResults(ts, r); continue;
            case StageStatus::WAITING:     rt_[ts].waiting_on = static_cast<int>(r); return;
            case StageStatus::NEEDS_BATCH: rt_[ts].ready = true; return;
            case StageStatus::ABORTED:     return;
        }
    }
}

PhaseBWriteResult EpochRunner::writePhase() {
    PhaseBWriteResult wr;
    for (;;) {
        DedupPlan plan;
        std::map<uint32_t, uint32_t> chosen_goid_to_slot;
        std::map<int, uint32_t>      load;

        // (15) Sum the deltas of every surviving txn per row and field.
        struct RowDeltas {
            Operat                      row;
            std::map<uint32_t, int64_t> sums;
            std::set<uint32_t>          writers;
        };
        std::map<ObjectKey, RowDeltas> deltas;
        for (uint32_t ts = 0; ts < txns_.size(); ++ts) {
            if (rt_[ts].aborted) continue;
            for (const auto& d : rt_[ts].deltas) {
                RowDeltas& rd = deltas[objectKeyOf(d.row)];
                if (rd.writers.empty()) rd.row = d.row;
                rd.sums[d.field] += d.amount;
                rd.writers.insert(ts);
            }
        }
        std::set<uint32_t> no_base;   // contributors of deltas that cannot be applied
        auto applyDeltas = [&](const ObjectKey& k, const RowDeltas& rd,
                               const std::string& row) -> std::string {
            const auto& fn = engine_.deltaApplier();
            std::string out = fn ? fn(k, row, rd.sums) : std::string{};
            if (out.empty()) no_base.insert(rd.writers.begin(), rd.writers.end());
            return out;
        };

        // Latest (max writer_ts) surviving version per key.  Aborted txns'
        // versions were removed in abortTxn, so every survivor commits.
        for (const auto& [k, ks] : store_.keys()) {
            if (ks.versions.empty()) continue;
            const CachedVersion& v = ks.versions.rbegin()->second;
            Operat op     = v.last_write;
            op.data_value = v.value;
            // A row created THIS epoch exists only as an INSERT version; if a
            // later txn updated it, the merged write must still be an INSERT
            // (executeOramOps rejects an UPDATE of an unknown row and sends an
            // INSERT of an existing row as UPDATE, so INSERT is always safe).
            for (const auto& [w, ver] : ks.versions)
                if (ver.last_write.type == OpType::INSERT) { op.type = OpType::INSERT; break; }
            // (15) increments ride on the row's single write
            auto dit = deltas.find(k);
            if (dit != deltas.end()) {
                op.data_value = applyDeltas(k, dit->second, v.value);
                deltas.erase(dit);
            }
            const uint32_t slot = static_cast<uint32_t>(plan.oram_bound_ops.size());
            chosen_goid_to_slot[op.global_op_id] = slot;
            load[bucket(op.server_id)]++;
            plan.oram_bound_ops.push_back(std::move(op));
        }
        // (15) Rows changed only by deltas: base value + increments.
        for (const auto& [k, rd] : deltas) {
            const KeyState* ks = store_.find(k);
            if (!ks || !ks->base_fetched || ks->base_value.empty()) {
                no_base.insert(rd.writers.begin(), rd.writers.end());
                continue;
            }
            Operat op           = rd.row;
            op.type             = OpType::UPDATE;
            op.data_value       = applyDeltas(k, rd, ks->base_value);
            op.global_op_id     = 0;        // not a txn op: no per-op decision
            op.txn_timestamp_id = 0;
            op.last_one         = false;
            load[bucket(op.server_id)]++;
            plan.oram_bound_ops.push_back(std::move(op));
        }
        if (!no_base.empty()) {
            for (uint32_t ts : no_base) abortTxn(ts, AbortReason::DELTA_NO_BASE);
            continue;   // rebuild the plan without them
        }

        // bwrite overflow.  What Obladi requires is only that overflow never
        // creates an extra observable batch.  Which txn to abort is OUR
        // implementation policy: we abort one txn contributing to the
        // overflowing batch — deterministically the youngest writer (max ts)
        // in that bucket — cascade, and rebuild the fixed-size batch.
        if (cfg_.enforce_batch_capacity) {
            int over = 0; bool has_over = false;
            for (const auto& [b, n] : load)
                if (n > cfg_.bwrite) { over = b; has_over = true; break; }
            if (has_over) {
                int64_t victim = -1;
                for (const auto& [k, ks] : store_.keys())
                    for (const auto& [w, v] : ks.versions)
                        if (bucket(v.last_write.server_id) == over)
                            victim = std::max<int64_t>(victim, w);
                for (const auto& [k, rd] : deltas)          // (15) delta-only rows
                    if (bucket(rd.row.server_id) == over)
                        victim = std::max<int64_t>(victim, *rd.writers.rbegin());
                if (victim >= 0) {
                    abortTxn(static_cast<uint32_t>(victim), AbortReason::WRITE_BATCH_FULL);
                    continue;   // rebuild the plan
                }
            }
        }

        for (const auto& t : txns_) {
            if (!t.is_committed) continue;
            for (const auto& op : t.operations) {
                if (!isWriteOp(op.type)) continue;
                OpDecision d;
                auto it = chosen_goid_to_slot.find(op.global_op_id);
                if (it != chosen_goid_to_slot.end()) {
                    d.kind = OpDecision::Kind::TO_ORAM; d.slot_id = it->second;
                } else {
                    d.kind = OpDecision::Kind::DROPPED;
                }
                plan.per_op_decision[op.global_op_id] = std::move(d);
            }
        }
        if (stats_) stats_->write_real_per_bucket = load;   // final, post-abort plan
        wr.plan = std::move(plan);
        break;
    }

    if (dispatch_failed_) {
        wr.er.success = false;
        wr.er.error_message = "read phase dispatch failed; write batch not sent";
        return wr;
    }
    wr.er = mrs_.executeOramOps(wr.plan.oram_bound_ops, cfg_.bwrite, cfg_.pad_batches,
                                cfg_.pad_uncontacted_shards);
    std::cout << "[writePhase] to_oram=" << wr.plan.oram_bound_ops.size()
              << "  decisions=" << wr.plan.per_op_decision.size() << std::endl;
    return wr;
}

std::vector<AssembledTxnResult> EpochRunner::run() {
    auto t_start = std::chrono::high_resolution_clock::now();
    qr_.start_epoch();
    reads_.resize(cfg_.R);
    if (stats_) {
        *stats_ = EpochStats{};
        stats_->R                   = cfg_.R;
        stats_->bread               = cfg_.bread;
        stats_->bwrite              = cfg_.bwrite;
        stats_->per_server_capacity = cfg_.per_server_capacity;
    }

    // ── Phase A: R read sub-epochs ─────────────────────────────────────
    for (uint32_t r = 0; r < cfg_.R; ++r) {
        const bool last = (r + 1 == cfg_.R);
        auto incoming = qr_.collect_sub_epoch(cfg_.sub_epoch_ms, /*close_intake_after=*/last);
        batch_load_.clear();
        rejected_load_.clear();

        // Parked stages of older txns first, then newly arrived txns.
        const size_t n_old = txns_.size();
        if (stats_) stats_->arrivals_per_sub_epoch.push_back(incoming.size());
        for (uint32_t ts = 0; ts < n_old; ++ts)
            if (rt_[ts].ready && !rt_[ts].aborted) drive(ts, r, /*batch_open=*/true);
        admit(std::move(incoming));
        for (uint32_t ts = static_cast<uint32_t>(n_old); ts < txns_.size(); ++ts)
            if (rt_[ts].ready && !rt_[ts].aborted) drive(ts, r, /*batch_open=*/true);

        // Real slots per capacity bucket in THIS sub-epoch's read batch
        // (batch_load_ counts only TO_ORAM ops; the rest of bread is dummy).
        if (stats_) {
            stats_->read_real_per_bucket.push_back(batch_load_);
            stats_->read_rejected_per_bucket.push_back(rejected_load_);
        }

        // Fixed-size read batch (padded to bread by executeOramOps).
        reads_[r].er = mrs_.executeOramOps(reads_[r].plan.oram_bound_ops,
                                           cfg_.bread, cfg_.pad_batches,
                                           cfg_.pad_uncontacted_shards);
        if (!reads_[r].er.success) {
            std::cerr << "[runEpoch] read batch " << r << " failed: "
                      << reads_[r].er.error_message << "\n";
            dispatch_failed_ = true;
            break;
        }
        store_.absorb(reads_[r].plan, reads_[r].er);

        size_t buffered = 0, coalesced = 0;
        for (const auto& kv : reads_[r].plan.per_op_decision) {
            buffered  += kv.second.kind == OpDecision::Kind::BUFFERED_FROM_WRITE;
            coalesced += kv.second.kind == OpDecision::Kind::COALESCED_READ;
        }
        uint32_t max_load = 0;
        for (const auto& kv : batch_load_) max_load = std::max(max_load, kv.second);
        std::cout << "[read sub-epoch " << (r + 1) << "/" << cfg_.R << "]"
                  << "  txns_total=" << txns_.size()
                  << "  to_oram=" << reads_[r].plan.oram_bound_ops.size()
                  << "  buffered=" << buffered << "  coalesced=" << coalesced
                  << "  max_bucket_load=" << max_load << "/" << cfg_.bread << std::endl;

        // Deliver results; next stages run now (no fetch) or next sub-epoch.
        for (uint32_t ts = 0; ts < txns_.size(); ++ts) {
            TxnRuntime& rt = rt_[ts];
            if (rt.aborted || rt.waiting_on != static_cast<int>(r)) continue;
            rt.waiting_on = -1;
            advance(ts, r, /*batch_open=*/false, stageResults(ts, r));
        }
    }

    // Anything still queued (only possible if the loop broke early).
    auto leftovers = qr_.end_intake();
    if (!leftovers.empty()) {
        const size_t first = txns_.size();
        admit(std::move(leftovers));
        for (size_t ts = first; ts < txns_.size(); ++ts)
            abortTxn(static_cast<uint32_t>(ts), AbortReason::UNFINISHED);
    }

    // Txns whose stages did not finish within R read batches abort.
    for (uint32_t ts = 0; ts < txns_.size(); ++ts)
        if (!rt_[ts].aborted && !rt_[ts].finished)
            abortTxn(ts, dispatch_failed_ ? AbortReason::DISPATCH_FAILED
                                          : AbortReason::UNFINISHED);

    // Cascade fixed point (abortTxn already cascades eagerly; this is a
    // cheap consistency check).
    std::set<uint32_t> aborted;
    for (uint32_t ts = 0; ts < txns_.size(); ++ts) if (rt_[ts].aborted) aborted.insert(ts);
    for (uint32_t v : store_.propagateAborts(aborted))
        if (!rt_[v].aborted) abortTxn(v, AbortReason::CASCADE);

    // ── Phase B: single write batch ────────────────────────────────────
    PhaseBWriteResult wr = writePhase();

    auto results = assembleResultsRBRW(txns_, reads_, wr);

    double ms = std::chrono::duration<double, std::milli>(
                    std::chrono::high_resolution_clock::now() - t_start).count();
    std::cout << "[runEpoch_RBR_W] DONE  R=" << cfg_.R << "  bread=" << cfg_.bread
              << "  bwrite=" << cfg_.bwrite << "  txns=" << txns_.size()
              << "  elapsed_ms=" << ms << "  aborts:";
    if (abort_counts_.empty()) std::cout << " none";
    for (const auto& [why, n] : abort_counts_) std::cout << " " << toString(why) << "=" << n;
    std::cout << std::endl;
    return results;
}

}  // namespace

std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg,
    const TxnEngine&         engine)
{
    EpochRunner runner(qr, mrs, cfg, engine);
    return runner.run();
}

// Same epoch, plus per-sub-epoch batch occupancy for the caller (RQ3).
// Instrumentation only: the epoch's behaviour is identical with stats == nullptr.
std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg,
    const TxnEngine&         engine,
    EpochStats*              stats)
{
    EpochRunner runner(qr, mrs, cfg, engine, stats);
    return runner.run();
}

std::vector<AssembledTxnResult> runEpoch_RBR_W(
    QueriesReceiving&        qr,
    MultiRingORAM_Servers&   mrs,
    const EpochConfig&       cfg)
{
    static const TxnEngine no_staged_types;
    return runEpoch_RBR_W(qr, mrs, cfg, no_staged_types);
}