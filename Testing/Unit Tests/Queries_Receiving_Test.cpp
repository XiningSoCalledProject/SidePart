// Unit tests for Queries_Receiving (fake in-memory ORAM in ./MultiRingORAM_Servers.h).
// Build in a scratch dir, NOT in the repo:
//   cp ../Queries_Receiving.h ../Queries_Receiving.cpp . && g++ -std=c++17 -pthread test_qr.cpp Queries_Receiving.cpp -o test_qr && ./test_qr
#include "Queries_Receiving.h"
#include <cassert>
#include <iostream>
#include <thread>
#include <atomic>

static int fails = 0;
#define CHECK(c) do { if (!(c)) { std::cerr << "  FAIL line " << __LINE__ << ": " #c "\n"; ++fails; } } while (0)

static Operat R(uint32_t pk, int s = 0, uint16_t tbl = 0) { Operat o; o.type = OpType::READ; o.data_primary_key = pk; o.server_id = s; o.table_id = tbl; return o; }
static Operat W(uint32_t pk, std::string v, int s = 0, uint16_t tbl = 0, OpType t = OpType::UPDATE) { Operat o; o.type = t; o.data_primary_key = pk; o.data_value = v; o.server_id = s; o.table_id = tbl; return o; }
static txn A(std::vector<Operat> ops) { if (!ops.empty()) ops.back().last_one = true; txn t; t.operations = ops; return t; }

// Run one epoch; txns are pushed (in order) during sub-epoch `at[i]`.
static std::vector<AssembledTxnResult> runWith(std::vector<txn> ts, std::vector<int> at,
                                               EpochConfig cfg, MultiRingORAM_Servers& mrs) {
    QueriesReceiving qr;
    std::thread prod([&] {
        for (size_t i = 0; i < ts.size(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10 + at[i] * cfg.sub_epoch_ms));
            while (!qr.push_txn(ts[i])) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    // pushes for sub-epoch k happen at ~ (k*sub + 10)ms, all before the window closes
    auto res = runEpoch_RBR_W(qr, mrs, cfg);
    prod.join();
    return res;
}
static EpochConfig C(uint32_t R = 2) { EpochConfig c; c.R = R; c.sub_epoch_ms = 150; c.bread = 8; c.bwrite = 8; c.pad_batches = true; return c; }

int main() {
    std::cout.setstate(std::ios::failbit);   // silence proxy logs
    // ── A: Obladi Fig.5 — T1:W(x) then T3:R(x): both commit, T3 sees T1's value
    { MultiRingORAM_Servers m; auto r = runWith({A({W(1,"10")}), A({R(2)}), A({R(1)})}, {0,0,0}, C(1), m);
      CHECK(r[0].committed && r[1].committed && r[2].committed);
      CHECK(r[2].per_op_result[0] == "10"); CHECK(m.db[1] == "10"); std::cerr << "A done\n"; }
    // ── B: write-too-late (staged): Ta ts0 R(y) → W(x); Tb ts1 R(x) in sub-epoch 1
    { MultiRingORAM_Servers m;
      txn ta = A({R(5)}); ta.operations.back().last_one = false;
      ta.next_stage = [](const std::vector<std::string>&) { TxnStage s; s.ops = {W(1,"new")}; s.done = true; return s; };
      auto r = runWith({ta, A({R(1)})}, {0,0}, C(2), m);
      CHECK(!r[0].committed && r[0].abort_reason == AbortReason::WRITE_TOO_LATE);
      CHECK(r[1].committed && r[1].per_op_result[0] == "v1"); CHECK(!m.db.count(1)); std::cerr << "B done\n"; }
    // ── C: cascade: Ta W(x) + R(y) → stage2 W(q); Tb R(x) (reads Ta); Tc R(q) → Ta too late → Tb cascades
    { MultiRingORAM_Servers m;
      txn ta; ta.operations = {W(1,"ax"), R(7)};
      ta.next_stage = [](const std::vector<std::string>&) { TxnStage s; s.ops = {W(9,"aq")}; s.done = true; return s; };
      auto r = runWith({ta, A({R(1)}), A({R(9)})}, {0,0,0}, C(2), m);
      CHECK(!r[0].committed && r[0].abort_reason == AbortReason::WRITE_TOO_LATE);
      CHECK(!r[1].committed && r[1].abort_reason == AbortReason::CASCADE);
      CHECK(r[2].committed); CHECK(m.db.empty()); std::cerr << "C done\n"; }
    // ── D: bread overflow per server (cap 2): 3rd txn aborts, other server unaffected
    { MultiRingORAM_Servers m; auto c = C(1); c.bread = 2;
      auto r = runWith({A({R(1)}), A({R(2)}), A({R(3)}), A({R(4,1)}), A({R(1)})}, {0,0,0,0,0}, c, m);
      CHECK(r[0].committed && r[1].committed && r[3].committed);
      CHECK(!r[2].committed && r[2].abort_reason == AbortReason::READ_BATCH_FULL);
      CHECK(r[4].committed && r[4].per_op_result[0] == "v1");   // coalesced, no new slot
      CHECK(m.batch_sizes[0] == 3); std::cerr << "D done\n"; }
    // ── E: dependent chain needs 3 fetch rounds, R=2 → UNFINISHED
    { MultiRingORAM_Servers m; int round = 0;
      txn t; t.operations = {R(1)};
      t.next_stage = [&](const std::vector<std::string>& res) { TxnStage s; ++round; s.ops = {R(100 + round)}; s.done = round >= 2; (void)res; return s; };
      auto r = runWith({t}, {0}, C(2), m);
      CHECK(!r[0].committed && r[0].abort_reason == AbortReason::UNFINISHED); std::cerr << "E done\n"; }
    // ── F: T6-style RMW: stage1 R(k), stage2 W(k)= f(result) runs locally after last batch; key known only from result
    { MultiRingORAM_Servers m;
      txn t; t.operations = {R(1)};
      t.next_stage = [](const std::vector<std::string>& res) { TxnStage s; s.ops = {W(1, res[0] + "+50")}; s.done = true; return s; };
      auto r = runWith({t}, {0}, C(1), m);
      CHECK(r[0].committed); CHECK(r[0].per_op_result.size() == 2); CHECK(m.db[1] == "v1+50"); std::cerr << "F done\n"; }
    // ── F2: T3-style: stage-2 keys come from stage-1 result
    { MultiRingORAM_Servers m; m.db[50] = "60,61";
      txn t; t.operations = {R(50)};
      t.next_stage = [](const std::vector<std::string>& res) { TxnStage s; s.ops = {R(res[0] == "60,61" ? 60 : 0), R(61)}; s.done = true; return s; };
      auto r = runWith({t}, {0}, C(2), m);
      CHECK(r[0].committed && r[0].per_op_result.size() == 3 && r[0].per_op_result[1] == "v60"); std::cerr << "F2 done\n"; }
    // ── G: incomplete: no last_one; declared count mismatch; last_one in middle
    { MultiRingORAM_Servers m;
      txn g1; g1.operations = {R(1), R(2)};                    // no last_one
      txn g2 = A({R(1), R(3)}); g2.declared_op_count = 3;       // gap
      txn g3 = A({R(1), R(4)}); g3.operations[0].last_one = true;
      auto r = runWith({g1, g2, g3, A({R(5)})}, {0,0,0,0}, C(1), m);
      CHECK(!r[0].committed && !r[1].committed && !r[2].committed && r[3].committed);
      CHECK(r[1].abort_reason == AbortReason::INCOMPLETE); std::cerr << "G done\n"; }
    // ── J: INSERT is a write everywhere; K: last committed version wins; L: table id separates keys
    { MultiRingORAM_Servers m;
      auto r = runWith({A({W(1,"ins",0,0,OpType::INSERT)}), A({R(1)}), A({W(1,"k2")}),
                        A({W(1,"t3",0,3)}), A({R(1,0,3)})}, {0,0,0,0,0}, C(1), m);
      CHECK(r[1].per_op_result[0] == "ins");   // J
      CHECK(r[4].per_op_result[0] == "t3");    // L: table 3 pk 1 ≠ table 0 pk 1
      bool all = true; for (auto& x : r) all &= x.committed; CHECK(all);
      std::cerr << "J/K/L done (write batch size " << m.batch_sizes.back() << ")\n";
      CHECK(m.batch_sizes.back() == 2); }   // K: (tbl0,pk1)="k2" and (tbl3,pk1)
    // ── H: legacy AssignAbort — same Fig.5 case no longer aborts T1
    { QueriesReceiving qr; std::vector<txn> v = {A({W(1,"10")}), A({R(2)}), A({R(1)})};
      for (uint32_t i = 0; i < v.size(); ++i) { v[i].timestamp_id = i; for (auto& op : v[i].operations) { op.txn_timestamp_id = i; op.global_op_id = i; } }
      qr.AssignAbort(v); CHECK(v[0].is_committed && v[2].is_committed);
      auto p = qr.Dedup(); CHECK(p.per_op_decision[2].kind == OpDecision::Kind::BUFFERED_FROM_WRITE);
      CHECK(p.per_op_decision[2].buffered_value == "10"); std::cerr << "H done\n"; }
    // ── I: push race — hammer push_txn while epochs open/close; every accepted txn must be processed
    { QueriesReceiving qr; MultiRingORAM_Servers m; std::atomic<bool> stop{false}; std::atomic<long> accepted{0};
      std::vector<std::thread> ps;
      for (int p = 0; p < 4; ++p) ps.emplace_back([&] { while (!stop) { if (qr.push_txn(A({R(1)}))) ++accepted; std::this_thread::sleep_for(std::chrono::microseconds(50)); } });
      long processed = 0; auto c = C(2); c.sub_epoch_ms = 2; c.enforce_batch_capacity = false;
      for (int e = 0; e < 200; ++e) processed += (long)runEpoch_RBR_W(qr, m, c).size();
      stop = true; for (auto& t : ps) t.join();
      std::cerr << "I: accepted=" << accepted << " processed=" << processed << "\n";
      CHECK(accepted == processed); }
    std::cerr << (fails ? "SOME TESTS FAILED\n" : "ALL TESTS PASSED\n");
    return fails;
}