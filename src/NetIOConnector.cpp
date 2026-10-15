//
// NetIOConnector.cpp - 完整实现
// 基于 RingORAM.cpp 的实际使用重新设计
//

#include "NetIOConnector.h"
#include "Config.h"
#include <iostream>
#include <cassert>
#include <algorithm>
#include <cstring>
#include <stdexcept>

using namespace std;

// ========================================
// 构造函数和析构函数
// ========================================

NetIOConnector::NetIOConnector(const std::string& host) {
    netio_ = new emp::NetIO(party_ == emp::ALICE ? nullptr : host.c_str(), server_port);
    std::cout << "[NetIOConnector] Setup on port " << server_port << std::endl;
}

NetIOConnector::NetIOConnector(const std::string& host, const std::string& oram_name) 
    : oram_name_(oram_name) {
    netio_ = new emp::NetIO(party_ == emp::ALICE ? nullptr : host.c_str(), server_port);
    std::cout << "[NetIOConnector] Setup on port " << server_port 
              << " for ORAM: " << oram_name << std::endl;
}

NetIOConnector::NetIOConnector(const std::string& host, const int& port, const std::string& oram_name) 
    : oram_name_(oram_name) {
    netio_ = new emp::NetIO(party_ == emp::ALICE ? nullptr : host.c_str(), port);
    std::cout << "[NetIOConnector] Setup on port " << port 
              << " for ORAM: " << oram_name << std::endl;
}

NetIOConnector::~NetIOConnector() {
    try {
        if (netio_ != nullptr) {
            // 发送关闭信号
            int8_t code = -1;
            netio_->send_data(&code, 1);
            std::cout << "[NetIOConnector] Sent close signal to server" << std::endl;
            
            // 释放资源
            delete netio_;
            netio_ = nullptr;
            std::cout << "[NetIOConnector] Connection closed" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "[NetIOConnector] Destructor error: " << e.what() << std::endl;
    }
}

// ========================================
// ✅ RingORAM 核心接口实现
// ========================================

void NetIOConnector::insert_slot(
    uint32_t global_slot_id,
    const std::string& cipher,
    const std::string& oram_name,
    uint32_t block_size
) {
    // ── Bulk-init silent mode (4/18/26) ────────────────────────────────
    // When silent_ is on, we swallow this call without contacting server.
    // This lets RingORAM's constructor finish in milliseconds instead of
    // making N insert_slot RTTs. A subsequent bulk_init_tree() will upload
    // all slots in one go. See NetIOConnector.h for full context.
    if (silent_) return;
    if (deferred_) {
        std::string c = cipher;
        c.resize(block_size, '\0');
        d_written_[global_slot_id] = c;
        d_writes_.emplace_back(global_slot_id, std::move(c));
        return;
    }

    int8_t code = 1;  // 操作码 1：INSERT_SLOT
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    // Step 1: 发送操作码
    netio_->send_data(&code, 1);

    // Step 2: 发送 ORAM 名称（固定长度 32 字节）
    std::string name_buf = oram_name_impl;
    name_buf.resize(name_length_bytes, '\0');  // 填充到 32 字节
    netio_->send_data(name_buf.data(), name_length_bytes);

    // Step 3: 发送 global_slot_id（网络字节序）
    uint32_t slot_id_network = htonl(global_slot_id);
    netio_->send_data(&slot_id_network, sizeof(slot_id_network));

    // Step 4: 发送块大小（网络字节序）
    uint32_t block_size_network = htonl(block_size);
    netio_->send_data(&block_size_network, sizeof(block_size_network));

    // Step 5: 发送加密数据（填充到 block_size）
    std::string padded_cipher = cipher;
    if (padded_cipher.size() > block_size) {
        padded_cipher.resize(block_size);  // 截断
    } else if (padded_cipher.size() < block_size) {
        padded_cipher.resize(block_size, '\0');  // 填充
    }
    netio_->send_data(padded_cipher.data(), block_size);

    #ifdef DEBUG_NETIO
    std::cout << "[INSERT_SLOT] global_slot_id=" << global_slot_id
              << ", block_size=" << block_size
              << ", oram=" << oram_name_impl << std::endl;
    #endif
}

std::string NetIOConnector::find_slot(
    uint32_t bucket_id,        // 用于调试/日志，实际通过 global_slot_id 访问
    uint32_t global_slot_id,
    const std::string& oram_name,
    uint32_t block_size
) {
    if (deferred_) {
        auto w = d_written_.find(global_slot_id);
        d_fetch_.emplace_back(bucket_id, global_slot_id);   // same trace either way
        if (w != d_written_.end()) return w->second;        // read-after-write in batch
        return make_placeholder(d_fetch_.size() - 1, block_size);
    }
    int8_t code = 3;  // 操作码 3：FIND_SLOT
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    // Step 1: 发送操作码
    netio_->send_data(&code, 1);

    // Step 2: 发送 ORAM 名称（固定长度 32 字节）
    std::string name_buf = oram_name_impl;
    name_buf.resize(name_length_bytes, '\0');
    netio_->send_data(name_buf.data(), name_length_bytes);

    // Step 3: 发送 bucket_id（网络字节序，可选参数）
    uint32_t bucket_id_network = htonl(bucket_id);
    netio_->send_data(&bucket_id_network, sizeof(bucket_id_network));

    // Step 4: 发送 global_slot_id（网络字节序）
    uint32_t slot_id_network = htonl(global_slot_id);
    netio_->send_data(&slot_id_network, sizeof(slot_id_network));

    // Step 5: 发送块大小（网络字节序）
    uint32_t block_size_network = htonl(block_size);
    netio_->send_data(&block_size_network, sizeof(block_size_network));

    // Step 6: 接收加密数据
    std::vector<int8_t> buffer(block_size);
    netio_->recv_data(buffer.data(), block_size);
    std::string encrypted_block(buffer.begin(), buffer.end());

    #ifdef DEBUG_NETIO
    std::cout << "[FIND_SLOT] bucket_id=" << bucket_id
              << ", global_slot_id=" << global_slot_id
              << ", block_size=" << block_size
              << ", received " << encrypted_block.size() << " bytes" << std::endl;
    #endif

    return encrypted_block;
}

// ========================================
// Batched FIND_SLOT — opcode 11 (10/8/26)
// ========================================
// Wire:  client -> [11][name 32][count u32][block_size u32][count x (bucket u32, slot u32)]
//        server -> [count x block_size]   (replies in request order)
// One message per call (split at kFindWindow requests), so a call costs one
// round trip instead of one per slot.
//
// Why a new opcode instead of pipelining opcode-3 requests: emp::NetIO uses a
// single FILE* for both directions of the socket.  If several requests are
// in flight, the listener's first fread pulls all of them into the stdio
// buffer, and its fwrite of the first reply switches the stream to write mode
// and discards the buffered, still-unread requests (seen on EC2: the
// connection died on the first batched read).  With one message the listener
// reads the whole request before it writes anything.
std::vector<std::string> NetIOConnector::find_slots(
    const std::vector<std::pair<uint32_t, uint32_t>>& reqs,
    const std::string& oram_name,
    uint32_t block_size
) {
    std::vector<std::string> out;
    out.reserve(reqs.size());
    if (reqs.empty()) return out;
    if (deferred_) {
        for (const auto& r : reqs) out.push_back(find_slot(r.first, r.second, oram_name, block_size));
        return out;
    }

    std::string name_buf = oram_name.empty() ? oram_name_ : oram_name;
    name_buf.resize(name_length_bytes, '\0');
    const uint32_t bs_net = htonl(block_size);

    std::string req_buf, rep_buf;
    for (size_t base = 0; base < reqs.size(); base += kFindWindow) {
        const size_t n = std::min(kFindWindow, reqs.size() - base);
        const uint32_t n_net = htonl(static_cast<uint32_t>(n));
        req_buf.clear();
        req_buf.reserve(1 + name_length_bytes + 8 + 8 * n);
        req_buf.push_back(static_cast<char>(11));          // FIND_SLOTS
        req_buf.append(name_buf);
        req_buf.append(reinterpret_cast<const char*>(&n_net), 4);
        req_buf.append(reinterpret_cast<const char*>(&bs_net), 4);
        for (size_t i = 0; i < n; ++i) {
            const uint32_t bucket_net = htonl(reqs[base + i].first);
            const uint32_t slot_net   = htonl(reqs[base + i].second);
            req_buf.append(reinterpret_cast<const char*>(&bucket_net), 4);
            req_buf.append(reinterpret_cast<const char*>(&slot_net), 4);
        }
        netio_->send_data(req_buf.data(), req_buf.size());
        rep_buf.assign(n * block_size, '\0');
        netio_->recv_data(&rep_buf[0], rep_buf.size());
        for (size_t i = 0; i < n; ++i) out.emplace_back(rep_buf, i * block_size, block_size);
    }
    return out;
}

// ========================================
// Deferred (batched) mode — 10/8/26
// ========================================
static const char kPlaceholderMagic[NetIOConnector::kPlaceholderMagicLen + 1] =
    "\xF7\x1D\xA3\x5C\x08\xE2\x9B\x46\xC1\x7F\x30\xD8\x6A\x15\xBE\x92";

std::string NetIOConnector::make_placeholder(uint64_t id, uint32_t block_size) {
    std::string p(kPlaceholderMagic, kPlaceholderMagicLen);
    p.append(reinterpret_cast<const char*>(&id), sizeof(id));
    p.resize(std::max<size_t>(block_size, p.size()), '\0');
    return p;
}

bool NetIOConnector::is_placeholder(const std::string& c, uint64_t* id) {
    if (c.size() < kPlaceholderMagicLen + sizeof(uint64_t)) return false;
    if (std::memcmp(c.data(), kPlaceholderMagic, kPlaceholderMagicLen) != 0) return false;
    if (id) std::memcpy(id, c.data() + kPlaceholderMagicLen, sizeof(uint64_t));
    return true;
}

void NetIOConnector::begin_deferred(const std::string& oram_name, uint32_t block_size) {
    if (deferred_) throw std::logic_error("[NetIOConnector] begin_deferred: already deferred");
    if (block_size < kPlaceholderMagicLen + sizeof(uint64_t))
        throw std::invalid_argument("[NetIOConnector] begin_deferred: block_size too small");
    deferred_ = true;
    d_name_ = oram_name;
    d_block_size_ = block_size;
    d_fetch_.clear(); d_writes_.clear(); d_written_.clear();
}

std::unordered_map<uint64_t, std::string> NetIOConnector::end_deferred() {
    if (!deferred_) throw std::logic_error("[NetIOConnector] end_deferred: not deferred");
    deferred_ = false;
    std::unordered_map<uint64_t, std::string> resolved;
    // 1) ONE round trip for every slot read in the batch (split only by
    //    kFindWindow).  Reads of slots written earlier in the batch are
    //    fetched too, so the server sees the same set of reads; their
    //    (stale) content is simply not used.
    const uint32_t bs = d_block_size_;
    if (!d_fetch_.empty()) {
        std::vector<std::string> got = find_slots(d_fetch_, d_name_, bs);
        for (size_t i = 0; i < got.size(); ++i) resolved.emplace(i, std::move(got[i]));
    }
    // 2) Stream the writes in batch order, placeholders replaced.  Writes
    //    get no reply, so they cost no round trip.
    for (auto& [slot, cipher] : d_writes_) {
        uint64_t id;
        if (is_placeholder(cipher, &id)) {
            auto it = resolved.find(id);
            if (it == resolved.end())
                throw std::logic_error("[NetIOConnector] end_deferred: unresolved placeholder");
            cipher = it->second;
        }
        insert_slot(slot, cipher, d_name_, bs);
    }
    d_fetch_.clear(); d_writes_.clear(); d_written_.clear();
    return resolved;
}

// ========================================
// ServerConnector 接口实现：基本操作
// ========================================

void NetIOConnector::clear(const std::string& oram_name) {
    int8_t code = 0;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    std::cout << "[CLEAR] Cleared ORAM: " << oram_name_impl << std::endl;
}

void NetIOConnector::close() {
    int8_t code = -1;
    netio_->send_data(&code, 1);
    std::cout << "[CLOSE] Sent close signal" << std::endl;
}

void NetIOConnector::setTupleLength(const std::string& oram_name, const uint32_t& tuple_length) {
    int8_t code = 9;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t length_network = htonl(tuple_length);
    netio_->send_data(&length_network, sizeof(length_network));

    std::cout << "[SET_LENGTH] Set tuple length=" << tuple_length
              << " for ORAM: " << oram_name_impl << std::endl;
}

// ========================================
// ServerConnector 接口实现：INSERT 操作
// ========================================

void NetIOConnector::insert(
    const uint32_t& id,
    const std::string& encrypted_block,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 1;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t id_network = htonl(id);
    netio_->send_data(&id_network, sizeof(id_network));

    sendString(encrypted_block, tuple_length);
}

void NetIOConnector::insert(
    const std::vector<std::pair<uint32_t, std::string>>& blocks,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 1;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t length = blocks.size();
    uint32_t length_network = htonl(length);
    netio_->send_data(&length_network, sizeof(length_network));

    for (const auto& [block_id, data] : blocks) {
        uint32_t id_network = htonl(block_id);
        netio_->send_data(&id_network, sizeof(id_network));
        sendString(data, tuple_length);
    }
}

void NetIOConnector::insert(
    const std::string* sbuffer,
    const uint32_t& low,
    const size_t& len,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 1;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t len_network = htonl(len);
    netio_->send_data(&len_network, sizeof(len_network));

    for (uint32_t i = low; i < low + len; ++i) {
        uint32_t id_network = htonl(i);
        netio_->send_data(&id_network, sizeof(id_network));
        sendString(sbuffer[i - low], tuple_length);
    }
}

void NetIOConnector::insert(
    const std::vector<std::pair<std::string, std::string>>& blocks,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 1;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t length = blocks.size();
    uint32_t length_network = htonl(length);
    netio_->send_data(&length_network, sizeof(length_network));

    for (const auto& [key, data] : blocks) {
        sendString(key, 4);  // 假设 key 是 4 字节
        sendString(data, tuple_length);
    }
}

void NetIOConnector::insertWithTag(
    const std::vector<std::pair<std::string, std::string>>& blocks,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 1;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t length = blocks.size();
    uint32_t length_network = htonl(length);
    netio_->send_data(&length_network, sizeof(length_network));

    uint32_t tag = 0;
    for (const auto& [key, data] : blocks) {
        sendString(key, 4);
        sendString(data, tuple_length);

        uint32_t tag_network = htonl(tag);
        netio_->send_data(&tag_network, sizeof(tag_network));
        tag++;
    }
}

// ========================================
// ServerConnector 接口实现：FIND 操作
// ========================================

std::string NetIOConnector::find(
    const uint32_t& id,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 3;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t id_network = htonl(id);
    netio_->send_data(&id_network, sizeof(id_network));

    return recvString(tuple_length);
}

void NetIOConnector::find(
    const std::vector<uint32_t>& ids,
    std::string* sbuffer,
    size_t& length,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 3;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t num_requests = ids.size();
    uint32_t num_requests_network = htonl(num_requests);
    netio_->send_data(&num_requests_network, sizeof(num_requests_network));

    length = 0;
    for (uint32_t id : ids) {
        uint32_t id_network = htonl(id);
        netio_->send_data(&id_network, sizeof(id_network));

        sbuffer[length] = recvString(tuple_length);
        ++length;
    }
}

std::string NetIOConnector::fetch(
    const std::string& id,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    // 简化实现：假设 id 是数字字符串
    uint32_t id_int = std::stoul(id);
    return find(id_int, oram_name, tuple_length);
}

void NetIOConnector::find(
    const uint32_t& low,
    const uint32_t& high,
    std::vector<std::string>& blocks,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 3;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    for (uint32_t id = low; id < high; ++id) {
        uint32_t id_network = htonl(id);
        netio_->send_data(&id_network, sizeof(id_network));

        blocks.push_back(recvString(tuple_length));
    }
}

void NetIOConnector::findByTag(
    const uint32_t& tag,
    std::string* sbuffer,
    size_t& length,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 3;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    length = 0;
    for (uint32_t t = 0; t < tag; ++t) {
        uint32_t t_network = htonl(t);
        netio_->send_data(&t_network, sizeof(t_network));

        sbuffer[length] = recvString(tuple_length);
        ++length;
    }
}

// ========================================
// ServerConnector 接口实现：UPDATE 操作
// ========================================

void NetIOConnector::update(
    const uint32_t& id,
    const std::string& data,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 4;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t id_network = htonl(id);
    netio_->send_data(&id_network, sizeof(id_network));

    sendString(data, tuple_length);
}

void NetIOConnector::update(
    const std::string* sbuffer,
    const uint32_t& low,
    const size_t& len,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 4;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    for (uint32_t i = low; i < low + len; ++i) {
        uint32_t id_network = htonl(i);
        netio_->send_data(&id_network, sizeof(id_network));
        sendString(sbuffer[i - low], tuple_length);
    }
}

void NetIOConnector::update(
    const std::vector<std::pair<uint32_t, std::string>> blocks,
    const std::string& oram_name,
    const uint32_t& tuple_length
) {
    int8_t code = 4;
    std::string oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    netio_->send_data(&code, 1);
    sendString(oram_name_impl, name_length_bytes);

    uint32_t length = blocks.size();
    uint32_t length_network = htonl(length);
    netio_->send_data(&length_network, sizeof(length_network));

    for (const auto& [block_id, data] : blocks) {
        uint32_t id_network = htonl(block_id);
        netio_->send_data(&id_network, sizeof(id_network));
        sendString(data, tuple_length);
    }
}

// ========================================
// 其他接口
// ========================================

NetIOConnector::iterator* NetIOConnector::scan() {
    // 不支持 scan 操作
    return nullptr;
}

// ============================================================================
//  Bulk init fast-path (4/18/26)
// ============================================================================
//
//  ⚠️  FIXED 4/26/26 — Critical chunk-alignment bug in the original direct
//  implementation:  this function used to slice the pre-built `buf` at byte
//  boundaries (10 MB chunks) and append a 1-byte CHUNK_BARRIER (opcode 10)
//  between chunks.  But CHUNK_BYTES is not a multiple of per_slot_bytes, so
//  chunks ended MID-SLOT.  The barrier byte landed inside the cipher of an
//  in-flight slot, the server consumed it as cipher data, and never saw the
//  CHUNK_BARRIER → never sent ACK → client deadlock on recv_data(&ack, 1).
//
//  Fix: forward to bulk_init_tree_stream(), which only flushes at slot
//  boundaries (it appends complete slots into `buf` and only sends when buf
//  ≥ CHUNK_BYTES).  This also eliminates the original OOM risk: bulk_init_tree
//  used to call `buf.reserve(per_slot_bytes * slots.size())` which for an
//  ORDER_LINE-sized tree is 16+ GB of contiguous memory.
//
//  Wire format remains identical (opcode 1 + slot data, with chunked barrier
//  ACKs from server).
// ============================================================================
void NetIOConnector::bulk_init_tree(
    const std::string& oram_name,
    uint32_t block_size,
    const std::vector<SlotPayload>& slots
) {
    if (slots.empty()) return;
    bulk_init_tree_stream(
        oram_name, block_size,
        static_cast<uint32_t>(slots.size()),
        [&slots](uint32_t i) { return slots[i]; });
}

// ============================================================================
//  bulk_init_tree_stream (v3, 4/20/26) — MEMORY-SAFE streaming upload
// ============================================================================
//
//  Unlike bulk_init_tree() which takes a std::vector<SlotPayload> (requires
//  caller to materialize ALL slots in memory first — 2-5 GB for ORDER_LINE,
//  leading to OOM kills under parallel 8-table init), this version pulls
//  slots from a generator callback one at a time.
//
//  Memory footprint: ~10 MB chunked buffer at any moment, regardless of
//  total tree size. This is CRITICAL for 8-table parallel init where each
//  child process otherwise allocates GB-scale transient buffers totaling
//  50+ GB on a 32 GB machine → OOM.
//
//  Protocol on the wire is IDENTICAL to bulk_init_tree — server reads the
//  same byte stream (same opcode=1, same per-slot layout).
// ============================================================================
void NetIOConnector::bulk_init_tree_stream(
    const std::string& oram_name,
    uint32_t block_size,
    uint32_t total_slots,
    const std::function<SlotPayload(uint32_t)>& slot_gen)
{
    if (total_slots == 0) return;

    const std::string& oram_name_impl = oram_name.empty() ? oram_name_ : oram_name;

    std::string name_buf = oram_name_impl;
    name_buf.resize(name_length_bytes, '\0');

    const int8_t opcode = 1;
    const size_t per_slot_bytes = 1 + name_length_bytes + 4 + 4 + block_size;

    const size_t CHUNK_BYTES = 10 * 1024 * 1024;
    std::vector<char> buf;
    buf.reserve(CHUNK_BYTES + per_slot_bytes);

    uint32_t bs_net = htonl(block_size);
    const char* bs_ptr = reinterpret_cast<const char*>(&bs_net);

    size_t total_sent = 0;
    size_t chunks_sent = 0;
    const size_t total_bytes = (size_t)total_slots * per_slot_bytes;

    // ── Helper: flush the current chunk, append a barrier (opcode 10),
    //    send, and wait for a 1-byte ACK from server. This prevents
    //    TCP send-buffer deadlock on multi-GB uploads: server only lets
    //    the client race ahead by CHUNK_BYTES + O(ack_latency) at a time.
    //    See the corresponding CHUNK_BARRIER handler in Server.cpp /
    //    Servers_MultiRingORAM.cpp.
    auto flush_with_barrier = [&]() {
        buf.push_back(static_cast<char>(10));   // opcode 10 = CHUNK_BARRIER
        netio_->send_data(buf.data(), buf.size());
        total_sent += buf.size();
        chunks_sent++;

        int8_t ack = 0;
        netio_->recv_data(&ack, 1);
        if (ack != 1) {
            throw std::runtime_error(
                "[bulk_init_tree_stream] Invalid ACK from server for ORAM '"
                + oram_name_impl + "' at chunk " + std::to_string(chunks_sent));
        }

        buf.clear();

        if (chunks_sent % 10 == 0) {
            std::cout << "  [bulk_init_tree_stream progress] "
                      << total_sent / (1024 * 1024) << " MB / "
                      << total_bytes / (1024 * 1024) << " MB sent+acked to '"
                      << oram_name_impl << "'\n";
        }
    };

    for (uint32_t i = 0; i < total_slots; ++i) {
        SlotPayload s = slot_gen(i);

        buf.push_back(static_cast<char>(opcode));
        buf.insert(buf.end(), name_buf.begin(), name_buf.end());
        uint32_t slot_net = htonl(s.global_slot_id);
        const char* slot_ptr = reinterpret_cast<const char*>(&slot_net);
        buf.insert(buf.end(), slot_ptr, slot_ptr + 4);
        buf.insert(buf.end(), bs_ptr, bs_ptr + 4);
        if (s.cipher.size() > block_size) s.cipher.resize(block_size);
        else if (s.cipher.size() < block_size) s.cipher.resize(block_size, '\0');
        buf.insert(buf.end(), s.cipher.begin(), s.cipher.end());

        // Flush when ≥ CHUNK_BYTES of slot data have accumulated. The
        // barrier byte is added inside flush_with_barrier.
        if (buf.size() >= CHUNK_BYTES) {
            flush_with_barrier();
        }
    }

    // Final (possibly partial) chunk.
    if (!buf.empty()) {
        flush_with_barrier();
    }

    std::cout << "[bulk_init_tree_stream] Sent " << total_slots
              << " slots (" << total_bytes << " bytes, "
              << chunks_sent << " chunks) to ORAM '"
              << oram_name_impl << "'. Peak mem ~"
              << CHUNK_BYTES / (1024 * 1024) << " MB. ACKs: "
              << chunks_sent << "/" << chunks_sent << ".\n";

    // ── FIX (silent footgun) ─────────────────────────────────────────────
    // If silent_ is still on at this point, the caller is about to enter
    // runtime operations (queries / evictions) on this connector with
    // insert_slot still being a no-op.  That silently breaks WriteBucket
    // (eviction never reaches server, server-side state diverges).
    //
    // We emit a loud warning here and recommend NetIOConnector::SilentScope
    // RAII guard (see header).  We do NOT auto-clear, because the caller
    // may have multiple ORAMs sharing this connector and want silent for
    // the next one too — auto-clear would be the wrong default.
    // ─────────────────────────────────────────────────────────────────────
    if (silent_) {
        std::cerr << "[bulk_init_tree_stream] WARNING: silent_ is still TRUE "
                  << "after bulk init for ORAM '" << oram_name_impl << "'.\n"
                  << "  → Subsequent insert_slot() calls will be NO-OPs.\n"
                  << "  → If you've finished initializing this ORAM, call "
                  << "set_silent(false) before queries, or use SilentScope.\n";
    }
}