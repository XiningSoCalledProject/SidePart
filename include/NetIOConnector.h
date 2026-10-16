#ifndef NETIOCONNECTOR_H
#define NETIOCONNECTOR_H

#include "ServerConnector.h"
#include <emp-tool/emp-tool.h>
#include <string>
#include <vector>
#include <cstdint>
#include <functional>    // for std::function (bulk_init_tree_stream)
#include <arpa/inet.h>  // for htonl, ntohl
#include <unordered_map>

// ============================================================================
//  Bulk-init support (added 4/18/26)
// ============================================================================
//  One SlotPayload = one slot worth of data to upload to the server.
//  Used only by bulk_init_tree() during TPC-C initialization.
// ============================================================================
struct SlotPayload {
    uint32_t    global_slot_id;
    std::string cipher;
};

class NetIOConnector : public ServerConnector {
private:
    emp::NetIO* netio_ = nullptr;
    std::string oram_name_;

    // 网络配置
    static constexpr int name_length_bytes = 32;  // ORAM 名称的固定长度
    int party_ = emp::BOB;  // 默认为 BOB (client)

    // ── Bulk-init silent mode (4/18/26) ──────────────────────────────────
    // When true, insert_slot() becomes a no-op. Used during TPC-C init to
    // let RingORAM's constructor populate its metadata without sending N
    // individual slot RTTs. A subsequent bulk_init_tree() uploads the
    // entire tree in one send_data call.
    bool silent_ = false;

    // ── Deferred (batched) mode (10/8/26) ───────────────────────────────
    // Between begin_deferred() and end_deferred() no I/O happens:
    //   find_slot  -> returns the slot's content if it was written earlier
    //                 in this batch, else a PLACEHOLDER (block_size bytes:
    //                 16-byte magic + 8-byte id) and records the fetch;
    //   insert_slot-> records the write and remembers the content locally.
    // end_deferred() then fetches every recorded slot in ONE FIND_SLOTS
    // round trip, replaces placeholders in the recorded writes, streams the
    // writes (no reply needed), and returns placeholder -> ciphertext so the
    // caller can resolve its own copies (stash, READ results).
    bool deferred_ = false;
    std::vector<std::pair<uint32_t, uint32_t>> d_fetch_;          // {bucket, slot}
    std::vector<std::pair<uint32_t, std::string>> d_writes_;      // {slot, cipher}
    std::unordered_map<uint32_t, std::string> d_written_;          // slot -> latest
    uint32_t d_block_size_ = 0;
    std::string d_name_;

    // ========================================
    // 辅助函数：字符串发送和接收
    // ========================================

    void sendString(const std::string& str, uint32_t expected_length) {
        std::string padded = str;
        if (padded.size() > expected_length) {
            padded.resize(expected_length);  // 截断
        } else if (padded.size() < expected_length) {
            padded.resize(expected_length, '\0');  // 填充
        }
        netio_->send_data(padded.data(), expected_length);
    }

    std::string recvString(uint32_t expected_length) {
        std::vector<int8_t> buffer(expected_length);
        netio_->recv_data(buffer.data(), expected_length);
        return std::string(buffer.begin(), buffer.end());
    }

public:
    // ========================================
    // 构造函数和析构函数
    // ========================================

    NetIOConnector(const std::string& host = "127.0.0.1");
    NetIOConnector(const std::string& host, const std::string& oram_name);
    NetIOConnector(const std::string& host, const int& port, const std::string& oram_name);

    ~NetIOConnector() override;

    // ========================================
    // ✅ RingORAM 核心接口（与 RingORAM.cpp 完全匹配）
    // ========================================

    /**
     * 插入数据到指定的 global slot
     * 对应 RingORAM.cpp 中的调用：
     * conn->insert_slot(global_slot_id, cipher, name, block_size);
     *
     * @param global_slot_id 全局 slot 索引
     * @param cipher 加密后的数据块
     * @param oram_name ORAM 名称
     * @param block_size 块大小（字节）
     */
    void insert_slot(
        uint32_t global_slot_id,
        const std::string& cipher,
        const std::string& oram_name,
        uint32_t block_size
    );

    /**
     * 从指定位置读取数据
     * 对应 RingORAM.cpp 中的调用：
     * conn->find_slot(bucket_id, global_slot_id, name, block_size);
     *
     * @param bucket_id Bucket 索引（用于调试，实际通过 global_slot_id 访问）
     * @param global_slot_id 全局 slot 索引
     * @param oram_name ORAM 名称
     * @param block_size 块大小（字节）
     * @return 读取的加密数据块
     */
    std::string find_slot(
        uint32_t bucket_id,
        uint32_t global_slot_id,
        const std::string& oram_name,
        uint32_t block_size
    );

    /**
     * Batched find_slot (10/8/26), listener opcode 11 (FIND_SLOTS).
     * reqs[i] = {bucket_id, global_slot_id}; returns the slots in request
     * order.  One round trip per kFindWindow requests instead of one per
     * slot.  Needs the listener from the same change (Servers_MultiRingORAM
     * with opcode 11); an old listener closes the connection on opcode 11.
     */
    std::vector<std::string> find_slots(
        const std::vector<std::pair<uint32_t, uint32_t>>& reqs,
        const std::string& oram_name,
        uint32_t block_size
    );
    // Max requests per FIND_SLOTS message (reply = kFindWindow x block_size).
    static constexpr size_t kFindWindow = size_t(1) << 20;   // one message per batch in practice

    // ========================================
    //  Bulk init fast-path (4/18/26)
    // ========================================
    //  Used only by TPC-C initialization. When silent_ is on, insert_slot
    //  is a no-op. Use bulk_init_tree to upload all slots in one send.
    //  These are NEW methods; existing code is unchanged.

    // ── Deferred mode (see the private members above) ────────────────
    void begin_deferred(const std::string& oram_name, uint32_t block_size);
    // Returns placeholder-id -> fetched ciphertext.
    std::unordered_map<uint64_t, std::string> end_deferred();
    bool is_deferred() const { return deferred_; }
    static constexpr size_t kPlaceholderMagicLen = 16;
    static bool is_placeholder(const std::string& c, uint64_t* id = nullptr);
    static std::string make_placeholder(uint64_t id, uint32_t block_size);
    uint64_t deferred_reads() const { return d_fetch_.size(); }
    uint64_t deferred_writes() const { return d_writes_.size(); }

    void set_silent(bool s) { silent_ = s; }
    bool is_silent() const { return silent_; }

    /**
     * Upload a whole set of slots in ONE network burst.
     * On the wire this looks identical to N separate insert_slot calls
     * (same opcode=1, same per-slot layout). Server reads it the same way.
     */
    void bulk_init_tree(
        const std::string& oram_name,
        uint32_t block_size,
        const std::vector<SlotPayload>& slots
    );

    /**
     * Streaming variant — MEMORY-SAFE for large trees (v3, 4/20/26).
     *
     * Generate slots on demand via callback, never materializing the full
     * vector. Peak memory stays at ~10 MB regardless of total_slots. This
     * is necessary for parallel 8-table init where each child's peak
     * memory would otherwise exceed machine RAM (OOM killer fires).
     *
     * The callback gets slot index (0..total_slots-1) and returns a
     * SlotPayload (global_slot_id + cipher). Wire format is identical
     * to bulk_init_tree — server sees the same byte stream.
     */
    void bulk_init_tree_stream(
        const std::string& oram_name,
        uint32_t block_size,
        uint32_t total_slots,
        const std::function<SlotPayload(uint32_t)>& slot_gen
    );
    // ========================================
    
    // ========================================
    // ServerConnector 接口实现（继承要求）
    // ========================================
    
    void clear(const std::string& ns = "") override;
    void close() override;
    
    void insert(const uint32_t& id, const std::string& encrypted_block, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void insert(const std::vector<std::pair<uint32_t, std::string>>& blocks, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void insert(const std::string* sbuffer, const uint32_t& low, const size_t& len, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void insert(const std::vector<std::pair<std::string, std::string>>& blocks, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void insertWithTag(const std::vector<std::pair<std::string, std::string>>& blocks, 
                       const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    iterator* scan() override;
    
    std::string find(const uint32_t& id, const std::string& ns = "", 
                     const uint32_t& tuple_length = 128) override;
    
    void find(const std::vector<uint32_t>& ids, std::string* sbuffer, size_t& length, 
              const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    std::string fetch(const std::string& id, const std::string& ns = "", 
                      const uint32_t& tuple_length = 128) override;
    
    void find(const uint32_t& low, const uint32_t& high, std::vector<std::string>& blocks, 
              const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void findByTag(const uint32_t& tag, std::string* sbuffer, size_t& length, 
                   const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void update(const uint32_t& id, const std::string& data, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void update(const std::string* sbuffer, const uint32_t& low, const size_t& len, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void update(const std::vector<std::pair<uint32_t, std::string>> blocks, 
                const std::string& ns = "", const uint32_t& tuple_length = 128) override;
    
    void setTupleLength(const std::string& oram_name, const uint32_t& tuple_length) override;

    // ── SilentScope: RAII guard for bulk-init silent mode (4/26/26) ───────
    //
    // Usage:
    //   {
    //       NetIOConnector::SilentScope guard(conn);
    //       // construct RingORAM, bulk_load_stash, bulk_init_tree_stream
    //   }   // silent_ auto-cleared here, runtime ops work normally
    //
    // Without this guard it's easy to forget set_silent(false) after init,
    // which silently breaks all subsequent EvictPath / EarlyReshuffle calls
    // (insert_slot becomes a no-op so server never gets the eviction writes).
    //
    // SilentScope is exception-safe and handles nested scopes correctly
    // (saves/restores the prior silent_ state).
    class SilentScope {
        NetIOConnector* conn_;
        bool prev_;
    public:
        explicit SilentScope(NetIOConnector* c)
            : conn_(c), prev_(c ? c->is_silent() : false) {
            if (conn_) conn_->set_silent(true);
        }
        ~SilentScope() {
            if (conn_) conn_->set_silent(prev_);
        }
        // Non-copyable, non-movable
        SilentScope(const SilentScope&) = delete;
        SilentScope& operator=(const SilentScope&) = delete;
        SilentScope(SilentScope&&) = delete;
        SilentScope& operator=(SilentScope&&) = delete;
    };
};

#endif // NETIOCONNECTOR_H