//
// ObjectKey.h — logical row identity shared by the proxy (MVTSO, Queries_Receiving)
// and the storage layer (pos_map, MultiRingORAM_Servers).
// Created 9/22/26.
//
// Invariant:  one logical row  ⇔  exactly one ObjectKey.
//
//   ObjectKey = (table_id, w_id, d_id, logical_id)
//
//   table_id   : which table (TpccTable below; 0 = untyped single-table benchmark)
//   w_id, d_id : warehouse / district prefix of the PK (0 if the table has none)
//   logical_id : 64-bit encoding of the REST of the PK
//
// Composite PKs are packed only by the tpcc_key:: encoders, never by hand.
// ORDER_LINE (W, D, O, OL_NUMBER) → (ORDER_LINE, W, D, pack2(O, OL_NUMBER)),
// which is collision-free for all 32-bit O and OL_NUMBER (no domain bounds
// needed, unlike O·OL_max + OL).
//
// server_id / block_id are NOT part of the identity — they are placement,
// resolved by the pos_map (PartitionPosMap.h).
//
#ifndef OBJECT_KEY_H
#define OBJECT_KEY_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <tuple>

struct ObjectKey {
    uint16_t table_id   = 0;
    uint32_t w_id       = 0;
    uint32_t d_id       = 0;
    uint64_t logical_id = 0;

    bool operator<(const ObjectKey& o) const {
        return std::tie(table_id, w_id, d_id, logical_id) <
               std::tie(o.table_id, o.w_id, o.d_id, o.logical_id);
    }
    bool operator==(const ObjectKey& o) const {
        return table_id == o.table_id && w_id == o.w_id &&
               d_id == o.d_id && logical_id == o.logical_id;
    }
    bool operator!=(const ObjectKey& o) const { return !(*this == o); }

    std::string str() const {
        return "(tbl=" + std::to_string(table_id) + ",w=" + std::to_string(w_id) +
               ",d=" + std::to_string(d_id) + ",id=" + std::to_string(logical_id) + ")";
    }
};

struct ObjectKeyHash {
    size_t operator()(const ObjectKey& k) const noexcept {
        // splitmix-style mixing of the four fields
        uint64_t h = k.logical_id * 0x9E3779B97F4A7C15ULL;
        h ^= (static_cast<uint64_t>(k.w_id) << 32 | k.d_id) + 0x632BE59BD9B4E019ULL + (h << 6) + (h >> 2);
        h ^= static_cast<uint64_t>(k.table_id) + 0x85EBCA77C2B2AE63ULL + (h << 6) + (h >> 2);
        h ^= h >> 31;
        return static_cast<size_t>(h);
    }
};

// Operat::logical_id sentinel: "no explicit logical id, use data_primary_key".
constexpr uint64_t kLogicalIdFromPk = UINT64_MAX;

// Templated so this header does not need Operat's definition (Operat lives in
// MultiRingORAM_Servers.h, which itself includes this header).
// Requires on Operat:  table_id, w_id, d_id, logical_id, data_primary_key.
template <class Op>
inline ObjectKey objectKeyOf(const Op& op) {
    const uint64_t id = (op.logical_id == kLogicalIdFromPk)
                            ? static_cast<uint64_t>(op.data_primary_key)
                            : op.logical_id;
    return ObjectKey{op.table_id, op.w_id, op.d_id, id};
}

template <class Op>
inline void setObjectKey(Op& op, const ObjectKey& k) {
    op.table_id   = k.table_id;
    op.w_id       = k.w_id;
    op.d_id       = k.d_id;
    op.logical_id = k.logical_id;
}

// ── TPC-C / DePart tables and canonical encoders ──────────────────────────
enum class TpccTable : uint16_t {
    WAREHOUSE = 1, DISTRICT = 2, CUSTOMER = 3, HISTORY = 4, NEW_ORDER = 5,
    ORDERS = 6, ORDER_LINE = 7, ITEM = 8, STOCK = 9
};

namespace tpcc_key {
inline constexpr uint64_t pack2(uint32_t hi, uint32_t lo) {
    return (static_cast<uint64_t>(hi) << 32) | lo;
}
inline ObjectKey make(TpccTable t, uint32_t w, uint32_t d, uint64_t id) {
    return ObjectKey{static_cast<uint16_t>(t), w, d, id};
}
inline ObjectKey warehouse(uint32_t w)                        { return make(TpccTable::WAREHOUSE, w, 0, 0); }
inline ObjectKey district(uint32_t w, uint32_t d)             { return make(TpccTable::DISTRICT,  w, d, 0); }
inline ObjectKey customer(uint32_t w, uint32_t d, uint32_t c) { return make(TpccTable::CUSTOMER,  w, d, c); }
inline ObjectKey orders(uint32_t w, uint32_t d, uint32_t o)   { return make(TpccTable::ORDERS,    w, d, o); }
inline ObjectKey newOrder(uint32_t w, uint32_t d, uint32_t o) { return make(TpccTable::NEW_ORDER, w, d, o); }
inline ObjectKey orderLine(uint32_t w, uint32_t d, uint32_t o, uint32_t ol)
                                                              { return make(TpccTable::ORDER_LINE, w, d, pack2(o, ol)); }
inline ObjectKey item(uint32_t i)                             { return make(TpccTable::ITEM,      0, 0, i); }
inline ObjectKey stock(uint32_t w, uint32_t i)                { return make(TpccTable::STOCK,     w, 0, i); }
// HISTORY has no PK in TPC-C: caller supplies a unique synthetic h_id.
inline ObjectKey history(uint32_t w, uint32_t d, uint64_t h_id)
                                                              { return make(TpccTable::HISTORY,   w, d, h_id); }
}  // namespace tpcc_key

#endif  // OBJECT_KEY_H