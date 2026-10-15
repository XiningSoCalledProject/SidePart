//
// PartitionPosMap.h — proxy-side position map:  ObjectKey → (server_id, block_id)
// Created 9/22/26.  Used by MultiRingORAM_Servers (replaces the old
// pk_to_blockid_[server_id][uint32 primary_key]).
//
// Why: the old map was keyed by a bare uint32 PK per server, so CUSTOMER(C=1)
// and ORDERS(O=1) on the same server collided (the later insert silently
// overwrote the earlier one), and ORDER_LINE rows of one order all collapsed
// to one entry.  MVTSO identity (ObjectKey) and ORAM routing identity must be
// the same key.
//
// Invariants (enforced; violations throw std::logic_error at load time):
//   * each ObjectKey is registered at most once, on exactly one server;
//   * block ids handed out by allocate() come from that server's dummy
//     filler blocks, so the ORAM tree size never changes.
//
// Thread-safety: load-time calls (addRecord / addFreeBlocks / ensureServer)
// and allocate() must run on one thread.  lookup()/serverOf() are const and
// safe to call concurrently when no writer is active.
//
#ifndef PARTITION_POS_MAP_H
#define PARTITION_POS_MAP_H

#include <cstdint>
#include <deque>
#include <iostream>
#include <map>
#include <stdexcept>
#include <unordered_map>
#include "ObjectKey.h"

class PartitionPosMap {
public:
    static constexpr uint32_t kNoBlock = UINT32_MAX;

    void clear() { key_to_block_.clear(); key_to_server_.clear(); free_blocks_.clear(); }

    void ensureServer(int server_id) {
        key_to_block_[server_id];
        free_blocks_[server_id];
    }

    // Load time: the real record `k` was inserted at `block_id` on `server_id`.
    void addRecord(int server_id, const ObjectKey& k, uint32_t block_id) {
        auto [it, fresh] = key_to_server_.emplace(k, server_id);
        if (!fresh) {
            throw std::logic_error(
                "[PartitionPosMap] ObjectKey " + k.str() + " registered twice (servers " +
                std::to_string(it->second) + " and " + std::to_string(server_id) +
                "). The ObjectKey mapper must give every logical row a unique key.");
        }
        key_to_block_[server_id].emplace(k, block_id);
    }

    // Load time: blocks [first, first+count) on `server_id` are dummy fillers
    // and may later hold newly inserted rows.
    void addFreeBlocks(int server_id, uint32_t first, uint32_t count) {
        auto& q = free_blocks_[server_id];
        for (uint32_t b = 0; b < count; ++b) q.push_back(first + b);
    }

    uint32_t lookup(int server_id, const ObjectKey& k) const {
        auto sit = key_to_block_.find(server_id);
        if (sit == key_to_block_.end()) return kNoBlock;
        auto it = sit->second.find(k);
        return it == sit->second.end() ? kNoBlock : it->second;
    }

    int serverOf(const ObjectKey& k) const {
        auto it = key_to_server_.find(k);
        return it == key_to_server_.end() ? -1 : it->second;
    }

    // New row: bind `k` to a free (dummy) block on `server_id`.
    // Returns kNoBlock if the key already exists or no free block is left.
    uint32_t allocate(int server_id, const ObjectKey& k) {
        if (key_to_server_.count(k)) return kNoBlock;
        auto fit = free_blocks_.find(server_id);
        if (fit == free_blocks_.end() || fit->second.empty()) return kNoBlock;
        const uint32_t b = fit->second.front();
        fit->second.pop_front();
        key_to_server_.emplace(k, server_id);
        key_to_block_[server_id].emplace(k, b);
        return b;
    }

    // Undo an allocate() (used when the dispatch that would have written
    // the row fails).  The block goes back to the front of the free list.
    void release(int server_id, const ObjectKey& k) {
        auto sit = key_to_block_.find(server_id);
        if (sit == key_to_block_.end()) return;
        auto it = sit->second.find(k);
        if (it == sit->second.end()) return;
        free_blocks_[server_id].push_front(it->second);
        sit->second.erase(it);
        key_to_server_.erase(k);
    }

    size_t size(int server_id) const {
        auto it = key_to_block_.find(server_id);
        return it == key_to_block_.end() ? 0 : it->second.size();
    }
    size_t freeBlocks(int server_id) const {
        auto it = free_blocks_.find(server_id);
        return it == free_blocks_.end() ? 0 : it->second.size();
    }
    size_t totalKeys() const { return key_to_server_.size(); }

    void printStats(std::ostream& os = std::cout) const {
        os << "[pos_map] keys=" << key_to_server_.size() << "  servers:";
        for (const auto& [sid, m] : key_to_block_)
            os << "  s" << sid << "=" << m.size() << " (+" << freeBlocks(sid) << " free)";
        os << std::endl;
    }

private:
    std::map<int, std::unordered_map<ObjectKey, uint32_t, ObjectKeyHash>> key_to_block_;
    std::unordered_map<ObjectKey, int, ObjectKeyHash>                     key_to_server_;
    std::map<int, std::deque<uint32_t>>                                   free_blocks_;
};

#endif  // PARTITION_POS_MAP_H