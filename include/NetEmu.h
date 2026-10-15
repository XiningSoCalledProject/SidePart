//
// NetEmu.h — batch-level emulation of the proxy <-> storage-server network on ONE host.
//
// All shards run on one EC2 instance, so proxy-to-shard traffic goes over loopback and costs almost
// nothing.  This header adds the cost a real network would charge, at the granularity SidePart's
// protocol works at: one padded batch per contacted shard per round (read sub-epoch or write phase).
//
//   delay(shard s, round) = RTT + bytes / BW
//       bytes = bytes_s                      (DEPART_NET_SHARED=0: every shard has its own link)
//             = sum_s bytes_s of the round   (DEPART_NET_SHARED=1, default: all batches leave
//                                             through the proxy's one NIC and share its bandwidth)
//       bytes_s = (#slots in the shard's padded batch) * bytes_per_slot(s)
//       bytes_per_slot(s) = (L_s + 1) * block_size * PATH_FACTOR   (Ring ORAM reads one block per
//                           bucket on the path; PATH_FACTOR > 1 adds amortized eviction traffic)
//
// The batches of one round are dispatched by parallel worker threads, so the round costs
// max_s (ORAM_s + delay_s): RTT is paid once per round no matter how many shards are contacted,
// while the bandwidth term grows with the number of contacted shards when the NIC is shared.
//
// Configuration (environment, read once per process; all off by default = unchanged behaviour):
//   DEPART_NET_RTT_MS          round-trip time added to every shard batch            (default 0)
//   DEPART_NET_BW_MBPS         link bandwidth in Mbit/s; 0 = infinite                 (default 0)
//   DEPART_NET_SHARED          1 = shards share the proxy NIC, 0 = one link per shard (default 1)
//   DEPART_NET_PATH_FACTOR     multiplier on the path bytes (eviction overhead)       (default 1)
//   DEPART_NET_BYTES_PER_SLOT  fallback when a driver did not set per-shard sizes     (default 4096)
//
#ifndef NET_EMU_H
#define NET_EMU_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace netemu {

struct Config {
    double   rtt_ms = 0.0;
    double   bw_mbps = 0.0;            // 0 = infinite
    bool     shared = true;
    double   path_factor = 1.0;
    uint64_t default_bytes_per_slot = 4096;
    bool enabled() const { return rtt_ms > 0.0 || bw_mbps > 0.0; }
};

inline double envDouble(const char* name, double dflt) {
    const char* e = std::getenv(name);
    return (e && *e) ? std::atof(e) : dflt;
}

inline const Config& config() {
    static const Config c = [] {
        Config k;
        k.rtt_ms                 = envDouble("DEPART_NET_RTT_MS", 0.0);
        k.bw_mbps                = envDouble("DEPART_NET_BW_MBPS", 0.0);
        k.shared                 = envDouble("DEPART_NET_SHARED", 1.0) != 0.0;
        k.path_factor            = envDouble("DEPART_NET_PATH_FACTOR", 1.0);
        k.default_bytes_per_slot = static_cast<uint64_t>(envDouble("DEPART_NET_BYTES_PER_SLOT", 4096));
        return k;
    }();
    return c;
}

// ── bytes per padded slot, per shard (set by the driver once the layout is known) ──
inline std::mutex& sizeMutex() { static std::mutex m; return m; }
inline std::unordered_map<int, uint64_t>& sizeMap() { static std::unordered_map<int, uint64_t> m; return m; }

inline void clearBytesPerSlot() { std::lock_guard<std::mutex> g(sizeMutex()); sizeMap().clear(); }
inline void setBytesPerSlot(int server_id, uint64_t bytes) {
    std::lock_guard<std::mutex> g(sizeMutex()); sizeMap()[server_id] = bytes;
}
inline uint64_t bytesPerSlot(int server_id) {
    std::lock_guard<std::mutex> g(sizeMutex());
    auto it = sizeMap().find(server_id);
    return it == sizeMap().end() ? config().default_bytes_per_slot : it->second;
}

// Ring ORAM online cost of one access: one block from each of the L+1 buckets on a path.
// A tree over n_blocks blocks with Z real slots per bucket has about n_blocks / Z leaves.
inline uint64_t ringOramBytesPerSlot(uint64_t n_blocks, uint32_t Z, uint32_t block_size) {
    const double leaves = std::max(1.0, std::ceil(double(n_blocks) / std::max<uint32_t>(1, Z)));
    const double levels = std::ceil(std::log2(leaves)) + 1.0;           // L + 1
    return static_cast<uint64_t>(std::llround(levels * block_size * config().path_factor));
}

// Delay charged to ONE shard batch of a round.
inline double delayMs(uint64_t shard_bytes, uint64_t round_bytes) {
    const Config& c = config();
    double d = c.rtt_ms;
    if (c.bw_mbps > 0.0) {
        const double bits = 8.0 * double(c.shared ? round_bytes : shard_bytes);
        d += bits / (c.bw_mbps * 1e6) * 1e3;
    }
    return d;
}

inline void sleepMs(double ms) {
    if (ms > 0.0)
        std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int64_t>(std::llround(ms * 1000.0))));
}

// ── counters (reset by the driver per measurement) ──
struct Counters {
    std::atomic<uint64_t> bytes{0};          // emulated bytes put on the wire
    std::atomic<uint64_t> batches{0};        // shard batches sent
    std::atomic<uint64_t> rounds{0};         // dispatch rounds (executeOramOps calls with work)
    std::atomic<uint64_t> round_delay_us{0}; // sum over rounds of the LARGEST shard delay
};
inline Counters& counters() { static Counters c; return c; }
inline void resetCounters() {
    Counters& c = counters();
    c.bytes = 0; c.batches = 0; c.rounds = 0; c.round_delay_us = 0;
}

inline std::string describe() {
    const Config& c = config();
    if (!c.enabled()) return "off (loopback only)";
    return "RTT=" + std::to_string(c.rtt_ms) + " ms, BW=" +
           (c.bw_mbps > 0 ? std::to_string(c.bw_mbps) + " Mbit/s" : std::string("inf")) +
           (c.shared ? ", shared proxy NIC" : ", one link per shard") +
           ", path_factor=" + std::to_string(c.path_factor);
}

}  // namespace netemu

#endif  // NET_EMU_H
