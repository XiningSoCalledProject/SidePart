//
// Created by Xining Yuan on 6/11/25.
//

//
// Created by Xining Yuan on 4/28/25.
//

#include "RingORAM.h"
#include <cmath>
#include <cryptopp/osrng.h>
#include <string>
#include "iostream"
#include "NetIOConnector.h"
#include <string>
#include <algorithm>
#include <unordered_set>
#include <vector>
#include <random>
#include <cstdlib>

using namespace CryptoPP;


RingORAM::RingORAM(const uint32_t &n, const uint32_t &buck_size,
                   const std::string &pathoram_name, const uint32_t &block_length,
                   NetIOConnector* ORAMO, const uint32_t &S_input,
                   const uint32_t noise_stash_reserve) {
    // noise_stash_reserve (renamed 9/22/26 from permanent_stash_reserve):
    //   EXTRA CLIENT-SIDE STASH CAPACITY needed because negative DP noise made
    //   the tree smaller than the real record count:
    //       noise_stash_reserve = max(0, n − ñ)
    //
    // What the privacy argument actually requires is only:
    //       |tree capacity| = ñ            (public, a function of ñ alone)
    //       all real records ⊆ tree ∪ stash
    // It does NOT require that a FIXED set of (n − ñ) records stays in the
    // stash forever.  Records move between stash and tree by normal RingORAM
    // eviction: A may be in the stash now and in the tree after the next
    // EvictPath, with X, Y taking its place.  The server never sees stash
    // membership or occupancy, so that churn leaks nothing; what would leak is
    // growing the TREE to n, which would reveal n > ñ (the sign of the noise).
    //
    // So this value is headroom, not a pinned record set.
    // parameters check;
    if (block_length <= sizeof(uint32_t)) {
        throw std::invalid_argument("tuple_length must be > 4");
    }
    if (buck_size == 0) {
        throw std::invalid_argument("buck_size must be > 0");
    }
    // ── FIX (4/26/26): guard against n=0 which makes log2(n) undefined.
    // For TPC-C with extreme epsilon, noisy_synopsis can theoretically be
    // 0 if Laplace noise is large negative; without this guard, height
    // becomes UB (cast from -infinity to uint32_t).
    if (n == 0) {
        throw std::invalid_argument(
            "RingORAM: n must be >= 1 (got 0). "
            "If this is a DP-noisy partition with zero records, "
            "use Math.max(1, noisy_synopsis) at the caller.");
    }

    // initialize the basic parameters;
    round = 0;
    bucket_size = buck_size;
    S = S_input;
    noise_stash_reserve_ = noise_stash_reserve;
    if (S > buck_size) {
        throw std::invalid_argument("S (dummy slots) must be <= bucket size (total slots)");
    }
    blocks_Z = buck_size - S;
    name = pathoram_name;
    n_blocks = n;
    height = static_cast<uint32_t>(ceil(log2((double) n))) + 1;
    std::cout << "The height of the ORAM is " << height << std::endl;
    num_leaves = 1 << (height - 1);  // the bottom leaves number;
    std::cout << " The number of leaves is " << num_leaves << std::endl;
    // Eviction frequency A: how many accesses between consecutive EvictPath() calls.
    // RingORAM paper recommends A = O(log N) = height.
    A = height;
    std::cout << "The A (eviction period) is " << A << std::endl;

    // stash_limit: absolute safe upper bound for stash occupancy.
    //
    // Two components:
    //   (a) Normal ORAM stash bound: max(6 × h × Z, 2 × N + 50)
    //       This covers steady-state stash occupancy and transient spikes from
    //       EarlyReshuffle (up to height × bucket_size blocks pushed at once).
    //
    //   (b) noise_stash_reserve_: with a tree sized to ñ < n, up to (n − ñ)
    //       real records cannot be in the tree at any given moment (which ones
    //       varies over time).  The limit must cover that on top of the normal
    //       steady-state bound.
    //       stash occupancy = normal ORAM stash pressure
    //                       + noise-induced capacity pressure (≤ this reserve)
    stash_limit = std::max(6u * height * blocks_Z, 2u * n_blocks + 50u)
                  + noise_stash_reserve_;

    std::cerr << "[Init] ORAM height = " << height
              << ", num_leaves = " << num_leaves
              << ", blocks_Z = " << blocks_Z
              << ", A (eviction period) = " << A
              << ", stash_limit = " << stash_limit
              << "  (n_blocks=" << n_blocks
              << ", noise_stash_reserve=" << noise_stash_reserve_ << ")" << std::endl;

    // initialize the encryption;
    ecrypt_key = new byte[Util::key_length];
    Util::prng.GenerateBlock(ecrypt_key, Util::key_length);


    // network setting;
    conn = ORAMO;
    block_size = block_length;
    conn -> setTupleLength(pathoram_name, block_size);  // set the fixed tuple length;

    std::cerr << "[Init] Fixed block length (B) = " << block_size
              << ", S = " << S << " dummy per bucket\n";

    // initialize the pos_map, 1-multi pos map; (every block_id to vector<ChunkPos>);
    block_chunks_pos.clear();  // use Put to fill in;

    // calculate the bucket count（binary tree structure）
    uint32_t total_buckets = (1 << height) - 1;
    std::cerr << "[Init] Total buckets = " << total_buckets << std::endl;

    buckets.clear();
    bucket_blockdata.clear();

    // initialize bucket and metadata，and fill in the dummy date;
    for (uint32_t i = 0; i < total_buckets; ++i) {
        BucketBlock Block;
        RingBucket bucket(bucket_size);

        Block.valids.resize(bucket_size, true);             // all valid
        std::unordered_set<uint32_t> real_slots;
        // randomly choose Z slots to put the real slots;
        while (real_slots.size() < blocks_Z) {
            real_slots.insert(Util::rand_int(bucket_size));
        }
        for (const auto& slot : real_slots) {
            Block.ptrs.push_back(slot);
    /* quieted: std::cout << "The real slot in the bucket " << i << " is " << slot  << std::endl; */
        }
        Block.count = 0;

        for (uint32_t slot = 0; slot < bucket_size; slot++) {
            // this slot is not in the real slot set or the selected_real_slots; put the dummy slot;
            std::string cipher;

            // fill in all slots with dummy data;
            std::string dummy_payload = Util::generate_random_block(block_size - Util::aes_block_size - sizeof(uint32_t));
            int32_t dummy_id = -1;
            std::string block_id_str(reinterpret_cast<char*>(&dummy_id), sizeof(uint32_t));
            std::string plain = block_id_str + dummy_payload;

            //std::cout << "The plain text for this dummy block is " << plain << std::endl;

            Util::aes_encrypt(plain, ecrypt_key, cipher);

            uint32_t global_slot_id = slotKey(i, slot);

            conn -> insert_slot(global_slot_id, cipher, name, block_size);

        }

        // save the information on the client;
        bucket_blockdata[i] = Block;
    }
    std::cerr << "[Init Done] RingORAM initialization complete.\n";
}


RingORAM::~RingORAM() {
    std::cout << "[~RingORAM] Destructor called. Peak stash size = "
              << stash_high_watermark << " / " << stash_limit << std::endl;
}

// Post-initialization eviction.
//
// After inserting all records during init the stash may hold far more than A
// entries.  We drain towards the NORMAL target A — never towards
// (A + noise_stash_reserve_).
//
// Rationale (9/22/26): the old target pinned (n − ñ) records in the stash by
// construction.  That is stronger than the privacy argument needs, and it
// makes a specific set of records permanently client-side.  The invariant is
// only "tree capacity = ñ, all real records ⊆ tree ∪ stash": push as many
// records into the tree as physically fit, and whatever does not fit stays in
// the stash — which records those are is free to change with every eviction.
//
// In Case B the tree fills up at ñ blocks and eviction then stops making
// progress; we detect that (instead of assuming a fixed residual) and stop.
// The residual is therefore ≈ n − ñ, but it is an OUTCOME, not a rule.
void RingORAM::flush_stash_if_needed() {
    const uint32_t target = A;
    if (stash.size() <= target) {
        std::cout << "[flush_stash] Stash size " << stash.size()
                  << " ≤ target=" << target << " (A), no extra eviction needed.\n";
        return;
    }
    const uint32_t initial_size = static_cast<uint32_t>(stash.size());
    uint32_t evict_count   = 0;
    uint32_t best          = initial_size;   // smallest stash seen so far
    uint32_t since_progress = 0;             // evictions since `best` last improved
    // One EvictPath touches a single reverse-lexicographic path, so a few
    // fruitless passes are normal; only a full sweep without progress means
    // the tree is genuinely full.
    const uint32_t stall_limit = std::max(2u * height, num_leaves);
    while (stash.size() > target) {
        EvictPath();
        ++evict_count;
        if (stash.size() < best) { best = static_cast<uint32_t>(stash.size()); since_progress = 0; }
        else                     { ++since_progress; }
        if (since_progress >= stall_limit) {
            std::cout << "[flush_stash] tree is full: " << stash.size()
                      << " entries remain after " << evict_count
                      << " evictions (no progress in " << since_progress
                      << ").  Expected when ñ < n; residual ≈ n − ñ ≤ reserve="
                      << noise_stash_reserve_ << ".\n";
            break;
        }
        // Absolute safety net only — termination is guaranteed by the stall
        // check above.  It must scale with the amount of work to do (init can
        // leave the whole partition in the stash), not just with the tree.
        if (evict_count > std::max(4u * num_leaves, 4u * initial_size)) {
            std::cerr << "[flush_stash] WARNING: stopping after " << evict_count
                      << " evictions with " << stash.size() << " entries left.\n";
            break;
        }
    }
    std::cout << "[flush_stash] " << evict_count << " extra eviction(s): "
              << initial_size << " → " << stash.size()
              << " stash entries  (target ≤ " << target
              << ", noise headroom=" << noise_stash_reserve_ << ")\n";
}

// Internal helper: insert one (block_id, chunk_id) → data entry into the stash.
// Enforces the fixed stash_limit and updates the high-watermark counter.
inline void RingORAM::stash_insert(uint32_t block_id, uint32_t chunk_id, const std::string& data) {
    stash[{block_id, chunk_id}] = data;   // overwrite if already present (READ re-inserts same key)
    uint32_t cur = static_cast<uint32_t>(stash.size());
    if (cur > stash_high_watermark) stash_high_watermark = cur;
    if (cur > stash_limit) {
        // This should be unreachable: stash_limit = max(6*h*Z, 2*N+50),
        // which is a true hard ceiling (there are only N blocks in the system,
        // each contributing at most 2 stash entries).
        // If we ever get here it indicates a bug in block accounting.
        std::cerr << "[STASH OVERFLOW – BUG] size=" << cur
                  << " exceeds hard ceiling=" << stash_limit
                  << "  (block_id=" << block_id << ", chunk_id=" << chunk_id << ")\n";
        std::abort();
    }
}

std::string RingORAM::get(const uint32_t & key) {
    std::string res;
    uint32_t int_key = key;
    std::string block = access(int_key, OpType::READ, res);
    return block;
}

std::string RingORAM::access(uint32_t block_id, OpType op, const std::string& data_in) {
    if (op == OpType::INSERT){
    /* quieted: std::cout << "We want to put " << data_in << " in the database." << std::endl; */
        //Step 1: encrypt the data block;
        std::string cipher;
        Util::aes_encrypt(data_in, ecrypt_key, cipher);
    /* quieted: std::cout << "After the encryption, the data block is " << cipher << std::endl; */
        size_t total = cipher.size();
        // Step 2: split the data block into the various chunks;
        size_t offset = 0;
        uint32_t chunk_id = 0;
        while (offset < total) {
            size_t take = std::min<size_t>(static_cast<size_t>(block_size), total - offset);
            std::string chunk = cipher.substr(offset, take);
            chunk.resize(block_size, '\0');  // keep the same length of the block size;
            // randomly choose a path for every chunk;
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(0, num_leaves - 1);
            uint32_t L = distrib(gen);
    /* quieted: std::cout << "The path ID is " << L << std::endl; */
            block_chunks_pos[block_id][chunk_id] = L;
            stash_insert(block_id, chunk_id, chunk);
            chunk_id += 1;
            offset += take;
        }
        round = (round + 1) % A;
        if (round == 0) {
            std::cout << "[EvictPath]: Triggers the EvictPath()." << std::endl;
            EvictPath();
        }
        return "";
    }

    // Step 1: change/ give the new position of the block_id;
    // ── FIX (4/26/26): defensive check for unknown block_id ───────────────
    // If proxy passes a block_id that was never INSERTed (typically a bug
    // in proxy's external_pk → block_id mapping), block_chunks_pos[block_id]
    // would silently insert an empty entry via operator[].  Then the access
    // loop runs 0 times, reassembled_data is empty, and Util::aes_decrypt("")
    // throws inside CryptoPP — child crashes, parent sees EOF, hard to debug.
    //
    // Instead: detect the missing block_id, log once per block_id, and return
    // empty string.  Proxy can check `per_op_results[i].empty()` and surface
    // it as a logical error (e.g., "primary_key 42 → block_id mapping wrong").
    {
        auto bcp_it = block_chunks_pos.find(block_id);
        if (bcp_it == block_chunks_pos.end() || bcp_it->second.empty()) {
            static std::unordered_map<uint32_t, uint64_t> miss_counts;
            uint64_t& cnt = miss_counts[block_id];
            cnt++;
            if (cnt == 1 || cnt % 1000 == 0) {
                std::cerr << "[access] WARNING: block_id=" << block_id
                          << " not found in pos_map (op="
                          << static_cast<int>(op) << ", cnt=" << cnt
                          << ").  Proxy mapping likely wrong.  "
                          << "Returning empty string.\n";
            }
            return "";   // graceful return; proxy detects via empty plaintext
        }
    }

    std::map<uint32_t, std::map<uint32_t, std::string>> block_to_chunks;
    for (const auto &[chunk_id, pos_id] : block_chunks_pos[block_id]) {
        uint32_t current_path = pos_id;
    /* quieted: std::cout << "The current path number is " << current_path << std::endl; */
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(0, num_leaves - 1);
        uint32_t L = distrib(gen);
    /* quieted: std::cout << "The path ID is " << L << std::endl; */
        // update the pos_map;
        block_chunks_pos[block_id][chunk_id] = L;

        // Step 2: Get the chunk in stash or the server;
        std::pair<uint32_t, uint32_t> block_chunk = {block_id, chunk_id};
        std::string corres_data = ReadPath(current_path, block_chunk);
        if (corres_data.empty()) {
            // this data must be in the stash, so find this data in the stash;
            corres_data = stash[{block_id, chunk_id}];
    /* quieted: std::cout << "The encrypted block is " << corres_data << std::endl; */
            block_to_chunks[block_id][chunk_id] = corres_data;
        } else {
            block_to_chunks[block_id][chunk_id] = corres_data;
        }
        EarlyReshuffle(current_path);
    }
    std::string reassembled_data;
    for (const auto &[chunk_id, encrypted_chunk]: block_to_chunks[block_id]) {
        reassembled_data += encrypted_chunk;
    }

    // ── FIX #6 (latent): multi-chunk reassembly safety check ─────────────
    // Each chunk was padded to block_size on INSERT.  If cipher.size() was
    // > block_size (multi-chunk), then reassembled_data now contains the
    // padding bytes from each chunk and AES decrypt will fail on the trailing
    // \0 bytes.  This is a known limitation: current callers use block_size
    // big enough to hold cipher in one chunk, so this branch is normally not
    // hit.  We assert here so any future caller hitting it gets a loud error
    // instead of silent garbage.
    if (block_to_chunks[block_id].size() > 1) {
        std::cerr << "[access] WARNING: multi-chunk read for block_id="
                  << block_id << " (chunks=" << block_to_chunks[block_id].size()
                  << "); reassembly does not strip per-chunk padding.  "
                  << "If decryption fails, encode the original cipher length "
                  << "in chunk 0 and truncate before decrypt.\n";
    }
    // ─────────────────────────────────────────────────────────────────────

    if (op == OpType::READ) {
        if (defer_decrypt_) return reassembled_data;   // access_batch resolves + decrypts
        std::string plain;
        Util::aes_decrypt(reassembled_data, ecrypt_key, plain);
    /* quieted: std::cout << "The plain block in the stash is " << plain << std::endl; */
        // Step 4: If the operations is READ, return the block directly;
    /* quieted: std::cout << "I have already read back from the stash or the database and needs to check the reshuffle." << std::endl; */
        return plain;
    }
        // Step 5: If the operation is WRITE, modify the data and put the block to the correct position (stash or server);
    else if (op == OpType::UPDATE) {
    /* quieted: std::cout << "We want to put " << data_in << " in the database." << std::endl; */
        std::string cipher;
        Util::aes_encrypt(data_in, ecrypt_key, cipher);
        reassembled_data = cipher;
        // ── GUARD (9/22/26) ──────────────────────────────────────────────
        // The loop below re-maps chunk ids 0..k-1 of the NEW ciphertext but
        // never removes ids >= k.  If the new value needs fewer chunks than
        // the stored one, READ would later reassemble stale trailing chunks
        // and decryption fails.  Callers must write fixed-width values (the
        // proxy pads to the load-time tuple width in executeOramOps).
        {
            const size_t new_chunks = (cipher.size() + block_size - 1) / block_size;
            auto bcp = block_chunks_pos.find(block_id);
            if (bcp != block_chunks_pos.end() && bcp->second.size() > new_chunks) {
                static uint64_t shrink_warned = 0;
                if (shrink_warned++ == 0)
                    std::cerr << "[access] WARNING: UPDATE of block_id=" << block_id
                              << " shrinks it from " << bcp->second.size() << " to "
                              << new_chunks << " chunks; stale chunks will corrupt later "
                                 "READs.  Write fixed-width values.  (reported once)\n";
            }
        }
        // Step 2: split the data block into the various chunks;
        size_t total = cipher.size();
        size_t offset = 0;
        uint32_t chunk_id = 0;
        while (offset < total) {
            size_t take = std::min<size_t>(static_cast<size_t>(block_size), total - offset);
            std::string chunk = cipher.substr(offset, take);
            chunk.resize(block_size, '\0');  // keep the same length of the block size;
            // ── FIX (9/26/26): drop a stale tree copy before writing the new value.
            // The read loop above already re-mapped this chunk to a new leaf and
            // put the OLD value in the stash; EarlyReshuffle may then have
            // evicted that old value into a bucket on the new path.  If it stays
            // valid there, the next ReadPath finds it and overwrites the new
            // stash value with the stale one.  Invalidate it (client metadata
            // only; the slot is simply treated as consumed, as in Ring ORAM).
            {
                auto bc = block_chunks_pos.find(block_id);
                if (bc != block_chunks_pos.end()) {
                    auto cc = bc->second.find(chunk_id);
                    if (cc != bc->second.end()) {
                        const uint32_t leaf = cc->second;
                        const std::pair<uint32_t, uint32_t> key{block_id, chunk_id};
                        for (uint32_t level = 0; level < height; ++level) {
                            const uint32_t bk = (1u << level) - 1u + (leaf >> (height - 1 - level));
                            auto bb = bucket_blockdata.find(bk);
                            if (bb == bucket_blockdata.end()) continue;
                            BucketBlock& Bk = bb->second;
                            for (size_t j = 0; j < Bk.addrs.size() && j < Bk.ptrs.size(); ++j)
                                if (Bk.addrs[j] == key && Bk.valids[Bk.ptrs[j]])
                                    Bk.valids[Bk.ptrs[j]] = false;
                        }
                    }
                }
            }
            // randomly choose a path for every chunk;
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(0, num_leaves - 1);
            uint32_t L = distrib(gen);
    /* quieted: std::cout << "The path ID is " << L << std::endl; */
            block_chunks_pos[block_id][chunk_id] = L;
            stash_insert(block_id, chunk_id, chunk);
            chunk_id += 1;
            offset += take;
        }
    /* quieted: std::cout << "I can run here, and put the data in the stash successfully." << std::endl; */
        // if the number of accessing the database is the multiple of the A, EvictPath;
        round = (round + 1) % A;
        if (round == 0) {
    /* quieted: std::cout << "Try to evict the data to the database." << std::endl; */
            EvictPath();
        }
    }

    return "";
}

// use the BucketBlock data structure on the client:
// check the bucket through bucket_idx, using the corresponding target_block_id, return the data;
// if no target block_id slot, return the dummy slot;
std::pair<uint32_t, bool> RingORAM::GetBlockOffset(uint32_t bucket_idx, std::pair <uint32_t, uint32_t> block_chunk_id) {
    BucketBlock& Block = bucket_blockdata[bucket_idx];
    std::vector<uint32_t> dummy_slot_id_to_pos;
    for (uint32_t i = 0; i < Block.addrs.size(); ++i) {
        if (Block.addrs[i] == block_chunk_id && Block.valids[Block.ptrs[i]]) {
    /* quieted: std::cout << "I find the target data block in the bucket " << bucket_idx << " slot " << Block.ptrs[i] << std::endl; */
            Block.valids[Block.ptrs[i]] = false;
            return {Block.ptrs[i], true};
        }
    }

    std::unordered_set<uint32_t> real_slots;

    for (uint32_t i = 0; i < Block.ptrs.size(); ++i) {
        real_slots.insert(Block.ptrs[i]);
    }

    for (uint32_t slot = 0; slot < bucket_size; ++slot) {
        if (real_slots.find(slot) == real_slots.end() && Block.valids[slot]) {
            dummy_slot_id_to_pos.push_back(slot);
    /* quieted: std::cout << "The dummy slot is " << slot << std::endl; */
        }
    }

    if (!dummy_slot_id_to_pos.empty()) {
        uint32_t idx = Util::rand_int(dummy_slot_id_to_pos.size());
        uint32_t chosen_slot = dummy_slot_id_to_pos[idx];
    /* quieted: std::cout << "We finally choose dummy slot " << dummy_slot_id_to_pos[idx] << " to get." << std::endl; */
        Block.valids[chosen_slot] = false;
        return {chosen_slot, false};
    }
    std::cerr << "[ERROR] No valid dummy slots or real blocks found in bucket " << bucket_idx << std::endl;
    exit(1);
}

// Input: the block with block_id that I want and the PathID this block is in;
// Output: return the encrypted data block;
std::string RingORAM::ReadPath(uint32_t PathID, std::pair <uint32_t, uint32_t> block_chunk_id) {
    // Batched (10/8/26): which slot to read in each bucket depends only on
    // client metadata (GetBlockOffset), never on data returned by the server,
    // so all `height` slots are chosen first and fetched in ONE round trip
    // (NetIOConnector::find_slots) instead of one find_slot RTT per bucket.
    // Same slots, same order, same metadata updates as before.
    std::string return_encrypted_block;
    std::vector<std::pair<uint32_t, uint32_t>> reqs;   // {bucket_id, global_slot_id}
    std::vector<bool> is_real_slot;
    reqs.reserve(height);
    is_real_slot.reserve(height);
    for (uint32_t i = 0; i < height; ++i) {
        const uint32_t bucket_id = (1u << i) - 1u + (PathID >> (height - 1 - i));
        auto [slot_id, is_real] = GetBlockOffset(bucket_id, block_chunk_id);
        bucket_blockdata[bucket_id].count += 1;
        reqs.push_back({bucket_id, slotKey(bucket_id, slot_id)});
        is_real_slot.push_back(is_real);
    }
    const std::vector<std::string> got = conn->find_slots(reqs, name, block_size);
    for (size_t j = 0; j < got.size(); ++j) {
        if (is_real_slot[j]) {
            stash_insert(block_chunk_id.first, block_chunk_id.second, got[j]);
            return_encrypted_block = got[j];
        }
    }
    return return_encrypted_block;
}


// input: the bucket that we want to read;
// Put Z blocks in the stash;
void RingORAM::ReadBucket(uint32_t bucket_idx) {
    ReadBuckets(std::vector<uint32_t>{bucket_idx});
}

// Batched (10/8/26): read every still-valid slot of all given buckets in ONE
// round trip (per kFindWindow slots) and move their real blocks to the stash.
// Reading never changes bucket metadata (WriteBucket resets it), so reading a
// set of buckets up front is equivalent to reading them one by one.
void RingORAM::ReadBuckets(const std::vector<uint32_t>& bucket_idxs) {
    std::vector<std::pair<uint32_t, uint32_t>> reqs;             // {bucket, global_slot}
    std::vector<std::pair<bool, std::pair<uint32_t, uint32_t>>> real;   // is_real, (bid, cid)
    for (uint32_t bucket_idx : bucket_idxs) {
        BucketBlock& Block = bucket_blockdata[bucket_idx];
        std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> real_slot_id;
        for (uint32_t k = 0; k < Block.addrs.size() && k < Block.ptrs.size(); ++k)
            real_slot_id[Block.ptrs[k]] = Block.addrs[k];
        for (uint32_t i = 0; i < bucket_size; ++i) {
            if (!Block.valids[i]) continue;
            reqs.push_back({bucket_idx, slotKey(bucket_idx, i)});
            auto it = real_slot_id.find(i);
            real.push_back(it == real_slot_id.end() ? std::make_pair(false, std::make_pair(0u, 0u))
                                                    : std::make_pair(true, it->second));
        }
    }
    const std::vector<std::string> got = conn->find_slots(reqs, name, block_size);
    for (size_t j = 0; j < got.size(); ++j)
        if (real[j].first) stash_insert(real[j].second.first, real[j].second.second, got[j]);
}


// When the access S times for one bucket, call the early reshuffling function;
// input: the bucket_idx that we need to do the early reshuffle;
// Reassign the slots order; reset the count as 0; reset the valid;
void RingORAM::EarlyReshuffle(uint32_t Path_ID) {
    // Step 1: Check every bucket along this Path;
    std::vector<uint32_t> buckets_need_early_reshuffle;
    for (size_t i = 0; i < height; ++i) {
        uint32_t bucket_idx = (1 << i) - 1 + (Path_ID >> (height - 1 - i));
    /* quieted: std::cout << "[Early Reshuffle Check]: The checking bucket is " << bucket_idx << std::endl; */
        BucketBlock& Block = bucket_blockdata[bucket_idx];
    /* quieted: std::cout << "The bucket count is " << Block.count << std::endl; */
        if(Block.count < S){
    /* quieted: std::cout << "The path " << i << " does not need to early reshuffle." << std::endl; */
            continue;
        } else {
            buckets_need_early_reshuffle.push_back(bucket_idx);

        }
    }
    std::reverse(buckets_need_early_reshuffle.begin(), buckets_need_early_reshuffle.end());
    if (buckets_need_early_reshuffle.size() == 0) {
        return;
    }

    // Step 2: EarlyReshuffle all buckets that count reaches S.
    // Batched (10/8/26): read all of them in one round trip, then rewrite
    // them leaf-first as before.  A block read from one of these buckets may
    // now be written into another of them if it lies on its path, which keeps
    // the Ring ORAM invariant (a block lives on its own path or in the stash).
    ReadBuckets(buckets_need_early_reshuffle);
    for (size_t j = 0; j < buckets_need_early_reshuffle.size(); ++j)
        WriteBucket(buckets_need_early_reshuffle[j]);
}

// input: bucket_idx: which bucket we have to write the data in；local_stash:
// put blocks that belongs to this buket in this bucket;
void RingORAM::WriteBucket(uint32_t bucket_idx) {
    // <block_id, chunk_id>, block> pair;
    std::unordered_map<std::pair<uint32_t, uint32_t>, std::string, pair_hash> block_chunk_string;
    uint32_t realblocks_count = 0;

    // ── FIX #1 (OOM): in-place iterate stash and erase, NO full copy ──────
    // Old code: `auto temp_stash = stash;` copied the entire stash (could be
    //   hundreds of MB after bulk_load_stash).  EvictPath calls WriteBucket
    //   `height` times → quadratic transient memory.
    // New code: walk stash with iterator, use `stash.erase(it)` which returns
    //   the next iterator (C++17 guarantees only the erased element's iterator
    //   is invalidated for unordered_map).
    //   Memory cost: O(blocks_Z) for `block_chunk_string` only.
    // ──────────────────────────────────────────────────────────────────────
    for (auto it = stash.begin();
         it != stash.end() && realblocks_count < blocks_Z; ) {
        const auto& key = it->first;          // {block_id, chunk_id}

        // Defensive: avoid block_chunks_pos[…][…] = … operator[] insertion
        // side-effect.  Only consider entries that already have a position.
        auto bid_it = block_chunks_pos.find(key.first);
        if (bid_it == block_chunks_pos.end()) { ++it; continue; }
        auto cid_it = bid_it->second.find(key.second);
        if (cid_it == bid_it->second.end())   { ++it; continue; }
        uint32_t leaf = cid_it->second;

        // Each path × bucket has at most one intersection level; walk levels
        // and check.  (For a given bucket_idx, only the level matching its
        // depth can yield bucket_idx, so this loop exits early.)
        bool matched = false;
        for (uint32_t level = 0; level < height; ++level) {
            uint32_t bucket_index =
                (1u << level) - 1u + (leaf >> (height - 1 - level));
            if (bucket_index == bucket_idx) {
                block_chunk_string[key] = std::move(it->second);
                it = stash.erase(it);
                ++realblocks_count;
                matched = true;
                break;
            }
        }
        if (!matched) ++it;
    }

    /* quieted: std::cout << "I can run here successfully." << std::endl; */
    // Step 2: permutation all slots and add dummy slots;
    // initialize bucket and metadata，and fill in the dummy date;
    BucketBlock& Block = bucket_blockdata[bucket_idx];
    RingBucket bucket(bucket_size);
    Block.valids.assign(bucket_size, true);          // all valid
    for (uint32_t i = 0; i < Block.valids.size(); ++i) {
    /* quieted: std::cout << "The slot " << i << " of the bucket " << bucket_idx << " is " << Block.valids[i] << std::endl; */
    }
    Block.ptrs.clear();
    Block.addrs.clear();
    std::unordered_set<uint32_t> real_slots;
    // randomly choose Z slots to put the real slots;
    while (real_slots.size() < blocks_Z) {
        real_slots.insert(Util::rand_int(bucket_size));
    }
    for (const auto& slot : real_slots) {
    /* quieted: std::cout << "The real slot in the bucket " << bucket_idx << " is " << slot << std::endl; */
        Block.ptrs.push_back(slot);
    }

    for (uint32_t i = 0; i < Block.ptrs.size(); ++i) {
    /* quieted: std::cout << "The real slot in the pointer vector is " << Block.ptrs[i] << std::endl; */
    }
    Block.count = 0;
    // slot_id, <block_id, chunk_id>
    std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> real_slot_id;
    for (const auto& [key, value] : block_chunk_string) {
        uint32_t block_id = key.first;
        uint32_t chunk_id = key.second;
        Block.addrs.push_back(std::make_pair(block_id, chunk_id));
    }

    // Write the real slots chunks in the server;
    for (uint32_t j = 0; j < Block.addrs.size(); ++j) {
        real_slot_id[Block.ptrs[j]] = std::make_pair(Block.addrs[j].first, Block.addrs[j].second);
        //uint32_t global_slot_id = slotKey(bucket_idx, Block.ptrs[j]);
        //conn -> insert_slot(global_slot_id, block_chunk_string[real_slot_id[Block.ptrs[j]]], name, block_size);
    }

    // Set S dummy blocks in the bucket;
    for (uint32_t k = 0; k < bucket_size; ++k) {
        if (real_slot_id.find(k) == real_slot_id.end()) {
            std::string cipher;
    /* quieted: std::cout << "The dummy slot in the bucket " << bucket_idx << " is " << k << std::endl; */
            std::string dummy_payload = Util::generate_random_block(block_size - Util::aes_block_size - sizeof(uint32_t));
            int32_t dummy_id = -1;
            std::string block_id_str(reinterpret_cast<char*>(&dummy_id), sizeof(uint32_t));
            std::string plain = block_id_str + dummy_payload;

            Util::aes_encrypt(plain, ecrypt_key, cipher);

            // ── FIX #2: write each dummy slot exactly ONCE ─────────────────
            // Old code did the chunk-loop write THEN a second insert_slot at
            // the same global_slot_id, causing every dummy slot to be written
            // twice (extra RTT + bandwidth).  Each dummy occupies exactly one
            // slot of size block_size, so we just pad the cipher to block_size
            // and do a single write.
            // ───────────────────────────────────────────────────────────────
            if (cipher.size() > block_size) {
                std::cerr << "[WriteBucket] cipher size " << cipher.size()
                          << " > block_size " << block_size
                          << "  (dummy must fit in one slot)\n";
                std::abort();
            }
            cipher.resize(block_size, '\0');
            bucket.block[k] = cipher;
            uint32_t global_slot_id = slotKey(bucket_idx, k);
            conn->insert_slot(global_slot_id, cipher, name, block_size);
        }
        else {
            auto it = real_slot_id.find(k);
            const auto& key = it->second; // pair<block_id, chunk_id>
            auto p = block_chunk_string.find(key);
            if (p == block_chunk_string.end()) {
                std::cerr << "[WriteBucket] Missing cipher for ("
                          << key.first << "," << key.second << ")\n";
                continue; // 或者 assert
            }
            const std::string& cipher = p->second;

            bucket.block[k] = cipher;
            uint32_t global_slot_id = slotKey(bucket_idx, k);
            conn->insert_slot(global_slot_id, cipher, name, block_size);
        }
    }
}

// helper function: reverse-lexicographic order;
uint32_t RingORAM::reverse_bits(uint32_t val, uint32_t bitlen) {
    uint32_t res = 0;
    val &= ((1 << bitlen) - 1); // mask to keep only lower `bitlen` bits
    for (uint32_t i = 0; i < bitlen; ++i) {
        if (val & (1 << i)) {
            res |= (1 << (bitlen - 1 - i));
        }
    }
    return res;
}

//524286


void RingORAM::EvictPath() {
    std::cout << "[EvictPath]: We need to evict data from stash to the RingORAM." << std::endl;
    uint32_t L = height - 1;
    uint32_t l = reverse_bits(G, L);  // reverse-lexicographic index
    /* quieted: std::cout << "The path id we choose is " << l << std::endl; */
    G = (G + 1) % (1 << L);           // increment with wrap-around
    // Step 1: Collect all bucket indices on the path from root to leaf;
    std::vector<uint32_t> path_buckets;
    for (uint32_t level = 0; level < height; ++level) {
        uint32_t bucket_idx = (1 << level) - 1 + (l >> (height - 1 - level));
        path_buckets.push_back(bucket_idx);
    }

    std::reverse(path_buckets.begin(), path_buckets.end());
    // Step 2 (batched 10/8/26, = Ring ORAM's EvictPath): read every bucket on
    // the path into the stash in ONE round trip, then write the path back from
    // the leaf to the root.  Previously each bucket was read (one RTT per
    // valid slot) and written in turn — ~height x valid-slots round trips.
    ReadBuckets(path_buckets);
    for (uint32_t i = 0; i < path_buckets.size(); ++i)
        WriteBucket(path_buckets[i]);
}

// ============================================================================
//  Bulk-load fast path (4/20/26) — for TPC-C ServerInitializationBulk only
// ============================================================================
//
//  Put all records directly into stash (no access(), no eviction, no network).
//  Tree remains full of dummies. Reads will hit stash-first path automatically.
//
//  INVARIANT PRESERVED:
//    - For every (block_id, chunk_id) inserted: block_chunks_pos is set.
//    - stash contains the encrypted record content.
//    - bucket_blockdata.addrs stays empty (no real records in tree).
//
//  Tree-side consistency: after this, caller MUST call
//    conn->bulk_init_tree(name, block_size, generate_dummy_tree_payload())
//  to fill the server-side with the same encrypted dummies that a normal
//  RingORAM constructor would have sent.
// ============================================================================

void RingORAM::bulk_load_stash(
    const std::vector<std::pair<uint32_t, std::string>>& records)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> leaf_dist(0, num_leaves - 1);

    // Raise stash_limit to accommodate all records at once.
    // Normal stash_limit is set for steady-state operation (~6h*Z + permanent).
    // Bulk load exceeds this briefly; eviction during runtime will reduce it.
    stash_limit = std::max(stash_limit,
                            static_cast<uint32_t>(records.size()) +
                            6u * height * blocks_Z + 50u);

    for (const auto& [block_id, plain_data] : records) {
        // ── Encrypt the plaintext using this ORAM's key ──────────────────
        std::string cipher;
        Util::aes_encrypt(plain_data, ecrypt_key, cipher);

        // ── Split cipher into chunks the same way access(INSERT) does ────
        size_t total = cipher.size();
        size_t offset = 0;
        uint32_t chunk_id = 0;

        while (offset < total) {
            size_t take = std::min<size_t>(static_cast<size_t>(block_size),
                                            total - offset);
            std::string chunk = cipher.substr(offset, take);
            chunk.resize(block_size, '\0');

            // Assign a random leaf path (same randomization as access INSERT)
            uint32_t L = leaf_dist(gen);
            block_chunks_pos[block_id][chunk_id] = L;

            // Direct stash insertion — bypass stash_insert to skip its
            // per-element size assertion which would fire on large bulk loads
            stash[{block_id, chunk_id}] = chunk;

            chunk_id++;
            offset += take;
        }
    }

    uint32_t cur_size = static_cast<uint32_t>(stash.size());
    if (cur_size > stash_high_watermark) stash_high_watermark = cur_size;

    std::cout << "[bulk_load_stash] Loaded " << records.size()
              << " records into stash; stash size now " << cur_size
              << " (limit " << stash_limit << ")\n";
}

// ============================================================================
//  generate_dummy_tree_payload — DEPRECATED, kept for backward compat only.
//
//  ⚠️  This function allocates a vector with ALL dummy slots at once.  For an
//  ORDER_LINE-sized tree (millions of slots × block_size bytes each) this
//  reaches several GB and triggers the OOM killer under multi-table parallel
//  init.
//
//  Prefer the streaming variant: get_total_slot_count() + generate_one_dummy_slot().
//
//  This wrapper now delegates to generate_one_dummy_slot() so that at least the
//  per-slot generation logic is shared (single source of truth).  Callers that
//  still hit this path on small trees keep working; large trees should switch.
// ============================================================================
std::vector<SlotPayload> RingORAM::generate_dummy_tree_payload() {
    uint32_t total = get_total_slot_count();

    // Heuristic guard: refuse to materialize > 256 MB worth of payload.
    // (This is approximate; SlotPayload wraps a string of ~block_size bytes.)
    constexpr size_t kSafeMaxBytes = 256ull * 1024 * 1024;
    size_t expected_bytes = static_cast<size_t>(total) * block_size;
    if (expected_bytes > kSafeMaxBytes) {
        std::cerr << "[generate_dummy_tree_payload] REFUSING — payload would be "
                  << (expected_bytes / (1024 * 1024)) << " MB.  "
                  << "Use bulk_init_tree_stream + generate_one_dummy_slot instead.\n";
        std::abort();
    }

    std::vector<SlotPayload> payload;
    payload.reserve(total);
    for (uint32_t i = 0; i < total; ++i) {
        payload.push_back(generate_one_dummy_slot(i));
    }

    std::cout << "[generate_dummy_tree_payload] Generated " << payload.size()
              << " dummy slots\n";
    return payload;
}

// ============================================================================
//  Streaming dummy generation (v4, 4/20/26) — OOM-safe alternative
// ============================================================================
//
//  generate_dummy_tree_payload() allocates a std::vector with ALL slots
//  pre-generated in memory (2-5 GB for ORDER_LINE-sized tables). Under
//  8-table parallel init, peak memory across children exceeds 32 GB →
//  OOM killer kills children → init fails.
//
//  These two methods let the caller stream slots one at a time:
//
//    uint32_t N = oram->get_total_slot_count();
//    conn->bulk_init_tree_stream(name, bs, N,
//      [&](uint32_t i){ return oram->generate_one_dummy_slot(i); });
//
//  Peak memory per child: ~10 MB (just the network chunk buffer), regardless
//  of tree size. Safe for 8-table parallel init on 32 GB hosts.
//
//  Wire-format output is IDENTICAL to generate_dummy_tree_payload() +
//  bulk_init_tree() — server sees the exact same byte stream.
// ============================================================================

uint32_t RingORAM::get_total_slot_count() const {
    uint32_t total_buckets = (1u << height) - 1;
    return total_buckets * bucket_size;
}

SlotPayload RingORAM::generate_one_dummy_slot(uint32_t slot_index) {
    uint32_t bucket_idx    = slot_index / bucket_size;
    uint32_t slot_in_bucket = slot_index % bucket_size;

    // Direct bulk placement: decide once, before the first slot is streamed.
    if (!bulk_placed_) bulk_place_stash_into_tree_();

    // On the FIRST slot of each bucket, populate bucket_blockdata for this
    // bucket exactly once. This mirrors the constructor's per-bucket setup:
    //   - blocks_Z of bucket_size slots are marked as "real slot positions"
    //     (Block.ptrs)
    //   - Block.addrs stays empty (no real records live in the tree initially;
    //     all real data lives in stash after bulk_load_stash)
    //   - Block.valids starts all true
    //
    // Silent-mode constructor does NOT run the per-bucket setup loop, so
    // bucket_blockdata[i].valids is empty on first touch here. This mirrors
    // exactly what generate_dummy_tree_payload() does.
    {
        BucketBlock& Block = bucket_blockdata[bucket_idx];
        if (Block.valids.empty()) {
            Block.valids.resize(bucket_size, true);
            std::unordered_set<uint32_t> real_slots;
            while (real_slots.size() < blocks_Z) {
                real_slots.insert(Util::rand_int(bucket_size));
            }
            for (uint32_t slot : real_slots) {
                Block.ptrs.push_back(slot);
            }
            Block.count = 0;
        }
    }

    // ── Direct bulk placement: attach this bucket's records, emit real slots ─
    // Block.addrs[j] lives in slot Block.ptrs[j] (same pairing ReadBucket /
    // GetBlockOffset / WriteBucket use).  The chunk leaves the stash only when
    // its ciphertext is actually streamed into the tree.
    if (!bulk_bucket_assign_.empty()) {
        BucketBlock& Block = bucket_blockdata[bucket_idx];
        auto as = bulk_bucket_assign_.find(bucket_idx);
        if (as != bulk_bucket_assign_.end()) {
            if (Block.addrs.empty() && Block.ptrs.size() >= as->second.size())
                Block.addrs = std::move(as->second);
            bulk_bucket_assign_.erase(as);
        }
    }
    {
        BucketBlock& Block = bucket_blockdata[bucket_idx];
        for (size_t j = 0; j < Block.addrs.size() && j < Block.ptrs.size(); ++j) {
            if (Block.ptrs[j] != slot_in_bucket) continue;
            auto st = stash.find(Block.addrs[j]);
            if (st == stash.end()) {
                // Already streamed once (slot generated twice) or never loaded:
                // either way the metadata would point at a dummy.  Loud failure.
                std::cerr << "[bulk_direct] BUG: bucket " << bucket_idx << " slot "
                          << slot_in_bucket << " expects (" << Block.addrs[j].first << ","
                          << Block.addrs[j].second << ") but it is not in the stash\n";
                std::abort();
            }
            SlotPayload sp;
            sp.global_slot_id = slotKey(bucket_idx, slot_in_bucket);
            sp.cipher = std::move(st->second);
            stash.erase(st);
            return sp;
        }
    }

    // Build this slot's encrypted dummy content
    std::string dummy_payload = Util::generate_random_block(
        block_size - Util::aes_block_size - sizeof(uint32_t));
    int32_t dummy_id = -1;
    std::string bid_str(reinterpret_cast<char*>(&dummy_id), sizeof(uint32_t));
    std::string plain = bid_str + dummy_payload;

    SlotPayload sp;
    sp.global_slot_id = slotKey(bucket_idx, slot_in_bucket);
    Util::aes_encrypt(plain, ecrypt_key, sp.cipher);
    return sp;
}

// ============================================================================
//  Direct bulk placement (9/26/26)
// ============================================================================
//  Standard ORAM bulk construction: every chunk already has a uniformly random
//  leaf (bulk_load_stash).  Place it in the deepest bucket on that leaf's path
//  with a free real slot.  This satisfies the Ring ORAM invariant (a block
//  lives on the path to its leaf, or in the stash) and costs O(N log N) CPU
//  with no extra network traffic: the ciphertexts ride along in the one tree
//  upload that already happens.  The server receives the same number of
//  equal-length AES ciphertexts as before, so it learns nothing new.
//
//  Chunks that do not fit (tree full) stay in the stash, exactly as after the
//  old flush.
// ============================================================================
void RingORAM::bulk_place_stash_into_tree_() {
    bulk_placed_ = true;
    if (stash.empty()) return;
    if (const char* e = std::getenv("DEPART_BULK_DIRECT"); e && std::string(e) == "0") {
        std::cout << "[bulk_direct] disabled (DEPART_BULK_DIRECT=0); "
                  << stash.size() << " chunks stay in the stash\n";
        return;
    }
    const uint32_t total_buckets = (1u << height) - 1u;
    std::vector<uint16_t> used(total_buckets, 0);
    size_t placed = 0, left = 0;
    std::vector<size_t> per_level(height, 0);
    for (const auto& kv : stash) {
        const auto& key = kv.first;
        auto b = block_chunks_pos.find(key.first);
        if (b == block_chunks_pos.end()) { ++left; continue; }
        auto c = b->second.find(key.second);
        if (c == b->second.end())        { ++left; continue; }
        const uint32_t leaf = c->second;
        bool ok = false;
        for (int32_t level = static_cast<int32_t>(height) - 1; level >= 0; --level) {
            const uint32_t bucket = (1u << level) - 1u + (leaf >> (height - 1 - level));
            if (used[bucket] < blocks_Z) {
                ++used[bucket];
                bulk_bucket_assign_[bucket].push_back(key);
                ++per_level[static_cast<size_t>(level)];
                ok = true;
                break;
            }
        }
        if (ok) ++placed; else ++left;
    }
    std::cout << "[bulk_direct] " << name << ": placed " << placed << " chunks directly in the tree ("
              << per_level[height - 1] << " at leaf level), " << left
              << " stay in the stash\n";
}

// ============================================================================
//  access_batch (10/8/26) — one network round trip per batch
// ============================================================================
//  Every decision Ring ORAM makes (which slot of each bucket to read, which
//  buckets to reshuffle or evict, which stash blocks go into which bucket) is
//  a function of CLIENT metadata only; the server's bytes are needed only as
//  ciphertext to move around and, for READ, to decrypt at the end.  So the
//  batch is executed once on the metadata with the connector deferred: a
//  slot read yields a placeholder (or, if the slot was rewritten earlier in
//  this batch, its new content), writes are recorded.  end_deferred() then
//  fetches all read slots in one FIND_SLOTS round trip, patches placeholders
//  into the recorded writes and streams them.  Finally placeholders left in
//  the stash and in READ results are replaced and the results decrypted.
// ============================================================================
std::vector<std::string> RingORAM::access_batch(const std::vector<BatchOp>& ops) {
    std::vector<std::string> out(ops.size());
    if (ops.empty()) return out;

    conn->begin_deferred(name, block_size);
    defer_decrypt_ = true;
    try {
        for (size_t i = 0; i < ops.size(); ++i)
            out[i] = access(ops[i].block_id, ops[i].op, ops[i].data);
    } catch (...) {
        defer_decrypt_ = false;
        conn->end_deferred();
        throw;
    }
    defer_decrypt_ = false;
    const std::unordered_map<uint64_t, std::string> resolved = conn->end_deferred();

    // Replace every placeholder chunk in a (multi-)chunk ciphertext.
    auto resolve = [&](std::string& c) {
        for (size_t off = 0; off + block_size <= c.size(); off += block_size) {
            uint64_t id;
            if (!NetIOConnector::is_placeholder(c.substr(off, block_size), &id)) continue;
            auto it = resolved.find(id);
            if (it == resolved.end()) {
                std::cerr << "[access_batch] BUG: unresolved placeholder " << id << "\n";
                std::abort();
            }
            c.replace(off, block_size, it->second);
        }
    };
    for (auto& [key, chunk] : stash) resolve(chunk);
    for (size_t i = 0; i < ops.size(); ++i) {
        if (ops[i].op != OpType::READ || out[i].empty()) continue;
        resolve(out[i]);
        std::string plain;
        Util::aes_decrypt(out[i], ecrypt_key, plain);
        out[i] = std::move(plain);
    }
    return out;
}
