//
// REDESIGNED: Two partitioning strategies based on max_partitions parameter
//

#include <random>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <cmath>
#include <ctime>
#include <vector>

#include <iostream>
#include <numeric>
#include <algorithm>
#include <queue>
#include "Repartition.h"
#include <cryptopp/osrng.h>
using namespace CryptoPP;

// ============================================
// Constructor & Destructor
// ============================================

Repartition::Repartition() : epsilon(1.0), last_partition_count_(0) {
    // CRITICAL: seed RNG with true entropy so every run produces different noise.
    // A fixed seed (e.g. 12345) makes all noise values predictable, completely
    // defeating the DP security guarantee.
    std::random_device rd;
    uint64_t seed = (static_cast<uint64_t>(rd()) << 32) | static_cast<uint64_t>(rd());
    rng_ = std::mt19937_64(seed);

    ecrypt_key = new byte[Util::key_length];
    Util::prng.GenerateBlock(ecrypt_key, Util::key_length);
}

Repartition::~Repartition() {
    delete[] ecrypt_key;
}

// helper function: split a CSV line by delimiter
static std::vector<std::string> splitCSVLine(const std::string& line, char delimiter = ',') {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream ss(line);
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// helper function: synopsis computation
// NOTE: expects raw CSV text lines (e.g. "1001,1,150,1000"), NOT binary tuples.
//       Parses fields directly by splitting on commas using attribute_indices.
BinInfo Repartition::computeSynopsis(
    const std::vector<std::string>& data,
    const std::string& schema_str,
    const std::vector<int>& attribute_indices,
    const std::string& separator,
    int primary_key_col
) {
    // 1. gain the info from the schema;
    std::vector<fieldType> field_types = tupleTypesFromSchema(schema_str);
    std::vector<std::string> field_names = tupleNamesFromSchema(schema_str);

    // 2. check the parameters;
    if (attribute_indices.empty()) {
        throw std::invalid_argument("attribute_indices cannot be empty");
    }

    int num_fields = static_cast<int>(field_names.size());
    for (int idx : attribute_indices) {
        if (idx < 0 || idx >= num_fields) {
            throw std::invalid_argument("Invalid attribute index: " + std::to_string(idx));
        }
    }

    // 3. collect all attributes info;
    std::vector<fieldType> selected_types;
    std::vector<std::string> selected_names;

    for (int idx : attribute_indices) {
        selected_types.push_back(field_types[idx]);
        selected_names.push_back(field_names[idx]);
    }

    // 4. construct the BinInfo;
    BinInfo bin_info(selected_types, selected_names);

    // 5. traverse the data — parse CSV lines directly by column index;
    uint64_t pk_collisions = 0;
    for (size_t data_idx = 0; data_idx < data.size(); ++data_idx) {
        const auto& line = data[data_idx];
        std::vector<std::string> fields = splitCSVLine(line, ',');

        std::string combined_value = "";
        for (size_t i = 0; i < attribute_indices.size(); ++i) {
            int idx = attribute_indices[i];

            if (idx >= static_cast<int>(fields.size())) {
                std::cerr << "[WARN] line has fewer fields than expected: \"" << line << "\"\n";
                break;
            }

            if (i > 0) {
                combined_value += separator;
            }
            combined_value += fields[idx];
        }

        bin_info.addValue(combined_value);
        bin_info.data_index_to_bin_key.push_back(combined_value);
        bin_info.bin_key_to_data_indices[combined_value].push_back(static_cast<uint32_t>(data_idx));

        // Extract primary key and build reverse mapping.
        // ── FIX (9/22/26): single-column PK is NOT unique for composite-key
        // tables (ORDER_LINE (W,D,O,OL), STOCK (W,I), CUSTOMER (W,D,C), ...).
        // The old code silently overwrote earlier rows.  We now keep the FIRST
        // row and count collisions.  Nothing downstream reads this map; the
        // authoritative row identity is ObjectKey (ObjectKey.h / pos_map).
        if (primary_key_col >= 0 && primary_key_col < static_cast<int>(fields.size())) {
            auto ins = bin_info.primary_key_to_data_index.emplace(
                fields[primary_key_col], static_cast<uint32_t>(data_idx));
            if (!ins.second) ++pk_collisions;
        }
    }

    if (pk_collisions > 0) {
        std::cerr << "[computeSynopsis] WARNING: " << pk_collisions
                  << " rows share a primary_key_col value with an earlier row "
                     "(composite-key table?).  primary_key_to_data_index keeps the "
                     "first row only; use ObjectKey for row identity.\n";
    }
    return bin_info;
}
// ============================================
// Main Repartitioning Entry Point
// ============================================
std::vector<Partition> Repartition::Repartition_Main(
    const std::vector<std::string>& data,
    const std::string& schema_str,
    const std::vector<int>& attribute_indices,
    double epsilon_max,
    double epsilon_threshold,
    double threshold_pub,
    bool fixed_number_part,
    uint32_t fixed_part_num,
    int primary_key_col,
    uint32_t num_servers        // hard cap: actual number of available servers
) {
    if (data.empty()) {
        std::cerr << "[ERROR][Repartition_Main] input data is empty.\n";
        return {};
    }

    bin_info_ = computeSynopsis(data, schema_str, attribute_indices, ",", primary_key_col);
    BinInfo& bin_info = bin_info_;  // alias for readability

    if (bin_info.synopsis.empty()) {
        std::cerr << "[ERROR][Repartition_Main] synopsis is empty after computeSynopsis().\n";
        return {};
    }

    // Step 1: Compute noised synopsis for each bin (Algorithm 2).
    // Returns SIGNED counts — DO NOT treat any individual value as a
    // meaningful "bin size" until you clamp it (e.g. for a synopsis query)
    // or sum over many bins and then clamp (e.g. for partition capacity).
    std::map<std::string, int64_t> noised_syn = Noised_Synopsis(bin_info, epsilon_max);

    // ── Persist the public noised synopsis into bin_info_ ────────────────
    // Downstream synopsis queries (count, range, min, max) need
    // non-negative values.  So the PUBLIC per-bin map is the clamped view.
    // (Clamping per bin here is ε-DP-safe: it's post-processing of an
    //  already-DP value.  The bias introduced is a per-bin property and
    //  does not compound across bins the way the old code's total did,
    //  because synopsis queries access ONE bin or a RANGE of bins whose
    //  variance still grows only as √#bins — not linearly.)
    bin_info_.noised_synopsis.clear();
    for (const auto& [k, signed_v] : noised_syn) {
        bin_info_.noised_synopsis[k] =
            (signed_v > 0) ? static_cast<uint32_t>(signed_v) : 0u;
    }

    // Total = Σ SIGNED bins.  E[Σ] = Σ real, i.e. UNBIASED.
    // Previously we summed clamped values → per-bin positive bias compounded
    // linearly with #bins, dominating the total for PK-binned tables.
    int64_t total_noised_signed = 0;
    std::cout << "\n=== Noised Synopsis for Each Bin (signed) ===\n";
    for (const auto& kv : noised_syn) {
        std::cout << "  Bin [" << kv.first << "]: noised=" << kv.second
                  << ", real=" << bin_info.synopsis.at(kv.first) << "\n";
        total_noised_signed += kv.second;
    }
    uint64_t total_noised_synopsis = (total_noised_signed > 0)
        ? static_cast<uint64_t>(total_noised_signed) : 0;

    // ── Σ_i max(0, DP_Bins[i]) — the quantity Algorithm 2's initial threshold
    //    uses (paper Section 4, MaxPartCount).  NOTE the clamp is PER BIN here,
    //    unlike total_noised_synopsis above, which clamps the SUM:
    //
    //        T_paper = max(1, ceil( Σ_i max(0, DP_Bins[i]) / k ))     ← this one
    //        T_code  = max(1, ceil( max(0, Σ_i DP_Bins[i])  / k ))    ← was used
    //
    //    They differ whenever any bin is negative.  DP_Bins = [10, -8, 10, -8],
    //    k = 2 gives T_paper = ceil(20/2) = 10 but T_code = ceil(4/2) = 2, and a
    //    5x smaller starting threshold produces different partition boundaries
    //    and a different partition count.
    //
    //    Both clamping rules stay in the file on purpose, for DIFFERENT jobs:
    //      * the THRESHOLD follows the paper (per-bin clamp), because that is
    //        the published algorithm;
    //      * a PARTITION's capacity keeps the signed sum, clamped once at the
    //        end (see cur.noisy_synopsis below).  Per-bin clamping there would
    //        reintroduce the positive bias that inflated ORAM trees ~70-100x on
    //        PK-binned tables, since E[max(0, X)] > E[X] for every bin.
    uint64_t total_clamped_bins = 0;
    for (const auto& kv : noised_syn)
        if (kv.second > 0) total_clamped_bins += static_cast<uint64_t>(kv.second);

    std::cout << "Total noised synopsis: sum-of-clamped-bins = " << total_clamped_bins
              << "  (paper's MaxPartCount input)\n"
              << "                       clamped-sum        = " << total_noised_synopsis
              << "  (raw signed sum: " << total_noised_signed << ")\n\n";

    // Build a sorted bins vector.
    // IMPORTANT: std::map<std::string> uses LEXICOGRAPHIC order, which is wrong for
    // numeric bin keys (e.g. "10" < "2" lexicographically but 10 > 2 numerically).
    // Sort numerically when possible; fall back to lexicographic for non-numeric keys.
    auto make_sorted_bins = [&]() {
        std::vector<std::pair<std::string, int64_t>> b(noised_syn.begin(), noised_syn.end());
        std::sort(b.begin(), b.end(), [](const auto& x, const auto& y) {
            try {
                return std::stoll(x.first) < std::stoll(y.first);
            } catch (...) {
                return x.first < y.first;  // fallback: lexicographic
            }
        });
        return b;
    };

    std::vector<Partition> parts;

    if (fixed_number_part) {
        // ============================================
        // Algorithm 3: Fixed-Partition-Count Post-processing
        // ============================================
        // Effective k = min(hint_k, num_servers).
        // fixed_part_num is the caller's "ideal" partition hint (e.g. hint_k=5),
        // but we can never exceed the number of physical servers.
        const uint32_t k_eff = (num_servers > 0)
                               ? std::min(fixed_part_num, num_servers)
                               : fixed_part_num;

        std::cout << "Using Algorithm 3: Fixed-Partition-Count"
                  << "  hint_k=" << fixed_part_num
                  << "  num_servers=" << num_servers
                  << "  k_eff=" << k_eff << "\n";

        const double alpha = 0.1;  // Threshold increase parameter

        // Paper Section 4, MaxPartCount:
        //     T = max( 1, ceil( Σ_i max(0, DP_Bins[i]) / k ) )
        // (Was: ceil(max(0, Σ_i DP_Bins[i]) / k), which clamps the sum instead
        //  of each bin and therefore starts from a smaller T whenever any bin
        //  came out negative — see the note next to total_clamped_bins.)
        double T = std::ceil(static_cast<double>(total_clamped_bins) / k_eff);

        // max(1, ·): the paper's outer guard.  It also keeps the retry loop
        // making progress — at T = 0, ceil((1+alpha)*0) = 0 would spin forever.
        if (T < 1.0) T = 1.0;

        std::cout << "Initial threshold T = " << T
                  << "   [= max(1, ceil(" << total_clamped_bins << " / " << k_eff << "))]\n";

        // Safety cap: never iterate more than this many times.
        // Each iteration multiplies T by ~1.1 (or adds 1 when small), so even
        // for huge total sizes this converges quickly.
        const int MAX_ITER = 500;
        int iter = 0;

        // Keep trying until we get <= k partitions
        while (true) {
            iter++;
            if (iter > MAX_ITER) {
                std::cerr << "[WARN] FixedPartCount: hit MAX_ITER="
                          << MAX_ITER << " at T=" << T
                          << "; accepting current " << parts.size()
                          << " partitions (may exceed k_eff=" << k_eff << ").\n";
                break;
            }
            parts.clear();
            Partition cur;
            cur.index.clear();
            cur.synopsis = 0;
            cur.noisy_synopsis = 0;
            cur.dummy_num = 0;

            // Packing accumulator uses the PER-BIN CLAMPED "size".
            // Semantic: T is a target "records per partition", so a bin's
            // contribution to the packing decision must be non-negative.
            // But partition CAPACITY (cur.noisy_synopsis) is computed
            // from the SIGNED sum below → unbiased, no clamp compounding.
            uint64_t acc = 0;
            uint32_t start_idx = 0;

            // Iterate through bins in NUMERIC order
            std::vector<std::pair<std::string, int64_t>> bins = make_sorted_bins();

            for (size_t i = 0; i < bins.size(); ++i) {
                int64_t  signed_count = bins[i].second;
                uint64_t packing_size =
                    (signed_count > 0) ? static_cast<uint64_t>(signed_count) : 0;

                acc += packing_size;

                if (acc >= T) {
                    // Close partition [start_idx .. i]
                    cur.index.clear();
                    cur.synopsis = 0;
                    int64_t signed_partition_sum = 0;

                    for (size_t j = start_idx; j <= i; ++j) {
                        const std::string& bin_key = bins[j].first;
                        cur.index.push_back(bin_key);
                        cur.synopsis         += bin_info.synopsis.at(bin_key);
                        signed_partition_sum += bins[j].second;  // SIGNED
                    }

                    // noisy_synopsis = max(0, Σ signed noise).
                    // Unbiased over many bins (cancellation);
                    // no systematic inflation.
                    cur.noisy_synopsis = (signed_partition_sum > 0)
                        ? static_cast<uint32_t>(signed_partition_sum)
                        : 0u;

                    // dummy_num / stash_overflow_count are set
                    // authoritatively by the Case A/B block below the loop.
                    cur.dummy_num = 0;
                    cur.noise_stash_reserve = 0;
                    cur.stash_overflow_count = 0;   // deprecated alias

                    parts.push_back(cur);

                    start_idx = i + 1;
                    acc = 0;
                }
            }

            // Handle remaining bins
            if (start_idx < bins.size()) {
                cur.index.clear();
                cur.synopsis = 0;
                int64_t signed_partition_sum = 0;

                for (size_t j = start_idx; j < bins.size(); ++j) {
                    const std::string& bin_key = bins[j].first;
                    cur.index.push_back(bin_key);
                    cur.synopsis         += bin_info.synopsis.at(bin_key);
                    signed_partition_sum += bins[j].second;
                }

                cur.noisy_synopsis = (signed_partition_sum > 0)
                    ? static_cast<uint32_t>(signed_partition_sum)
                    : 0u;
                cur.dummy_num = 0;
                cur.noise_stash_reserve = 0;
                cur.stash_overflow_count = 0;   // deprecated alias

                parts.push_back(cur);
            }

            std::cout << "Generated " << parts.size() << " partitions with T=" << T << "\n";

            // Check if we have <= k_eff partitions (respects num_servers hard cap)
            if (parts.size() <= k_eff) {
                break;  // Equalization is applied after the loop (see below)
            }

            // Too many partitions, increase threshold and retry.
            // Use max(ceil((1+alpha)*T), T+1) to guarantee forward progress
            // even when T is very small (otherwise ceil(1.1*1)=2, ceil(1.1*2)=3,
            // which is OK, but better to be explicit).
            double new_T = std::ceil((1.0 + alpha) * T);
            if (new_T <= T) new_T = T + 1;
            T = new_T;
            std::cout << "Too many partitions, increasing T to " << T << "\n";
        }

    } else {
        // ============================================
        // Algorithm 4: Fixed-Threshold Post-processing
        // ============================================
        std::cout << "Using Algorithm 4: Fixed-Threshold (T=" << threshold_pub << ")\n";

        // Threshold is FIXED (no Laplace noise added).
        // Rationale: the bin synopses have already been DP-protected by Noised_Synopsis().
        // Adding separate noise to the threshold consumed epsilon_2 budget without
        // providing meaningful additional privacy, and caused highly unstable partition
        // counts (especially for small ε where scale ≈ 333).
        // With a fixed threshold, the full epsilon budget is allocated to bin noising,
        // and partition count = ceil(data_size / T) is deterministic and predictable.
        double T = threshold_pub;
        std::cout << "[FixedThreshold] Using fixed T=" << T << " (no threshold noise)\n";
        Partition cur;
        cur.index.clear();
        cur.synopsis = 0;
        cur.noisy_synopsis = 0;
        cur.dummy_num = 0;

        uint64_t acc = 0;
        uint32_t start_idx = 0;

        // Iterate through bins in NUMERIC order
        std::vector<std::pair<std::string, int64_t>> bins = make_sorted_bins();

        for (size_t i = 0; i < bins.size(); ++i) {
            int64_t  signed_count = bins[i].second;
            uint64_t packing_size =
                (signed_count > 0) ? static_cast<uint64_t>(signed_count) : 0;

            acc += packing_size;

            if (acc >= T) {
                // Close partition [start_idx .. i] with SIGNED sum for capacity.
                cur.index.clear();
                cur.synopsis = 0;
                int64_t signed_partition_sum = 0;

                for (size_t j = start_idx; j <= i; ++j) {
                    const std::string& bin_key = bins[j].first;
                    cur.index.push_back(bin_key);
                    cur.synopsis         += bin_info.synopsis.at(bin_key);
                    signed_partition_sum += bins[j].second;
                }

                cur.noisy_synopsis = (signed_partition_sum > 0)
                    ? static_cast<uint32_t>(signed_partition_sum)
                    : 0u;
                cur.dummy_num = 0;
                cur.noise_stash_reserve = 0;
                cur.stash_overflow_count = 0;   // deprecated alias

                parts.push_back(cur);
                start_idx = i + 1;
                acc = 0;
            }
        }

        // Handle remaining bins
        if (start_idx < bins.size()) {
            cur.index.clear();
            cur.synopsis = 0;
            int64_t signed_partition_sum = 0;

            for (size_t j = start_idx; j < bins.size(); ++j) {
                const std::string& bin_key = bins[j].first;
                cur.index.push_back(bin_key);
                cur.synopsis         += bin_info.synopsis.at(bin_key);
                signed_partition_sum += bins[j].second;
            }

            cur.noisy_synopsis = (signed_partition_sum > 0)
                ? static_cast<uint32_t>(signed_partition_sum)
                : 0u;
            cur.dummy_num = 0;
            cur.noise_stash_reserve = 0;
            cur.stash_overflow_count = 0;   // deprecated alias

            parts.push_back(cur);
        }
    }

    // ── Per-partition capacity ────────────────────────────────────────────────
    //
    // Privacy invariant: the ORAM tree is ALWAYS sized to noisy_synopsis.
    // The server can only observe the ORAM tree height (→ tree capacity), so
    // sizing it to the real record count would leak the sign of the Laplace
    // noise (positive noise → tree larger than real, negative noise → tree
    // equals real).  By always using noisy_synopsis, this side channel is closed.
    //
    //   Case A  noisy >= real:
    //     ORAM tree capacity  = noisy_synopsis  (= real + dummy_num)
    //     dummy_num           = noisy - real
    //     noise_stash_reserve  = 0
    //
    //   Case B  noisy < real:
    //     ORAM tree capacity  = noisy_synopsis  (< real — server sees smaller tree)
    //     dummy_num           = 0               (no padding needed)
    //     noise_stash_reserve  = real - noisy   (extra stash CAPACITY to reserve)
    //       → RingORAM stash_limit is enlarged by stash_overflow_count so that
    //         the (real - noisy) overflow records permanently reside in the stash
    //         without triggering a stash overflow abort.
    //       → ServerInitialization inserts ALL real records; the overflow set
    //         naturally lands in the stash because the tree is already full at
    //         noisy capacity.  flush_stash_if_needed() drains to
    //         A + stash_overflow_count (not just A).
    if (!parts.empty()) {
        for (auto& p : parts) {
            // ── FIX (4/26/26): ensure noisy_synopsis >= 1 ─────────────────
            // RingORAM constructor throws on n == 0 (height = log2(0) UB).
            // A partition can hit noisy_synopsis = 0 when its bins' signed
            // noise sums ≤ 0 (rare but possible for small ε + small partition).
            // Clamping to 1 is post-processing of DP-protected values, so the
            // ε-DP guarantee is preserved.  The leak is at most "this partition
            // has ≥1 tree slot" — but that's already implied by the partition
            // existing at all (every partition holds at least 1 bin which had
            // a real count > 0 when computed).  Bias on tree height: log2(1)
            // vs. log2(0+ε) — negligible.
            if (p.noisy_synopsis < 1u) {
                std::cerr << "[Repartition] Partition with noisy_synopsis=0 "
                          << "(real=" << p.synopsis << ", "
                          << p.index.size() << " bins, signed sum was "
                          << "non-positive); clamping to 1 to satisfy "
                          << "RingORAM n>=1 constraint.\n";
                p.noisy_synopsis = 1u;
            }
            // ──────────────────────────────────────────────────────────────

            if (p.noisy_synopsis >= p.synopsis) {
                // Case A: noisy >= real
                p.dummy_num            = p.noisy_synopsis - p.synopsis;
                p.noise_stash_reserve  = 0;
                p.stash_overflow_count = 0;   // deprecated alias
            } else {
                // Case B: noisy < real — overflow permanently in stash
                // Tree stays at noisy_synopsis; the (real − noisy) records that
                // cannot fit in it live in the stash — an outcome of the tree
                // being full, not a fixed set of records.  Reserve capacity:
                p.dummy_num            = 0;
                p.noise_stash_reserve  = p.synopsis - p.noisy_synopsis;
                p.stash_overflow_count = p.noise_stash_reserve;   // deprecated alias
            }
            // ORAM tree size is always noisy_synopsis (privacy invariant above).
            std::cout << "[Capacity]  partition:"
                      << "  real="            << p.synopsis
                      << "  noisy="           << p.noisy_synopsis
                      << "  dummy="           << p.dummy_num
                      << "  noise_stash_reserve=" << p.noise_stash_reserve
                      << "  ORAM_tree_size="  << p.noisy_synopsis << "\n";
        }
    }

    return parts;
}

// Helper function - Laplace noise
double Repartition::laplace_noise(double epsilon, std::mt19937_64& rng, double delta) {
    if (epsilon <= 0.0) throw std::invalid_argument("epsilon must be > 0");
    if (delta <= 0.0) throw std::invalid_argument("delta must be > 0");

    const double scale = delta / epsilon;
    std::uniform_real_distribution<double> uni(0.0, 1.0);

    const double u = uni(rng) - 0.5;
    return -scale * std::copysign(1.0, u) * std::log(1.0 - 2.0 * std::abs(u));
}

// Algorithm 2: Compute Noised Bin Synopses
//
// Returns per-bin noised counts as SIGNED integers (int64_t). Do NOT clamp
// to zero here — the clamp is the cause of the capacity-inflation bug.
// See Repartition.h for the full explanation and usage rules.
std::map<std::string, int64_t> Repartition::Noised_Synopsis(
    const BinInfo& current_part,
    double epsilon_max
) {
    std::map<std::string, int64_t> bin_noised_synopsis;

    for (const auto& kv : current_part.synopsis) {
        const std::string& key = kv.first;
        uint32_t real_count = kv.second;

        // noised = real + Laplace(1/ε). May be negative (half the time).
        double noisy_on_bin = static_cast<double>(real_count) +
                             laplace_noise(epsilon_max, rng_, /*delta=*/1.0);

        // Round to nearest integer; PRESERVE SIGN.
        bin_noised_synopsis[key] =
            static_cast<int64_t>(std::llround(noisy_on_bin));
    }

    return bin_noised_synopsis;
}