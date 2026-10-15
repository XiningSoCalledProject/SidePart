//
// Created by Xining Yuan on 7/15/25.
// REDESIGNED: Two partitioning strategies based on max_partitions parameter
// v2 (4/17/26): BinInfo now carries noised_synopsis (public) alongside raw
//               synopsis (private). This is the "Public" bar chart in the
//               OPT-ORAM Partitions Overview slide: per-bin independent
//               Laplace noise, ε-DP by parallel composition. Downstream
//               synopsis queries (min / max / range / count) are pure
//               post-processing of noised_synopsis — no additional budget.
//

#ifndef REPARTITION_H
#define REPARTITION_H

#pragma once
#include <vector>
#include "Util.h"
#include <string>
#include <cstdint>
#include "csv_reader.h"
#include <random>
#include <cmath>
#include <stdexcept>

// ============================================
// Partition Structure
// ============================================
struct BinInfo {
    std::vector<fieldType> attribute_types;
    std::vector<std::string> attribute_names;

    // ── Private side (left bar chart in design slide) ────────────────────
    // Raw per-bin counts. Never leave the trusted data owner.
    std::map<std::string, uint32_t> synopsis;

    // ── Public side (right bar chart in design slide) ────────────────────
    // Per-bin Laplace-noised counts. ε-DP by parallel composition over
    // disjoint bins. Populated inside Repartition_Main (Algorithm 2) and
    // read by downstream synopsis queries without consuming additional
    // privacy budget.
    std::map<std::string, uint32_t> noised_synopsis;

    std::vector<std::string> data_index_to_bin_key;
    std::map<std::string, std::vector<uint32_t>> bin_key_to_data_indices;  // bin_key -> [data_index, ...]
    std::map<std::string, uint32_t> primary_key_to_data_index;            // primary_key -> data_index

    // Default constructor
    BinInfo() = default;

    // Constructor with types and names
    BinInfo(const std::vector<fieldType>& types,
            const std::vector<std::string>& names)
        : attribute_types(types), attribute_names(names) {
        if (types.size() != names.size()) {
            throw std::invalid_argument("types and names must have same size");
        }
    }

    void addValue(const std::string& value) {
        synopsis[value]++;
    }

    uint32_t getTotalCount() const {
        uint32_t total = 0;
        for (const auto& [key, count] : synopsis) {
            total += count;
        }
        return total;
    }

    uint32_t getDistinctCount() const {
        return synopsis.size();
    }

    void printSynopsis() const {
        std::cout << "BinInfo for attributes: ";
        for (size_t i = 0; i < attribute_names.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << attribute_names[i];
        }
        std::cout << "\nSynopsis:\n";
        for (const auto& [value, count] : synopsis) {
            std::cout << "  " << value << ": " << count << "\n";
        }
    }
};

struct Partition {
    std::vector<std::string> index;    // bin indices in this partition
    uint32_t synopsis = 0;             // real data count (total records in this partition)
    uint32_t noisy_synopsis = 0;       // noisy data count (DP protected)
    uint32_t dummy_num = 0;            // dummy records added to fill ORAM to target size
    // ── Renamed 9/22/26: stash_overflow_count → noise_stash_reserve ────────
    // When DP noise is negative (noisy < real) the tree is sized to noisy, so
    // not all real records fit in it:
    //     noise_stash_reserve = max(0, synopsis − noisy_synopsis)
    // This is EXTRA CLIENT-SIDE STASH CAPACITY to reserve, not a set of records
    // pinned in the stash.  The invariant the DP argument needs is
    //     |tree capacity| = noisy_synopsis      (public, function of ñ only)
    //     all real records ⊆ tree ∪ stash
    // Which records happen to be in the stash changes with every eviction; the
    // server sees neither stash membership nor occupancy.  ALL real records are
    // inserted at init; whatever does not fit in the tree stays in the stash.
    uint32_t noise_stash_reserve = 0;

    // DEPRECATED alias, kept in sync so existing benchmark code still compiles.
    // Remove once all call sites use noise_stash_reserve.
    uint32_t stash_overflow_count = 0;
};

// ============================================
// Repartition Class
// ============================================
class Repartition {
public:
    Repartition();
    ~Repartition();

    BinInfo computeSynopsis(
    const std::vector<std::string>& data,
    const std::string& schema_str,
    const std::vector<int>& attribute_indices,
    const std::string& separator,
    int primary_key_col
);

    std::vector<Partition> Repartition_Main(
    const std::vector<std::string>& data,
    const std::string& schema_str,
    const std::vector<int>& attribute_indices,
    double epsilon_max,
    double epsilon_threshold,
    double threshold_pub,
    bool fixed_number_part,
    uint32_t fixed_part_num,
    int primary_key_col = 0,
    uint32_t num_servers = 0  // hard cap on partition count (0 = use fixed_part_num only)
);

    double laplace_noise(double epsilon, std::mt19937_64& rng, double delta = 1.0);

    // Returns per-bin noised counts as SIGNED integers (int64_t).
    //
    // Why signed: Laplace noise is symmetric around 0, so E[noise] = 0.
    // When we previously clamped each bin's output to max(0, real+noise),
    // every bin picked up a positive bias of ≈ (1/ε)·(1/2)·exp(-real/(1/ε)).
    // For PK-binned tables (ITEM, ORDERLINE) where most bins have real=1-2
    // and ε is small, this bias sums across 100K+ bins to inflate
    // partition capacity by 70-100×, blowing up ORAM tree size and OOM-ing
    // the server (the symptom: noisy=7M for a table with real=100K).
    //
    // With signed returns, callers aggregate SIGNED noise across bins,
    // positives and negatives cancel, and the sum is unbiased:
    //   E[Σ signed_bin] = Σ real_bin.
    // Clamping to ≥0 happens once, at the point of use:
    //   • for per-bin synopsis queries (count/range/min/max)
    //     → populate bin_info_.noised_synopsis[k] = max(0, signed[k])
    //   • for partition ORAM capacity
    //     → partition.noisy_synopsis = max(0, Σ signed_bin_in_partition)
    std::map<std::string, int64_t> Noised_Synopsis(
    const BinInfo& current_part,
    double  epsilon_max
);

    // Access the BinInfo computed during Repartition_Main.
    // After Repartition_Main returns, bin_info_.noised_synopsis is populated
    // with the public (ε-DP) per-bin counts used by downstream queries.
    const BinInfo& getBinInfo() const { return bin_info_; }

private:
    std::mt19937_64 rng_;  // seeded in constructor via std::random_device (never fixed!)
    double epsilon;      // Current epsilon value
    byte* ecrypt_key;    // Encryption key
    uint32_t last_partition_count_;  // Track actual partition count for connector sizing
    double epsilon_max;
    double epsilon_threshold;
    bool fixed_number_part;
    BinInfo bin_info_;   // Stored after Repartition_Main for later use
};

#endif //REPARTITION_H