#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace common {

struct HnswReaderConfig {
    // The maximum number of neighbors to keep for each node
    int M = 64;

    // The number of neighbors to consider during construction
    int efConstruction = 100;

    // The number of neighbors to consider during search
    int efSearch = 100;

    // The alpha parameter for the RNG hueristic
    float alpha = 1.2;

    HnswReaderConfig() = default;

    static HnswReaderConfig construct(
        const std::unordered_map<std::string, common::Value>& options);
};

} // namespace common
} // namespace kuzu
