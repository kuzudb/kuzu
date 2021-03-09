#pragma once

#include <cstdint>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

inline uint64_t murmurhash64(uint64_t x) {
    return x * UINT64_C(0xbf58476d1ce4e5b9);
}

struct HashNodeID {
    static inline uint64_t operation(nodeID_t nodeID) {
        return murmurhash64(nodeID.offset) ^ murmurhash64(nodeID.label);
    }
};

} // namespace operation
} // namespace common
} // namespace graphflow
