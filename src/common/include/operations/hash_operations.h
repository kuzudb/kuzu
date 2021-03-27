#pragma once

#include <cstdint>
#include <unordered_set>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

inline uint64_t murmurhash64(uint64_t x) {
    return x * UINT64_C(0xbf58476d1ce4e5b9);
}

struct Hash {
    template<class T>
    static inline uint64_t operation(const T& key) {
        throw invalid_argument("Hash type: " + string(typeid(T).name()) + " is not supported.");
    }
};

template<>
inline uint64_t Hash::operation(const nodeID_t& nodeID) {
    return murmurhash64(nodeID.offset) ^ murmurhash64(nodeID.label);
}

template<>
inline uint64_t Hash::operation(const unordered_set<string>& key) {
    auto result = 0ul;
    for (auto&& s : key) {
        result ^= std::hash<string>()(s);
    }
    return result;
}

} // namespace operation
} // namespace common
} // namespace graphflow
