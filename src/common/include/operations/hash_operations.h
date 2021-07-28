#pragma once

#include <cstdint>
#include <unordered_set>

#include "src/common/include/types.h"
#include "src/common/include/utils.h"

namespace graphflow {
namespace common {
namespace operation {

inline uint64_t murmurhash64(uint64_t x) {
    return x * UINT64_C(0xbf58476d1ce4e5b9);
}

struct Hash {
    template<class T>
    static inline void operation(const T& key, uint64_t& result) {
        throw invalid_argument(
            StringUtils::string_format("Hash type: %s is not supported.", typeid(T).name()));
    }
};

template<>
inline void Hash::operation(const uint32_t& key, uint64_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint64_t& key, uint64_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int64_t& key, uint64_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const string& key, uint64_t& result) {
    result = std::hash<string>()(key);
}

template<>
inline void Hash::operation(const nodeID_t& nodeID, uint64_t& result) {
    result = murmurhash64(nodeID.offset) ^ murmurhash64(nodeID.label);
}

template<>
inline void Hash::operation(const unordered_set<string>& key, uint64_t& result) {
    for (auto&& s : key) {
        result ^= std::hash<string>()(s);
    }
}

} // namespace operation
} // namespace common
} // namespace graphflow
