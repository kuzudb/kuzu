#pragma once

#include <cstdint>
#include <unordered_set>

#include "src/common/include/types.h"
#include "src/common/include/utils.h"

namespace graphflow {
namespace common {
namespace operation {

inline hash_t murmurhash64(uint64_t x) {
    return x * UINT64_C(0xbf58476d1ce4e5b9);
}

inline hash_t combineHashScalar(hash_t a, hash_t b) {
    return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

struct Hash {
    template<class T>
    static inline void operation(const T& key, hash_t& result) {
        throw invalid_argument(
            StringUtils::string_format("Hash type: %s is not supported.", typeid(T).name()));
    }
};

struct CombineHash {
    template<class T>
    static inline void operation(const T& key, hash_t& result) {
        uint64_t otherHash;
        Hash::operation(key, otherHash);
        result = combineHashScalar(result, otherHash);
    }
};

template<>
inline void Hash::operation(const uint32_t& key, hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint64_t& key, hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int64_t& key, hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const string& key, hash_t& result) {
    result = std::hash<string>()(key);
}

template<>
inline void Hash::operation(const nodeID_t& nodeID, hash_t& result) {
    result = murmurhash64(nodeID.offset) ^ murmurhash64(nodeID.label);
}

template<>
inline void Hash::operation(const unordered_set<string>& key, hash_t& result) {
    for (auto&& s : key) {
        result ^= std::hash<string>()(s);
    }
}

} // namespace operation
} // namespace common
} // namespace graphflow
