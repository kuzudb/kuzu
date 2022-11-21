#pragma once

#include <cstdint>
#include <unordered_set>

#include "common/type_utils.h"
#include "common/utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

constexpr const uint64_t NULL_HASH = UINT64_MAX;

inline hash_t murmurhash64(uint64_t x) {
    return x * UINT64_C(0xbf58476d1ce4e5b9);
}

inline hash_t combineHashScalar(hash_t a, hash_t b) {
    return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

struct Hash {
    template<class T>
    static inline void operation(const T& key, hash_t& result) {
        throw RuntimeException(
            StringUtils::string_format("Hash type: %s is not supported.", typeid(T).name()));
    }

    template<class T>
    static inline void operation(const T& key, bool isNull, hash_t& result) {
        if (isNull) {
            result = NULL_HASH;
            return;
        }
        operation(key, result);
    }
};

struct CombineHash {
    static inline void operation(hash_t& left, hash_t& right, hash_t& result) {
        result = combineHashScalar(left, right);
    }
};

template<>
inline void Hash::operation(const bool& key, hash_t& result) {
    result = murmurhash64(key);
}

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
inline void Hash::operation(const double_t& key, hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const string& key, hash_t& result) {
    result = std::hash<string>()(key);
}

template<>
inline void Hash::operation(const ku_string_t& key, hash_t& result) {
    result = std::hash<string>()(key.getAsString());
}

template<>
inline void Hash::operation(const date_t& key, hash_t& result) {
    result = murmurhash64(key.days);
}

template<>
inline void Hash::operation(const timestamp_t& key, hash_t& result) {
    result = murmurhash64(key.value);
}

template<>
inline void Hash::operation(const interval_t& key, hash_t& result) {
    result = combineHashScalar(murmurhash64(key.months),
        combineHashScalar(murmurhash64(key.days), murmurhash64(key.micros)));
}

template<>
inline void Hash::operation(const nodeID_t& key, hash_t& result) {
    result = murmurhash64(key.offset) ^ murmurhash64(key.tableID);
}

template<>
inline void Hash::operation(const unordered_set<string>& key, hash_t& result) {
    for (auto&& s : key) {
        result ^= std::hash<string>()(s);
    }
}

} // namespace operation
} // namespace function
} // namespace kuzu
