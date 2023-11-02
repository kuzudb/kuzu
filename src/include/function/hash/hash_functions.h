#pragma once

#include <cmath>
#include <cstdint>
#include <unordered_set>

#include "common/exception/runtime.h"
#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_list.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

constexpr const uint64_t NULL_HASH = UINT64_MAX;

inline common::hash_t murmurhash64(uint64_t x) {
    return x * UINT64_C(0xbf58476d1ce4e5b9);
}

inline common::hash_t combineHashScalar(common::hash_t a, common::hash_t b) {
    return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

struct Hash {
    template<class T>
    static inline void operation(const T& /*key*/, common::hash_t& /*result*/) {
        throw common::RuntimeException(
            "Hash type: " + std::string(typeid(T).name()) + " is not supported.");
    }

    template<class T>
    static inline void operation(const T& key, bool isNull, common::hash_t& result) {
        if (isNull) {
            result = NULL_HASH;
            return;
        }
        operation(key, result);
    }
};

struct CombineHash {
    static inline void operation(
        common::hash_t& left, common::hash_t& right, common::hash_t& result) {
        result = combineHashScalar(left, right);
    }
};

template<>
inline void Hash::operation(const common::internalID_t& key, common::hash_t& result) {
    result = murmurhash64(key.offset) ^ murmurhash64(key.tableID);
}

template<>
inline void Hash::operation(const bool& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint8_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint16_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint32_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint64_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int64_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int32_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int16_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int8_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const common::int128_t& key, common::hash_t& result) {
    result = murmurhash64(key.low) ^ murmurhash64(key.high);
}

template<>
inline void Hash::operation(const double_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const float_t& key, common::hash_t& result) {
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const std::string& key, common::hash_t& result) {
    result = std::hash<std::string>()(key);
}

template<>
inline void Hash::operation(const common::ku_string_t& key, common::hash_t& result) {
    result = std::hash<std::string>()(key.getAsString());
}

template<>
inline void Hash::operation(const common::interval_t& key, common::hash_t& result) {
    result = combineHashScalar(murmurhash64(key.months),
        combineHashScalar(murmurhash64(key.days), murmurhash64(key.micros)));
}

template<>
inline void Hash::operation(const common::ku_list_t& /*key*/, common::hash_t& /*result*/) {
    throw common::RuntimeException(
        "Computing hash value of list DataType is currently unsupported.");
}

template<>
inline void Hash::operation(const std::unordered_set<std::string>& key, common::hash_t& result) {
    for (auto&& s : key) {
        result ^= std::hash<std::string>()(s);
    }
}

struct InternalIDHasher {
    std::size_t operator()(const common::internalID_t& internalID) const {
        common::hash_t result;
        function::Hash::operation<common::internalID_t>(internalID, result);
        return result;
    }
};

} // namespace function
} // namespace kuzu
