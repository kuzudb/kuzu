#pragma once

#include <cstdint>
#include <unordered_set>

#include "src/common/include/gf_string.h"
#include "src/common/include/types.h"
#include "src/common/include/utils.h"
#include "src/common/include/value.h"

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
    static inline void operation(const T& key, bool isNull, hash_t& result) {
        assert(!isNull);
        throw invalid_argument(
            StringUtils::string_format("Hash type: %s is not supported.", typeid(T).name()));
    }
};

struct CombineHash {
    template<class T>
    static inline void operation(const T& key, bool isNull, hash_t& result) {
        assert(!isNull);
        uint64_t otherHash;
        Hash::operation(key, isNull, otherHash);
        result = combineHashScalar(result, otherHash);
    }
};

template<>
inline void Hash::operation(const bool& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint32_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const uint64_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const int64_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const double_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key);
}

template<>
inline void Hash::operation(const gf_string_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = std::hash<string>()(key.getAsString());
}

template<>
inline void Hash::operation(const date_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key.days);
}

template<>
inline void Hash::operation(const timestamp_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key.value);
}

template<>
inline void Hash::operation(const interval_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = combineHashScalar(murmurhash64(key.months),
        combineHashScalar(murmurhash64(key.days), murmurhash64(key.micros)));
}

template<>
inline void Hash::operation(const nodeID_t& key, bool isNull, hash_t& result) {
    assert(!isNull);
    result = murmurhash64(key.offset) ^ murmurhash64(key.label);
}

template<>
inline void Hash::operation(const unordered_set<string>& key, bool isNull, hash_t& result) {
    assert(!isNull);
    for (auto&& s : key) {
        result ^= std::hash<string>()(s);
    }
}

/**********************************************
 **                                          **
 **   Specialized Value implementations      **
 **                                          **
 **********************************************/

struct HashOnValue {
    static void operation(Value& key, bool isNull, hash_t& result) {
        assert(!isNull);
        switch (key.dataType) {
        case NODE_ID: {
            Hash::operation<nodeID_t>(key.val.nodeID, isNull, result);
        } break;
        case BOOL: {
            Hash::operation<bool>(key.val.booleanVal, isNull, result);
        } break;
        case INT64: {
            Hash::operation<int64_t>(key.val.int64Val, isNull, result);
        } break;
        case DOUBLE: {
            Hash::operation<double_t>(key.val.doubleVal, isNull, result);
        } break;
        case STRING: {
            Hash::operation<gf_string_t>(key.val.strVal, isNull, result);
        } break;
        case DATE: {
            Hash::operation<date_t>(key.val.dateVal, isNull, result);
        } break;
        case TIMESTAMP: {
            Hash::operation<timestamp_t>(key.val.timestampVal, isNull, result);
        } break;
        case INTERVAL: {
            Hash::operation<interval_t>(key.val.intervalVal, isNull, result);
        } break;
        default: {
            throw invalid_argument(
                "Cannot hash data type " + TypeUtils::dataTypeToString(key.dataType));
        }
        }
    }
};

/**********************************************
 **                                          **
 **   Specialized Bytes implementations      **
 **                                          **
 **********************************************/

struct HashOnBytes {
    static inline void operation(DataType dataType, uint8_t* key, bool isNull, hash_t& result) {
        switch (dataType) {
        case NODE_ID: {
            Hash::operation<nodeID_t>(*(nodeID_t*)key, isNull, result);
        } break;
        case BOOL: {
            Hash::operation<bool>(*(bool*)key, isNull, result);
        } break;
        case INT64: {
            Hash::operation<int64_t>(*(int64_t*)key, isNull, result);
        } break;
        case DOUBLE: {
            Hash::operation<double_t>(*(double_t*)key, isNull, result);
        } break;
        case STRING: {
            Hash::operation<gf_string_t>(*(gf_string_t*)key, isNull, result);
        } break;
        case DATE: {
            Hash::operation<date_t>(*(date_t*)key, isNull, result);
        } break;
        case TIMESTAMP: {
            Hash::operation<timestamp_t>(*(timestamp_t*)key, isNull, result);
        } break;
        case INTERVAL: {
            Hash::operation<interval_t>(*(interval_t*)key, isNull, result);
        } break;
        case UNSTRUCTURED: {
            HashOnValue::operation(*(Value*)key, isNull, result);
        } break;
        default: {
            throw invalid_argument(
                "Cannot hash data type " + TypeUtils::dataTypeToString(dataType));
        }
        }
    }
};

} // namespace operation
} // namespace common
} // namespace graphflow
