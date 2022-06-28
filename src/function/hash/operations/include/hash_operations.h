#pragma once

#include <cstdint>
#include <unordered_set>

#include "src/common/include/type_utils.h"
#include "src/common/include/utils.h"
#include "src/common/types/include/value.h"

using namespace graphflow::common;

namespace graphflow {
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
inline void Hash::operation(const gf_string_t& key, hash_t& result) {
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
    result = murmurhash64(key.offset) ^ murmurhash64(key.label);
}

template<>
inline void Hash::operation(const unordered_set<string>& key, hash_t& result) {
    for (auto&& s : key) {
        result ^= std::hash<string>()(s);
    }
}

/**********************************************
 **                                          **
 **   Specialized Value implementations      **
 **                                          **
 **********************************************/

template<>
inline void Hash::operation(const Value& key, hash_t& result) {
    switch (key.dataType.typeID) {
    case NODE_ID: {
        Hash::operation<nodeID_t>(key.val.nodeID, result);
    } break;
    case BOOL: {
        Hash::operation<bool>(key.val.booleanVal, result);
    } break;
    case INT64: {
        Hash::operation<int64_t>(key.val.int64Val, result);
    } break;
    case DOUBLE: {
        Hash::operation<double_t>(key.val.doubleVal, result);
    } break;
    case STRING: {
        Hash::operation<gf_string_t>(key.val.strVal, result);
    } break;
    case DATE: {
        Hash::operation<date_t>(key.val.dateVal, result);
    } break;
    case TIMESTAMP: {
        Hash::operation<timestamp_t>(key.val.timestampVal, result);
    } break;
    case INTERVAL: {
        Hash::operation<interval_t>(key.val.intervalVal, result);
    } break;
    default: {
        throw RuntimeException(
            "Cannot hash data type " + Types::dataTypeToString(key.dataType.typeID));
    }
    }
}

struct HashOnValue {
    static void operation(Value& key, bool isNull, hash_t& result) {
        if (isNull) {
            result = NULL_HASH;
            return;
        }
        Hash::operation(key, result);
    }
};

/**********************************************
 **                                          **
 **   Specialized Bytes implementations      **
 **                                          **
 **********************************************/

struct HashOnBytes {
    static inline void operation(DataTypeID dataTypeID, uint8_t* key, bool isNull, hash_t& result) {
        switch (dataTypeID) {
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
            throw RuntimeException("Cannot hash data type " + Types::dataTypeToString(dataTypeID));
        }
        }
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
