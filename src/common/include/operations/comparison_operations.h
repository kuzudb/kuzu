#pragma once

#include "src/common/include/types.h"

namespace graphflow {
namespace common {
namespace operation {

struct Equals {
    template<class T>
    static inline uint8_t operation(const T& left, const T& right) {
        return left == right;
    }
};

struct NotEquals {
    template<class T>
    static inline uint8_t operation(const T& left, const T& right) {
        return left != right;
    }
};

struct GreaterThan {
    template<class T>
    static inline uint8_t operation(const T& left, const T& right) {
        return left > right;
    }
};

struct GreaterThanEquals {
    template<class T>
    static inline uint8_t operation(const T& left, const T& right) {
        return left >= right;
    }
};

struct LessThan {
    template<class T>
    static inline uint8_t operation(const T& left, const T& right) {
        return left < right;
    }
};

struct LessThanEquals {
    template<class T>
    static inline uint8_t operation(const T& left, const T& right) {
        return left <= right;
    }
};

struct IsNull {
    template<class T>
    static inline uint8_t operation(const T& value) {
        throw std::invalid_argument("Unsupported type for IsNull.");
    }
};

struct IsNotNull {
    template<class T>
    static inline uint8_t operation(const T& value) {
        throw std::invalid_argument("Unsupported type for IsNotNull.");
    }
};

// Specialized operation(s) for IsNull and IsNotNull.
template<>
inline uint8_t IsNull::operation(const int32_t& value) {
    return value == NULL_INT32 ? TRUE : FALSE;
};

template<>
inline uint8_t IsNull::operation(const double_t& value) {
    return value == NULL_DOUBLE ? TRUE : FALSE;
};

template<>
inline uint8_t IsNull::operation(const uint8_t& value) {
    return value == NULL_BOOL ? TRUE : FALSE;
};

template<>
inline uint8_t IsNotNull::operation(const int32_t& value) {
    return value != NULL_INT32 ? TRUE : FALSE;
};

template<>
inline uint8_t IsNotNull::operation(const double_t& value) {
    return value != NULL_DOUBLE ? TRUE : FALSE;
};

template<>
inline uint8_t IsNotNull::operation(const uint8_t& value) {
    return value != NULL_BOOL ? TRUE : FALSE;
};

// Specialized operation(s) for bool (uint8_t).
template<>
inline uint8_t GreaterThan::operation(const uint8_t& left, const uint8_t& right) {
    return !right && left;
};

template<>
inline uint8_t LessThan::operation(const uint8_t& left, const uint8_t& right) {
    return !left && right;
};

// Specialized operation(s) for nodeID_t.
template<>
inline uint8_t Equals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label == right.label && left.offset == right.offset;
};

template<>
inline uint8_t NotEquals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label != right.label && left.offset != right.offset;
};

template<>
inline uint8_t GreaterThan::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label > right.label || (left.label == right.label && left.offset > right.offset);
};

template<>
inline uint8_t GreaterThanEquals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label >= right.label || (left.label == right.label && left.offset >= right.offset);
};

template<>
inline uint8_t LessThan::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label < right.label || (left.label == right.label && left.offset < right.offset);
};

template<>
inline uint8_t LessThanEquals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label <= right.label || (left.label == right.label && left.offset <= right.offset);
};

// Specialized operation(s) for gf_string_t.
struct StringComparisonOperators {
    template<bool INVERSE>
    static inline bool EqualsOrNot(const gf_string_t& left, const gf_string_t& right) {
        // first compare the length and prefix of the strings.
        if (memcmp(&left, &right, gf_string_t::STR_LENGTH_PLUS_PREFIX_LENGTH) == 0) {
            // length and prefix of a and b are equal
            if (left.isShort()) {
                // compare the 8 byte suffix.
                if (memcmp(left.data, right.data, left.len - gf_string_t::PREFIX_LENGTH) == 0) {
                    return INVERSE ? FALSE : TRUE;
                }
            } else {
                // compare the overflow buffers.
                if (memcmp(reinterpret_cast<uint8_t*>(left.overflowPtr),
                        reinterpret_cast<uint8_t*>(right.overflowPtr),
                        left.len - gf_string_t::PREFIX_LENGTH) == 0) {
                    return INVERSE ? FALSE : TRUE;
                }
            }
        }
        return INVERSE ? TRUE : FALSE;
    }
};

template<>
inline uint8_t Equals::operation(const gf_string_t& left, const gf_string_t& right) {
    return StringComparisonOperators::EqualsOrNot<false>(left, right);
};

template<>
inline uint8_t NotEquals::operation(const gf_string_t& left, const gf_string_t& right) {
    return StringComparisonOperators::EqualsOrNot<true>(left, right);
};

// compare up to shared length. if still the same, compare lengths
template<class FUNC>
static uint8_t compare(const gf_string_t& left, const gf_string_t& right) {
    auto len = left.len < right.len ? left.len : right.len;
    auto memcmpResult = memcmp(left.prefix, right.prefix,
        len <= gf_string_t::PREFIX_LENGTH ? len : gf_string_t::PREFIX_LENGTH);
    if (len > gf_string_t::PREFIX_LENGTH && memcmpResult == 0) {
        memcmpResult = memcmp(left.getData(), right.getData(), len - gf_string_t::PREFIX_LENGTH);
    }
    return memcmpResult == 0 ? FUNC::operation(left.len, right.len) :
                               FUNC::operation(memcmpResult, 0);
};

template<>
inline uint8_t GreaterThan::operation(const gf_string_t& left, const gf_string_t& right) {
    return compare<GreaterThan>(left, right);
};

template<>
inline uint8_t GreaterThanEquals::operation(const gf_string_t& left, const gf_string_t& right) {
    return compare<GreaterThanEquals>(left, right);
};

template<>
inline uint8_t LessThan::operation(const gf_string_t& left, const gf_string_t& right) {
    return compare<LessThan>(left, right);
};

template<>
inline uint8_t LessThanEquals::operation(const gf_string_t& left, const gf_string_t& right) {
    return compare<LessThanEquals>(left, right);
};

} // namespace operation
} // namespace common
} // namespace graphflow
