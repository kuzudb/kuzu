#pragma once

#include <cassert>
#include <functional>

#include "src/common/include/types.h"
#include "src/common/include/value.h"

namespace graphflow {
namespace common {
namespace operation {

struct Equals {
    template<class A, class B>
    static inline uint8_t operation(const A& left, const B& right) {
        return left == right;
    }
};

struct NotEquals {
    template<class A, class B>
    static inline uint8_t operation(const A& left, const B& right) {
        return left != right;
    }
};

struct GreaterThan {
    template<class A, class B>
    static inline uint8_t operation(const A& left, const B& right) {
        return left > right;
    }
};

struct GreaterThanEquals {
    template<class A, class B>
    static inline uint8_t operation(const A& left, const B& right) {
        return left >= right;
    }
};

struct LessThan {
    template<class A, class B>
    static inline uint8_t operation(const A& left, const B& right) {
        return left < right;
    }
};

struct LessThanEquals {
    template<class A, class B>
    static inline uint8_t operation(const A& left, const B& right) {
        return left <= right;
    }
};

struct IsNull {
    template<class T>
    static inline bool operation(const T& value) {
        throw std::invalid_argument("Unsupported type for IsNull.");
    }
};

struct IsNotNull {
    template<class T>
    static inline bool operation(const T& value) {
        throw std::invalid_argument("Unsupported type for IsNotNull.");
    }
};

/*******************************************
 **                                       **
 **   Specialized =, <> implementations   **
 **                                       **
 *******************************************/

struct EqualsOrNotEqualsValues {
    template<bool equals>
    static inline uint8_t operation(const Value& left, const Value& right) {
        if (left.dataType == right.dataType) {
            switch (left.dataType) {
            case BOOL:
                return equals ? Equals::operation(left.val.booleanVal, right.val.booleanVal) :
                                NotEquals::operation(left.val.booleanVal, right.val.booleanVal);
            case INT64:
                return equals ? Equals::operation(left.val.int64Val, right.val.int64Val) :
                                NotEquals::operation(left.val.int64Val, right.val.int64Val);
            case DOUBLE:
                return equals ? Equals::operation(left.val.doubleVal, right.val.doubleVal) :
                                NotEquals::operation(left.val.doubleVal, right.val.doubleVal);
            case STRING:
                return equals ? Equals::operation(left.val.strVal, right.val.strVal) :
                                NotEquals::operation(left.val.strVal, right.val.strVal);
            case DATE:
                return equals ? Equals::operation(left.val.dateVal, right.val.dateVal) :
                                NotEquals::operation(left.val.dateVal, right.val.dateVal);
            default:
                assert(false);
            }
        } else if (left.dataType == INT64 && right.dataType == DOUBLE) {
            return equals ? Equals::operation(left.val.int64Val, right.val.doubleVal) :
                            NotEquals::operation(left.val.int64Val, right.val.doubleVal);
        } else if (left.dataType == DOUBLE && right.dataType == INT64) {
            return equals ? Equals::operation(left.val.doubleVal, right.val.int64Val) :
                            NotEquals::operation(left.val.doubleVal, right.val.int64Val);
        } else {
            return equals ? FALSE : TRUE;
        }
    }
};

// specialized for Value.
template<>
inline uint8_t Equals::operation(const Value& left, const Value& right) {
    return EqualsOrNotEqualsValues::operation<true>(left, right);
};

template<>
inline uint8_t NotEquals::operation(const Value& left, const Value& right) {
    return EqualsOrNotEqualsValues::operation<false>(left, right);
};

// Equals and NotEquals function for gf_string_t.
struct StringComparisonOperators {
    template<bool equals>
    static inline bool EqualsOrNot(const gf_string_t& left, const gf_string_t& right) {
        // first compare the length and prefix of the strings.
        if (memcmp(&left, &right, gf_string_t::STR_LENGTH_PLUS_PREFIX_LENGTH) == 0) {
            // length and prefix of a and b are equal.
            if (memcmp(left.getData(), right.getData(), left.len) == 0) {
                return equals ? TRUE : FALSE;
            }
        }
        return equals ? FALSE : TRUE;
    }
};

// specialized for gf_string_t.
template<>
inline uint8_t Equals::operation(const gf_string_t& left, const gf_string_t& right) {
    return StringComparisonOperators::EqualsOrNot<true>(left, right);
};

template<>
inline uint8_t NotEquals::operation(const gf_string_t& left, const gf_string_t& right) {
    return StringComparisonOperators::EqualsOrNot<false>(left, right);
};

// specialized for nodeID_t.
template<>
inline uint8_t Equals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label == right.label && left.offset == right.offset;
};

template<>
inline uint8_t NotEquals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label != right.label || left.offset != right.offset;
};

/*******************************************************
 **                                                   **
 **   Specialized >, >=, <, <= implementations        **
 **                                                   **
 *******************************************************/

struct CompareValues {
    template<class FUNC = function<uint8_t(Value, Value)>>
    static inline uint8_t operation(const Value& left, const Value& right) {
        if (left.dataType == right.dataType) {
            switch (left.dataType) {
            case BOOL:
                return FUNC::operation(left.val.booleanVal, right.val.booleanVal);
            case INT64:
                return FUNC::operation(left.val.int64Val, right.val.int64Val);
            case DOUBLE:
                return FUNC::operation(left.val.doubleVal, right.val.doubleVal);
            case STRING:
                return FUNC::operation(left.val.strVal, right.val.strVal);
            case DATE:
                return FUNC::operation(left.val.dateVal, right.val.dateVal);
            default:
                assert(false);
            }
        } else if (left.dataType == INT64 && right.dataType == DOUBLE) {
            return FUNC::operation(left.val.int64Val, right.val.doubleVal);
        } else if (left.dataType == DOUBLE && right.dataType == INT64) {
            return FUNC::operation(left.val.doubleVal, right.val.int64Val);
        } else {
            return NULL_BOOL;
        }
    }
};

// specialized for Value.
template<>
inline uint8_t GreaterThan::operation(const Value& left, const Value& right) {
    return CompareValues::operation<GreaterThan>(left, right);
};

template<>
inline uint8_t GreaterThanEquals::operation(const Value& left, const Value& right) {
    return CompareValues::operation<GreaterThanEquals>(left, right);
};

template<>
inline uint8_t LessThan::operation(const Value& left, const Value& right) {
    return CompareValues::operation<LessThan>(left, right);
};

template<>
inline uint8_t LessThanEquals::operation(const Value& left, const Value& right) {
    return CompareValues::operation<LessThanEquals>(left, right);
};

// specialized for uint8_t.
template<>
inline uint8_t GreaterThan::operation(const uint8_t& left, const uint8_t& right) {
    return !right && left;
};

template<>
inline uint8_t LessThan::operation(const uint8_t& left, const uint8_t& right) {
    return !left && right;
};

// compare gf_string_t up to shared length. if still the same, compare lengths.
template<class FUNC>
static uint8_t compare(const gf_string_t& left, const gf_string_t& right) {
    auto len = left.len < right.len ? left.len : right.len;
    auto memcmpResult = memcmp(left.prefix, right.prefix,
        len <= gf_string_t::PREFIX_LENGTH ? len : gf_string_t::PREFIX_LENGTH);
    if (memcmpResult == 0 && len > gf_string_t::PREFIX_LENGTH) {
        memcmpResult = memcmp(left.getData(), right.getData(), len);
    }
    return memcmpResult == 0 ? FUNC::operation(left.len, right.len) :
                               FUNC::operation(memcmpResult, 0);
};

// specialized for gf_string_t.
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

// specialized for nodeID_t.
template<>
inline uint8_t GreaterThan::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label > right.label || (left.label == right.label && left.offset > right.offset);
};

template<>
inline uint8_t GreaterThanEquals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label > right.label || (left.label == right.label && left.offset >= right.offset);
};

template<>
inline uint8_t LessThan::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label < right.label || (left.label == right.label && left.offset < right.offset);
};

template<>
inline uint8_t LessThanEquals::operation(const nodeID_t& left, const nodeID_t& right) {
    return left.label < right.label || (left.label == right.label && left.offset <= right.offset);
};

/********************************************
 **                                        **
 **   Specialized IsNull implementations   **
 **                                        **
 ********************************************/

template<>
inline bool IsNull::operation(const uint8_t& value) {
    return value == NULL_BOOL;
};

template<>
inline bool IsNull::operation(const int64_t& value) {
    return value == NULL_INT64;
};

template<>
inline bool IsNull::operation(const double_t& value) {
    return value == NULL_DOUBLE;
};

template<>
inline bool IsNull::operation(const date_t& value) {
    return value == NULL_DATE;
};

/***********************************************
 **                                           **
 **   Specialized IsNotNull implementations   **
 **                                           **
 ***********************************************/
template<>
inline bool IsNotNull::operation(const uint8_t& value) {
    return value != NULL_BOOL;
};

template<>
inline bool IsNotNull::operation(const int64_t& value) {
    return value != NULL_INT64;
};

template<>
inline bool IsNotNull::operation(const double_t& value) {
    return value != NULL_DOUBLE;
};

} // namespace operation
} // namespace common
} // namespace graphflow
