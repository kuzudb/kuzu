#pragma once

#include <cassert>

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

/*******************************************
 **                                       **
 **   Specialized =, <> implementations   **
 **                                       **
 *******************************************/

struct EqualsOrNotEqualsValues {
    template<bool equals>
    static inline uint8_t operation(const Value& left, const Value& right) {
        switch (left.dataType) {
        case BOOL:
            switch (right.dataType) {
            case BOOL:
                return equals ?
                           Equals::operation(left.primitive.boolean_, right.primitive.boolean_) :
                           NotEquals::operation(left.primitive.boolean_, right.primitive.boolean_);
            case INT32:
            case DOUBLE:
            case STRING:
                return equals ? FALSE : TRUE;
            default:
                assert(false); // should never happen.
            }
        case INT32:
            switch (right.dataType) {
            case INT32:
                return equals ?
                           Equals::operation(left.primitive.integer_, right.primitive.integer_) :
                           NotEquals::operation(left.primitive.integer_, right.primitive.integer_);
            case DOUBLE:
                return equals ?
                           Equals::operation(left.primitive.integer_, right.primitive.double_) :
                           NotEquals::operation(left.primitive.integer_, right.primitive.double_);
            case BOOL:
            case STRING:
                return equals ? FALSE : TRUE;
            default:
                assert(false); // should never happen.
            }
        case DOUBLE:
            switch (right.dataType) {
            case INT32:
                return equals ?
                           Equals::operation(left.primitive.double_, right.primitive.integer_) :
                           NotEquals::operation(left.primitive.double_, right.primitive.integer_);
            case DOUBLE:
                return equals ?
                           Equals::operation(left.primitive.double_, right.primitive.double_) :
                           NotEquals::operation(left.primitive.double_, right.primitive.double_);
            case BOOL:
            case STRING:
                return equals ? FALSE : TRUE;
            default:
                assert(false); // should never happen.
            }
        case STRING:
            switch (right.dataType) {
            case BOOL:
            case INT32:
            case DOUBLE:
                return equals ? FALSE : TRUE;
            case STRING:
                return equals ? Equals::operation(left.str, right.str) :
                                NotEquals::operation(left.str, right.str);
            default:
                assert(false); // should never happen.
            }
        default:
            assert(false); // should never happen.
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
            // length and prefix of a and b are equal
            if (left.isShort()) {
                // compare the 8 byte suffix.
                if (memcmp(left.data, right.data, left.len - gf_string_t::PREFIX_LENGTH) == 0) {
                    return equals ? TRUE : FALSE;
                }
            } else {
                // compare the overflow buffers.
                if (memcmp(reinterpret_cast<uint8_t*>(left.overflowPtr),
                        reinterpret_cast<uint8_t*>(right.overflowPtr),
                        left.len - gf_string_t::PREFIX_LENGTH) == 0) {
                    return equals ? TRUE : FALSE;
                }
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
    return left.label != right.label && left.offset != right.offset;
};

/*******************************************************
 **                                                   **
 **   Specialized >, >=, <, <= implementations        **
 **                                                   **
 *******************************************************/

struct CompareValues {
    template<class FUNC = function<uint8_t(Value, Value)>>
    static inline uint8_t operation(const Value& left, const Value& right) {
        switch (left.dataType) {
        case BOOL:
            switch (right.dataType) {
            case BOOL:
                return FUNC::operation(left.primitive.boolean_, right.primitive.boolean_);
            case INT32:
            case DOUBLE:
            case STRING:
                return NULL_BOOL;
            default:
                assert(false); // should never happen.
            }
        case INT32:
            switch (right.dataType) {
            case INT32:
                return FUNC::operation(left.primitive.integer_, right.primitive.integer_);
            case DOUBLE:
                return FUNC::operation(left.primitive.integer_, right.primitive.double_);
            case BOOL:
            case STRING:
                return NULL_BOOL;
            default:
                assert(false); // should never happen.
            }
        case DOUBLE:
            switch (right.dataType) {
            case INT32:
                return FUNC::operation(left.primitive.double_, right.primitive.integer_);
            case DOUBLE:
                return FUNC::operation(left.primitive.double_, right.primitive.double_);
            case BOOL:
            case STRING:
                return NULL_BOOL;
            default:
                assert(false); // should never happen.
            }
        case STRING:
            switch (right.dataType) {
            case BOOL:
            case INT32:
            case DOUBLE:
                return NULL_BOOL;
            case STRING:
                return FUNC::operation(left.str, right.str);
            default:
                assert(false); // should never happen.
            }
        default:
            assert(false); // should never happen.
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
    if (len > gf_string_t::PREFIX_LENGTH && memcmpResult == 0) {
        memcmpResult = memcmp(left.getData(), right.getData(), len - gf_string_t::PREFIX_LENGTH);
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

/********************************************
 **                                        **
 **   Specialized IsNull implementations   **
 **                                        **
 ********************************************/

template<>
inline uint8_t IsNull::operation(const uint8_t& value) {
    return value == NULL_BOOL ? TRUE : FALSE;
};

template<>
inline uint8_t IsNull::operation(const int32_t& value) {
    return value == NULL_INT32 ? TRUE : FALSE;
};

template<>
inline uint8_t IsNull::operation(const double_t& value) {
    return value == NULL_DOUBLE ? TRUE : FALSE;
};

/***********************************************
 **                                           **
 **   Specialized IsNotNull implementations   **
 **                                           **
 ***********************************************/
template<>
inline uint8_t IsNotNull::operation(const uint8_t& value) {
    return value != NULL_BOOL ? TRUE : FALSE;
};

template<>
inline uint8_t IsNotNull::operation(const int32_t& value) {
    return value != NULL_INT32 ? TRUE : FALSE;
};

template<>
inline uint8_t IsNotNull::operation(const double_t& value) {
    return value != NULL_DOUBLE ? TRUE : FALSE;
};

} // namespace operation
} // namespace common
} // namespace graphflow
