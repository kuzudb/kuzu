#pragma once

#include <functional>

#include "src/common/include/types.h"
#include "src/common/include/value.h"

namespace graphflow {
namespace common {
namespace operation {

struct Equals {
    template<class A, class B>
    static inline void operation(
        const A& left, const B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left == right;
    }
};

struct NotEquals {
    template<class A, class B>
    static inline void operation(
        const A& left, const B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left != right;
    }
};

struct GreaterThan {
    template<class A, class B>
    static inline void operation(
        const A& left, const B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left > right;
    }
};

struct GreaterThanEquals {
    template<class A, class B>
    static inline void operation(
        const A& left, const B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left >= right;
    }
};

struct LessThan {
    template<class A, class B>
    static inline void operation(
        const A& left, const B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left < right;
    }
};

struct LessThanEquals {
    template<class A, class B>
    static inline void operation(
        const A& left, const B& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left <= right;
    }
};

//! This function is ONLY used by ValueVector::fillOperandNullMask.
struct IsNullValue {
    template<class T>
    static inline bool operation(const T& value) {
        throw std::invalid_argument("Unsupported type for IsNullValue.");
    }
};

struct IsNull {
    template<class T>
    static inline void operation(T value, bool isNull, uint8_t& result) {
        result = isNull;
    }
};

struct IsNotNull {
    template<class T>
    static inline bool operation(T value, bool isNull, uint8_t& result) {
        result = !isNull;
    }
};

/*******************************************
 **                                       **
 **   Specialized =, <> implementations   **
 **                                       **
 *******************************************/

struct EqualsOrNotEqualsValues {
    template<bool equals>
    static inline void operation(
        const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
        if (left.dataType == right.dataType) {
            switch (left.dataType) {
            case BOOL: {
                if constexpr (equals) {
                    Equals::operation(
                        left.val.booleanVal, right.val.booleanVal, result, isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(
                        left.val.booleanVal, right.val.booleanVal, result, isLeftNull, isRightNul);
                }
            } break;
            case INT64: {
                if constexpr (equals) {
                    Equals::operation(
                        left.val.int64Val, right.val.int64Val, result, isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(
                        left.val.int64Val, right.val.int64Val, result, isLeftNull, isRightNul);
                }
            } break;
            case DOUBLE: {
                if constexpr (equals) {
                    Equals::operation(
                        left.val.doubleVal, right.val.doubleVal, result, isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(
                        left.val.doubleVal, right.val.doubleVal, result, isLeftNull, isRightNul);
                }
            } break;
            case STRING: {
                if constexpr (equals) {
                    Equals::operation(
                        left.val.strVal, right.val.strVal, result, isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(
                        left.val.strVal, right.val.strVal, result, isLeftNull, isRightNul);
                }
            } break;
            case DATE: {
                if constexpr (equals) {
                    Equals::operation(
                        left.val.dateVal, right.val.dateVal, result, isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(
                        left.val.dateVal, right.val.dateVal, result, isLeftNull, isRightNul);
                }
            } break;
            case TIMESTAMP: {
                if constexpr (equals) {
                    Equals::operation(left.val.timestampVal, right.val.timestampVal, result,
                        isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(left.val.timestampVal, right.val.timestampVal, result,
                        isLeftNull, isRightNul);
                }
            } break;
            case INTERVAL: {
                if constexpr (equals) {
                    Equals::operation(left.val.intervalVal, right.val.intervalVal, result,
                        isLeftNull, isRightNul);
                } else {
                    NotEquals::operation(left.val.intervalVal, right.val.intervalVal, result,
                        isLeftNull, isRightNul);
                }
            } break;
            default:
                assert(false);
            }
        } else if (left.dataType == INT64 && right.dataType == DOUBLE) {
            if constexpr (equals) {
                Equals::operation(
                    left.val.int64Val, right.val.doubleVal, result, isLeftNull, isRightNul);
            } else {
                NotEquals::operation(
                    left.val.int64Val, right.val.doubleVal, result, isLeftNull, isRightNul);
            }
        } else if (left.dataType == DOUBLE && right.dataType == INT64) {
            if constexpr (equals) {
                Equals::operation(
                    left.val.doubleVal, right.val.int64Val, result, isLeftNull, isRightNul);
            } else {
                NotEquals::operation(
                    left.val.doubleVal, right.val.int64Val, result, isLeftNull, isRightNul);
            }
        } else {
            result = equals ? FALSE : TRUE;
        }
    }
};

// specialized for Value.
template<>
inline void Equals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    EqualsOrNotEqualsValues::operation<true>(left, right, result, isLeftNull, isRightNul);
};

template<>
inline void NotEquals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    EqualsOrNotEqualsValues::operation<false>(left, right, result, isLeftNull, isRightNul);
};

// Equals and NotEquals function for gf_string_t.
struct StringComparisonOperators {
    template<bool equals>
    static inline void EqualsOrNot(
        const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
        // first compare the length and prefix of the strings.
        if (memcmp(&left, &right, gf_string_t::STR_LENGTH_PLUS_PREFIX_LENGTH) == 0) {
            // length and prefix of a and b are equal.
            if (memcmp(left.getData(), right.getData(), left.len) == 0) {
                result = equals ? TRUE : FALSE;
                return;
            }
        }
        result = equals ? FALSE : TRUE;
    }

    // compare gf_string_t up to shared length. if still the same, Compare lengths.
    template<class FUNC>
    static void Compare(const gf_string_t& left, const gf_string_t& right, uint8_t& result,
        bool isLeftNull, bool isRightNul) {
        auto len = left.len < right.len ? left.len : right.len;
        auto memcmpResult = memcmp(left.prefix, right.prefix,
            len <= gf_string_t::PREFIX_LENGTH ? len : gf_string_t::PREFIX_LENGTH);
        if (memcmpResult == 0 && len > gf_string_t::PREFIX_LENGTH) {
            memcmpResult = memcmp(left.getData(), right.getData(), len);
        }
        if (memcmpResult == 0) {
            FUNC::operation(left.len, right.len, result, isLeftNull, isRightNul);
        } else {
            FUNC::operation(memcmpResult, 0, result, isLeftNull, isRightNul);
        }
    };
};

// specialized for gf_string_t.
template<>
inline void Equals::operation(const gf_string_t& left, const gf_string_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    StringComparisonOperators::EqualsOrNot<true>(left, right, result);
};

template<>
inline void NotEquals::operation(const gf_string_t& left, const gf_string_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    StringComparisonOperators::EqualsOrNot<false>(left, right, result);
};

// specialized for nodeID_t.
template<>
inline void Equals::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left.label == right.label && left.offset == right.offset;
};

template<>
inline void NotEquals::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left.label != right.label || left.offset != right.offset;
};

/*******************************************************
 **                                                   **
 **   Specialized >, >=, <, <= implementations        **
 **                                                   **
 *******************************************************/

struct CompareValues {
    template<class FUNC = std::function<uint8_t(Value, Value)>>
    static inline void operation(
        const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
        if (left.dataType == right.dataType) {
            switch (left.dataType) {
            case BOOL: {
                FUNC::operation(
                    left.val.booleanVal, right.val.booleanVal, result, isLeftNull, isRightNul);
            } break;
            case INT64: {
                FUNC::operation(
                    left.val.int64Val, right.val.int64Val, result, isLeftNull, isRightNul);
            } break;
            case DOUBLE: {
                FUNC::operation(
                    left.val.doubleVal, right.val.doubleVal, result, isLeftNull, isRightNul);
            } break;
            case STRING: {
                FUNC::operation(left.val.strVal, right.val.strVal, result, isLeftNull, isRightNul);
            } break;
            case DATE: {
                FUNC::operation(
                    left.val.dateVal, right.val.dateVal, result, isLeftNull, isRightNul);
            } break;
            case TIMESTAMP: {
                FUNC::operation(
                    left.val.timestampVal, right.val.timestampVal, result, isLeftNull, isRightNul);
            } break;
            case INTERVAL: {
                FUNC::operation(
                    left.val.intervalVal, right.val.intervalVal, result, isLeftNull, isRightNul);
            } break;
            default:
                assert(false);
            }
        } else if (left.dataType == INT64 && right.dataType == DOUBLE) {
            FUNC::operation(left.val.int64Val, right.val.doubleVal, result, isLeftNull, isRightNul);
        } else if (left.dataType == DOUBLE && right.dataType == INT64) {
            FUNC::operation(left.val.doubleVal, right.val.int64Val, result, isLeftNull, isRightNul);
        } else {
            result = NULL_BOOL;
        }
    }
};

// specialized for Value.
template<>
inline void GreaterThan::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<GreaterThan>(left, right, result, isLeftNull, isRightNul);
};

template<>
inline void GreaterThanEquals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<GreaterThanEquals>(left, right, result, isLeftNull, isRightNul);
};

template<>
inline void LessThan::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<LessThan>(left, right, result, isLeftNull, isRightNul);
};

template<>
inline void LessThanEquals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<LessThanEquals>(left, right, result, isLeftNull, isRightNul);
};

// specialized for uint8_t.
template<>
inline void GreaterThan::operation(
    const uint8_t& left, const uint8_t& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = !right && left;
};

template<>
inline void LessThan::operation(
    const uint8_t& left, const uint8_t& right, uint8_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = !left && right;
};

// specialized for gf_string_t.
template<>
inline void GreaterThan::operation(const gf_string_t& left, const gf_string_t& right,
    uint8_t& result, bool isLeftNull, bool isRightNul) {
    StringComparisonOperators::Compare<GreaterThan>(left, right, result, isLeftNull, isRightNul);
};

template<>
inline void GreaterThanEquals::operation(const gf_string_t& left, const gf_string_t& right,
    uint8_t& result, bool isLeftNull, bool isRightNul) {
    StringComparisonOperators::Compare<GreaterThanEquals>(
        left, right, result, isLeftNull, isRightNul);
};

template<>
inline void LessThan::operation(const gf_string_t& left, const gf_string_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNul) {
    StringComparisonOperators::Compare<LessThan>(left, right, result, isLeftNull, isRightNul);
};

template<>
inline void LessThanEquals::operation(const gf_string_t& left, const gf_string_t& right,
    uint8_t& result, bool isLeftNull, bool isRightNul) {
    StringComparisonOperators::Compare<LessThanEquals>(left, right, result, isLeftNull, isRightNul);
};

// specialized for nodeID_t.
template<>
inline void GreaterThan::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result =
        (left.label > right.label) || (left.label == right.label && left.offset > right.offset);
};

template<>
inline void GreaterThanEquals::operation(const nodeID_t& left, const nodeID_t& right,
    uint8_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left.label > right.label || (left.label == right.label && left.offset >= right.offset);
};

template<>
inline void LessThan::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left.label < right.label || (left.label == right.label && left.offset < right.offset);
};

template<>
inline void LessThanEquals::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result,
    bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left.label < right.label || (left.label == right.label && left.offset <= right.offset);
};

/*************************************************
 **                                             **
 **   Specialized IsNullValue implementations   **
 **                                             **
 ************************************************/

template<>
inline bool IsNullValue::operation(const uint8_t& value) {
    return value == NULL_BOOL;
};

template<>
inline bool IsNullValue::operation(const int64_t& value) {
    return value == NULL_INT64;
};

template<>
inline bool IsNullValue::operation(const double_t& value) {
    return value == NULL_DOUBLE;
};

template<>
inline bool IsNullValue::operation(const date_t& value) {
    return value == NULL_DATE;
};

template<>
inline bool IsNullValue::operation(const timestamp_t& value) {
    return value == NULL_TIMESTAMP;
};

template<>
inline bool IsNullValue::operation(const interval_t& value) {
    return value == NULL_INTERVAL;
};

} // namespace operation
} // namespace common
} // namespace graphflow
