#pragma once

#include <functional>

#include "src/common/include/types.h"
#include "src/common/include/value.h"

namespace graphflow {
namespace common {
namespace operation {

struct Equals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result) {
        result = left == right;
    }
};

struct NotEquals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result) {
        result = left != right;
    }
};

struct GreaterThan {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result) {
        result = left > right;
    }
};

struct GreaterThanEquals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result) {
        result = left >= right;
    }
};

struct LessThan {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result) {
        result = left < right;
    }
};

struct LessThanEquals {
    template<class A, class B>
    static inline void operation(const A& left, const B& right, uint8_t& result) {
        result = left <= right;
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
    static inline void operation(const Value& left, const Value& right, uint8_t& result) {
        if (left.dataType == right.dataType) {
            switch (left.dataType) {
            case BOOL:
                if constexpr (equals) {
                    Equals::operation(left.val.booleanVal, right.val.booleanVal, result);
                } else {
                    NotEquals::operation(left.val.booleanVal, right.val.booleanVal, result);
                }
                return;
            case INT64:
                if constexpr (equals) {
                    Equals::operation(left.val.int64Val, right.val.int64Val, result);
                } else {
                    NotEquals::operation(left.val.int64Val, right.val.int64Val, result);
                }
                return;
            case DOUBLE:
                if constexpr (equals) {
                    Equals::operation(left.val.doubleVal, right.val.doubleVal, result);
                } else {
                    NotEquals::operation(left.val.doubleVal, right.val.doubleVal, result);
                }
                return;
            case STRING:
                if constexpr (equals) {
                    Equals::operation(left.val.strVal, right.val.strVal, result);
                } else {
                    NotEquals::operation(left.val.strVal, right.val.strVal, result);
                }
                return;
            case DATE:
                if constexpr (equals) {
                    Equals::operation(left.val.dateVal, right.val.dateVal, result);
                } else {
                    NotEquals::operation(left.val.dateVal, right.val.dateVal, result);
                }
                return;
            case TIMESTAMP:
                if constexpr (equals) {
                    Equals::operation(left.val.timestampVal, right.val.timestampVal, result);
                } else {
                    NotEquals::operation(left.val.timestampVal, right.val.timestampVal, result);
                }
                return;
            case INTERVAL:
                if constexpr (equals) {
                    Equals::operation(left.val.intervalVal, right.val.intervalVal, result);
                } else {
                    NotEquals::operation(left.val.intervalVal, right.val.intervalVal, result);
                }
                return;
            default:
                assert(false);
            }
        } else if (left.dataType == INT64 && right.dataType == DOUBLE) {
            if constexpr (equals) {
                Equals::operation(left.val.int64Val, right.val.doubleVal, result);
            } else {
                NotEquals::operation(left.val.int64Val, right.val.doubleVal, result);
            }
        } else if (left.dataType == DOUBLE && right.dataType == INT64) {
            if constexpr (equals) {
                Equals::operation(left.val.doubleVal, right.val.int64Val, result);
            } else {
                NotEquals::operation(left.val.doubleVal, right.val.int64Val, result);
            }
        } else {
            result = equals ? FALSE : TRUE;
        }
    }
};

// specialized for Value.
template<>
inline void Equals::operation(const Value& left, const Value& right, uint8_t& result) {
    EqualsOrNotEqualsValues::operation<true>(left, right, result);
};

template<>
inline void NotEquals::operation(const Value& left, const Value& right, uint8_t& result) {
    EqualsOrNotEqualsValues::operation<false>(left, right, result);
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
    static void Compare(const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
        auto len = left.len < right.len ? left.len : right.len;
        auto memcmpResult = memcmp(left.prefix, right.prefix,
            len <= gf_string_t::PREFIX_LENGTH ? len : gf_string_t::PREFIX_LENGTH);
        if (memcmpResult == 0 && len > gf_string_t::PREFIX_LENGTH) {
            memcmpResult = memcmp(left.getData(), right.getData(), len);
        }
        if (memcmpResult == 0) {
            FUNC::operation(left.len, right.len, result);
        } else {
            FUNC::operation(memcmpResult, 0, result);
        }
    };
};

// specialized for gf_string_t.
template<>
inline void Equals::operation(const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
    StringComparisonOperators::EqualsOrNot<true>(left, right, result);
};

template<>
inline void NotEquals::operation(
    const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
    StringComparisonOperators::EqualsOrNot<false>(left, right, result);
};

// specialized for nodeID_t.
template<>
inline void Equals::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result) {
    result = left.label == right.label && left.offset == right.offset;
};

template<>
inline void NotEquals::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result) {
    result = left.label != right.label || left.offset != right.offset;
};

/*******************************************************
 **                                                   **
 **   Specialized >, >=, <, <= implementations        **
 **                                                   **
 *******************************************************/

struct CompareValues {
    template<class FUNC = std::function<uint8_t(Value, Value)>>
    static inline void operation(const Value& left, const Value& right, uint8_t& result) {
        if (left.dataType == right.dataType) {
            switch (left.dataType) {
            case BOOL:
                FUNC::operation(left.val.booleanVal, right.val.booleanVal, result);
                break;
            case INT64:
                FUNC::operation(left.val.int64Val, right.val.int64Val, result);
                break;
            case DOUBLE:
                FUNC::operation(left.val.doubleVal, right.val.doubleVal, result);
                break;
            case STRING:
                FUNC::operation(left.val.strVal, right.val.strVal, result);
                break;
            case DATE:
                FUNC::operation(left.val.dateVal, right.val.dateVal, result);
                break;
            case TIMESTAMP:
                FUNC::operation(left.val.timestampVal, right.val.timestampVal, result);
                break;
            case INTERVAL:
                FUNC::operation(left.val.intervalVal, right.val.intervalVal, result);
                break;
            default:
                assert(false);
            }
        } else if (left.dataType == INT64 && right.dataType == DOUBLE) {
            FUNC::operation(left.val.int64Val, right.val.doubleVal, result);
        } else if (left.dataType == DOUBLE && right.dataType == INT64) {
            FUNC::operation(left.val.doubleVal, right.val.int64Val, result);
        } else {
            result = NULL_BOOL;
        }
    }
};

// specialized for Value.
template<>
inline void GreaterThan::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<GreaterThan>(left, right, result);
};

template<>
inline void GreaterThanEquals::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<GreaterThanEquals>(left, right, result);
};

template<>
inline void LessThan::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<LessThan>(left, right, result);
};

template<>
inline void LessThanEquals::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<LessThanEquals>(left, right, result);
};

// specialized for uint8_t.
template<>
inline void GreaterThan::operation(const uint8_t& left, const uint8_t& right, uint8_t& result) {
    result = !right && left;
};

template<>
inline void LessThan::operation(const uint8_t& left, const uint8_t& right, uint8_t& result) {
    result = !left && right;
};

// specialized for gf_string_t.
template<>
inline void GreaterThan::operation(
    const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
    StringComparisonOperators::Compare<GreaterThan>(left, right, result);
};

template<>
inline void GreaterThanEquals::operation(
    const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
    StringComparisonOperators::Compare<GreaterThanEquals>(left, right, result);
};

template<>
inline void LessThan::operation(
    const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
    StringComparisonOperators::Compare<LessThan>(left, right, result);
};

template<>
inline void LessThanEquals::operation(
    const gf_string_t& left, const gf_string_t& right, uint8_t& result) {
    StringComparisonOperators::Compare<LessThanEquals>(left, right, result);
};

// specialized for nodeID_t.
template<>
inline void GreaterThan::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result) {
    result =
        (left.label > right.label) || (left.label == right.label && left.offset > right.offset);
};

template<>
inline void GreaterThanEquals::operation(
    const nodeID_t& left, const nodeID_t& right, uint8_t& result) {
    result = left.label > right.label || (left.label == right.label && left.offset >= right.offset);
};

template<>
inline void LessThan::operation(const nodeID_t& left, const nodeID_t& right, uint8_t& result) {
    result = left.label < right.label || (left.label == right.label && left.offset < right.offset);
};

template<>
inline void LessThanEquals::operation(
    const nodeID_t& left, const nodeID_t& right, uint8_t& result) {
    result = left.label < right.label || (left.label == right.label && left.offset <= right.offset);
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

template<>
inline bool IsNull::operation(const timestamp_t& value) {
    return value == NULL_TIMESTAMP;
};

template<>
inline bool IsNull::operation(const interval_t& value) {
    return value == NULL_INTERVAL;
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

template<>
inline bool IsNotNull::operation(const date_t& value) {
    return value != NULL_DATE;
}

template<>
inline bool IsNotNull::operation(const timestamp_t& value) {
    return value != NULL_TIMESTAMP;
}

template<>
inline bool IsNotNull::operation(const interval_t& value) {
    return value != NULL_INTERVAL;
}

} // namespace operation
} // namespace common
} // namespace graphflow
