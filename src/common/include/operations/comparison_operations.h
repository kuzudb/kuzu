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

struct IsNull {
    template<class T>
    static inline void operation(T value, bool isNull, uint8_t& result) {
        result = isNull;
    }
};

struct IsNotNull {
    template<class T>
    static inline void operation(T value, bool isNull, uint8_t& result) {
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

} // namespace operation
} // namespace common
} // namespace graphflow
