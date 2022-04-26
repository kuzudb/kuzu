#pragma once

#include <cassert>
#include <functional>

#include "src/common/include/type_utils.h"
#include "src/common/types/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
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

/*******************************************
 **                                       **
 **   Specialized =, <> implementations   **
 **                                       **
 *******************************************/

struct EqualsOrNotEqualsValues {
    template<bool equals>
    static inline void operation(
        const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
        if (left.dataType.typeID == right.dataType.typeID) {
            switch (left.dataType.typeID) {
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
                if constexpr (equals) {
                    throw RuntimeException(
                        "Cannot equals `" + Types::dataTypeToString(left.dataType.typeID) +
                        "` and `" + Types::dataTypeToString(right.dataType.typeID) + "`");
                } else {
                    throw RuntimeException(
                        "Cannot not equals `" + Types::dataTypeToString(left.dataType.typeID) +
                        "` and `" + Types::dataTypeToString(right.dataType.typeID) + "`");
                }
            }
        } else if (left.dataType.typeID == INT64 && right.dataType.typeID == DOUBLE) {
            if constexpr (equals) {
                Equals::operation(
                    left.val.int64Val, right.val.doubleVal, result, isLeftNull, isRightNul);
            } else {
                NotEquals::operation(
                    left.val.int64Val, right.val.doubleVal, result, isLeftNull, isRightNul);
            }
        } else if (left.dataType.typeID == DOUBLE && right.dataType.typeID == INT64) {
            if constexpr (equals) {
                Equals::operation(
                    left.val.doubleVal, right.val.int64Val, result, isLeftNull, isRightNul);
            } else {
                NotEquals::operation(
                    left.val.doubleVal, right.val.int64Val, result, isLeftNull, isRightNul);
            }
        } else if (left.dataType.typeID == DATE && right.dataType.typeID == TIMESTAMP) {
            if constexpr (equals) {
                Equals::operation(
                    left.val.dateVal, right.val.timestampVal, result, isLeftNull, isRightNul);
            } else {
                NotEquals::operation(
                    left.val.dateVal, right.val.timestampVal, result, isLeftNull, isRightNul);
            }
        } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == DATE) {
            if constexpr (equals) {
                Equals::operation(
                    left.val.timestampVal, right.val.dateVal, result, isLeftNull, isRightNul);
            } else {
                NotEquals::operation(
                    left.val.timestampVal, right.val.dateVal, result, isLeftNull, isRightNul);
            }
        } else {
            if constexpr (equals) {
                throw RuntimeException("Cannot equals `" +
                                       Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            } else {
                throw RuntimeException("Cannot not equals `" +
                                       Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
        }
    }
};

// specialized for Value.
template<>
inline void Equals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    EqualsOrNotEqualsValues::operation<true>(left, right, result, isLeftNull, isRightNul);
}

template<>
inline void NotEquals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    EqualsOrNotEqualsValues::operation<false>(left, right, result, isLeftNull, isRightNul);
}

/*******************************************************
 **                                                   **
 **   Specialized >, >=, <, <= implementations        **
 **                                                   **
 *******************************************************/

struct CompareValues {
    template<class FUNC, const char* comparisonOpStr>
    static inline void operation(
        const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
        if (left.dataType.typeID == right.dataType.typeID) {
            switch (left.dataType.typeID) {
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
        } else if (left.dataType.typeID == INT64 && right.dataType.typeID == DOUBLE) {
            FUNC::operation(left.val.int64Val, right.val.doubleVal, result, isLeftNull, isRightNul);
        } else if (left.dataType.typeID == DOUBLE && right.dataType.typeID == INT64) {
            FUNC::operation(left.val.doubleVal, right.val.int64Val, result, isLeftNull, isRightNul);
        } else if (left.dataType.typeID == DATE && right.dataType.typeID == TIMESTAMP) {
            FUNC::operation(
                left.val.dateVal, right.val.timestampVal, result, isLeftNull, isRightNul);
        } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == DATE) {
            FUNC::operation(
                left.val.timestampVal, right.val.dateVal, result, isLeftNull, isRightNul);
        } else {
            throw RuntimeException("Cannot " + string(comparisonOpStr) + " `" +
                                   Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
    }
};

static const char greaterThanStr[] = "greater_than";
static const char greaterThanEqualsStr[] = "greater_than_equals";
static const char lessThanStr[] = "less_than";
static const char lessThanEqualsStr[] = "less_than_equals";

// specialized for Value.
template<>
inline void GreaterThan::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<GreaterThan, greaterThanStr>(
        left, right, result, isLeftNull, isRightNul);
}

template<>
inline void GreaterThanEquals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<GreaterThanEquals, greaterThanEqualsStr>(
        left, right, result, isLeftNull, isRightNul);
}

template<>
inline void LessThan::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<LessThan, lessThanStr>(left, right, result, isLeftNull, isRightNul);
}

template<>
inline void LessThanEquals::operation(
    const Value& left, const Value& right, uint8_t& result, bool isLeftNull, bool isRightNul) {
    CompareValues::operation<LessThanEquals, lessThanEqualsStr>(
        left, right, result, isLeftNull, isRightNul);
}

} // namespace operation
} // namespace function
} // namespace graphflow
