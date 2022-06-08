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

/*******************************************************
 **                                                   **
 **   Specialized >, >=, <, <= implementations        **
 **                                                   **
 *******************************************************/

struct CompareValues {
    template<class FUNC, const char* comparisonOpStr>
    static inline void operation(const Value& left, const Value& right, uint8_t& result) {
        if (left.dataType.typeID == right.dataType.typeID) {
            switch (left.dataType.typeID) {
            case BOOL: {
                FUNC::operation(left.val.booleanVal, right.val.booleanVal, result);
            } break;
            case INT64: {
                FUNC::operation(left.val.int64Val, right.val.int64Val, result);
            } break;
            case DOUBLE: {
                FUNC::operation(left.val.doubleVal, right.val.doubleVal, result);
            } break;
            case STRING: {
                FUNC::operation(left.val.strVal, right.val.strVal, result);
            } break;
            case DATE: {
                FUNC::operation(left.val.dateVal, right.val.dateVal, result);
            } break;
            case TIMESTAMP: {
                FUNC::operation(left.val.timestampVal, right.val.timestampVal, result);
            } break;
            case INTERVAL: {
                FUNC::operation(left.val.intervalVal, right.val.intervalVal, result);
            } break;
            default:
                assert(false);
            }
        } else if (left.dataType.typeID == INT64 && right.dataType.typeID == DOUBLE) {
            FUNC::operation(left.val.int64Val, right.val.doubleVal, result);
        } else if (left.dataType.typeID == DOUBLE && right.dataType.typeID == INT64) {
            FUNC::operation(left.val.doubleVal, right.val.int64Val, result);
        } else if (left.dataType.typeID == DATE && right.dataType.typeID == TIMESTAMP) {
            FUNC::operation(left.val.dateVal, right.val.timestampVal, result);
        } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == DATE) {
            FUNC::operation(left.val.timestampVal, right.val.dateVal, result);
        } else {
            throw RuntimeException("Cannot " + string(comparisonOpStr) + " `" +
                                   Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
    }
};

static const char equalsStr[] = "equals";
static const char notEqualsStr[] = "not_equals";
static const char greaterThanStr[] = "greater_than";
static const char greaterThanEqualsStr[] = "greater_than_equals";
static const char lessThanStr[] = "less_than";
static const char lessThanEqualsStr[] = "less_than_equals";

template<>
inline void Equals::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<Equals, equalsStr>(left, right, result);
}

template<>
inline void NotEquals::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<NotEquals, notEqualsStr>(left, right, result);
}

template<>
inline void GreaterThan::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<GreaterThan, greaterThanStr>(left, right, result);
}

template<>
inline void GreaterThanEquals::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<GreaterThanEquals, greaterThanEqualsStr>(left, right, result);
}

template<>
inline void LessThan::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<LessThan, lessThanStr>(left, right, result);
}

template<>
inline void LessThanEquals::operation(const Value& left, const Value& right, uint8_t& result) {
    CompareValues::operation<LessThanEquals, lessThanEqualsStr>(left, right, result);
}

} // namespace operation
} // namespace function
} // namespace graphflow
