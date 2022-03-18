#pragma once

#include <cmath>
#include <cstdlib>

#include "src/common/types/include/value.h"
#include "src/function/string/operations/include/concat_operation.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Add {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left + right;
    }
};

struct Subtract {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left - right;
    }
};

struct Multiply {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left * right;
    }
};

struct Divide {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left / right;
    }
};

struct Modulo {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        assert(false);
    }
};

struct Power {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = pow(left, right);
    }
};

struct Negate {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = -input;
    }
};

struct Abs {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = abs(input);
    }
};

struct Floor {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = floor(input);
    }
};

struct Ceil {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = ceil(input);
    }
};

/********************************************
 **                                        **
 **   Specialized Modulo implementations   **
 **                                        **
 ********************************************/

template<>
inline void Modulo::operation(
    int64_t& left, int64_t& right, int64_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = left % right;
}

template<>
inline void Modulo::operation(
    int64_t& left, double_t& right, double_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = fmod(left, right);
}

template<>
inline void Modulo::operation(
    double_t& left, int64_t& right, double_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = fmod(left, right);
}

template<>
inline void Modulo::operation(
    double_t& left, double_t& right, double_t& result, bool isLeftNull, bool isRightNull) {
    assert(!isLeftNull && !isRightNull);
    result = fmod(left, right);
}

/**********************************************
 **                                          **
 **   Specialized Value(s) implementations   **
 **                                          **
 **********************************************/

struct ArithmeticOnValues {
    template<class FUNC, const char* arithmeticOpStr>
    static void operation(
        Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
        switch (left.dataType) {
        case INT64:
            switch (right.dataType) {
            case INT64: {
                result.dataType = INT64;
                FUNC::operation(left.val.int64Val, right.val.int64Val, result.val.int64Val,
                    isLeftNull, isRightNull);
            } break;
            case DOUBLE: {
                result.dataType = DOUBLE;
                FUNC::operation(left.val.int64Val, right.val.doubleVal, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `INT64` and `" +
                                       TypeUtils::dataTypeToString(right.dataType) + "`");
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT64: {
                result.dataType = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.int64Val, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            case DOUBLE: {
                result.dataType = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.doubleVal, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `DOUBLE` and `" +
                                       TypeUtils::dataTypeToString(right.dataType) + "`");
            }
            break;
        default:
            throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                   TypeUtils::dataTypeToString(left.dataType) + "` and `" +
                                   TypeUtils::dataTypeToString(right.dataType) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& input, bool isNull, Value& result) {
        assert(!isNull);
        switch (input.dataType) {
        case INT64: {
            result.dataType = INT64;
            FUNC::operation(input.val.int64Val, isNull, result.val.int64Val);
        } break;
        case DOUBLE: {
            result.dataType = DOUBLE;
            FUNC::operation(input.val.doubleVal, isNull, result.val.doubleVal);
        } break;
        default:
            throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                   TypeUtils::dataTypeToString(input.dataType) + "`");
        }
    }
};

static const char addStr[] = "add";
static const char subtractStr[] = "subtract";
static const char multiplyStr[] = "multiply";
static const char divideStr[] = "divide";
static const char moduloStr[] = "modulo";
static const char powerStr[] = "power";
static const char negateStr[] = "negate";
static const char absStr[] = "abs";
static const char floorStr[] = "floor";
static const char ceilStr[] = "ceil";

template<>
inline void Add::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType == STRING || right.dataType == STRING) {
        result.dataType = STRING;
        if (left.dataType == STRING && right.dataType == STRING) {
            Concat::operation(
                left.val.strVal, right.val.strVal, result.val.strVal, isLeftNull, isRightNull);
        } else {
            // This is the case when one of the left or right unstructured Value needs to be cast
            // to string. Because we don't have access to overflow buffers, we need to treat them
            // as regular strings.
            auto lVal = left.toString();
            auto rVal = right.toString();
            Concat::operation(lVal, rVal, result.val.strVal, isLeftNull, isRightNull);
        }
        return;
    } else if (left.dataType == DATE && right.dataType == INTERVAL) {
        result.dataType = DATE;
        Add::operation(
            left.val.dateVal, right.val.intervalVal, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == INT64) {
        result.dataType = DATE;
        Add::operation(
            left.val.dateVal, right.val.int64Val, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == TIMESTAMP) {
        assert(right.dataType == INTERVAL);
        result.dataType = TIMESTAMP;
        Add::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType == INTERVAL) {
        assert(right.dataType == INTERVAL);
        result.dataType = INTERVAL;
        Add::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Add, addStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Subtract::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType == DATE && right.dataType == INTERVAL) {
        result.dataType = DATE;
        Subtract::operation(
            left.val.dateVal, right.val.intervalVal, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == INT64) {
        result.dataType = DATE;
        Subtract::operation(
            left.val.dateVal, right.val.int64Val, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == DATE && right.dataType == DATE) {
        result.dataType = INT64;
        Subtract::operation(
            left.val.dateVal, right.val.dateVal, result.val.int64Val, isLeftNull, isRightNull);
        return;
    } else if (left.dataType == TIMESTAMP && right.dataType == INTERVAL) {
        result.dataType = TIMESTAMP;
        Subtract::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType == TIMESTAMP && right.dataType == TIMESTAMP) {
        result.dataType = INTERVAL;
        Subtract::operation(left.val.timestampVal, right.val.timestampVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType == INTERVAL && right.dataType == INTERVAL) {
        result.dataType = INTERVAL;
        Subtract::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Subtract, subtractStr>(
        left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Multiply::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    ArithmeticOnValues::operation<Multiply, multiplyStr>(
        left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Divide::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType == INTERVAL && right.dataType == INT64) {
        result.dataType = INTERVAL;
        Divide::operation(left.val.intervalVal, right.val.int64Val, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Divide, divideStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Modulo::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    ArithmeticOnValues::operation<Modulo, moduloStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Power::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    ArithmeticOnValues::operation<Power, powerStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Negate::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Negate, negateStr>(operand, isNull, result);
}

template<>
inline void Abs::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Abs, absStr>(operand, isNull, result);
}

template<>
inline void Floor::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Floor, floorStr>(operand, isNull, result);
};

template<>
inline void Ceil::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Ceil, ceilStr>(operand, isNull, result);
};

} // namespace operation
} // namespace function
} // namespace graphflow
