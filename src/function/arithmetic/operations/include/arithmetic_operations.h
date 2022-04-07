#pragma once

#include <cmath>
#include <cstdlib>

#include "src/common/include/type_utils.h"
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

struct Sin {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = sin(input);
    }
};

struct Cos {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = cos(input);
    }
};

struct Tan {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = tan(input);
    }
};

struct Cot {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        double tanValue;
        Tan::operation(input, isNull, tanValue);
        result = 1 / tanValue;
    }
};

struct Asin {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = asin(input);
    }
};

struct Acos {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = acos(input);
    }
};

struct Atan {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = atan(input);
    }
};

struct Even {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = input >= 0 ? ceil(input) : floor(input);
        if (result % 2) {
            result += (input >= 0 ? 1 : -1);
        }
    }
};

struct Factorial {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = tgamma(input + 1);
    }
};

struct Sign {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = (input > 0) - (input < 0);
    }
};

struct Sqrt {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = sqrt(input);
    }
};

struct Cbrt {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = cbrt(input);
    }
};

struct Gamma {
    template<class T>
    static inline void operation(T& input, bool isNull, T& result) {
        assert(!isNull);
        result = tgamma(input);
    }
};

struct Lgamma {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = lgamma(input);
    }
};

struct Ln {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = log(input);
    }
};

struct Log {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = log10(input);
    }
};

struct Log2 {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = log2(input);
    }
};

struct Degrees {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = input * 180 / M_PI;
    }
};

struct Radians {
    template<class T, class R>
    static inline void operation(T& input, bool isNull, R& result) {
        assert(!isNull);
        result = input * M_PI / 180;
    }
};

struct Atan2 {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = atan2(left, right);
    }
};

struct Round {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        auto multiplier = pow(10, right);
        result = round(left * multiplier) / multiplier;
    }
};

struct BitWiseXor {
    template<class T, class R>
    static inline void operation(T& left, T& right, R& result, bool isLeftNull, bool isRightNull) {
        assert(!isLeftNull && !isRightNull);
        result = left ^ right;
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

template<class FUNC>
inline constexpr bool isFuncInputNumericOutputDouble() {
    return is_same<FUNC, Sin>::value || is_same<FUNC, Cos>::value || is_same<FUNC, Tan>::value ||
           is_same<FUNC, Cot>::value || is_same<FUNC, Asin>::value || is_same<FUNC, Acos>::value ||
           is_same<FUNC, Atan>::value || is_same<FUNC, Sqrt>::value || is_same<FUNC, Cbrt>::value ||
           is_same<FUNC, Lgamma>::value || is_same<FUNC, Ln>::value || is_same<FUNC, Log>::value ||
           is_same<FUNC, Log2>::value || is_same<FUNC, Degrees>::value ||
           is_same<FUNC, Radians>::value;
}

template<class FUNC>
inline constexpr bool isFuncInputNumericOutputInt64() {
    return is_same<FUNC, Even>::value || is_same<FUNC, Sign>::value;
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
        switch (left.dataType.typeID) {
        case INT64:
            switch (right.dataType.typeID) {
            case INT64: {
                result.dataType.typeID = INT64;
                FUNC::operation(left.val.int64Val, right.val.int64Val, result.val.int64Val,
                    isLeftNull, isRightNull);
            } break;
            case DOUBLE: {
                result.dataType.typeID = DOUBLE;
                FUNC::operation(left.val.int64Val, right.val.doubleVal, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `INT64` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
            break;
        case DOUBLE:
            switch (right.dataType.typeID) {
            case INT64: {
                result.dataType.typeID = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.int64Val, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            case DOUBLE: {
                result.dataType.typeID = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.doubleVal, result.val.doubleVal,
                    isLeftNull, isRightNull);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `DOUBLE` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
            break;
        default:
            throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                   Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& input, bool isNull, Value& result) {
        assert(!isNull);
        if constexpr (isFuncInputNumericOutputDouble<FUNC>()) {
            result.dataType.typeID = DOUBLE;
            switch (input.dataType.typeID) {
            case INT64: {
                FUNC::operation(input.val.int64Val, isNull, result.val.doubleVal);
            } break;
            case DOUBLE: {
                FUNC::operation(input.val.doubleVal, isNull, result.val.doubleVal);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                       Types::dataTypeToString(input.dataType.typeID) + "`");
            }
        } else if constexpr (isFuncInputNumericOutputInt64<FUNC>()) {
            result.dataType.typeID = INT64;
            switch (input.dataType.typeID) {
            case INT64: {
                FUNC::operation(input.val.int64Val, isNull, result.val.int64Val);
            } break;
            case DOUBLE: {
                FUNC::operation(input.val.doubleVal, isNull, result.val.int64Val);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                       Types::dataTypeToString(input.dataType.typeID) + "`");
            }
        } else {
            switch (input.dataType.typeID) {
            case INT64: {
                result.dataType.typeID = INT64;
                FUNC::operation(input.val.int64Val, isNull, result.val.int64Val);
            } break;
            case DOUBLE: {
                result.dataType.typeID = DOUBLE;
                FUNC::operation(input.val.doubleVal, isNull, result.val.doubleVal);
            } break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                       Types::dataTypeToString(input.dataType.typeID) + "`");
            }
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
static const char sinStr[] = "sin";
static const char cosStr[] = "cos";
static const char tanStr[] = "tan";
static const char cotStr[] = "cot";
static const char asinStr[] = "asin";
static const char acosStr[] = "acos";
static const char atanStr[] = "atan";
static const char evenStr[] = "even";
static const char factorialStr[] = "factorial";
static const char signStr[] = "sign";
static const char sqrtStr[] = "sqrt";
static const char cbrtStr[] = "cbrt";
static const char gammaStr[] = "gamma";
static const char lgammaStr[] = "lgamma";
static const char lnStr[] = "ln";
static const char logStr[] = "log";
static const char log2Str[] = "log2";
static const char degreesStr[] = "degrees";
static const char radiansStr[] = "radians";
static const char bitCountStr[] = "bitCount";
static const char atan2Str[] = "atan2";
static const char roundStr[] = "round";
static const char xorStr[] = "xor";

template<>
inline void Add::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType.typeID == STRING || right.dataType.typeID == STRING) {
        result.dataType.typeID = STRING;
        if (left.dataType.typeID == STRING && right.dataType.typeID == STRING) {
            Concat::operation(
                left.val.strVal, right.val.strVal, result.val.strVal, isLeftNull, isRightNull);
        } else {
            // This is the case when one of the left or right unstructured Value needs to be cast
            // to string. Because we don't have access to overflow buffers, we need to treat them
            // as regular strings.
            auto lVal = TypeUtils::toString(left);
            auto rVal = TypeUtils::toString(right);
            Concat::operation(lVal, rVal, result.val.strVal, isLeftNull, isRightNull);
        }
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = DATE;
        Add::operation(
            left.val.dateVal, right.val.intervalVal, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == INT64) {
        result.dataType.typeID = DATE;
        Add::operation(
            left.val.dateVal, right.val.int64Val, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == TIMESTAMP) {
        assert(right.dataType.typeID == INTERVAL);
        result.dataType.typeID = TIMESTAMP;
        Add::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == INTERVAL) {
        assert(right.dataType.typeID == INTERVAL);
        result.dataType.typeID = INTERVAL;
        Add::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    }
    ArithmeticOnValues::operation<Add, addStr>(left, right, result, isLeftNull, isRightNull);
}

template<>
inline void Subtract::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    if (left.dataType.typeID == DATE && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = DATE;
        Subtract::operation(
            left.val.dateVal, right.val.intervalVal, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == INT64) {
        result.dataType.typeID = DATE;
        Subtract::operation(
            left.val.dateVal, right.val.int64Val, result.val.dateVal, isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == DATE) {
        result.dataType.typeID = INT64;
        Subtract::operation(
            left.val.dateVal, right.val.dateVal, result.val.int64Val, isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = TIMESTAMP;
        Subtract::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == TIMESTAMP) {
        result.dataType.typeID = INTERVAL;
        Subtract::operation(left.val.timestampVal, right.val.timestampVal, result.val.intervalVal,
            isLeftNull, isRightNull);
        return;
    } else if (left.dataType.typeID == INTERVAL && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = INTERVAL;
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
    if (left.dataType.typeID == INTERVAL && right.dataType.typeID == INT64) {
        result.dataType.typeID = INTERVAL;
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

template<>
inline void Sin::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Sin, sinStr>(operand, isNull, result);
};

template<>
inline void Cos::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Cos, cosStr>(operand, isNull, result);
};

template<>
inline void Tan::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Tan, tanStr>(operand, isNull, result);
};

template<>
inline void Cot::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Cot, cotStr>(operand, isNull, result);
};

template<>
inline void Asin::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Asin, asinStr>(operand, isNull, result);
};

template<>
inline void Acos::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Acos, acosStr>(operand, isNull, result);
};

template<>
inline void Atan::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Atan, atanStr>(operand, isNull, result);
};

template<>
inline void Even::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Even, evenStr>(operand, isNull, result);
};

template<>
inline void Factorial::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Factorial, factorialStr>(operand, isNull, result);
};

template<>
inline void Sign::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Sign, signStr>(operand, isNull, result);
};

template<>
inline void Sqrt::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Sqrt, sqrtStr>(operand, isNull, result);
};

template<>
inline void Cbrt::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Cbrt, cbrtStr>(operand, isNull, result);
};

template<>
inline void Gamma::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Gamma, gammaStr>(operand, isNull, result);
};

template<>
inline void Lgamma::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Lgamma, lgammaStr>(operand, isNull, result);
};

template<>
inline void Ln::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Ln, lnStr>(operand, isNull, result);
};

template<>
inline void Log::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Log, logStr>(operand, isNull, result);
};

template<>
inline void Log2::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Log2, log2Str>(operand, isNull, result);
};

template<>
inline void Degrees::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Degrees, degreesStr>(operand, isNull, result);
};

template<>
inline void Radians::operation(Value& operand, bool isNull, Value& result) {
    ArithmeticOnValues::operation<Radians, radiansStr>(operand, isNull, result);
};

template<>
inline void Atan2::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    result.dataType.typeID = DOUBLE;
    switch (left.dataType.typeID) {
    case INT64:
        switch (right.dataType.typeID) {
        case INT64: {
            Atan2::operation(left.val.int64Val, right.val.int64Val, result.val.doubleVal,
                isLeftNull, isRightNull);
        } break;
        case DOUBLE: {
            Atan2::operation(left.val.int64Val, right.val.doubleVal, result.val.doubleVal,
                isLeftNull, isRightNull);
        } break;
        default:
            throw invalid_argument("Cannot atan2 `INT64` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
        break;
    case DOUBLE:
        switch (right.dataType.typeID) {
        case INT64: {
            Atan2::operation(left.val.doubleVal, right.val.int64Val, result.val.doubleVal,
                isLeftNull, isRightNull);
        } break;
        case DOUBLE: {
            Atan2::operation(left.val.doubleVal, right.val.doubleVal, result.val.doubleVal,
                isLeftNull, isRightNull);
        } break;
        default:
            throw invalid_argument("Cannot atan2 `DOUBLE` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
        break;
    default:
        throw invalid_argument("Cannot atan2 `INT64` and `" +
                               Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                               Types::dataTypeToString(right.dataType.typeID) + "`");
    }
}

template<>
inline void Round::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    result.dataType.typeID = DOUBLE;
    Round::operation(
        left.val.doubleVal, right.val.int64Val, result.val.doubleVal, isLeftNull, isRightNull);
}

template<>
inline void BitWiseXor::operation(
    Value& left, Value& right, Value& result, bool isLeftNull, bool isRightNull) {
    result.dataType.typeID = INT64;
    BitWiseXor::operation(
        left.val.int64Val, right.val.int64Val, result.val.int64Val, isLeftNull, isRightNull);
}

} // namespace operation
} // namespace function
} // namespace graphflow
