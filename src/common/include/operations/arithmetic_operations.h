#pragma once

#include <math.h>

#include "cassert"

#include "src/common/include/value.h"

namespace graphflow {
namespace common {
namespace operation {

struct Add {
    template<class A, class B>
    static inline auto operation(A left, B right) {
        return left + right;
    }
};

struct Subtract {
    template<class A, class B>
    static inline auto operation(A left, B right) {
        return left - right;
    }
};

struct Multiply {
    template<class A, class B>
    static inline auto operation(A left, B right) {
        return left * right;
    }
};

struct Divide {
    template<class A, class B>
    static inline auto operation(A left, B right) {
        return left / right;
    }
};

struct Modulo {
    template<class A, class B>
    static inline auto operation(A left, B right) {
        assert(false);
    }
};

struct Power {
    template<class T>
    static inline double_t operation(T left, double_t right) {
        return pow(left, right);
    }
};

struct Negate {
    template<class T>
    static inline T operation(T input) {
        return -input;
    }
};

/********************************************
 **                                        **
 **   Specialized Modulo implementations   **
 **                                        **
 ********************************************/

template<>
inline auto Modulo::operation(int32_t left, int32_t right) {
    return right % left;
};

template<>
inline auto Modulo::operation(int32_t left, double_t right) {
    return fmod(left, right);
};

template<>
inline auto Modulo::operation(double_t left, int32_t right) {
    return fmod(left, right);
};

template<>
inline auto Modulo::operation(double_t left, double_t right) {
    return fmod(left, right);
};

/*******************************************
 **                                       **
 **   Specialized Concat implementation   **
 **                                       **
 *******************************************/

struct ConcatStrings {
    static inline void operation(gf_string_t& left, gf_string_t& right, gf_string_t& result) {
        auto len = left.len + right.len;
        if (len <= gf_string_t::SHORT_STR_LENGTH /* concat isShort() */) {
            result.deleteOverflowBufferIfAllocated();
            memcpy(result.prefix, left.prefix, left.len);
            memcpy(result.prefix + left.len, right.prefix, right.len);
        } else {
            auto buffer = result.resizeOverflowBufferIfNecessary(len);
            memcpy(buffer, left.getData(), left.len);
            memcpy(buffer + left.len, right.getData(), right.len);
            memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
        }
        result.len = len;
    }
};

/**********************************************
 **                                          **
 **   Specialized Value(s) implementations   **
 **                                          **
 **********************************************/

struct ArithmeticOnValues {
    template<class arithmeticOp, const char* arithmeticOpStr>
    static void operation(Value& left, Value& right, Value& result) {
        switch (left.dataType) {
        case INT32:
            switch (left.dataType) {
            case INT32:
                result.dataType = INT32;
                result.primitive.int32Val =
                    arithmeticOp::operation(left.primitive.int32Val, right.primitive.int32Val);
                break;
            case DOUBLE:
                result.dataType = DOUBLE;
                result.primitive.doubleVal =
                    arithmeticOp::operation(left.primitive.int32Val, right.primitive.doubleVal);
                break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `INT32` and `" +
                                       dataTypeToString(right.dataType) + "`");
                break;
            }
        case DOUBLE:
            switch (left.dataType) {
            case INT32:
                result.dataType = DOUBLE;
                result.primitive.doubleVal =
                    arithmeticOp::operation(left.primitive.doubleVal, right.primitive.int32Val);
                break;
            case DOUBLE:
                result.dataType = DOUBLE;
                result.primitive.doubleVal =
                    arithmeticOp::operation(left.primitive.doubleVal, right.primitive.doubleVal);
                break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `DOUBLE` and `" +
                                       dataTypeToString(right.dataType) + "`");
                break;
            }
            break;
        default:
            throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `" +
                                   dataTypeToString(left.dataType) + "` and `" +
                                   dataTypeToString(right.dataType) + "`");
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

struct AddOrConcatValues {
    static inline void operation(Value& left, Value& right, Value& result) {
        if (left.dataType == STRING || right.dataType == STRING) {
            if (left.dataType != STRING) {
                left.castToString();
            }
            if (right.dataType != STRING) {
                right.castToString();
            }
            result.dataType = STRING;
            ConcatStrings::operation(left.strVal, right.strVal, result.strVal);
            return;
        }
        ArithmeticOnValues::operation<Add, addStr>(left, right, result);
    }
};

struct SubtractValues {
    static inline void operation(Value& left, Value& right, Value& result) {
        ArithmeticOnValues::operation<Subtract, subtractStr>(left, right, result);
    }
};

struct MultiplyValues {
    static inline void operation(Value& left, Value& right, Value& result) {
        ArithmeticOnValues::operation<Multiply, multiplyStr>(left, right, result);
    }
};

struct DivideValues {
    static inline void operation(Value& left, Value& right, Value& result) {
        ArithmeticOnValues::operation<Divide, divideStr>(left, right, result);
    }
};

struct ModuloValues {
    static inline void operation(Value& left, Value& right, Value& result) {
        ArithmeticOnValues::operation<Modulo, moduloStr>(left, right, result);
    }
};

struct PowerValues {
    static inline void operation(Value& left, Value& right, Value& result) {
        ArithmeticOnValues::operation<Power, powerStr>(left, right, result);
    }
};

struct NegateValues {
    static inline void operation(Value& operand, Value& result) {
        switch (operand.dataType) {
        case INT32:
            result.dataType = INT32;
            result.primitive.int32Val = -operand.primitive.int32Val;
            break;
        case DOUBLE:
            result.dataType = DOUBLE;
            result.primitive.doubleVal = -operand.primitive.doubleVal;
            break;
        default:
            throw invalid_argument("Cannot negate `" + DataTypeNames[operand.dataType] + "`.");
        }
    }
};

} // namespace operation
} // namespace common
} // namespace graphflow