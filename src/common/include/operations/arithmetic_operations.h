#pragma once

#include "cassert"
#include <cmath>

#include "src/common/include/value.h"

namespace graphflow {
namespace common {
namespace operation {

struct Add {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        result = left + right;
    }
};

struct Subtract {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        result = left - right;
    }
};

struct Multiply {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        result = left * right;
    }
};

struct Divide {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        result = left / right;
    }
};

struct Modulo {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        assert(false);
    }
};

struct Power {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        result = pow(left, right);
    }
};

struct Negate {
    template<class T>
    static inline void operation(T& input, T& result) {
        result = -input;
    }
};

/********************************************
 **                                        **
 **   Specialized Modulo implementations   **
 **                                        **
 ********************************************/

template<>
inline void Modulo::operation(int64_t& left, int64_t& right, int64_t& result) {
    result = right % left;
};

template<>
inline void Modulo::operation(int64_t& left, double_t& right, double_t& result) {
    result = fmod(left, right);
};

template<>
inline void Modulo::operation(double_t& left, int64_t& right, double_t& result) {
    result = fmod(left, right);
};

template<>
inline void Modulo::operation(double_t& left, double_t& right, double_t& result) {
    result = fmod(left, right);
};

/*******************************************
 **                                       **
 **   Specialized Concat implementation   **
 **                                       **
 *******************************************/

template<>
inline void Add::operation(gf_string_t& left, gf_string_t& right, gf_string_t& result) {
    auto len = left.len + right.len;
    if (len <= gf_string_t::SHORT_STR_LENGTH /* concat's result is short */) {
        memcpy(result.prefix, left.prefix, left.len);
        memcpy(result.prefix + left.len, right.prefix, right.len);
    } else {
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        memcpy(buffer, left.getData(), left.len);
        memcpy(buffer + left.len, right.getData(), right.len);
        memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
    result.len = len;
};

/**********************************************
 **                                          **
 **   Specialized Value(s) implementations   **
 **                                          **
 **********************************************/

struct ArithmeticOnValues {
    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& left, Value& right, Value& result) {
        switch (left.dataType) {
        case INT64:
            switch (right.dataType) {
            case INT64:
                result.dataType = INT64;
                FUNC::operation(left.val.int64Val, right.val.int64Val, result.val.int64Val);
                break;
            case DOUBLE:
                result.dataType = DOUBLE;
                FUNC::operation(left.val.int64Val, right.val.doubleVal, result.val.doubleVal);
                break;
            default:
                throw invalid_argument("Cannot " + string(arithmeticOpStr) + " `INT64` and `" +
                                       TypeUtils::dataTypeToString(right.dataType) + "`");
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT64:
                result.dataType = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.int64Val, result.val.doubleVal);
                break;
            case DOUBLE:
                result.dataType = DOUBLE;
                FUNC::operation(left.val.doubleVal, right.val.doubleVal, result.val.doubleVal);
                break;
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
};

static const char addStr[] = "add";
static const char subtractStr[] = "subtract";
static const char multiplyStr[] = "multiply";
static const char divideStr[] = "divide";
static const char moduloStr[] = "modulo";
static const char powerStr[] = "power";
static const char negateStr[] = "negate";

template<>
inline void Add::operation(Value& left, Value& right, Value& result) {
    if (left.dataType == STRING) {
        assert(right.dataType == STRING);
        result.dataType = STRING;
        Add::operation(left.val.strVal, right.val.strVal, result.val.strVal);
        return;
    }
    ArithmeticOnValues::operation<Add, addStr>(left, right, result);
};

template<>
inline void Subtract::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Subtract, subtractStr>(left, right, result);
};

template<>
inline void Multiply::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Multiply, multiplyStr>(left, right, result);
};

template<>
inline void Divide::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Divide, divideStr>(left, right, result);
};

template<>
inline void Modulo::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Modulo, moduloStr>(left, right, result);
};

template<>
inline void Power::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Power, powerStr>(left, right, result);
};

template<>
inline void Negate::operation(Value& operand, Value& result) {
    switch (operand.dataType) {
    case INT64:
        result.dataType = INT64;
        result.val.int64Val = -operand.val.int64Val;
        break;
    case DOUBLE:
        result.dataType = DOUBLE;
        result.val.doubleVal = -operand.val.doubleVal;
        break;
    default:
        throw invalid_argument("Cannot negate `" + DataTypeNames[operand.dataType] + "`.");
    }
};

} // namespace operation
} // namespace common
} // namespace graphflow
