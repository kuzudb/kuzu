#pragma once

#include <cmath>
#include <cstdlib>

#include "src/common/include/type_utils.h"
#include "src/common/types/include/value.h"
#include "src/function/string/operations/include/concat_operation.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
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
        result = fmod(left, right);
    }
};

template<>
inline void Modulo::operation(int64_t& left, int64_t& right, int64_t& result) {
    result = left % right;
}

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

struct Abs {
    template<class T>
    static inline void operation(T& input, T& result) {
        result = abs(input);
    }
};

struct Floor {
    template<class T>
    static inline void operation(T& input, T& result) {
        result = floor(input);
    }
};

struct Ceil {
    template<class T>
    static inline void operation(T& input, T& result) {
        result = ceil(input);
    }
};

struct Sin {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = sin(input);
    }
};

struct Cos {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = cos(input);
    }
};

struct Tan {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = tan(input);
    }
};

struct Cot {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        double tanValue;
        Tan::operation(input, tanValue);
        result = 1 / tanValue;
    }
};

struct Asin {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = asin(input);
    }
};

struct Acos {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = acos(input);
    }
};

struct Atan {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = atan(input);
    }
};

struct Even {
    template<class T>
    static inline void operation(T& input, int64_t& result) {
        result = input >= 0 ? ceil(input) : floor(input);
        if (result % 2) {
            result += (input >= 0 ? 1 : -1);
        }
    }
};

struct Factorial {
    static inline void operation(int64_t& input, int64_t& result) {
        result = 1;
        for (int64_t i = 2; i <= input; i++) {
            result *= i;
        }
    }
};

struct Sign {
    template<class T>
    static inline void operation(T& input, int64_t& result) {
        result = (input > 0) - (input < 0);
    }
};

struct Sqrt {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = sqrt(input);
    }
};

struct Cbrt {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = cbrt(input);
    }
};

struct Gamma {
    template<class T>
    static inline void operation(T& input, T& result) {
        result = tgamma(input);
    }
};

struct Lgamma {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = lgamma(input);
    }
};

struct Ln {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = log(input);
    }
};

struct Log {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = log10(input);
    }
};

struct Log2 {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = log2(input);
    }
};

struct Degrees {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = input * 180 / M_PI;
    }
};

struct Radians {
    template<class T>
    static inline void operation(T& input, double_t& result) {
        result = input * M_PI / 180;
    }
};

struct Atan2 {
    template<class A, class B>
    static inline void operation(A& left, B& right, double_t& result) {
        result = atan2(left, right);
    }
};

struct Round {
    template<class A, class B>
    static inline void operation(A& left, B& right, double_t& result) {
        auto multiplier = pow(10, right);
        result = round(left * multiplier) / multiplier;
    }
};

struct BitwiseXor {
    static inline void operation(int64_t& left, int64_t& right, int64_t& result) {
        result = left ^ right;
    }
};

struct Pi {
    static inline void operation(double_t& result) { result = M_PI; }
};

/**********************************************
 **                                          **
 **   Specialized Value(s) implementations   **
 **                                          **
 **********************************************/

struct ArithmeticOnValues {
    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& left, Value& right, Value& result) {
        switch (left.dataType.typeID) {
        case INT64:
            switch (right.dataType.typeID) {
            case INT64: {
                result.dataType.typeID = INT64;
                FUNC::template operation<int64_t, int64_t, int64_t>(
                    left.val.int64Val, right.val.int64Val, result.val.int64Val);
            } break;
            case DOUBLE: {
                result.dataType.typeID = DOUBLE;
                FUNC::template operation<int64_t, double_t, double_t>(
                    left.val.int64Val, right.val.doubleVal, result.val.doubleVal);
            } break;
            default:
                throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `INT64` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
            break;
        case DOUBLE:
            switch (right.dataType.typeID) {
            case INT64: {
                result.dataType.typeID = DOUBLE;
                FUNC::template operation<double_t, int64_t, double_t>(
                    left.val.doubleVal, right.val.int64Val, result.val.doubleVal);
            } break;
            case DOUBLE: {
                result.dataType.typeID = DOUBLE;
                FUNC::template operation<double_t, double_t, double_t>(
                    left.val.doubleVal, right.val.doubleVal, result.val.doubleVal);
            } break;
            default:
                throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `DOUBLE` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
            break;
        default:
            throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `" +
                                   Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& left, Value& right, double_t& result) {
        switch (left.dataType.typeID) {
        case INT64:
            switch (right.dataType.typeID) {
            case INT64: {
                FUNC::template operation<int64_t, int64_t>(
                    left.val.int64Val, right.val.int64Val, result);
            } break;
            case DOUBLE: {
                FUNC::template operation<int64_t, double_t>(
                    left.val.int64Val, right.val.doubleVal, result);
            } break;
            default:
                throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `INT64` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
            break;
        case DOUBLE:
            switch (right.dataType.typeID) {
            case INT64: {
                FUNC::template operation<double_t, int64_t>(
                    left.val.doubleVal, right.val.int64Val, result);
            } break;
            case DOUBLE: {
                FUNC::template operation<double_t, double_t>(
                    left.val.doubleVal, right.val.doubleVal, result);
            } break;
            default:
                throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `DOUBLE` and `" +
                                       Types::dataTypeToString(right.dataType.typeID) + "`");
            }
            break;
        default:
            throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `" +
                                   Types::dataTypeToString(left.dataType.typeID) + "` and `" +
                                   Types::dataTypeToString(right.dataType.typeID) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& input, Value& result) {
        switch (input.dataType.typeID) {
        case INT64: {
            result.dataType.typeID = INT64;
            FUNC::operation(input.val.int64Val, result.val.int64Val);
        } break;
        case DOUBLE: {
            result.dataType.typeID = DOUBLE;
            FUNC::operation(input.val.doubleVal, result.val.doubleVal);
        } break;
        default:
            throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `" +
                                   Types::dataTypeToString(input.dataType.typeID) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& input, double_t& result) {
        switch (input.dataType.typeID) {
        case INT64: {
            FUNC::operation(input.val.int64Val, result);
        } break;
        case DOUBLE: {
            FUNC::operation(input.val.doubleVal, result);
        } break;
        default:
            throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `" +
                                   Types::dataTypeToString(input.dataType.typeID) + "`");
        }
    }

    template<class FUNC, const char* arithmeticOpStr>
    static void operation(Value& input, int64_t& result) {
        switch (input.dataType.typeID) {
        case INT64: {
            FUNC::operation(input.val.int64Val, result);
        } break;
        case DOUBLE: {
            FUNC::operation(input.val.doubleVal, result);
        } break;
        default:
            throw RuntimeException("Cannot " + string(arithmeticOpStr) + " `" +
                                   Types::dataTypeToString(input.dataType.typeID) + "`");
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
static const char atan2Str[] = "atan2";

template<>
inline void Add::operation(Value& left, Value& right, Value& result) {
    if (left.dataType.typeID == DATE && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = DATE;
        Add::operation(left.val.dateVal, right.val.intervalVal, result.val.dateVal);
        return;
    } else if (left.dataType.typeID == INTERVAL && right.dataType.typeID == DATE) {
        result.dataType.typeID = DATE;
        Add::operation(left.val.intervalVal, right.val.dateVal, result.val.dateVal);
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == INT64) {
        result.dataType.typeID = DATE;
        Add::operation(left.val.dateVal, right.val.int64Val, result.val.dateVal);
        return;
    } else if (left.dataType.typeID == INT64 && right.dataType.typeID == DATE) {
        result.dataType.typeID = DATE;
        Add::operation(left.val.int64Val, right.val.dateVal, result.val.dateVal);
        return;
    } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = TIMESTAMP;
        Add::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal);
        return;
    } else if (left.dataType.typeID == INTERVAL && right.dataType.typeID == TIMESTAMP) {
        result.dataType.typeID = TIMESTAMP;
        Add::operation(left.val.intervalVal, right.val.timestampVal, result.val.timestampVal);
        return;
    } else if (left.dataType.typeID == INTERVAL && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = INTERVAL;
        Add::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal);
        return;
    }
    ArithmeticOnValues::operation<Add, addStr>(left, right, result);
}

template<>
inline void Subtract::operation(Value& left, Value& right, Value& result) {
    if (left.dataType.typeID == DATE && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = DATE;
        Subtract::operation(left.val.dateVal, right.val.intervalVal, result.val.dateVal);
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == INT64) {
        result.dataType.typeID = DATE;
        Subtract::operation(left.val.dateVal, right.val.int64Val, result.val.dateVal);
        return;
    } else if (left.dataType.typeID == DATE && right.dataType.typeID == DATE) {
        result.dataType.typeID = INT64;
        Subtract::operation(left.val.dateVal, right.val.dateVal, result.val.int64Val);
        return;
    } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = TIMESTAMP;
        Subtract::operation(left.val.timestampVal, right.val.intervalVal, result.val.timestampVal);
        return;
    } else if (left.dataType.typeID == TIMESTAMP && right.dataType.typeID == TIMESTAMP) {
        result.dataType.typeID = INTERVAL;
        Subtract::operation(left.val.timestampVal, right.val.timestampVal, result.val.intervalVal);
        return;
    } else if (left.dataType.typeID == INTERVAL && right.dataType.typeID == INTERVAL) {
        result.dataType.typeID = INTERVAL;
        Subtract::operation(left.val.intervalVal, right.val.intervalVal, result.val.intervalVal);
        return;
    }
    ArithmeticOnValues::operation<Subtract, subtractStr>(left, right, result);
}

template<>
inline void Multiply::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Multiply, multiplyStr>(left, right, result);
}

template<>
inline void Divide::operation(Value& left, Value& right, Value& result) {
    if (left.dataType.typeID == INTERVAL && right.dataType.typeID == INT64) {
        result.dataType.typeID = INTERVAL;
        Divide::operation(left.val.intervalVal, right.val.int64Val, result.val.intervalVal);
        return;
    }
    ArithmeticOnValues::operation<Divide, divideStr>(left, right, result);
}

template<>
inline void Modulo::operation(Value& left, Value& right, Value& result) {
    ArithmeticOnValues::operation<Modulo, moduloStr>(left, right, result);
}

template<>
inline void Power::operation(Value& left, Value& right, double_t& result) {
    ArithmeticOnValues::operation<Power, powerStr>(left, right, result);
}

template<>
inline void Negate::operation(Value& operand, Value& result) {
    ArithmeticOnValues::operation<Negate, negateStr>(operand, result);
}

template<>
inline void Abs::operation(Value& operand, Value& result) {
    ArithmeticOnValues::operation<Abs, absStr>(operand, result);
}

template<>
inline void Floor::operation(Value& operand, Value& result) {
    ArithmeticOnValues::operation<Floor, floorStr>(operand, result);
}

template<>
inline void Ceil::operation(Value& operand, Value& result) {
    ArithmeticOnValues::operation<Ceil, ceilStr>(operand, result);
}

template<>
inline void Sin::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Sin, sinStr>(operand, result);
}

template<>
inline void Cos::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Cos, cosStr>(operand, result);
}

template<>
inline void Tan::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Tan, tanStr>(operand, result);
}

template<>
inline void Cot::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Cot, cotStr>(operand, result);
}

template<>
inline void Asin::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Asin, asinStr>(operand, result);
}

template<>
inline void Acos::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Acos, acosStr>(operand, result);
}

template<>
inline void Atan::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Atan, atanStr>(operand, result);
}

template<>
inline void Even::operation(Value& operand, int64_t& result) {
    ArithmeticOnValues::operation<Even, evenStr>(operand, result);
}

template<>
inline void Sign::operation(Value& operand, int64_t& result) {
    ArithmeticOnValues::operation<Sign, signStr>(operand, result);
}

template<>
inline void Sqrt::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Sqrt, sqrtStr>(operand, result);
}

template<>
inline void Cbrt::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Cbrt, cbrtStr>(operand, result);
}

template<>
inline void Gamma::operation(Value& operand, Value& result) {
    ArithmeticOnValues::operation<Gamma, gammaStr>(operand, result);
}

template<>
inline void Lgamma::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Lgamma, lgammaStr>(operand, result);
}

template<>
inline void Ln::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Ln, lnStr>(operand, result);
}

template<>
inline void Log::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Log, logStr>(operand, result);
}

template<>
inline void Log2::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Log2, log2Str>(operand, result);
}

template<>
inline void Degrees::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Degrees, degreesStr>(operand, result);
}

template<>
inline void Radians::operation(Value& operand, double_t& result) {
    ArithmeticOnValues::operation<Radians, radiansStr>(operand, result);
}

template<>
inline void Atan2::operation(Value& left, Value& right, double_t& result) {
    ArithmeticOnValues::operation<Atan2, atan2Str>(left, right, result);
}

template<>
inline void Round::operation(Value& left, Value& right, double_t& result) {
    if (right.dataType.typeID != INT64) {
        throw RuntimeException("Round: Invalid right argument datatype: " +
                               Types::dataTypeToString(right.dataType.typeID));
    }
    switch (left.dataType.typeID) {
    case INT64: {
        Round::operation(left.val.int64Val, right.val.int64Val, result);
    } break;
    case DOUBLE: {
        Round::operation(left.val.doubleVal, right.val.int64Val, result);
    } break;
    default: {
        throw RuntimeException("Round: Invalid left argument datatype: " +
                               Types::dataTypeToString(left.dataType.typeID));
    }
    }
}

} // namespace operation
} // namespace function
} // namespace kuzu
