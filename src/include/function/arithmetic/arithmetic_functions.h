#pragma once

#include <cmath>
#include <cstdlib>

#include "common/type_utils.h"

namespace kuzu {
namespace function {

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

template<>
inline void Divide::operation(int64_t& left, int64_t& right, int64_t& result) {
    if (right == 0) {
        // According to c++ standard, only INT64 / 0(INT64) is undefined. (eg. DOUBLE / 0(INT64) and
        // INT64 / 0.0(DOUBLE) are well-defined).
        throw common::RuntimeException("Divide by zero.");
    }
    result = left / right;
}

struct Modulo {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result) {
        result = fmod(left, right);
    }
};

template<>
inline void Modulo::operation(int64_t& left, int64_t& right, int64_t& result) {
    if (right == 0) {
        // According to c++ standard, only INT64 % 0(INT64) is undefined. (eg. DOUBLE % 0(INT64) and
        // INT64 % 0.0(DOUBLE) are well-defined).
        throw common::RuntimeException("Modulo by zero.");
    }
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
    static inline void operation(T& input, double_t& result) {
        result = input >= 0 ? ceil(input) : floor(input);
        // Note: c++ doesn't support double % integer, so we have to use the following code to check
        // whether result is odd or even.
        if (std::floor(result / 2) * 2 != result) {
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

struct BitwiseAnd {
    static inline void operation(int64_t& left, int64_t& right, int64_t& result) {
        result = left & right;
    }
};

struct BitwiseOr {
    static inline void operation(int64_t& left, int64_t& right, int64_t& result) {
        result = left | right;
    }
};

struct BitShiftLeft {
    static inline void operation(int64_t& left, int64_t& right, int64_t& result) {
        result = left << right;
    }
};

struct BitShiftRight {
    static inline void operation(int64_t& left, int64_t& right, int64_t& result) {
        result = left >> right;
    }
};

struct Pi {
    static inline void operation(double_t& result) { result = M_PI; }
};

} // namespace function
} // namespace kuzu
