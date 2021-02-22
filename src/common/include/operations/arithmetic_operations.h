#pragma once

#include <math.h>

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
        throw std::invalid_argument("Unsupported type for Modulo.");
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

// Specialized operation(s).
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

} // namespace operation
} // namespace common
} // namespace graphflow
