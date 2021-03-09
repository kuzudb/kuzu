#pragma once

// TODO: add specialized gf_string_t support.
namespace graphflow {
namespace common {
namespace operation {

struct Equals {
    template<class T>
    static inline uint8_t operation(T left, T right) {
        return left == right;
    }
};

struct NotEquals {
    template<class T>
    static inline uint8_t operation(T left, T right) {
        return left != right;
    }
};

struct GreaterThan {
    template<class T>
    static inline uint8_t operation(T left, T right) {
        return left > right;
    }
};

struct GreaterThanEquals {
    template<class T>
    static inline uint8_t operation(T left, T right) {
        return left >= right;
    }
};

struct LessThan {
    template<class T>
    static inline uint8_t operation(T left, T right) {
        return left < right;
    }
};

struct LessThanEquals {
    template<class T>
    static inline uint8_t operation(T left, T right) {
        return left <= right;
    }
};

// Specialized operation(s).
template<>
inline uint8_t GreaterThan::operation(uint8_t left, uint8_t right) {
    return !right && left;
};

template<>
inline uint8_t LessThan::operation(uint8_t left, uint8_t right) {
    return !left && right;
};

} // namespace operation
} // namespace common
} // namespace graphflow
