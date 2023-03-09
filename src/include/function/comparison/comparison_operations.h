#pragma once

#include <cassert>
#include <functional>

#include "common/type_utils.h"

namespace kuzu {
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

} // namespace operation
} // namespace function
} // namespace kuzu
