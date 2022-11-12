#pragma once

#include <functional>

namespace kuzu {
namespace function {
namespace operation {

struct IsNull {
    template<class T>
    static inline void operation(T value, bool isNull, uint8_t& result) {
        result = isNull;
    }
};

struct IsNotNull {
    template<class T>
    static inline void operation(T value, bool isNull, uint8_t& result) {
        result = !isNull;
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
