#pragma once

#include <functional>

#include "src/common/types/include/value.h"

namespace graphflow {
namespace common {
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
} // namespace common
} // namespace graphflow
