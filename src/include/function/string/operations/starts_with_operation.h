#pragma once

#include "common/types/ku_string.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct StartsWith {
    static inline void operation(ku_string_t& left, ku_string_t& right, uint8_t& result) {
        auto lStr = left.getAsString();
        auto rStr = right.getAsString();
        result = lStr.starts_with(rStr);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
