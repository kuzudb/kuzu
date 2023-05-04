#pragma once

#include "common/types/ku_string.h"
#include "re2.h"

namespace kuzu {
namespace function {
namespace operation {

struct RegexpFullMatch : BaseRegexpOperation {
    static inline void operation(
        common::ku_string_t& left, common::ku_string_t& right, uint8_t& result) {
        result = RE2::FullMatch(left.getAsString(), parseCypherPatten(right.getAsString()));
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
