#pragma once

#include "common/types/ku_string.h"
#include "re2.h"

namespace kuzu {
namespace function {
namespace operation {

struct RegexpReplace : BaseRegexpOperation {
    static inline void operation(common::ku_string_t& value, common::ku_string_t& pattern,
        common::ku_string_t& replacement, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        std::string resultStr = value.getAsString();
        RE2::Replace(
            &resultStr, parseCypherPatten(pattern.getAsString()), replacement.getAsString());
        copyToKuzuString(resultStr, result, resultValueVector);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
