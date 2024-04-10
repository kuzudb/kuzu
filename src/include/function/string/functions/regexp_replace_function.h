#pragma once

#include "common/types/ku_string.h"
#include "function/string/functions/base_regexp_function.h"
#include "re2.h"

namespace kuzu {
namespace function {

struct RegexpReplace : BaseRegexpOperation {
    static inline void operation(common::ku_string_t& value, common::ku_string_t& pattern,
        common::ku_string_t& replacement, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        std::string resultStr = value.getAsString();
        RE2::Replace(&resultStr, parseCypherPatten(pattern.getAsString()),
            replacement.getAsString());
        copyToKuzuString(resultStr, result, resultValueVector);
    }
};

} // namespace function
} // namespace kuzu
