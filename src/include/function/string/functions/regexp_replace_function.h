#pragma once

#include "common/types/ku_string.h"
#include "function/string/functions/base_regexp_function.h"
#include "function/string/vector_string_functions.h"
#include "re2.h"

namespace kuzu {
namespace function {

struct RegexpReplace : BaseRegexpOperation {
    static void operation(common::ku_string_t& value, common::ku_string_t& pattern,
        common::ku_string_t& replacement, common::ku_string_t& result,
        common::ValueVector& resultValueVector, void* bindData) {
        auto regexBindData = reinterpret_cast<RegexReplaceBindData*>(bindData);
        std::string resultStr = value.getAsString();
        switch (regexBindData->option) {
        case RegexpReplaceFunction::RegexReplaceOption::GLOBAL: {
            RE2::GlobalReplace(&resultStr, parseCypherPattern(pattern.getAsString()),
                replacement.getAsString());
        } break;
        case RegexpReplaceFunction::RegexReplaceOption::FIRST_OCCUR: {
            RE2::Replace(&resultStr, parseCypherPattern(pattern.getAsString()),
                replacement.getAsString());
        } break;
        default:
            KU_UNREACHABLE;
        }
        copyToKuzuString(resultStr, result, resultValueVector);
    }
};

} // namespace function
} // namespace kuzu
