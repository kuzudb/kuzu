#pragma once

#include <regex>

#include "common/types/ku_string.h"
#include "function/string/operations/find_operation.h"
#include "re2.h"

using namespace kuzu::common;
using namespace kuzu::regex;

namespace kuzu {
namespace function {
namespace operation {

struct REMatch {
    static inline void operation(ku_string_t& left, ku_string_t& right, uint8_t& result) {
        // Cypher parses escape characters with 2 backslash eg. for expressing '.' requires '\\.'
        // Since Regular Expression requires only 1 backslash '\.' we need to replace double slash
        // with single
        string pattern = std::regex_replace(right.getAsString(), std::regex(R"(\\\\)"), "\\");
        result = RE2::FullMatch(left.getAsString(), pattern);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
