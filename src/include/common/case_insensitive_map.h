#pragma once

#include <string>
#include <unordered_map>

#include "common/string_utils.h"

namespace kuzu {
namespace common {

struct CaseInsensitiveStringHashFunction {
    uint64_t operator()(const std::string& str) const {
        return common::StringUtils::caseInsensitiveHash(str);
    }
};

struct CaseInsensitiveStringEquality {
    bool operator()(const std::string& left, const std::string& right) const {
        return common::StringUtils::caseInsensitiveEquals(left, right);
    }
};

template<typename T>
using case_insensitive_map_t = std::unordered_map<std::string, T, CaseInsensitiveStringHashFunction,
    CaseInsensitiveStringEquality>;

} // namespace common
} // namespace kuzu
