#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "common/api.h"

namespace kuzu {
namespace common {

struct CaseInsensitiveStringHashFunction {
    KUZU_API uint64_t operator()(const std::string& str) const;
};

struct CaseInsensitiveStringEquality {
    KUZU_API bool operator()(const std::string& lhs, const std::string& rhs) const;
};

template<typename T>
using case_insensitive_map_t = std::unordered_map<std::string, T, CaseInsensitiveStringHashFunction,
    CaseInsensitiveStringEquality>;

} // namespace common
} // namespace kuzu
