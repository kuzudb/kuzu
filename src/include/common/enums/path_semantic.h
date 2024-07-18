#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class PathSemantic : uint8_t {
    WALK = 0,
    TRAIL = 1,
    ACYCLIC = 2,
};

struct PathSemanticUtils {
    static PathSemantic fromString(const std::string& str);
    static std::string toString(PathSemantic semantic);
};

} // namespace common
} // namespace kuzu
