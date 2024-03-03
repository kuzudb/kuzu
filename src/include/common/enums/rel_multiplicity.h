#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class RelMultiplicity : uint8_t { MANY, ONE };
struct RelMultiplicityUtils {
    static RelMultiplicity getFwd(const std::string& multiplicityStr);
    static RelMultiplicity getBwd(const std::string& multiplicityStr);
};

} // namespace common
} // namespace kuzu
