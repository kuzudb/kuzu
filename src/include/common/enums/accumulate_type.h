#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class AccumulateType : uint8_t {
    REGULAR = 0,
    OPTIONAL_ = 1,
};

struct AccumulateTypeUtil {
    static std::string toString(AccumulateType type);
};

} // namespace common
} // namespace kuzu
