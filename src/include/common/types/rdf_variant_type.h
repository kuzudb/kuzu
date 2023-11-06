#pragma once

#include "types.h"

namespace kuzu {
namespace common {

struct RdfVariantType {
    static std::unique_ptr<LogicalType> getType();
};

} // namespace common
} // namespace kuzu
