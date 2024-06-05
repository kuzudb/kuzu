#pragma once

#include <cstdint>

namespace kuzu {
namespace storage {

enum class ResidencyState : uint8_t { IN_MEMORY = 0, ON_DISK = 1 };

} // namespace storage
} // namespace kuzu
