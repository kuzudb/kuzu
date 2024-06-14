#pragma once

#include <cstdint>

namespace kuzu {
namespace storage {

// TEMPORARY is used for column chunk data in memory and not going to be persisted to disk.
enum class ResidencyState : uint8_t { IN_MEMORY = 0, ON_DISK = 1, TEMPORARY = 2 };

} // namespace storage
} // namespace kuzu
