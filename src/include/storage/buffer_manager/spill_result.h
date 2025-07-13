#pragma once

#include <cstdint>

namespace kuzu {
namespace storage {

struct SpillResult {
    uint64_t memoryFreed = 0;
    uint64_t memoryNowEvictable = 0;
};

} // namespace storage
} // namespace kuzu
