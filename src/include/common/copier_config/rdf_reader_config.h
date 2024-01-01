#pragma once

#include <cstdint>

#include "common/types/value/value.h"

namespace kuzu {
namespace common {

enum class RdfReaderMode : uint8_t {
    RESOURCE = 0,
    LITERAL = 1,
    RESOURCE_TRIPLE = 2,
    LITERAL_TRIPLE = 3,
    ALL = 4,
};

struct RdfReaderConfig {
    bool inMemory;

    RdfReaderConfig() : inMemory{false} {}
    RdfReaderConfig(const RdfReaderConfig& other) : inMemory{other.inMemory} {}

    static RdfReaderConfig construct(const std::unordered_map<std::string, common::Value>& options);
};

} // namespace common
} // namespace kuzu
