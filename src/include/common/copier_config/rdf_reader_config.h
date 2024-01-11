#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace common {

struct RdfReaderConfig {
    bool inMemory;

    RdfReaderConfig() : inMemory{false} {}
    RdfReaderConfig(const RdfReaderConfig& other) : inMemory{other.inMemory} {}

    static RdfReaderConfig construct(const std::unordered_map<std::string, common::Value>& options);
};

} // namespace common
} // namespace kuzu
