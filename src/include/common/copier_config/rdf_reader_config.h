#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace common {

struct RdfReaderConfig {
    bool inMemory;
    bool strict;

    RdfReaderConfig() : inMemory{false}, strict{false} {}
    RdfReaderConfig(const RdfReaderConfig& other)
        : inMemory{other.inMemory}, strict{other.strict} {}

    static RdfReaderConfig construct(const std::unordered_map<std::string, common::Value>& options);
};

} // namespace common
} // namespace kuzu
