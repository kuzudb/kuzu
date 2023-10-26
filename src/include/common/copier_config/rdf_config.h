#pragma once

#include <cstdint>
#include <memory>

namespace kuzu {

namespace storage {
class PrimaryKeyIndex;
}

namespace common {

enum class RdfReaderMode : uint8_t {
    RESOURCE = 0,
    LITERAL = 1,
    RESOURCE_TRIPLE = 2,
    LITERAL_TRIPLE = 3,
};

struct RdfReaderConfig {
    RdfReaderMode mode;
    storage::PrimaryKeyIndex* index;

    RdfReaderConfig(RdfReaderMode mode, storage::PrimaryKeyIndex* index)
        : mode{mode}, index{index} {}
    RdfReaderConfig(const RdfReaderConfig& other) : mode{other.mode}, index{other.index} {}

    inline std::unique_ptr<RdfReaderConfig> copy() const {
        return std::make_unique<RdfReaderConfig>(*this);
    }
};

} // namespace common
} // namespace kuzu
