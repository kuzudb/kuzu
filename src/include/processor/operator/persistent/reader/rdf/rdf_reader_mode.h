#pragma once

#include <cstdint>

namespace kuzu {
namespace processor {

enum class RdfReaderMode : uint8_t {
    RESOURCE = 0,
    LITERAL = 1,
    RESOURCE_TRIPLE = 2,
    LITERAL_TRIPLE = 3,
};

}
} // namespace kuzu
