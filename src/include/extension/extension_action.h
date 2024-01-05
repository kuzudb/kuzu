#pragma once

#include <cstdint>

namespace kuzu {
namespace extension {

enum class ExtensionAction : uint8_t {
    INSTALL = 0,
    LOAD = 1,
};

} // namespace extension
} // namespace kuzu
