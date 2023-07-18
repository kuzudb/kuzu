#pragma once

#include <cstdint>

namespace kuzu {
namespace binder {

enum class UpdateTableType : uint8_t {
    NODE = 0,
    REL = 1,
};

}
} // namespace kuzu
