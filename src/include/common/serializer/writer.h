#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

class Writer {
public:
    virtual void write(const uint8_t* data, uint64_t size) = 0;
    virtual ~Writer() = default;
};

} // namespace common
} // namespace kuzu
