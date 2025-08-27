#pragma once

#include <cstddef>
#include <cstdint>
namespace kuzu::common {

//! Compute a checksum over a buffer of size size
uint64_t checksum(uint8_t* buffer, size_t size);

} // namespace kuzu::common
