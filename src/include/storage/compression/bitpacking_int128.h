// Adapted from
// https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/bitpacking.hpp

#pragma once

#include "common/types/int128_t.h"
#include "storage/compression/compression.h"

namespace kuzu::storage {

struct Int128Packer {
    static void pack(const common::int128_t* __restrict in, uint32_t* __restrict out,
        uint8_t width);
    static void unpack(const uint32_t* __restrict in, common::int128_t* __restrict out,
        uint8_t width);
};

namespace bitpacking_utils {
template<std::integral CompressedType, IntegerBitpackingType UncompressedType>
void unpackSingle(const CompressedType* __restrict& in, UncompressedType* __restrict out,
    uint16_t delta, uint16_t shiftRight);

template<std::integral CompressedType, IntegerBitpackingType UncompressedType>
void packSingle(const UncompressedType in, CompressedType* __restrict& out, uint16_t delta,
    uint16_t shiftLeft, UncompressedType mask);
}; // namespace bitpacking_utils

} // namespace kuzu::storage
