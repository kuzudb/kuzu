// Adapted from
// https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/bitpacking.hpp

#pragma once

#include "storage/compression/compression.h"

namespace kuzu::storage {

template<IntegerBitpackingType UncompressedType>
struct SingleValuePacker {};

template<IntegerBitpackingType UncompressedType>
    requires(sizeof(UncompressedType) >= sizeof(uint32_t))
struct SingleValuePacker<UncompressedType> {
    static void unpackSingle(const uint32_t* __restrict& src, UncompressedType* __restrict dst,
        uint16_t bitWidth, size_t srcOffset);

    static void packSingle(const UncompressedType src, uint32_t* __restrict& dstCursor,
        uint16_t bitWidth, size_t dstOffset);
};

template<IntegerBitpackingType UncompressedType>
    requires(sizeof(UncompressedType) < sizeof(uint32_t))
struct SingleValuePacker<UncompressedType> {
    static void unpackSingle(const uint8_t* __restrict& src, UncompressedType* __restrict dst,
        uint16_t bitWidth, size_t srcOffset);

    static void packSingle(const UncompressedType src, uint8_t* __restrict& dstCursor,
        uint16_t bitWidth, size_t dstOffset);
};

namespace bitpacking_utils {

template<IntegerBitpackingType UncompressedType>
decltype(auto) getInitialSrcCursor(const uint8_t* src, uint16_t bitWidth, size_t srcOffset) {
    using CompressedType =
        std::conditional_t<sizeof(UncompressedType) >= sizeof(uint32_t), uint32_t, uint8_t>;
    static constexpr size_t sizeOfCompressedTypeBits = sizeof(CompressedType) * 8;
    return (CompressedType*)src + srcOffset * bitWidth / sizeOfCompressedTypeBits;
}

template<IntegerBitpackingType UncompressedType>
decltype(auto) castCompressedCursor(uint8_t* src) {
    using CompressedType =
        std::conditional_t<sizeof(UncompressedType) >= sizeof(uint32_t), uint32_t, uint8_t>;
    return reinterpret_cast<CompressedType*>(src);
}
}; // namespace bitpacking_utils

} // namespace kuzu::storage
