// packSingle/unpackSingle are adapted from
// https://github.com/duckdb/duckdb/blob/main/src/storage/compression/bitpacking_hugeint.cpp

#include "storage/compression/bitpacking_utils.h"

#include "common/utils.h"

namespace kuzu::storage {
namespace {
template<size_t compressed_field, std::integral CompressedType,
    IntegerBitpackingType UncompressedType>
void unpackSingleUnrolled(const CompressedType* __restrict in, UncompressedType* __restrict out,
    uint16_t delta, uint16_t shiftRight) {
    static constexpr size_t compressedFieldSizeBits = sizeof(CompressedType) * 8;

    if constexpr (compressed_field == 0) {
        *out = static_cast<UncompressedType>(in[0]) >> shiftRight;
    } else {
        unpackSingleUnrolled<compressed_field - 1>(in, out, delta, shiftRight);
        KU_ASSERT(
            sizeof(UncompressedType) * 8 > compressed_field * compressedFieldSizeBits - shiftRight);
        *out |= static_cast<UncompressedType>(in[compressed_field])
                << (compressed_field * compressedFieldSizeBits - shiftRight);
    }
}

template<std::integral CompressedType, IntegerBitpackingType UncompressedType>
const CompressedType* unpackSingleImpl(const CompressedType* __restrict in,
    UncompressedType* __restrict out, uint16_t delta, uint16_t shiftRight) {
    static_assert(sizeof(UncompressedType) <= 4 * sizeof(CompressedType));

    static constexpr size_t compressedFieldSizeBits = sizeof(CompressedType) * 8;

    if (delta + shiftRight <= compressedFieldSizeBits) {
        unpackSingleUnrolled<0>(in, out, delta, shiftRight);
    } else if (delta + shiftRight > compressedFieldSizeBits &&
               delta + shiftRight <= 2 * compressedFieldSizeBits) {
        unpackSingleUnrolled<1>(in, out, delta, shiftRight);
    } else if (delta + shiftRight > 2 * compressedFieldSizeBits &&
               delta + shiftRight <= 3 * compressedFieldSizeBits) {
        unpackSingleUnrolled<2>(in, out, delta, shiftRight);
    } else if (delta + shiftRight > 3 * compressedFieldSizeBits &&
               delta + shiftRight <= 4 * compressedFieldSizeBits) {
        unpackSingleUnrolled<3>(in, out, delta, shiftRight);
    } else if (delta + shiftRight > 4 * compressedFieldSizeBits) {
        unpackSingleUnrolled<4>(in, out, delta, shiftRight);
    }

    // we previously copy over the entire most significant field
    // zero out the bits that are not actually part of the compressed value
    *out &= common::BitmaskUtils::all1sMaskForLeastSignificantBits<UncompressedType>(delta);
    return in + (delta + shiftRight) / compressedFieldSizeBits;
}

template<std::integral T>
void setValueForBitsMatchingMask(T& out, T valueToSet, T mask) {
    const T bitsToSet = valueToSet & mask;
    const T bitsToClear = ~mask | valueToSet;
    out = (out | bitsToSet) & bitsToClear;
}

template<size_t compressed_field, std::integral CompressedType,
    IntegerBitpackingType UncompressedType>
void packSingleUnrolled(const UncompressedType in, CompressedType* __restrict out, uint16_t delta,
    uint16_t shiftLeft, UncompressedType mask) {
    static constexpr size_t compressedFieldSizeBits = sizeof(CompressedType) * 8;

    if constexpr (compressed_field == 0) {
        if (shiftLeft == 0) {
            // out[0] = static_cast<CompressedType>(in & mask);
            setValueForBitsMatchingMask(out[0], static_cast<CompressedType>(in & mask),
                static_cast<CompressedType>(mask));
        } else {
            // out[0] |= static_cast<CompressedType>((in & mask) << shiftLeft);
            setValueForBitsMatchingMask(out[0],
                static_cast<CompressedType>((in & mask) << shiftLeft),
                static_cast<CompressedType>(mask << shiftLeft));
        }
    } else {
        packSingleUnrolled<compressed_field - 1>(in, out, delta, shiftLeft, mask);
        KU_ASSERT(
            sizeof(UncompressedType) * 8 > compressed_field * compressedFieldSizeBits - shiftLeft);
        // out[compressed_field] = static_cast<CompressedType>(
        //     (in & mask) >> (compressed_field * compressedFieldSizeBits - shiftLeft));
        setValueForBitsMatchingMask(out[compressed_field],
            static_cast<CompressedType>(
                (in & mask) >> (compressed_field * compressedFieldSizeBits - shiftLeft)),
            static_cast<CompressedType>(
                mask >> (compressed_field * compressedFieldSizeBits - shiftLeft)));
    }
}

template<std::integral CompressedType, IntegerBitpackingType UncompressedType>
CompressedType* packSingleImpl(const UncompressedType in, CompressedType* __restrict out,
    uint16_t delta, uint16_t shiftLeft, UncompressedType mask) {
    static_assert(sizeof(UncompressedType) <= 4 * sizeof(CompressedType));

    static constexpr size_t compressedFieldSizeBits = sizeof(CompressedType) * 8;

    if (delta + shiftLeft <= compressedFieldSizeBits) {
        packSingleUnrolled<0>(in, out, delta, shiftLeft, mask);
    } else if (delta + shiftLeft > compressedFieldSizeBits &&
               delta + shiftLeft <= 2 * compressedFieldSizeBits) {
        packSingleUnrolled<1>(in, out, delta, shiftLeft, mask);
    } else if (delta + shiftLeft > 2 * compressedFieldSizeBits &&
               delta + shiftLeft <= 3 * compressedFieldSizeBits) {
        packSingleUnrolled<2>(in, out, delta, shiftLeft, mask);
    } else if (delta + shiftLeft > 3 * compressedFieldSizeBits &&
               delta + shiftLeft <= 4 * compressedFieldSizeBits) {
        packSingleUnrolled<3>(in, out, delta, shiftLeft, mask);
    } else if (delta + shiftLeft > 4 * compressedFieldSizeBits) {
        packSingleUnrolled<4>(in, out, delta, shiftLeft, mask);
    }

    return out + (delta + shiftLeft) / compressedFieldSizeBits;
}
} // namespace

template<IntegerBitpackingType UncompressedType>
const uint8_t* BitpackingUtils<UncompressedType>::unpackSingle(const uint8_t* __restrict srcCursor,
    UncompressedType* __restrict dst, uint16_t bitWidth, size_t srcOffset) {
    const size_t shiftRight = srcOffset * bitWidth % sizeOfCompressedTypeBits;

    const auto* castedSrcCursor = reinterpret_cast<const CompressedType*>(srcCursor);
    return reinterpret_cast<const uint8_t*>(
        unpackSingleImpl(castedSrcCursor, dst, bitWidth, shiftRight));
}

template<IntegerBitpackingType UncompressedType>
uint8_t* BitpackingUtils<UncompressedType>::packSingle(const UncompressedType src,
    uint8_t* __restrict dstCursor, uint16_t bitWidth, size_t dstOffset) {
    const size_t shiftLeft = dstOffset * bitWidth % sizeOfCompressedTypeBits;

    auto* castedDstCursor = reinterpret_cast<CompressedType*>(dstCursor);
    return reinterpret_cast<uint8_t*>(packSingleImpl(src, castedDstCursor, bitWidth, shiftLeft,
        common::BitmaskUtils::all1sMaskForLeastSignificantBits<UncompressedType>(bitWidth)));
}

template<IntegerBitpackingType UncompressedType>
const uint8_t* BitpackingUtils<UncompressedType>::getInitialSrcCursor(const uint8_t* src,
    uint16_t bitWidth, size_t srcOffset) {
    const auto* compressedTypeAlignedCursor = reinterpret_cast<const CompressedType*>(src) +
                                              srcOffset * bitWidth / sizeOfCompressedTypeBits;
    return reinterpret_cast<const uint8_t*>(compressedTypeAlignedCursor);
}

template<IntegerBitpackingType UncompressedType>
uint8_t* BitpackingUtils<UncompressedType>::getInitialSrcCursor(uint8_t* src, uint16_t bitWidth,
    size_t srcOffset) {
    auto* compressedTypeAlignedCursor =
        reinterpret_cast<CompressedType*>(src) + srcOffset * bitWidth / sizeOfCompressedTypeBits;
    return reinterpret_cast<uint8_t*>(compressedTypeAlignedCursor);
}

template struct BitpackingUtils<common::int128_t>;
template struct BitpackingUtils<uint64_t>;
template struct BitpackingUtils<uint32_t>;
template struct BitpackingUtils<uint16_t>;
template struct BitpackingUtils<uint8_t>;
} // namespace kuzu::storage
