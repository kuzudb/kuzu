// packSingle/unpackSingle are adapted from
// https://github.com/duckdb/duckdb/blob/main/src/storage/compression/bitpacking_hugeint.cpp

#include "storage/compression/bitpacking_utils.h"

#include "common/utils.h"

namespace kuzu::storage {

template<std::integral CompressedType, IntegerBitpackingType UncompressedType>
void unpackSingleImpl(const CompressedType* __restrict& in, UncompressedType* __restrict out,
    uint16_t delta, uint16_t shiftRight) {
    static_assert(sizeof(UncompressedType) <= 4 * sizeof(CompressedType));

    static constexpr size_t compressedFieldSizeBits = sizeof(CompressedType) * 8;

    if (delta + shiftRight < compressedFieldSizeBits) {
        *out =
            ((static_cast<UncompressedType>(in[0])) >> shiftRight) % (UncompressedType(1) << delta);
    }

    else if (delta + shiftRight >= compressedFieldSizeBits &&
             delta + shiftRight < 2 * compressedFieldSizeBits) {
        *out = static_cast<UncompressedType>(in[0]) >> shiftRight;
        ++in;

        if (delta + shiftRight > compressedFieldSizeBits) {
            const uint16_t NEXT_SHR = shiftRight + delta - compressedFieldSizeBits;
            *out |= static_cast<UncompressedType>((*in) % (1U << NEXT_SHR))
                    << (compressedFieldSizeBits - shiftRight);
        }
    }

    else if (delta + shiftRight >= 2 * compressedFieldSizeBits &&
             delta + shiftRight < 3 * compressedFieldSizeBits) {
        *out = static_cast<UncompressedType>(in[0]) >> shiftRight;
        *out |= static_cast<UncompressedType>(in[1]) << (compressedFieldSizeBits - shiftRight);
        in += 2;

        if (delta + shiftRight > 2 * compressedFieldSizeBits) {
            const uint16_t NEXT_SHR = delta + shiftRight - 2 * compressedFieldSizeBits;
            *out |= static_cast<UncompressedType>((*in) % (1U << NEXT_SHR))
                    << (2 * compressedFieldSizeBits - shiftRight);
        }
    }

    else if (delta + shiftRight >= 3 * compressedFieldSizeBits &&
             delta + shiftRight < 4 * compressedFieldSizeBits) {
        *out = static_cast<UncompressedType>(in[0]) >> shiftRight;
        *out |= static_cast<UncompressedType>(in[1]) << (compressedFieldSizeBits - shiftRight);
        *out |= static_cast<UncompressedType>(in[2]) << (2 * compressedFieldSizeBits - shiftRight);
        in += 3;

        if (delta + shiftRight > 3 * compressedFieldSizeBits) {
            const uint16_t NEXT_SHR = delta + shiftRight - 3 * compressedFieldSizeBits;
            *out |= static_cast<UncompressedType>((*in) % (1U << NEXT_SHR))
                    << (3 * compressedFieldSizeBits - shiftRight);
        }
    }

    else if (delta + shiftRight >= 4 * compressedFieldSizeBits) {
        *out = static_cast<UncompressedType>(in[0]) >> shiftRight;
        *out |= static_cast<UncompressedType>(in[1]) << (compressedFieldSizeBits - shiftRight);
        *out |= static_cast<UncompressedType>(in[2]) << (2 * compressedFieldSizeBits - shiftRight);
        *out |= static_cast<UncompressedType>(in[3]) << (3 * compressedFieldSizeBits - shiftRight);
        in += 4;

        if (delta + shiftRight > 4 * compressedFieldSizeBits) {
            const uint16_t NEXT_SHR = delta + shiftRight - 4 * compressedFieldSizeBits;
            *out |= static_cast<UncompressedType>((*in) % (1U << NEXT_SHR))
                    << (4 * compressedFieldSizeBits - shiftRight);
        }
    }
}

template<std::integral CompressedType, IntegerBitpackingType UncompressedType>
void packSingleImpl(const UncompressedType in, CompressedType* __restrict& out, uint16_t delta,
    uint16_t shiftLeft, UncompressedType mask) {
    static_assert(sizeof(UncompressedType) <= 4 * sizeof(CompressedType));

    static constexpr size_t compressedFieldSizeBits = sizeof(CompressedType) * 8;

    if (delta + shiftLeft < compressedFieldSizeBits) {

        if (shiftLeft == 0) {
            out[0] = static_cast<CompressedType>(in & mask);
        } else {
            out[0] |= static_cast<CompressedType>((in & mask) << shiftLeft);
        }

    } else if (delta + shiftLeft >= compressedFieldSizeBits &&
               delta + shiftLeft < 2 * compressedFieldSizeBits) {

        if (shiftLeft == 0) {
            out[0] = static_cast<CompressedType>(in & mask);
        } else {
            out[0] |= static_cast<CompressedType>((in & mask) << shiftLeft);
        }
        ++out;

        if (delta + shiftLeft > compressedFieldSizeBits) {
            *out =
                static_cast<CompressedType>((in & mask) >> (compressedFieldSizeBits - shiftLeft));
        }
    }

    else if (delta + shiftLeft >= 2 * compressedFieldSizeBits &&
             delta + shiftLeft < 3 * compressedFieldSizeBits) {

        if (shiftLeft == 0) {
            out[0] = static_cast<CompressedType>(in & mask);
        } else {
            out[0] |= static_cast<CompressedType>(in << shiftLeft);
        }

        out[1] = static_cast<CompressedType>((in & mask) >> (compressedFieldSizeBits - shiftLeft));
        out += 2;

        if (delta + shiftLeft > 2 * compressedFieldSizeBits) {
            *out = static_cast<CompressedType>(
                (in & mask) >> (2 * compressedFieldSizeBits - shiftLeft));
        }
    }

    else if (delta + shiftLeft >= 3 * compressedFieldSizeBits &&
             delta + shiftLeft < 4 * compressedFieldSizeBits) {
        if (shiftLeft == 0) {
            out[0] = static_cast<CompressedType>(in & mask);
        } else {
            out[0] |= static_cast<CompressedType>(in << shiftLeft);
        }

        out[1] = static_cast<CompressedType>((in & mask) >> (compressedFieldSizeBits - shiftLeft));
        out[2] =
            static_cast<CompressedType>((in & mask) >> (2 * compressedFieldSizeBits - shiftLeft));
        out += 3;

        if (delta + shiftLeft > 3 * compressedFieldSizeBits) {
            *out = static_cast<CompressedType>(
                (in & mask) >> (3 * compressedFieldSizeBits - shiftLeft));
        }
    }

    else if (delta + shiftLeft >= 4 * compressedFieldSizeBits) {
        if (shiftLeft == 0) {
            out[0] = static_cast<CompressedType>(in & mask);
        } else {
            out[0] |= static_cast<CompressedType>(in << shiftLeft);
        }
        out[1] = static_cast<CompressedType>((in & mask) >> (compressedFieldSizeBits - shiftLeft));
        out[2] =
            static_cast<CompressedType>((in & mask) >> (2 * compressedFieldSizeBits - shiftLeft));
        out[3] =
            static_cast<CompressedType>((in & mask) >> (3 * compressedFieldSizeBits - shiftLeft));
        out += 4;

        if (delta + shiftLeft > 4 * compressedFieldSizeBits) {
            *out = static_cast<CompressedType>(
                (in & mask) >> (4 * compressedFieldSizeBits - shiftLeft));
        }
    }
}

template<IntegerBitpackingType UncompressedType>
void BitpackingUtils<UncompressedType>::unpackSingle(const uint8_t* __restrict& srcCursor,
    UncompressedType* __restrict dst, uint16_t bitWidth, size_t srcOffset) {
    const size_t shiftRight = srcOffset * bitWidth % sizeOfCompressedTypeBits;

    const auto* castedSrcCursor = reinterpret_cast<const CompressedType*>(srcCursor);
    unpackSingleImpl(castedSrcCursor, dst, bitWidth, shiftRight);
    srcCursor = reinterpret_cast<const uint8_t*>(castedSrcCursor);
}

template<IntegerBitpackingType UncompressedType>
void BitpackingUtils<UncompressedType>::packSingle(const UncompressedType src,
    uint8_t* __restrict& dstCursor, uint16_t bitWidth, size_t dstOffset) {
    const size_t shiftLeft = dstOffset * bitWidth % sizeOfCompressedTypeBits;

    auto* castedDstCursor = reinterpret_cast<CompressedType*>(dstCursor);
    packSingleImpl(src, castedDstCursor, bitWidth, shiftLeft,
        common::BitmaskUtils::all1sMaskForLeastSignificantBits<UncompressedType>(bitWidth));
    dstCursor = reinterpret_cast<uint8_t*>(castedDstCursor);
}

template<IntegerBitpackingType UncompressedType>
const uint8_t* BitpackingUtils<UncompressedType>::getInitialSrcCursor(const uint8_t* src,
    uint16_t bitWidth, size_t srcOffset) {
    const auto* compressedTypeAlignedCursor = reinterpret_cast<const CompressedType*>(src) +
                                              srcOffset * bitWidth / sizeOfCompressedTypeBits;
    return reinterpret_cast<const uint8_t*>(compressedTypeAlignedCursor);
}

template struct BitpackingUtils<common::int128_t>;
template struct BitpackingUtils<uint64_t>;
template struct BitpackingUtils<uint32_t>;
template struct BitpackingUtils<uint16_t>;
template struct BitpackingUtils<uint8_t>;
} // namespace kuzu::storage
