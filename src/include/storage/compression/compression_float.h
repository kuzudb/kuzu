#pragma once

#include "storage/compression/compression.h"
#include <concepts>
namespace kuzu {
namespace common {
class ValueVector;
class NullMask;
} // namespace common

namespace storage {
class ColumnChunkData;

struct PageCursor;

// Augmented with Frame of Reference encoding using an offset stored in the compression metadata
template<std::floating_point T>
class FloatCompression final : public CompressionAlg {
public:
    using EncodedType = int64_t;

public:
    FloatCompression() = default;

    static uint64_t getMaxExceptionCountPerPage(size_t pageSize,
        const CompressionMetadata& metadata);

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

    static uint64_t numValues(uint64_t dataSize, const CompressionMetadata& metadata);

    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const final;

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const struct CompressionMetadata& metadata) const final;

    static bool canUpdateInPlace(std::span<const T> value, const CompressionMetadata& metadata,
        const std::optional<common::NullMask>& nullMask = std::nullopt,
        uint64_t nullMaskOffset = 0);

    CompressionType getCompressionType() const override { return CompressionType::FLOAT; }

private:
    IntegerBitpacking<int64_t> encodedFloatBitpacker;
};

} // namespace storage
} // namespace kuzu
