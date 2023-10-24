#pragma once

#include <array>
#include <cstdint>
#include <limits>

#include "common/types/types.h"

namespace kuzu {
namespace common {
class ValueVector;
}

namespace storage {

struct PageElementCursor;

// Returns the size of the data type in bytes
uint32_t getDataTypeSizeInChunk(const common::LogicalType& dataType);

// Compression type is written to the data header both so we can usually catch issues when we
// decompress uncompressed data by mistake, and to allow for runtime-configurable compression in
// future.
enum class CompressionType : uint8_t {
    UNCOMPRESSED = 0,
    INTEGER_BITPACKING = 1,
    BOOLEAN_BITPACKING = 2,
};

struct CompressionMetadata {
    static constexpr uint8_t DATA_SIZE = 9;
    CompressionType compression;
    // Extra data to be used to store codec-specific information
    std::array<uint8_t, DATA_SIZE> data;
    explicit CompressionMetadata(CompressionType compression = CompressionType::UNCOMPRESSED,
        std::array<uint8_t, DATA_SIZE> data = {})
        : compression{compression}, data{data} {}

    // Returns the number of values which will be stored in the given data size
    // This must be consistent with the compression implementation for the given size
    uint64_t numValues(uint64_t dataSize, const common::LogicalType& dataType) const;
    // Returns true if and only if the provided value within the vector can be updated
    // in this chunk in-place.
    bool canUpdateInPlace(
        const common::ValueVector& vector, uint32_t pos, common::PhysicalTypeID physicalType) const;
    bool canAlwaysUpdateInPlace() const;
};

class CompressionAlg {
public:
    virtual ~CompressionAlg() = default;

    // Takes a single uncompressed value from the srcBuffer and compresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    virtual void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc,
        uint8_t* dstBuffer, common::offset_t posInDst,
        const CompressionMetadata& metadata) const = 0;

    // Reads a value from the buffer at the given position and stores it at the given memory address
    // dst should point to an uncompressed value
    virtual inline void getValue(const uint8_t* buffer, common::offset_t posInBuffer, uint8_t* dst,
        common::offset_t posInDst, const CompressionMetadata& metadata) const = 0;

    // Returns compression metadata, including any relevant parameters specific to this dataset
    // which will need to be passed to compressNextPage. Since this may need to scan the entire
    // buffer, which is slow, it should only be called once for each set of data to compress.
    virtual CompressionMetadata getCompressionMetadata(
        const uint8_t* srcBuffer, uint64_t numValues) const = 0;

    // Takes uncompressed data from the srcBuffer and compresses it into the dstBuffer
    //
    // stores only as much data in dstBuffer as will fit, and advances the srcBuffer pointer
    // to the beginning of the next value to store.
    // (This means that we can't start the next page on an unaligned value.
    // Maybe instead we could use value offsets, but the compression algorithms
    // usually work on aligned chunks anyway)
    //
    // dstBufferSize is the size in bytes
    // numValuesRemaining is the number of values remaining in the srcBuffer to be compressed.
    //      compressNextPage must store the least of either the number of values per page
    //      (as calculated by CompressionMetadata::numValues), or the remaining number of values.
    //
    // returns the size in bytes of the compressed data within the page (rounded up to the nearest
    // byte)
    virtual uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const = 0;

    // Takes compressed data from the srcBuffer and decompresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    // srcBuffer points to the beginning of a page
    virtual void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
        uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& metadata) const = 0;
};

// Compression alg which does not compress values and instead just copies them.
class Uncompressed : public CompressionAlg {
public:
    explicit Uncompressed(const common::LogicalType& logicalType)
        : logicalType{logicalType}, numBytesPerValue{getDataTypeSizeInChunk(this->logicalType)} {}

    Uncompressed(const Uncompressed&) = default;

    inline void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc,
        uint8_t* dstBuffer, common::offset_t posInDst,
        const CompressionMetadata& /*metadata*/) const final {
        memcpy(dstBuffer + posInDst * numBytesPerValue, srcBuffer + posInSrc * numBytesPerValue,
            numBytesPerValue);
    }

    inline void getValue(const uint8_t* buffer, common::offset_t posInBuffer, uint8_t* dst,
        common::offset_t posInDst, const CompressionMetadata& /*metadata*/) const override {
        memcpy(dst + posInDst * numBytesPerValue, buffer + posInBuffer * numBytesPerValue,
            numBytesPerValue);
    }

    static inline uint64_t numValues(uint64_t dataSize, const common::LogicalType& logicalType) {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType);
        return numBytesPerValue == 0 ? UINT64_MAX : dataSize / numBytesPerValue;
    }

    inline CompressionMetadata getCompressionMetadata(
        const uint8_t* /*srcBuffer*/, uint64_t /*numValues*/) const override {
        return CompressionMetadata();
    }

    inline uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& /*metadata*/) const override {
        if (numBytesPerValue == 0) {
            return 0;
        }
        uint64_t numValues = std::min(numValuesRemaining, dstBufferSize / numBytesPerValue);
        uint64_t sizeToCopy = numValues * numBytesPerValue;
        assert(sizeToCopy <= dstBufferSize);
        std::memcpy(dstBuffer, srcBuffer, sizeToCopy);
        srcBuffer += sizeToCopy;
        return sizeToCopy;
    }

    inline void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& /*metadata*/) const override {
        std::memcpy(dstBuffer + dstOffset * numBytesPerValue,
            srcBuffer + srcOffset * numBytesPerValue, numValues * numBytesPerValue);
    }

protected:
    common::LogicalType logicalType;
    const uint32_t numBytesPerValue;
};

// Serialized as nine bytes.
// In the first byte:
//  Six bits are needed for the bit width (fewer for smaller types, but the header byte is the same
//  for simplicity)
//  One bit (the eighth) is needed to indicate if there are negative values
//  The seventh bit is unused
// The eight remaining bytes are used for the offset
struct BitpackHeader {
    uint8_t bitWidth;
    bool hasNegative;
    // Offset to apply to all values
    // Stored as unsigned, but for signed variants of IntegerBitpacking
    // it gets cast to a signed type on use, letting it also store negative offsets
    uint64_t offset;
    static const uint8_t NEGATIVE_FLAG = 0b10000000;
    static const uint8_t BITWIDTH_MASK = 0b01111111;

    std::array<uint8_t, CompressionMetadata::DATA_SIZE> getData() const;

    static BitpackHeader readHeader(
        const std::array<uint8_t, CompressionMetadata::DATA_SIZE>& data);
};

// Augmented with Frame of Reference encoding using an offset stored in the compression metadata
template<typename T>
class IntegerBitpacking : public CompressionAlg {
    using U = std::make_unsigned_t<T>;
    // This is an implementation detail of the fastpfor bitpacking algorithm
    static constexpr uint64_t CHUNK_SIZE = 32;

public:
    IntegerBitpacking() = default;
    IntegerBitpacking(const IntegerBitpacking&) = default;

    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst, const CompressionMetadata& metadata) const final;

    // Read a single value from the buffer
    void getValue(const uint8_t* buffer, common::offset_t posInBuffer, uint8_t* dst,
        common::offset_t posInDst, const CompressionMetadata& metadata) const final;

    BitpackHeader getBitWidth(const uint8_t* srcBuffer, uint64_t numValues) const;

    static inline uint64_t numValues(uint64_t dataSize, const BitpackHeader& header) {
        if (header.bitWidth == 0) {
            return UINT64_MAX;
        }
        auto numValues = dataSize * 8 / header.bitWidth;
        // Round down to nearest multiple of CHUNK_SIZE to ensure that we don't write any extra
        // values Rounding up could overflow the buffer
        // TODO(bmwinger): Pack extra values into the space at the end. This will probably be
        // slower, but only needs to be done once.
        numValues -= numValues % CHUNK_SIZE;
        return numValues;
    }

    CompressionMetadata getCompressionMetadata(
        const uint8_t* srcBuffer, uint64_t numValues) const override {
        auto header = getBitWidth(srcBuffer, numValues);
        CompressionMetadata metadata{CompressionType::INTEGER_BITPACKING, header.getData()};
        return metadata;
    }

    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const final;

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const struct CompressionMetadata& metadata) const final;

    static bool canUpdateInPlace(T value, const BitpackHeader& header);

protected:
    // Read multiple values from within a chunk. Cannot span multiple chunks.
    void getValues(const uint8_t* chunkStart, uint8_t pos, uint8_t* dst, uint8_t numValuesToRead,
        const BitpackHeader& header) const;

    inline const uint8_t* getChunkStart(
        const uint8_t* buffer, uint64_t pos, uint8_t bitWidth) const {
        // Order of operations is important so that pos is rounded down to a multiple of CHUNK_SIZE
        return buffer + (pos / CHUNK_SIZE) * bitWidth * CHUNK_SIZE / 8;
    }
};

class BooleanBitpacking : public CompressionAlg {
public:
    BooleanBitpacking() = default;
    BooleanBitpacking(const BooleanBitpacking&) = default;

    void setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc, uint8_t* dstBuffer,
        common::offset_t posInDst, const CompressionMetadata& metadata) const final;

    void getValue(const uint8_t* buffer, common::offset_t posInBuffer, uint8_t* dst,
        common::offset_t posInDst, const CompressionMetadata& metadata) const final;

    static inline uint64_t numValues(uint64_t dataSize) { return dataSize * 8; }

    inline CompressionMetadata getCompressionMetadata(
        const uint8_t* /*srcBuffer*/, uint64_t /*numValues*/) const override {
        return CompressionMetadata{CompressionType::BOOLEAN_BITPACKING};
    }
    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const final;

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues, const CompressionMetadata& metadata) const final;
};

class CompressedFunctor {
public:
    CompressedFunctor(const CompressedFunctor&) = default;

protected:
    explicit CompressedFunctor(const common::LogicalType& logicalType)
        : uncompressed{logicalType}, physicalType{logicalType.getPhysicalType()} {}
    const Uncompressed uncompressed;
    const BooleanBitpacking booleanBitpacking;
    const common::PhysicalTypeID physicalType;
};

class ReadCompressedValuesFromPageToVector : public CompressedFunctor {
public:
    explicit ReadCompressedValuesFromPageToVector(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    ReadCompressedValuesFromPageToVector(const ReadCompressedValuesFromPageToVector&) = default;

    void operator()(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& metadata);
};

class ReadCompressedValuesFromPage : public CompressedFunctor {
public:
    explicit ReadCompressedValuesFromPage(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    ReadCompressedValuesFromPage(const ReadCompressedValuesFromPage&) = default;

    void operator()(uint8_t* frame, PageElementCursor& pageCursor, uint8_t* result,
        uint32_t startPosInResult, uint64_t numValuesToRead, const CompressionMetadata& metadata);
};

class WriteCompressedValueToPage : public CompressedFunctor {
public:
    explicit WriteCompressedValueToPage(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    WriteCompressedValueToPage(const WriteCompressedValueToPage&) = default;

    void operator()(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& metadata);
};

} // namespace storage
} // namespace kuzu
