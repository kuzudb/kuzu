#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <optional>

#include "common/types/types.h"

namespace kuzu {
namespace common {
class ValueVector;
class NullMask;
} // namespace common

namespace storage {
class ColumnChunk;

struct PageCursor;

// Returns the size of the data type in bytes
uint32_t getDataTypeSizeInChunk(const common::LogicalType& dataType);
uint32_t getDataTypeSizeInChunk(const common::PhysicalTypeID& dataType);

// Compression type is written to the data header both so we can usually catch issues when we
// decompress uncompressed data by mistake, and to allow for runtime-configurable compression.
enum class CompressionType : uint8_t {
    UNCOMPRESSED = 0,
    INTEGER_BITPACKING = 1,
    BOOLEAN_BITPACKING = 2,
    CONSTANT = 3,
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
    bool canUpdateInPlace(const uint8_t* data, uint32_t pos,
        common::PhysicalTypeID physicalType) const;
    bool canAlwaysUpdateInPlace() const;
    inline bool isConstant() const { return compression == CompressionType::CONSTANT; }

    std::string toString() const;
};

class CompressionAlg {
public:
    virtual ~CompressionAlg() = default;

    // Takes a single uncompressed value from the srcBuffer and compresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    //
    // nullMask may be null if no mask is available (all values are non-null)
    // Storage of null values is handled by the implementation and decompression of null values
    // does not have to produce the original value passed to this function.
    virtual void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const = 0;

    // Returns compression metadata, including any relevant parameters specific to this dataset
    // which will need to be passed to compressNextPage. Since this may need to scan the entire
    // buffer, which is slow, it should only be called once for each set of data to compress.
    virtual CompressionMetadata getCompressionMetadata(const uint8_t* srcBuffer,
        uint64_t numValues) const = 0;

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

class ConstantCompression final : public CompressionAlg {
public:
    explicit ConstantCompression(const common::LogicalType& logicalType)
        : numBytesPerValue{static_cast<uint8_t>(getDataTypeSizeInChunk(logicalType))},
          dataType{logicalType.getPhysicalType()} {}
    static std::optional<CompressionMetadata> analyze(const ColumnChunk& chunk);
    template<typename T>
    static const T& getValue(const CompressionMetadata& metadata) {
        return *reinterpret_cast<const T*>(metadata.data.data());
    }

    // Shouldn't be used, there's a special case when compressing which ends early for constant
    // compression
    uint64_t compressNextPage(const uint8_t*&, uint64_t, uint8_t*, uint64_t,
        const struct CompressionMetadata&) const override {
        return 0;
    };

    void decompressFromPage(const uint8_t* /*srcBuffer*/, uint64_t /*srcOffset*/,
        uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& metadata) const override;

    void copyFromPage(const uint8_t* /*srcBuffer*/, uint64_t /*srcOffset*/, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues, const CompressionMetadata& metadata) const;

    // Shouldn't be used. No type exclusively uses constant compression
    CompressionMetadata getCompressionMetadata(const uint8_t* /*srcBuffer*/,
        uint64_t /*numValues*/) const override {
        KU_UNREACHABLE;
    }

    // Nothing to do; constant compressed data is only updated if the update is to the same value
    void setValuesFromUncompressed(const uint8_t*, common::offset_t, uint8_t*, common::offset_t,
        common::offset_t, const CompressionMetadata&,
        const common::NullMask* /*nullMask*/) const override {};

private:
    uint8_t numBytesPerValue;
    common::PhysicalTypeID dataType;
};

// Compression alg which does not compress values and instead just copies them.
class Uncompressed : public CompressionAlg {
public:
    explicit Uncompressed(const common::LogicalType& logicalType)
        : numBytesPerValue{getDataTypeSizeInChunk(logicalType)} {}
    explicit Uncompressed(uint8_t numBytesPerValue) : numBytesPerValue{numBytesPerValue} {}

    Uncompressed(const Uncompressed&) = default;

    inline void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& /*metadata*/, const common::NullMask* /*nullMask*/) const final {
        memcpy(dstBuffer + dstOffset * numBytesPerValue, srcBuffer + srcOffset * numBytesPerValue,
            numBytesPerValue * numValues);
    }

    static inline uint64_t numValues(uint64_t dataSize, const common::LogicalType& logicalType) {
        auto numBytesPerValue = getDataTypeSizeInChunk(logicalType);
        return numBytesPerValue == 0 ? UINT64_MAX : dataSize / numBytesPerValue;
    }

    inline CompressionMetadata getCompressionMetadata(const uint8_t* /*srcBuffer*/,
        uint64_t /*numValues*/) const override {
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
        KU_ASSERT(sizeToCopy <= dstBufferSize);
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

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

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

    CompressionMetadata getCompressionMetadata(const uint8_t* srcBuffer,
        uint64_t numValues) const override {
        auto header = getBitWidth(srcBuffer, numValues);
        // Use uncompressed if the bitwidth is equal to the size of the type in bits
        if (header.bitWidth >= sizeof(T) * 8) {
            return CompressionMetadata();
        }
        return CompressionMetadata(CompressionType::INTEGER_BITPACKING, header.getData());
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

    inline const uint8_t* getChunkStart(const uint8_t* buffer, uint64_t pos,
        uint8_t bitWidth) const {
        // Order of operations is important so that pos is rounded down to a multiple of CHUNK_SIZE
        return buffer + (pos / CHUNK_SIZE) * bitWidth * CHUNK_SIZE / 8;
    }
};

class BooleanBitpacking : public CompressionAlg {
public:
    BooleanBitpacking() = default;
    BooleanBitpacking(const BooleanBitpacking&) = default;

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

    static inline uint64_t numValues(uint64_t dataSize) { return dataSize * 8; }

    inline CompressionMetadata getCompressionMetadata(const uint8_t* /*srcBuffer*/,
        uint64_t /*numValues*/) const override {
        return CompressionMetadata{CompressionType::BOOLEAN_BITPACKING};
    }
    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const final;

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues, const CompressionMetadata& metadata) const final;

    void copyFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues, const CompressionMetadata& metadata) const;
};

class CompressedFunctor {
public:
    CompressedFunctor(const CompressedFunctor&) = default;

protected:
    explicit CompressedFunctor(const common::LogicalType& logicalType)
        : constant{logicalType}, uncompressed{logicalType},
          physicalType{logicalType.getPhysicalType()} {}
    const ConstantCompression constant;
    const Uncompressed uncompressed;
    const BooleanBitpacking booleanBitpacking;
    const common::PhysicalTypeID physicalType;
};

class ReadCompressedValuesFromPageToVector : public CompressedFunctor {
public:
    explicit ReadCompressedValuesFromPageToVector(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    ReadCompressedValuesFromPageToVector(const ReadCompressedValuesFromPageToVector&) = default;

    void operator()(const uint8_t* frame, PageCursor& pageCursor, common::ValueVector* resultVector,
        uint32_t posInVector, uint32_t numValuesToRead, const CompressionMetadata& metadata);
};

class ReadCompressedValuesFromPage : public CompressedFunctor {
public:
    explicit ReadCompressedValuesFromPage(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    ReadCompressedValuesFromPage(const ReadCompressedValuesFromPage&) = default;

    void operator()(const uint8_t* frame, PageCursor& pageCursor, uint8_t* result,
        uint32_t startPosInResult, uint64_t numValuesToRead, const CompressionMetadata& metadata);
};

class WriteCompressedValuesToPage : public CompressedFunctor {
public:
    explicit WriteCompressedValuesToPage(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    WriteCompressedValuesToPage(const WriteCompressedValuesToPage&) = default;

    void operator()(uint8_t* frame, uint16_t posInFrame, const uint8_t* data,
        common::offset_t dataOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask = nullptr);

    void operator()(uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& metadata);
};

} // namespace storage
} // namespace kuzu
