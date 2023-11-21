#pragma once

#include <functional>

#include "common/constants.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/compression/compression.h"

namespace kuzu {
namespace storage {

class NullColumnChunk;
class CompressionAlg;

struct ColumnChunkMetadata {
    common::page_idx_t pageIdx;
    common::page_idx_t numPages;
    uint64_t numValues;
    CompressionMetadata compMeta;

    ColumnChunkMetadata() : pageIdx{common::INVALID_PAGE_IDX}, numPages{0}, numValues{UINT64_MAX} {}
    ColumnChunkMetadata(common::page_idx_t pageIdx, common::page_idx_t numPages,
        uint64_t numNodesInChunk, const CompressionMetadata& compMeta)
        : pageIdx(pageIdx), numPages(numPages), numValues(numNodesInChunk), compMeta(compMeta) {}
};

// Base data segment covers all fixed-sized data types.
class ColumnChunk {
public:
    friend class ColumnChunkFactory;
    friend class VarListDataColumnChunk;

    // ColumnChunks must be initialized after construction, so this constructor should only be used
    // through the ColumnChunkFactory
    ColumnChunk(std::unique_ptr<common::LogicalType> dataType, uint64_t capacity,
        bool enableCompression = true, bool hasNullChunk = true);

    virtual ~ColumnChunk() = default;

    template<typename T>
    inline T getValue(common::offset_t pos) const {
        return ((T*)buffer.get())[pos];
    }

    inline NullColumnChunk* getNullChunk() { return nullChunk.get(); }
    inline common::LogicalType* getDataType() const { return dataType.get(); }

    virtual void resetToEmpty();

    // Note that the startPageIdx is not known, so it will always be common::INVALID_PAGE_IDX
    virtual ColumnChunkMetadata getMetadataToFlush() const;

    virtual void append(common::ValueVector* vector);
    virtual void append(
        ColumnChunk* other, common::offset_t startPosInOtherChunk, uint32_t numValuesToAppend);

    ColumnChunkMetadata flushBuffer(
        BMFileHandle* dataFH, common::page_idx_t startPageIdx, const ColumnChunkMetadata& metadata);

    static inline common::page_idx_t getNumPagesForBytes(uint64_t numBytes) {
        return (numBytes + common::BufferPoolConstants::PAGE_4KB_SIZE - 1) /
               common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

    inline uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    inline uint8_t* getData() { return buffer.get(); }

    // TODO(Guodong): In general, this is not a good interface. Instead of passing in
    // `offsetInVector`, we should flatten the vector to pos at `offsetInVector`.
    virtual void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk);
    virtual void write(
        common::ValueVector* vector, common::ValueVector* offsetsInChunk, bool isCSR);

    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    virtual void resize(uint64_t newCapacity);

    template<typename T>
    inline void setValue(T val, common::offset_t pos) {
        ((T*)buffer.get())[pos] = val;
    }

    void populateWithDefaultVal(common::ValueVector* defaultValueVector);
    virtual void finalize() { // DO NOTHING.
    }

    inline uint64_t getCapacity() const { return capacity; }
    inline uint64_t getNumValues() const { return numValues; }
    void setNumValues(uint64_t numValues_);

protected:
    // Initializes the data buffer. Is (and should be) only called in constructor.
    void initializeBuffer(common::offset_t capacity);
    void initializeFunction(bool enableCompression);

    common::offset_t getOffsetInBuffer(common::offset_t pos) const;

    virtual void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk);

private:
    uint64_t getBufferSize() const;

protected:
    std::unique_ptr<common::LogicalType> dataType;
    uint32_t numBytesPerValue;
    uint64_t bufferSize;
    uint64_t capacity;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<NullColumnChunk> nullChunk;
    uint64_t numValues;
    std::function<ColumnChunkMetadata(
        const uint8_t*, uint64_t, BMFileHandle*, common::page_idx_t, const ColumnChunkMetadata&)>
        flushBufferFunction;
    std::function<ColumnChunkMetadata(const uint8_t*, uint64_t, uint64_t, uint64_t)>
        getMetadataFunction;
};

template<>
inline void ColumnChunk::setValue(bool val, common::offset_t pos) {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    common::NullMask::setNull((uint64_t*)buffer.get(), pos, val);
}

template<>
inline bool ColumnChunk::getValue(common::offset_t pos) const {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    return common::NullMask::isNull((uint64_t*)buffer.get(), pos);
}

// Stored as bitpacked booleans in-memory and on-disk
class BoolColumnChunk : public ColumnChunk {
public:
    explicit BoolColumnChunk(uint64_t capacity, bool hasNullChunk = true)
        : ColumnChunk(common::LogicalType::BOOL(), capacity,
              // Booleans are always compressed
              false /* enableCompression */, hasNullChunk) {}

    void append(common::ValueVector* vector) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector,
        bool isCSR) final;
};

class NullColumnChunk : public BoolColumnChunk {
public:
    NullColumnChunk(uint64_t capacity)
        : BoolColumnChunk(capacity, false /*hasNullChunk*/), mayHaveNullValue{false} {}
    // Maybe this should be combined with BoolColumnChunk if the only difference is these functions?
    inline bool isNull(common::offset_t pos) const { return getValue<bool>(pos); }
    inline void setNull(common::offset_t pos, bool isNull) {
        setValue(isNull, pos);
        if (isNull) {
            mayHaveNullValue = true;
        }
        // TODO(Guodong): Better let NullColumnChunk also support `append` a vector.
        if (pos >= numValues) {
            numValues = pos + 1;
        }
    }

    inline bool mayHaveNull() const { return mayHaveNullValue; }

    inline void resetToEmpty() final {
        resetToNoNull();
        numValues = 0;
    }
    inline void resetToNoNull() {
        memset(buffer.get(), 0 /* non null */, bufferSize);
        mayHaveNullValue = false;
    }
    inline void resetToAllNull() {
        memset(buffer.get(), 0xFF /* null */, bufferSize);
        mayHaveNullValue = true;
    }

    inline void copyFromBuffer(uint64_t* srcBuffer, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBits, bool invert = false) {
        if (common::NullMask::copyNullMask(
                srcBuffer, srcOffset, (uint64_t*)buffer.get(), dstOffset, numBits, invert)) {
            mayHaveNullValue = true;
        }
    }

    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;

protected:
    bool mayHaveNullValue;
};

struct ColumnChunkFactory {
    static std::unique_ptr<ColumnChunk> createColumnChunk(
        std::unique_ptr<common::LogicalType> dataType, bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);
};

} // namespace storage
} // namespace kuzu
