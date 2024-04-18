#pragma once

#include <functional>

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "common/enums/rel_multiplicity.h"
#include "common/null_mask.h"
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

    // TODO: Delete copy.
    ColumnChunkMetadata() : pageIdx{common::INVALID_PAGE_IDX}, numPages{0}, numValues{0} {}
    ColumnChunkMetadata(common::page_idx_t pageIdx, common::page_idx_t numPages,
        uint64_t numNodesInChunk, const CompressionMetadata& compMeta)
        : pageIdx(pageIdx), numPages(numPages), numValues(numNodesInChunk), compMeta(compMeta) {}
};

// Base data segment covers all fixed-sized data types.
class ColumnChunk {
public:
    friend struct ColumnChunkFactory;
    friend struct ListDataColumnChunk;

    // ColumnChunks must be initialized after construction, so this constructor should only be used
    // through the ColumnChunkFactory
    ColumnChunk(common::LogicalType dataType, uint64_t capacity, bool enableCompression = true,
        bool hasNullChunk = true);

    virtual ~ColumnChunk() = default;

    template<typename T>
    inline T getValue(common::offset_t pos) const {
        KU_ASSERT(pos < numValues);
        return ((T*)buffer.get())[pos];
    }

    inline NullColumnChunk* getNullChunk() { return nullChunk.get(); }
    inline const NullColumnChunk& getNullChunk() const { return *nullChunk; }
    inline common::LogicalType& getDataType() { return dataType; }
    inline const common::LogicalType& getDataType() const { return dataType; }

    virtual void resetToEmpty();

    // Note that the startPageIdx is not known, so it will always be common::INVALID_PAGE_IDX
    virtual ColumnChunkMetadata getMetadataToFlush() const;

    virtual void append(common::ValueVector* vector, const common::SelectionVector& selVector);
    virtual void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    ColumnChunkMetadata flushBuffer(BMFileHandle* dataFH, common::page_idx_t startPageIdx,
        const ColumnChunkMetadata& metadata);

    static inline common::page_idx_t getNumPagesForBytes(uint64_t numBytes) {
        return (numBytes + common::BufferPoolConstants::PAGE_4KB_SIZE - 1) /
               common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

    inline uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    inline uint8_t* getData() const { return buffer.get(); }

    virtual void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const;

    // TODO(Guodong): In general, this is not a good interface. Instead of passing in
    // `offsetInVector`, we should flatten the vector to pos at `offsetInVector`.
    virtual void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk);
    virtual void write(ColumnChunk* chunk, ColumnChunk* offsetsInChunk,
        common::RelMultiplicity multiplicity);
    virtual void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy);
    // TODO(Guodong): Used in `applyDeletionsToChunk`. Should unify with `write`.
    virtual void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy);

    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    virtual void resize(uint64_t newCapacity);

    template<typename T>
    void setValue(T val, common::offset_t pos) {
        KU_ASSERT(pos < capacity);
        ((T*)buffer.get())[pos] = val;
        if (pos >= numValues) {
            numValues = pos + 1;
        }
    }

    void populateWithDefaultVal(common::ValueVector* defaultValueVector);
    virtual void finalize() { // DO NOTHING.
    }

    inline uint64_t getCapacity() const { return capacity; }
    inline uint64_t getNumValues() const { return numValues; }
    virtual void setNumValues(uint64_t numValues_);
    virtual bool numValuesSanityCheck() const;
    bool isCompressionEnabled() const { return enableCompression; }

    virtual bool sanityCheck();

protected:
    // Initializes the data buffer. Is (and should be) only called in constructor.
    void initializeBuffer(common::offset_t capacity);
    void initializeFunction();

    virtual void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk,
        const common::SelectionVector& selVector);

private:
    uint64_t getBufferSize(uint64_t capacity_) const;

protected:
    common::LogicalType dataType;
    uint32_t numBytesPerValue;
    uint64_t bufferSize;
    uint64_t capacity;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<NullColumnChunk> nullChunk;
    uint64_t numValues;
    std::function<ColumnChunkMetadata(const uint8_t*, uint64_t, BMFileHandle*, common::page_idx_t,
        const ColumnChunkMetadata&)>
        flushBufferFunction;
    std::function<ColumnChunkMetadata(const uint8_t*, uint64_t, uint64_t, uint64_t)>
        getMetadataFunction;
    bool enableCompression;
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
    explicit BoolColumnChunk(uint64_t capacity, bool enableCompression, bool hasNullChunk = true)
        : ColumnChunk(*common::LogicalType::BOOL(), capacity,
              // Booleans are always bitpacked, but this can also enable constant compression
              enableCompression, hasNullChunk) {}

    void append(common::ValueVector* vector, const common::SelectionVector& sel) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunk* chunk, ColumnChunk* dstOffsets,
        common::RelMultiplicity multiplicity) final;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
};

class NullColumnChunk final : public BoolColumnChunk {
public:
    explicit NullColumnChunk(uint64_t capacity, bool enableCompression)
        : BoolColumnChunk(capacity, enableCompression, false /*hasNullChunk*/),
          mayHaveNullValue{false} {}
    // Maybe this should be combined with BoolColumnChunk if the only difference is these functions?
    inline bool isNull(common::offset_t pos) const { return getValue<bool>(pos); }
    void setNull(common::offset_t pos, bool isNull);

    inline bool mayHaveNull() const { return mayHaveNullValue; }

    inline void resetToEmpty() override {
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
        if (common::NullMask::copyNullMask(srcBuffer, srcOffset, (uint64_t*)buffer.get(), dstOffset,
                numBits, invert)) {
            mayHaveNullValue = true;
        }
    }

    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    common::NullMask getNullMask() const {
        return common::NullMask(std::span(reinterpret_cast<uint64_t*>(getData()), capacity / 64));
    }

protected:
    bool mayHaveNullValue;
};

struct ColumnChunkFactory {
    // inMemory starts string column chunk dictionaries at zero instead of reserving space for
    // values to grow
    static std::unique_ptr<ColumnChunk> createColumnChunk(common::LogicalType dataType,
        bool enableCompression, uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE,
        bool inMemory = false);

    static std::unique_ptr<ColumnChunk> createNullColumnChunk(bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE) {
        return std::make_unique<NullColumnChunk>(capacity, enableCompression);
    }
};

} // namespace storage
} // namespace kuzu
