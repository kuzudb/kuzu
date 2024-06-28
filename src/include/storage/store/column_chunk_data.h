#pragma once

#include <functional>

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "common/enums/rel_multiplicity.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/compression/compression.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace storage {

class NullChunkData;
class BMFileHandle;

struct ColumnChunkMetadata {
    common::page_idx_t pageIdx;
    common::page_idx_t numPages;
    uint64_t numValues;
    CompressionMetadata compMeta;

    // TODO(Guodong): Delete copy constructor.
    ColumnChunkMetadata()
        : pageIdx{common::INVALID_PAGE_IDX}, numPages{0}, numValues{0},
          compMeta(StorageValue(), StorageValue(), CompressionType::CONSTANT) {}
    ColumnChunkMetadata(common::page_idx_t pageIdx, common::page_idx_t numPages,
        uint64_t numNodesInChunk, const CompressionMetadata& compMeta)
        : pageIdx(pageIdx), numPages(numPages), numValues(numNodesInChunk), compMeta(compMeta) {}
};

// Base data segment covers all fixed-sized data types.
class ColumnChunkData {
public:
    friend struct ColumnChunkFactory;

    // ColumnChunks must be initialized after construction, so this constructor should only be used
    // through the ColumnChunkFactory
    ColumnChunkData(common::LogicalType dataType, uint64_t capacity, bool enableCompression = true,
        bool hasNull = true);

    virtual ~ColumnChunkData() = default;

    template<typename T>
    T getValue(common::offset_t pos) const {
        KU_ASSERT(pos < numValues);
        return reinterpret_cast<T*>(buffer.get())[pos];
    }
    template<typename T>
    void setValue(T val, common::offset_t pos) {
        KU_ASSERT(pos < capacity);
        reinterpret_cast<T*>(buffer.get())[pos] = val;
        if (pos >= numValues) {
            numValues = pos + 1;
        }
    }

    bool isNull(common::offset_t pos) const;
    NullChunkData* getNullChunk() { return nullChunk.get(); }
    const NullChunkData& getNullChunk() const { return *nullChunk; }
    std::optional<common::NullMask> getNullMask() const;
    common::LogicalType& getDataType() { return dataType; }
    const common::LogicalType& getDataType() const { return dataType; }

    virtual void resetToAllNull();
    virtual void resetToEmpty();

    // Note that the startPageIdx is not known, so it will always be common::INVALID_PAGE_IDX
    virtual ColumnChunkMetadata getMetadataToFlush() const;

    virtual void append(common::ValueVector* vector, const common::SelectionVector& selVector);
    virtual void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    ColumnChunkMetadata flushBuffer(BMFileHandle* dataFH, common::page_idx_t startPageIdx,
        const ColumnChunkMetadata& metadata) const;

    static common::page_idx_t getNumPagesForBytes(uint64_t numBytes) {
        return (numBytes + common::BufferPoolConstants::PAGE_4KB_SIZE - 1) /
               common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

    uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    uint8_t* getData() const { return buffer.get(); }

    virtual void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const;

    // TODO(Guodong): In general, this is not a good interface. Instead of passing in
    // `offsetInVector`, we should flatten the vector to pos at `offsetInVector`.
    virtual void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk);
    virtual void write(ColumnChunkData* chunk, ColumnChunkData* offsetsInChunk,
        common::RelMultiplicity multiplicity);
    virtual void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy);
    // TODO(Guodong): Used in `applyDeletionsToChunk`. Should unify with `write`.
    virtual void copy(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy);

    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    virtual void resize(uint64_t newCapacity);

    void populateWithDefaultVal(evaluator::ExpressionEvaluator& defaultEvaluator,
        uint64_t& numValues_);
    virtual void finalize() { // DO NOTHING.
    }

    uint64_t getCapacity() const { return capacity; }
    uint64_t getNumValues() const { return numValues; }
    virtual void setNumValues(uint64_t numValues_);
    virtual bool numValuesSanityCheck() const;
    bool isCompressionEnabled() const { return enableCompression; }

    virtual bool sanityCheck();

    template<typename TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<ColumnChunkData&, TARGET&>(*this);
    }
    template<typename TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<const ColumnChunkData&, const TARGET&>(*this);
    }

protected:
    // Initializes the data buffer and functions. They are (and should be) only called in
    // constructor.
    void initializeBuffer();
    void initializeFunction();

    virtual void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk,
        const common::SelectionVector& selVector);

private:
    uint64_t getBufferSize(uint64_t capacity_) const;

protected:
    using flush_buffer_func_t = std::function<ColumnChunkMetadata(const uint8_t*, uint64_t,
        BMFileHandle*, common::page_idx_t, const ColumnChunkMetadata&)>;
    using get_metadata_func_t = std::function<ColumnChunkMetadata(const uint8_t*, uint64_t,
        uint64_t, uint64_t, StorageValue, StorageValue)>;

    common::LogicalType dataType;
    uint32_t numBytesPerValue;
    uint64_t bufferSize;
    uint64_t capacity;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<NullChunkData> nullChunk;
    uint64_t numValues;
    flush_buffer_func_t flushBufferFunction;
    get_metadata_func_t getMetadataFunction;
    bool enableCompression;
};

template<>
inline void ColumnChunkData::setValue(bool val, common::offset_t pos) {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    common::NullMask::setNull(reinterpret_cast<uint64_t*>(buffer.get()), pos, val);
}

template<>
inline bool ColumnChunkData::getValue(common::offset_t pos) const {
    // Buffer is rounded up to the nearest 8 bytes so that this cast is safe
    return common::NullMask::isNull(reinterpret_cast<uint64_t*>(buffer.get()), pos);
}

// Stored as bitpacked booleans in-memory and on-disk
class BoolChunkData : public ColumnChunkData {
public:
    explicit BoolChunkData(uint64_t capacity, bool enableCompression, bool hasNullChunk = true)
        : ColumnChunkData(common::LogicalType::BOOL(), capacity,
              // Booleans are always bitpacked, but this can also enable constant compression
              enableCompression, hasNullChunk) {}

    void append(common::ValueVector* vector, const common::SelectionVector& sel) final;
    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
        common::RelMultiplicity multiplicity) final;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
};

class NullChunkData final : public BoolChunkData {
public:
    explicit NullChunkData(uint64_t capacity, bool enableCompression)
        : BoolChunkData(capacity, enableCompression, false /*hasNullChunk*/),
          mayHaveNullValue{false} {}
    // Maybe this should be combined with BoolChunkData if the only difference is these functions?
    bool isNull(common::offset_t pos) const { return getValue<bool>(pos); }
    void setNull(common::offset_t pos, bool isNull);

    bool mayHaveNull() const { return mayHaveNullValue; }

    void resetToEmpty() override {
        resetToNoNull();
        numValues = 0;
    }
    void resetToNoNull() {
        memset(buffer.get(), 0 /* non null */, bufferSize);
        mayHaveNullValue = false;
    }
    void resetToAllNull() override {
        memset(buffer.get(), 0xFF /* null */, bufferSize);
        mayHaveNullValue = true;
    }

    void copyFromBuffer(uint64_t* srcBuffer, uint64_t srcOffset, uint64_t dstOffset,
        uint64_t numBits, bool invert = false) {
        if (common::NullMask::copyNullMask(srcBuffer, srcOffset,
                reinterpret_cast<uint64_t*>(buffer.get()), dstOffset, numBits, invert)) {
            mayHaveNullValue = true;
        }
    }

    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    common::NullMask getNullMask() const {
        return common::NullMask(std::span(reinterpret_cast<uint64_t*>(getData()), capacity / 64),
            mayHaveNullValue);
    }

protected:
    bool mayHaveNullValue;
};

struct ColumnChunkFactory {
    // inMemory starts string column chunk dictionaries at zero instead of reserving space for
    // values to grow
    static std::unique_ptr<ColumnChunkData> createColumnChunkData(common::LogicalType dataType,
        bool enableCompression, uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE,
        bool inMemory = false, bool hasNull = true);

    static std::unique_ptr<ColumnChunkData> createNullChunkData(bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE) {
        return std::make_unique<NullChunkData>(capacity, enableCompression);
    }
};

} // namespace storage
} // namespace kuzu
