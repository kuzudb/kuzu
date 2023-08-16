#pragma once

#include "common/copier_config/copier_config.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace arrow {
class Array;
}

namespace kuzu {
namespace storage {

class NullColumnChunk;

struct ColumnChunkMetadata {
    common::page_idx_t pageIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t numPages = 0;

    ColumnChunkMetadata() = default;
    ColumnChunkMetadata(common::page_idx_t pageIdx, common::page_idx_t numPages)
        : pageIdx(pageIdx), numPages(numPages) {}
};

struct MainColumnChunkMetadata : public ColumnChunkMetadata {
    uint64_t numValues;

    MainColumnChunkMetadata() = default;
    MainColumnChunkMetadata(
        common::page_idx_t pageIdx, common::page_idx_t numPages, uint64_t numNodesInChunk)
        : ColumnChunkMetadata{pageIdx, numPages}, numValues(numNodesInChunk) {}
};

struct OverflowColumnChunkMetadata : public ColumnChunkMetadata {
    common::offset_t lastOffsetInPage;

    OverflowColumnChunkMetadata() = default;
    OverflowColumnChunkMetadata(
        common::page_idx_t pageIdx, common::page_idx_t numPages, common::offset_t lastOffsetInPage)
        : ColumnChunkMetadata{pageIdx, numPages}, lastOffsetInPage(lastOffsetInPage) {}
};

// Base data segment covers all fixed-sized data types.
// Some template functions are almost duplicated from `InMemColumnChunk`, which is intended.
// Currently, `InMemColumnChunk` is used to populate rel columns. Eventually, we will merge them.
class ColumnChunk {
public:
    friend class ColumnChunkFactory;
    friend class VarListDataColumnChunk;

    // ColumnChunks must be initialized after construction, so this constructor should only be used
    // through the ColumnChunkFactory
    explicit ColumnChunk(common::LogicalType dataType, common::CopyDescription* copyDescription,
        bool hasNullChunk = true);
    virtual ~ColumnChunk() = default;

    template<typename T>
    inline T getValue(common::offset_t pos) const {
        return ((T*)buffer.get())[pos];
    }

    inline NullColumnChunk* getNullChunk() { return nullChunk.get(); }
    inline common::LogicalType getDataType() const { return dataType; }

    inline common::vector_idx_t getNumChildren() const { return childrenChunks.size(); }
    inline ColumnChunk* getChild(common::vector_idx_t idx) {
        assert(idx < childrenChunks.size());
        return childrenChunks[idx].get();
    }

    virtual void resetToEmpty();

    // Include pages for null and children segments.
    virtual common::page_idx_t getNumPages() const;

    virtual void append(common::ValueVector* vector, common::offset_t startPosInChunk);

    void append(
        common::ValueVector* vector, common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    virtual void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    virtual void append(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    virtual common::page_idx_t flushBuffer(BMFileHandle* dataFH, common::page_idx_t startPageIdx);

    // Returns the size of the data type in bytes
    static uint32_t getDataTypeSizeInChunk(common::LogicalType& dataType);

    template<typename T>
    void setValueFromString(const char* value, uint64_t length, common::offset_t pos) {
        auto val = common::StringCastUtils::castToNum<T>(value, length);
        setValue<T>(val, pos);
    }

    static inline common::page_idx_t getNumPagesForBytes(uint64_t numBytes) {
        return (numBytes + common::BufferPoolConstants::PAGE_4KB_SIZE - 1) /
               common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

    inline uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    inline uint64_t getNumBytes() const { return bufferSize; }
    inline uint8_t* getData() { return buffer.get(); }

    virtual void write(const common::Value& val, uint64_t posToWrite);

    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    virtual void resize(uint64_t numValues);

    template<typename T>
    inline void setValue(T val, common::offset_t pos) {
        ((T*)buffer.get())[pos] = val;
    }

    void populateWithDefaultVal(common::ValueVector* defaultValueVector);

    inline uint64_t getNumValues() const { return numValues; }

    inline void setNumValues(uint64_t numValues_) { this->numValues = numValues_; }

protected:
    // Initializes the data buffer. Is (and should be) only called in constructor.
    virtual void initialize(common::offset_t capacity);

    template<typename T>
    void templateCopyArrowArray(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);
    template<typename ARROW_TYPE>
    void templateCopyStringArrowArray(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);
    // TODO(Guodong/Ziyi): The conversion from string to values should be handled inside ReadFile.
    // ARROW_TYPE can be either arrow::StringArray or arrow::LargeStringArray.
    template<typename KU_TYPE, typename ARROW_TYPE>
    void templateCopyValuesAsString(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    virtual inline common::page_idx_t getNumPagesForBuffer() const {
        return getNumPagesForBytes(bufferSize);
    }

    common::offset_t getOffsetInBuffer(common::offset_t pos) const;

    void copyVectorToBuffer(common::ValueVector* vector, common::offset_t startPosInChunk);

protected:
    common::LogicalType dataType;
    uint32_t numBytesPerValue;
    uint64_t bufferSize;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<NullColumnChunk> nullChunk;
    std::vector<std::unique_ptr<ColumnChunk>> childrenChunks;
    const common::CopyDescription* copyDescription;
    uint64_t numValues;
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

class BoolColumnChunk : public ColumnChunk {
public:
    BoolColumnChunk(common::CopyDescription* copyDescription, bool hasNullChunk = true)
        : ColumnChunk(
              common::LogicalType(common::LogicalTypeID::BOOL), copyDescription, hasNullChunk) {}

    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

    void append(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
};

class NullColumnChunk : public BoolColumnChunk {
public:
    NullColumnChunk() : BoolColumnChunk(nullptr /*copyDescription*/, false /*hasNullChunk*/) {}
    // Maybe this should be combined with BoolColumnChunk if the only difference is these functions?
    inline bool isNull(common::offset_t pos) const { return getValue<bool>(pos); }
    inline void setNull(common::offset_t pos, bool isNull) { setValue(isNull, pos); }

    void resize(uint64_t numValues) final;

    inline void resetNullBuffer() { memset(buffer.get(), 0 /* non null */, bufferSize); }

protected:
    inline uint64_t numBytesForValues(common::offset_t numValues) const {
        // 8 values per byte, and we need a buffer size which is a multiple of 8 bytes
        return ceil(numValues / 8.0 / 8.0) * 8;
    }
    inline void initialize(common::offset_t numValues) final {
        numBytesPerValue = 0;
        bufferSize = numBytesForValues(numValues);
        // Each byte defaults to 0, indicating everything is non-null
        buffer = std::make_unique<uint8_t[]>(bufferSize);
    }
};

class FixedListColumnChunk : public ColumnChunk {
public:
    FixedListColumnChunk(common::LogicalType dataType, common::CopyDescription* copyDescription)
        : ColumnChunk(std::move(dataType), copyDescription, true /* hasNullChunk */) {}

    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void write(const common::Value& fixedListVal, uint64_t posToWrite) final;
};

class SerialColumnChunk : public ColumnChunk {
public:
    SerialColumnChunk()
        : ColumnChunk{common::LogicalType(common::LogicalTypeID::SERIAL),
              nullptr /* copyDescription */, false /* hasNullChunk */} {}

    inline void initialize(common::offset_t numValues) final {
        numBytesPerValue = 0;
        bufferSize = 0;
        // Each byte defaults to 0, indicating everything is non-null
        buffer = std::make_unique<uint8_t[]>(bufferSize);
    }
};

struct ColumnChunkFactory {
    static std::unique_ptr<ColumnChunk> createColumnChunk(
        const common::LogicalType& dataType, common::CopyDescription* copyDescription = nullptr);
};

template<>
void ColumnChunk::templateCopyArrowArray<bool>(
    arrow::Array* array, common::offset_t startPosInSegment, uint32_t numValuesToAppend);
template<>
void ColumnChunk::templateCopyArrowArray<uint8_t*>(
    arrow::Array* array, common::offset_t startPosInSegment, uint32_t numValuesToAppend);
// BOOL
template<>
void ColumnChunk::setValueFromString<bool>(const char* value, uint64_t length, uint64_t pos);
// FIXED_LIST
template<>
void ColumnChunk::setValueFromString<uint8_t*>(const char* value, uint64_t length, uint64_t pos);
// INTERVAL
template<>
void ColumnChunk::setValueFromString<common::interval_t>(
    const char* value, uint64_t length, uint64_t pos);
// DATE
template<>
void ColumnChunk::setValueFromString<common::date_t>(
    const char* value, uint64_t length, uint64_t pos);
// TIMESTAMP
template<>
void ColumnChunk::setValueFromString<common::timestamp_t>(
    const char* value, uint64_t length, uint64_t pos);

} // namespace storage
} // namespace kuzu
