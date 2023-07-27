#pragma once

#include "common/copier_config/copier_config.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class NullColumnChunk;

// Base data segment covers all fixed-sized data types.
// Some template functions are almost duplicated from `InMemColumnChunk`, which is intended.
// Currently, `InMemColumnChunk` is used to populate rel columns. Eventually, we will merge them.
class ColumnChunk {
public:
    friend class ColumnChunkFactory;

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

    virtual void write(const common::Value& val, uint64_t posToWrite);

    // numValues must be at least the number of values the ColumnChunk was first initialized
    // with
    virtual void resize(uint64_t numValues);

    template<typename T>
    inline void setValue(T val, common::offset_t pos) {
        ((T*)buffer.get())[pos] = val;
    }

protected:
    // Initializes the data buffer. Is (and should be) only called in constructor.
    virtual void initialize(common::offset_t numValues);

    template<typename T>
    void templateCopyArrowArray(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);
    // TODO(Guodong/Ziyi): The conversion from string to values should be handled inside ReadFile.
    template<typename T>
    void templateCopyValuesAsString(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    virtual inline common::page_idx_t getNumPagesForBuffer() const {
        return getNumPagesForBytes(numBytes);
    }

    common::offset_t getOffsetInBuffer(common::offset_t pos) const;

protected:
    common::LogicalType dataType;
    uint32_t numBytesPerValue;
    uint64_t numBytes;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<NullColumnChunk> nullChunk;
    std::vector<std::unique_ptr<ColumnChunk>> childrenChunks;
    const common::CopyDescription* copyDescription;
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

    void setRangeNoNull(common::offset_t startPosInChunk, uint32_t numValuesToSet);

    inline void resetNullBuffer() { memset(buffer.get(), 0 /* non null */, numBytes); }

protected:
    uint64_t numBytesForValues(common::offset_t numValues) const {
        // 8 values per byte, and we need a buffer size which is a multiple of 8 bytes
        return ceil(numValues / 8.0 / 8.0) * 8;
    }
    void initialize(common::offset_t numValues) final {
        numBytesPerValue = 0;
        numBytes = numBytesForValues(numValues);
        // Each byte defaults to 0, indicating everything is non-null
        buffer = std::make_unique<uint8_t[]>(numBytes);
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
