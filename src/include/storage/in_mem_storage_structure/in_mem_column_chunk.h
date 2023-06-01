#pragma once

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/scalar.h"
#include "common/types/types.h"
#include "storage/copier/table_copy_executor.h"
#include "storage/storage_structure/in_mem_file.h"

namespace kuzu {
namespace storage {

struct StructFieldIdxAndValue {
    StructFieldIdxAndValue(common::struct_field_idx_t fieldIdx, std::string fieldValue)
        : fieldIdx{fieldIdx}, fieldValue{fieldValue} {}

    common::struct_field_idx_t fieldIdx;
    std::string fieldValue;
};

class InMemColumnChunk {
public:
    InMemColumnChunk(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, const common::CopyDescription* copyDescription,
        bool requireNullBits = true);

    virtual ~InMemColumnChunk() = default;

    inline common::LogicalType getDataType() const { return dataType; }

    template<typename T>
    inline T getValue(common::offset_t pos) const {
        return ((T*)buffer.get())[pos];
    }

    void setValueAtPos(const uint8_t* val, common::offset_t pos);

    inline bool isNull(common::offset_t pos) const {
        assert(nullChunk);
        return nullChunk->getValue<bool>(pos);
    }
    inline uint8_t* getData() const { return buffer.get(); }
    inline uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    inline uint64_t getNumBytes() const { return numBytes; }
    inline InMemColumnChunk* getNullChunk() { return nullChunk.get(); }
    virtual void copyArrowArray(arrow::Array& arrowArray);
    virtual void flush(common::FileInfo* walFileInfo);

    template<typename T>
    void templateCopyValuesToPage(arrow::Array& array);
    template<typename T>
    void templateCopyValuesAsStringToPage(arrow::Array& array);

    template<typename T, typename... Args>
    void setValueFromString(
        const char* value, uint64_t length, common::offset_t pos, Args... args) {
        auto val = common::TypeUtils::convertStringToNumber<T>(value);
        setValue(val, pos);
    }

    template<typename T>
    inline void setValue(T val, common::offset_t pos) {
        ((T*)buffer.get())[pos] = val;
    }

private:
    inline virtual common::offset_t getOffsetInBuffer(common::offset_t pos) {
        return pos * numBytesPerValue;
    }

    static uint32_t getDataTypeSizeInColumn(common::LogicalType& dataType);

    void setFixedListVal(arrow::FixedSizeListArray& array, uint64_t pos);

protected:
    common::LogicalType dataType;
    common::offset_t startNodeOffset;
    std::uint64_t numBytesPerValue;
    std::uint64_t numBytes;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<InMemColumnChunk> nullChunk;
    const common::CopyDescription* copyDescription;
};

class InMemColumnChunkWithOverflow : public InMemColumnChunk {
public:
    InMemColumnChunkWithOverflow(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, const common::CopyDescription* copyDescription,
        InMemOverflowFile* inMemOverflowFile)
        : InMemColumnChunk{std::move(dataType), startNodeOffset, endNodeOffset, copyDescription},
          inMemOverflowFile{inMemOverflowFile} {}

    void copyArrowArray(arrow::Array& array) override;

    template<typename T>
    void templateCopyValuesAsStringToPageWithOverflow(arrow::Array& array);

    void copyValuesToPageWithOverflow(arrow::Array& array);

    template<typename T>
    void setValWithOverflow(const char* value, uint64_t length, uint64_t pos) {
        assert(false);
    }

private:
    storage::InMemOverflowFile* inMemOverflowFile;
    PageByteCursor overflowCursor;
};

class InMemStructColumnChunk : public InMemColumnChunk {
public:
    InMemStructColumnChunk(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, const common::CopyDescription* copyDescription);

    inline InMemColumnChunk* getFieldChunk(common::struct_field_idx_t fieldIdx) {
        return fieldChunks[fieldIdx].get();
    }

    inline void addFieldChunk(std::unique_ptr<InMemColumnChunk> fieldChunk) {
        fieldChunks.push_back(std::move(fieldChunk));
    }

    void copyArrowArray(arrow::Array& array) override;

private:
    void setStructFields(const char* value, uint64_t length, uint64_t pos);

    void setValueToStructField(common::offset_t pos, const std::string& structFieldValue,
        common::struct_field_idx_t structFiledIdx);

    std::vector<StructFieldIdxAndValue> parseStructFieldNameAndValues(
        common::LogicalType& type, const std::string& structString);

    std::string parseStructFieldName(const std::string& structString, uint64_t& curPos);

    std::string parseStructFieldValue(const std::string& structString, uint64_t& curPos);

private:
    std::vector<std::unique_ptr<InMemColumnChunk>> fieldChunks;
};

class InMemFixedListColumnChunk : public InMemColumnChunk {
public:
    InMemFixedListColumnChunk(common::LogicalType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, const common::CopyDescription* copyDescription);

    void flush(common::FileInfo* walFileInfo) override;

private:
    common::offset_t getOffsetInBuffer(common::offset_t pos) override;

private:
    uint64_t numElementsInAPage;
};

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(arrow::Array& array);
template<>
void InMemColumnChunk::templateCopyValuesToPage<uint8_t*>(arrow::Array& array);
template<>
void InMemColumnChunk::templateCopyValuesToPage<common::interval_t>(arrow::Array& array);
template<>
void InMemColumnChunk::templateCopyValuesToPage<common::ku_list_t>(arrow::Array& array);

// BOOL
template<>
void InMemColumnChunk::setValueFromString<bool>(
    const char* value, uint64_t length, common::offset_t pos);
// FIXED_LIST
template<>
void InMemColumnChunk::setValueFromString<uint8_t*>(
    const char* value, uint64_t length, uint64_t pos);
// INTERVAL
template<>
void InMemColumnChunk::setValueFromString<common::interval_t>(
    const char* value, uint64_t length, uint64_t pos);
// DATE
template<>
void InMemColumnChunk::setValueFromString<common::date_t>(
    const char* value, uint64_t length, uint64_t pos);
// TIMESTAMP
template<>
void InMemColumnChunk::setValueFromString<common::timestamp_t>(
    const char* value, uint64_t length, uint64_t pos);

} // namespace storage
} // namespace kuzu
