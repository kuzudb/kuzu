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

class InMemColumnChunk {
public:
    InMemColumnChunk(common::DataType dataType, common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, bool requireNullBits = true);

    inline common::DataType getDataType() const { return dataType; }

    template<typename T>
    inline T getValue(common::offset_t pos) const {
        return ((T*)buffer.get())[pos];
    }

    void setValue(uint8_t* val, common::offset_t pos);

    inline bool isNull(common::offset_t pos) const {
        assert(nullChunk);
        return nullChunk->getValue<bool>(pos);
    }
    inline uint8_t* getData() const { return buffer.get(); }
    inline uint64_t getNumBytesPerValue() const { return numBytesPerValue; }
    inline uint64_t getNumBytes() const { return numBytes; }
    inline InMemColumnChunk* getChildChunk(common::vector_idx_t childIdx) {
        return childChunks[childIdx].get();
    }
    inline InMemColumnChunk* getNullChunk() { return nullChunk.get(); }
    void flush(common::FileInfo* walFileInfo);

    template<typename T, typename... Args>
    void templateCopyValuesToPage(arrow::Array& array, Args... args) {
        const auto& arrayData = array.data();
        auto valuesInArray = arrayData->GetValues<T>(1 /* value buffer */);
        auto valuesInChunk = (T*)(buffer.get());
        if (arrayData->MayHaveNulls()) {
            for (auto i = 0u; i < array.length(); i++) {
                if (arrayData->IsNull(i)) {
                    continue;
                }
                valuesInChunk[i] = valuesInArray[i];
                nullChunk->setValue<bool>(false, i);
            }
        } else {
            for (auto i = 0u; i < array.length(); i++) {
                valuesInChunk[i] = valuesInArray[i];
                nullChunk->setValue<bool>(false, i);
            }
        }
    }

    template<typename T, typename... Args>
    void templateCopyValuesAsStringToPage(arrow::Array& array, Args... args) {
        auto& stringArray = (arrow::StringArray&)array;
        auto arrayData = stringArray.data();
        if (arrayData->MayHaveNulls()) {
            for (auto i = 0u; i < stringArray.length(); i++) {
                if (arrayData->IsNull(i)) {
                    continue;
                }
                auto value = stringArray.GetView(i);
                setValueFromString<T, Args...>(value.data(), value.length(), i, args...);
                nullChunk->setValue<bool>(false, i);
            }
        } else {
            for (auto i = 0u; i < stringArray.length(); i++) {
                auto value = stringArray.GetView(i);
                setValueFromString<T, Args...>(value.data(), value.length(), i, args...);
                nullChunk->setValue<bool>(false, i);
            }
        }
    }

    template<typename T, typename... Args>
    void setValueFromString(
        const char* value, uint64_t length, common::offset_t pos, Args... args) {
        auto val = common::TypeUtils::convertStringToNumber<T>(value);
        setValue(val, pos);
    }

private:
    template<typename T>
    inline void setValue(T val, common::offset_t pos) {
        ((T*)buffer.get())[pos] = val;
    }

    static uint32_t getDataTypeSizeInColumn(common::DataType& dataType);

private:
    common::DataType dataType;
    common::offset_t startNodeOffset;
    std::uint64_t numBytesPerValue;
    std::uint64_t numBytes;
    std::unique_ptr<uint8_t[]> buffer;
    std::unique_ptr<InMemColumnChunk> nullChunk;
    std::vector<std::unique_ptr<InMemColumnChunk>> childChunks;
};

class InMemStructColumnChunk {
public:
    static common::field_idx_t getStructFieldIdx(
        std::vector<std::string> structFieldNames, std::string structFieldName);

    static void setValueToStructColumnField(InMemColumnChunk* chunk, common::offset_t pos,
        common::field_idx_t structFieldIdx, const std::string& structFieldValue,
        const common::DataType& dataType);
};

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(arrow::Array& array);
template<>
void InMemColumnChunk::templateCopyValuesToPage<common::interval_t>(arrow::Array& array);
template<>
void InMemColumnChunk::templateCopyValuesToPage<common::ku_list_t>(arrow::Array& array);

// Specialized optimization for copy string values from arrow to pages.
// The optimization is to use string_view to avoid creation of std::string.
// Possible switches: date, timestamp, interval, fixed/var list, string, struct
template<>
void InMemColumnChunk::templateCopyValuesToPage<std::string, InMemOverflowFile*, PageByteCursor&,
    common::CopyDescription&>(arrow::Array& array, InMemOverflowFile* overflowFile,
    PageByteCursor& overflowCursor, common::CopyDescription& copyDesc);

// BOOL
template<>
void InMemColumnChunk::setValueFromString<bool>(
    const char* value, uint64_t length, common::offset_t pos);
// STRING
template<>
void InMemColumnChunk::setValueFromString<common::ku_string_t, InMemOverflowFile*, PageByteCursor&>(
    const char* value, uint64_t length, uint64_t pos, InMemOverflowFile* overflowFile,
    PageByteCursor& overflowCursor);
// FIXED_LIST
template<>
void InMemColumnChunk::setValueFromString<uint8_t*, common::CopyDescription&>(
    const char* value, uint64_t length, uint64_t pos, common::CopyDescription& copyDescription);
// VAR_LIST
template<>
void InMemColumnChunk::setValueFromString<common::ku_list_t, InMemOverflowFile*, PageByteCursor&,
    common::CopyDescription&>(const char* value, uint64_t length, uint64_t pos,
    InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor,
    common::CopyDescription& copyDescription);
// STRUCT
template<>
void InMemColumnChunk::setValueFromString<common::struct_entry_t, InMemOverflowFile*,
    PageByteCursor&, common::CopyDescription&>(const char* value, uint64_t length, uint64_t pos,
    InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor,
    common::CopyDescription& copyDescription);
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
