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
    InMemColumnChunk() = default;

    InMemColumnChunk(common::DataType dataType, common::offset_t startOffset,
        common::offset_t endOffset, uint16_t numBytesForElement, uint64_t numElementsInAPage);

    virtual ~InMemColumnChunk() = default;

    inline uint8_t* getPage(common::page_idx_t pageIdx) {
        assert(pageIdx <= endPageIdx && pageIdx >= startPageIdx);
        auto pageIdxInSet = pageIdx - startPageIdx;
        return pages.get() + (pageIdxInSet * common::BufferPoolConstants::PAGE_4KB_SIZE);
    }
    inline void copyValue(
        common::page_idx_t pageIdx, common::offset_t posInPage, const uint8_t* val) {
        auto elemPosInPageInBytes = posInPage * numBytesForElement;
        memcpy(getPage(pageIdx) + elemPosInPageInBytes, val, numBytesForElement);
    }
    inline uint8_t* getValue(common::offset_t nodeOffset) {
        auto cursor = CursorUtils::getPageElementCursor(nodeOffset, numElementsInAPage);
        auto elemPosInPageInBytes = cursor.elemPosInPage * numBytesForElement;
        return getPage(cursor.pageIdx) + elemPosInPageInBytes;
    }
    inline common::DataType getDataType() const { return dataType; }

    inline uint64_t getNumElementsInAPage() const { return numElementsInAPage; }

    template<typename T, typename... Args>
    void templateCopyValuesToPage(const PageElementCursor& pageCursor, arrow::Array& array,
        uint64_t posInArray, uint64_t numValues, Args... args) {
        const auto& data = array.data();
        auto valuesInArray = data->GetValues<T>(1);
        auto valuesInPage = (T*)(getPage(pageCursor.pageIdx));
        if (data->MayHaveNulls()) {
            for (auto i = 0u; i < numValues; i++) {
                if (data->IsNull(i + posInArray)) {
                    continue;
                }
                valuesInPage[i + pageCursor.elemPosInPage] = valuesInArray[i + posInArray];
            }
        } else {
            for (auto i = 0u; i < numValues; i++) {
                valuesInPage[i + pageCursor.elemPosInPage] = valuesInArray[i + posInArray];
            }
        }
    }

    template<typename T, typename... Args>
    void templateCopyValuesAsStringToPage(const PageElementCursor& pageCursor, arrow::Array& array,
        uint64_t posInArray, uint64_t numValues, Args... args) {
        auto& stringArray = (arrow::StringArray&)array;
        auto data = stringArray.data();
        if (data->MayHaveNulls()) {
            for (auto i = 0u; i < numValues; i++) {
                if (data->IsNull(i + posInArray)) {
                    continue;
                }
                auto value = stringArray.GetView(i + posInArray);
                setValueFromString<T, Args...>(value.data(), value.length(), pageCursor.pageIdx,
                    i + pageCursor.elemPosInPage, args...);
            }
        } else {
            for (auto i = 0u; i < numValues; i++) {
                auto value = stringArray.GetView(i + posInArray);
                setValueFromString<T, Args...>(value.data(), value.length(), pageCursor.pageIdx,
                    i + pageCursor.elemPosInPage, args...);
            }
        }
    }

    template<typename T, typename... Args>
    void setValueFromString(const char* value, uint64_t length, common::page_idx_t pageIdx,
        uint64_t posInPage, Args... args) {
        auto num = common::TypeUtils::convertStringToNumber<T>(value);
        copyValue(pageIdx, posInPage, (uint8_t*)&num);
    }

    virtual void copyStructValueToFields(arrow::Array& array, uint64_t posInArray,
        const common::CopyDescription& copyDescription, common::offset_t nodeOffset,
        uint64_t numValues) {
        assert(false);
    }

protected:
    common::DataType dataType;
    uint16_t numBytesForElement;
    common::page_idx_t startPageIdx;
    common::page_idx_t endPageIdx;
    std::unique_ptr<uint8_t[]> pages;
    uint64_t numElementsInAPage;
};

class InMemStructColumnChunk : public InMemColumnChunk {
public:
    InMemStructColumnChunk(
        common::DataType dataType, common::offset_t startOffset, common::offset_t endOffset);

    void copyStructValueToFields(arrow::Array& array, uint64_t posInArray,
        const common::CopyDescription& copyDescription, common::offset_t nodeOffset,
        uint64_t numValues) override;

    std::vector<InMemColumnChunk*> getInMemColumnChunksForFields();

    uint64_t getMinNumValuesLeftOnPage(common::offset_t nodeOffset);

private:
    static common::field_idx_t getStructFieldIdx(
        std::vector<std::string> structFieldNames, std::string structFieldName);

    void copyValueToStructColumnField(common::offset_t nodeOffset,
        common::field_idx_t structFieldIdx, const std::string& structFieldValue,
        const common::DataType& dataType);

private:
    std::vector<std::unique_ptr<InMemColumnChunk>> inMemColumnChunksForFields;
};

class InMemColumnChunkFactory {
public:
    static std::unique_ptr<InMemColumnChunk> getInMemColumnChunk(common::DataType dataType,
        common::offset_t startOffset, common::offset_t endOffset, uint16_t numBytesForElement,
        uint64_t numElementsInAPage);
};

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(const PageElementCursor& pageCursor,
    arrow::Array& array, uint64_t posInArray, uint64_t numValues);
template<>
void InMemColumnChunk::templateCopyValuesToPage<common::interval_t>(
    const PageElementCursor& pageCursor, arrow::Array& array, uint64_t posInArray,
    uint64_t numValues);
template<>
void InMemColumnChunk::templateCopyValuesToPage<common::ku_list_t>(
    const PageElementCursor& pageCursor, arrow::Array& array, uint64_t posInArray,
    uint64_t numValues);
// Specialized optimization for copy string values from arrow to pages.
// The optimization is to use string_view to avoid creation of std::string.
// Possible switches: date, timestamp, interval, fixed/var list, string
template<>
void InMemColumnChunk::templateCopyValuesToPage<std::string, InMemOverflowFile*, PageByteCursor&,
    common::CopyDescription&>(const PageElementCursor& pageCursor, arrow::Array& array,
    uint64_t posInArray, uint64_t numValues, InMemOverflowFile* overflowFile,
    PageByteCursor& overflowCursor, common::CopyDescription& copyDesc);
template<>
void InMemColumnChunk::templateCopyValuesToPage<std::string, common::CopyDescription&,
    common::offset_t>(const PageElementCursor& pageCursor, arrow::Array& array, uint64_t posInArray,
    uint64_t numValues, common::CopyDescription& copyDesc, common::offset_t nodeOffset);

template<>
void InMemColumnChunk::setValueFromString<bool>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage);
template<>
void InMemColumnChunk::setValueFromString<common::ku_string_t, InMemOverflowFile*, PageByteCursor&>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage,
    InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor);
template<>
void InMemColumnChunk::setValueFromString<uint8_t*, common::CopyDescription&>(const char* value,
    uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage,
    common::CopyDescription& copyDescription);
template<>
void InMemColumnChunk::setValueFromString<common::ku_list_t, InMemOverflowFile*, PageByteCursor&,
    common::CopyDescription&>(const char* value, uint64_t length, common::page_idx_t pageIdx,
    uint64_t posInPage, InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor,
    common::CopyDescription& copyDescription);
template<>
void InMemColumnChunk::setValueFromString<common::interval_t>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage);
template<>
void InMemColumnChunk::setValueFromString<common::date_t>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage);
template<>
void InMemColumnChunk::setValueFromString<common::timestamp_t>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage);

} // namespace storage
} // namespace kuzu
