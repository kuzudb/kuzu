#pragma once

#include "common/types/blob.h"
#include "storage/storage_structure/in_mem_file.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StringColumnChunk : public ColumnChunk {
public:
    StringColumnChunk(
        common::LogicalType dataType, std::unique_ptr<common::CSVReaderConfig> csvReaderConfig);

    void resetToEmpty() final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;
    void append(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void update(common::ValueVector* vector, common::vector_idx_t vectorIdx) override;

    template<typename T>
    void setValueFromString(const char* value, uint64_t length, uint64_t pos) {
        throw common::NotImplementedException("VarSizedColumnChunk::setValueFromString");
    }
    template<typename T>
    T getValue(common::offset_t pos) const {
        throw common::NotImplementedException("VarSizedColumnChunk::getValue");
    }

    common::page_idx_t flushOverflowBuffer(BMFileHandle* dataFH, common::page_idx_t startPageIdx);

    inline InMemOverflowFile* getOverflowFile() { return overflowFile.get(); }
    inline common::offset_t getLastOffsetInPage() { return overflowCursor.offsetInPage; }

private:
    template<typename T>
    void templateCopyStringArrowArray(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);
    template<typename KU_TYPE, typename ARROW_TYPE>
    void templateCopyStringValues(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    void appendStringColumnChunk(StringColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    void write(const common::Value& val, uint64_t posToWrite) override;

private:
    std::unique_ptr<InMemOverflowFile> overflowFile;
    PageByteCursor overflowCursor;
};

// BLOB
template<>
void StringColumnChunk::setValueFromString<common::blob_t>(
    const char* value, uint64_t length, uint64_t pos);
// STRING
template<>
void StringColumnChunk::setValueFromString<common::ku_string_t>(
    const char* value, uint64_t length, uint64_t pos);

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(common::offset_t pos) const;

} // namespace storage
} // namespace kuzu
