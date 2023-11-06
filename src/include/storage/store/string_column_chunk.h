#pragma once

#include "common/exception/not_implemented.h"
#include "storage/storage_structure/in_mem_file.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StringColumnChunk : public ColumnChunk {
public:
    explicit StringColumnChunk(common::LogicalType dataType, uint64_t capacity);

    void resetToEmpty() final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void write(common::ValueVector* vector, common::offset_t startOffsetInChunk) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector) final;

    template<typename T>
    T getValue(common::offset_t /*pos*/) const {
        throw common::NotImplementedException("VarSizedColumnChunk::getValue");
    }

    common::page_idx_t flushOverflowBuffer(BMFileHandle* dataFH, common::page_idx_t startPageIdx);

    inline InMemOverflowFile* getOverflowFile() { return overflowFile.get(); }
    inline common::offset_t getLastOffsetInPage() const { return overflowCursor.offsetInPage; }

private:
    void appendStringColumnChunk(StringColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend);

    void setValueFromString(const char* value, uint64_t length, uint64_t pos);

private:
    std::unique_ptr<InMemOverflowFile> overflowFile;
    PageByteCursor overflowCursor;
};

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(common::offset_t pos) const;

} // namespace storage
} // namespace kuzu
