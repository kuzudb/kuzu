#pragma once

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/store/column_chunk_data.h"
#include "transaction/transaction.h"

namespace kuzu::storage {

class ColumnReader;

template<typename OutputType>
using read_value_from_page_func_t = std::function<void(uint8_t*, PageCursor&, OutputType, uint32_t,
    uint64_t, const CompressionMetadata&)>;

template<typename OutputType>
using read_values_from_page_func_t = std::function<void(uint8_t*, PageCursor&, OutputType, uint32_t,
    uint64_t, const CompressionMetadata&)>;

using filter_func_t = std::function<bool(common::offset_t, common::offset_t)>;

struct ColumnReaderFactory {
    static std::unique_ptr<ColumnReader> createColumnReader(common::PhysicalTypeID dataType,
        BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal);
};

class ColumnReader {
public:
    ColumnReader(BMFileHandle* dataFH, BufferManager* bufferManager, WAL* wal);

    virtual ~ColumnReader() = default;

    virtual void readCompressedValueToPage(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::offset_t nodeOffset,
        uint8_t* result, uint32_t offsetInResult,
        read_value_from_page_func_t<uint8_t*> readFunc) = 0;

    virtual void readCompressedValueToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::offset_t nodeOffset,
        common::ValueVector* result, uint32_t offsetInResult,
        read_value_from_page_func_t<common::ValueVector*> readFunc) = 0;

    virtual uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, uint8_t* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<uint8_t*> readFunc,
        std::function<bool(common::offset_t, common::offset_t)> filterFunc) = 0;

    virtual uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage, common::ValueVector* result,
        uint32_t startOffsetInResult, uint64_t startNodeOffset, uint64_t endNodeOffset,
        read_values_from_page_func_t<common::ValueVector*> readFunc,
        std::function<bool(common::offset_t, common::offset_t)> filterFunc) = 0;

protected:
    std::pair<common::offset_t, PageCursor> getOffsetAndCursor(common::offset_t nodeOffset,
        const ColumnChunkMetadata& metadata, uint64_t numValuesPerPage);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    PageCursor getPageCursorForOffsetInGroup(common::offset_t offsetInChunk,
        common::page_idx_t groupPageIdx, uint64_t numValuesPerPage) const;

private:
    BMFileHandle* dataFH;
    BufferManager* bufferManager;
    WAL* wal;
};

} // namespace kuzu::storage
