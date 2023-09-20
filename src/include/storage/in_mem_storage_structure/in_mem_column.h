#pragma once

#include "storage/file_handle.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

namespace kuzu {
namespace storage {

class InMemColumn {
public:
    InMemColumn(std::string filePath, common::LogicalType dataType, bool requireNullBits = true);

    // Encode and flush null bits.
    void saveToFile();

    void flushChunk(InMemColumnChunk* chunk);

    std::unique_ptr<InMemColumnChunk> createInMemColumnChunk(common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, common::CSVReaderConfig* csvReaderConfig) {
        auto csvReaderConfigCopy = csvReaderConfig ? csvReaderConfig->copy() : nullptr;
        switch (dataType.getPhysicalType()) {
        case common::PhysicalTypeID::STRING:
        case common::PhysicalTypeID::VAR_LIST: {
            return std::make_unique<InMemColumnChunkWithOverflow>(dataType, startNodeOffset,
                endNodeOffset, std::move(csvReaderConfigCopy), inMemOverflowFile.get());
        }
        case common::PhysicalTypeID::FIXED_LIST: {
            return std::make_unique<InMemFixedListColumnChunk>(
                dataType, startNodeOffset, endNodeOffset, std::move(csvReaderConfigCopy));
        }
        default: {
            return std::make_unique<InMemColumnChunk>(
                dataType, startNodeOffset, endNodeOffset, std::move(csvReaderConfigCopy));
        }
        }
    }

    inline common::LogicalType getDataType() { return dataType; }

protected:
    std::string filePath;
    std::unique_ptr<FileHandle> fileHandle;
    common::LogicalType dataType;
    std::unique_ptr<InMemColumn> nullColumn;
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
};

} // namespace storage
} // namespace kuzu
