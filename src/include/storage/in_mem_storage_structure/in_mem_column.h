#pragma once

#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

namespace kuzu {
namespace storage {

class InMemColumn {
public:
    InMemColumn(std::string filePath, common::LogicalType dataType, bool requireNullBits = true);

    // Encode and flush null bits.
    void saveToFile();

    void flushChunk(InMemColumnChunk* chunk);

    std::unique_ptr<InMemColumnChunk> getInMemColumnChunk(common::offset_t startNodeOffset,
        common::offset_t endNodeOffset, const common::CopyDescription* copyDescription) {
        switch (dataType.getPhysicalType()) {
        case common::PhysicalTypeID::STRING:
        case common::PhysicalTypeID::VAR_LIST: {
            return std::make_unique<InMemColumnChunkWithOverflow>(
                dataType, startNodeOffset, endNodeOffset, copyDescription, inMemOverflowFile.get());
        }
        case common::PhysicalTypeID::FIXED_LIST: {
            return std::make_unique<InMemFixedListColumnChunk>(
                dataType, startNodeOffset, endNodeOffset, copyDescription);
        }
        case common::PhysicalTypeID::STRUCT: {
            auto inMemStructColumnChunk = std::make_unique<InMemStructColumnChunk>(
                dataType, startNodeOffset, endNodeOffset, copyDescription);
            for (auto& fieldColumn : childColumns) {
                inMemStructColumnChunk->addFieldChunk(fieldColumn->getInMemColumnChunk(
                    startNodeOffset, endNodeOffset, copyDescription));
            }
            return inMemStructColumnChunk;
        }
        default: {
            return std::make_unique<InMemColumnChunk>(
                dataType, startNodeOffset, endNodeOffset, copyDescription);
        }
        }
    }

    inline common::LogicalType getDataType() { return dataType; }
    inline InMemOverflowFile* getInMemOverflowFile() { return inMemOverflowFile.get(); }

protected:
    std::string filePath;
    std::unique_ptr<FileHandle> fileHandle;
    common::LogicalType dataType;
    std::unique_ptr<InMemColumn> nullColumn;
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
    std::vector<std::unique_ptr<InMemColumn>> childColumns;
};

} // namespace storage
} // namespace kuzu
