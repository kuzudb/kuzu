#pragma once

#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

namespace kuzu {
namespace storage {
// TODO(GUODONG): Currently, we have both InMemNodeColumn and InMemColumn. This is a temporary
// solution for now to allow gradual refactorings. Eventually, we should only have InMemColumn.
class InMemColumn {
public:
    InMemColumn(std::string filePath, common::DataType dataType, bool requireNullBits = true);

    // Encode and flush null bits.
    void saveToFile();

    void flushChunk(InMemColumnChunk* chunk);

    inline common::DataType getDataType() { return dataType; }
    inline InMemOverflowFile* getInMemOverflowFile() { return inMemOverflowFile.get(); }
    inline uint16_t getNumBytesForValue() const { return numBytesForValue; }

protected:
    std::string filePath;
    uint16_t numBytesForValue;
    std::unique_ptr<FileHandle> fileHandle;
    common::DataType dataType;
    std::unique_ptr<InMemColumn> nullColumn;
    std::unique_ptr<InMemOverflowFile> inMemOverflowFile;
    std::vector<std::unique_ptr<InMemColumn>> childColumns;
};

} // namespace storage
} // namespace kuzu
