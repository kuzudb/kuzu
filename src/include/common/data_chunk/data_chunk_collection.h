#pragma once

#include "common/data_chunk/data_chunk.h"

namespace kuzu {
namespace common {

// TODO(Guodong/Ziyi): We should extend this to ColumnDataCollection, which takes ResultSet into
// consideration for storage and scan.
class DataChunkCollection {
public:
    explicit DataChunkCollection(storage::MemoryManager* mm);

    void append(DataChunk& chunk);
    void append(std::unique_ptr<DataChunk> chunk);
    std::vector<common::DataChunk*> getChunks() const;

    inline void merge(DataChunkCollection* other) {
        for (auto& chunk : other->chunks) {
            append(std::move(chunk));
        }
    }

private:
    DataChunk* allocateChunk(DataChunk& chunk);

    void initTypes(DataChunk chunk);

private:
    storage::MemoryManager* mm;
    std::vector<std::unique_ptr<LogicalType>> types;
    std::vector<std::unique_ptr<DataChunk>> chunks;
};

} // namespace common
} // namespace kuzu
