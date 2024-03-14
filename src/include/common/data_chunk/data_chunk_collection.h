#pragma once

#include "common/data_chunk/data_chunk.h"

namespace kuzu {
namespace common {

// TODO(Guodong): Should rework this to use ColumnChunk.
class DataChunkCollection {
public:
    explicit DataChunkCollection(storage::MemoryManager* mm);
    DELETE_COPY_DEFAULT_MOVE(DataChunkCollection);

    void append(DataChunk& chunk);
    inline const std::vector<common::DataChunk>& getChunks() const { return chunks; }
    inline std::vector<common::DataChunk>& getChunksUnsafe() { return chunks; }
    inline uint64_t getNumChunks() const { return chunks.size(); }
    inline const DataChunk& getChunk(uint64_t idx) const {
        KU_ASSERT(idx < chunks.size());
        return chunks[idx];
    }
    inline DataChunk& getChunkUnsafe(uint64_t idx) {
        KU_ASSERT(idx < chunks.size());
        return chunks[idx];
    }
    inline void merge(DataChunkCollection* other) {
        for (auto& chunk : other->chunks) {
            merge(std::move(chunk));
        }
    }
    void merge(DataChunk chunk);

private:
    void allocateChunk(DataChunk& chunk);

    void initTypes(DataChunk& chunk);

private:
    storage::MemoryManager* mm;
    std::vector<LogicalType> types;
    std::vector<DataChunk> chunks;
};

} // namespace common
} // namespace kuzu
