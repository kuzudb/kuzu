#pragma once

#include <unordered_set>

#include "common/data_chunk/data_chunk.h"
#include "processor/data_pos.h"
#include "result_set_descriptor.h"

namespace kuzu {
namespace processor {

class ResultSet {
public:
    explicit ResultSet(uint32_t numDataChunks) : multiplicity{1}, dataChunks(numDataChunks) {}
    ResultSet(ResultSetDescriptor* resultSetDescriptor, storage::MemoryManager* memoryManager);

    inline void insert(uint32_t pos, std::shared_ptr<common::DataChunk> dataChunk) {
        KU_ASSERT(dataChunks.size() > pos);
        dataChunks[pos] = std::move(dataChunk);
    }

    inline std::shared_ptr<common::DataChunk> getDataChunk(data_chunk_pos_t dataChunkPos) {
        return dataChunks[dataChunkPos];
    }
    inline std::shared_ptr<common::ValueVector> getValueVector(const DataPos& dataPos) const {
        return dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
    }

    // Our projection does NOT explicitly remove dataChunk from resultSet. Therefore, caller should
    // always provide a set of positions when reading from multiple dataChunks.
    inline uint64_t getNumTuples(const std::unordered_set<uint32_t>& dataChunksPosInScope) {
        return getNumTuplesWithoutMultiplicity(dataChunksPosInScope) * multiplicity;
    }

    uint64_t getNumTuplesWithoutMultiplicity(
        const std::unordered_set<uint32_t>& dataChunksPosInScope);

public:
    uint64_t multiplicity;
    std::vector<std::shared_ptr<common::DataChunk>> dataChunks;
};

} // namespace processor
} // namespace kuzu
