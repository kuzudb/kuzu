#include "common/data_chunk/data_chunk_collection.h"

namespace kuzu {
namespace common {

DataChunkCollection::DataChunkCollection(storage::MemoryManager* mm) : mm{mm} {}

void DataChunkCollection::append(DataChunk& chunk) {
    auto numTuplesToAppend = chunk.state->selVector->selectedSize;
    auto numTuplesAppended = 0u;
    while (numTuplesAppended < numTuplesToAppend) {
        if (chunks.empty() ||
            chunks.back().state->selVector->selectedSize == DEFAULT_VECTOR_CAPACITY) {
            allocateChunk(chunk);
        }
        auto& chunkToAppend = chunks.back();
        auto numTuplesToCopy = std::min(numTuplesToAppend - numTuplesAppended,
            DEFAULT_VECTOR_CAPACITY - chunkToAppend.state->selVector->selectedSize);
        for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
            for (auto i = 0u; i < numTuplesToCopy; i++) {
                auto srcPos = chunk.state->selVector->selectedPositions[numTuplesAppended + i];
                auto dstPos = chunkToAppend.state->selVector->selectedSize + i;
                chunkToAppend.getValueVector(vectorIdx)->copyFromVectorData(dstPos,
                    chunk.getValueVector(vectorIdx).get(), srcPos);
            }
        }
        chunkToAppend.state->selVector->selectedSize += numTuplesToCopy;
        numTuplesAppended += numTuplesToCopy;
    }
}

void DataChunkCollection::merge(DataChunk chunk) {
    if (chunks.empty()) {
        initTypes(chunk);
    }
    KU_ASSERT(chunk.getNumValueVectors() == types.size());
    for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
        KU_ASSERT(chunk.getValueVector(vectorIdx)->dataType == types[vectorIdx]);
    }
    chunks.push_back(std::move(chunk));
}

void DataChunkCollection::initTypes(DataChunk& chunk) {
    types.clear();
    types.reserve(chunk.getNumValueVectors());
    for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
        types.push_back(chunk.getValueVector(vectorIdx)->dataType);
    }
}

void DataChunkCollection::allocateChunk(DataChunk& chunk) {
    if (chunks.empty()) {
        types.reserve(chunk.getNumValueVectors());
        for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
            types.push_back(chunk.getValueVector(vectorIdx)->dataType);
        }
    }
    DataChunk newChunk(types.size(), std::make_shared<DataChunkState>());
    for (auto i = 0u; i < types.size(); i++) {
        newChunk.insert(i, std::make_shared<ValueVector>(types[i], mm));
    }
    chunks.push_back(std::move(newChunk));
}

} // namespace common
} // namespace kuzu
