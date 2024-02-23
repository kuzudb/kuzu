#include "common/data_chunk/data_chunk_collection.h"

namespace kuzu {
namespace common {

DataChunkCollection::DataChunkCollection(storage::MemoryManager* mm) : mm{mm} {}

void DataChunkCollection::append(DataChunk& chunk) {
    auto numTuplesToAppend = chunk.state->selVector->selectedSize;
    auto chunkToAppendInfo = chunks.empty() ? allocateChunk(chunk) : chunks.back().get();
    auto numTuplesAppended = 0u;
    while (numTuplesAppended < numTuplesToAppend) {
        if (chunkToAppendInfo->state->selVector->selectedSize == DEFAULT_VECTOR_CAPACITY) {
            chunkToAppendInfo = allocateChunk(chunk);
        }
        auto numTuplesToCopy = std::min(numTuplesToAppend - numTuplesAppended,
            DEFAULT_VECTOR_CAPACITY - chunkToAppendInfo->state->selVector->selectedSize);
        for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
            for (auto i = 0u; i < numTuplesToCopy; i++) {
                auto srcPos = chunk.state->selVector->selectedPositions[numTuplesAppended + i];
                auto dstPos = chunkToAppendInfo->state->selVector->selectedSize + i;
                chunkToAppendInfo->getValueVector(vectorIdx)->copyFromVectorData(
                    dstPos, chunk.getValueVector(vectorIdx).get(), srcPos);
            }
        }
        chunkToAppendInfo->state->selVector->selectedSize += numTuplesToCopy;
        numTuplesAppended += numTuplesToCopy;
    }
}

void DataChunkCollection::append(std::unique_ptr<DataChunk> chunk) {
    KU_ASSERT(chunk);
    if (chunks.empty()) {
        initTypes(*chunk);
    }
    KU_ASSERT(chunk->getNumValueVectors() == types.size());
    for (auto vectorIdx = 0u; vectorIdx < chunk->getNumValueVectors(); vectorIdx++) {
        KU_ASSERT(chunk->getValueVector(vectorIdx)->dataType == types[vectorIdx]);
    }
    chunks.push_back(std::move(chunk));
}

void DataChunkCollection::initTypes(DataChunk& chunk) {
    types.reserve(chunk.getNumValueVectors());
    for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
        types.push_back(chunk.getValueVector(vectorIdx)->dataType);
    }
}

std::vector<common::DataChunk*> DataChunkCollection::getChunks() const {
    std::vector<common::DataChunk*> ret;
    ret.reserve(chunks.size());
    for (auto& chunk : chunks) {
        ret.push_back(chunk.get());
    }
    return ret;
}

DataChunk* DataChunkCollection::allocateChunk(DataChunk& chunk) {
    if (chunks.empty()) {
        types.reserve(chunk.getNumValueVectors());
        for (auto vectorIdx = 0u; vectorIdx < chunk.getNumValueVectors(); vectorIdx++) {
            types.push_back(chunk.getValueVector(vectorIdx)->dataType);
        }
    }
    auto newChunk = std::make_unique<DataChunk>(types.size(), std::make_shared<DataChunkState>());
    for (auto i = 0u; i < types.size(); i++) {
        newChunk->insert(i, std::make_shared<ValueVector>(types[i], mm));
    }
    chunks.push_back(std::move(newChunk));
    return chunks.back().get();
}

} // namespace common
} // namespace kuzu
