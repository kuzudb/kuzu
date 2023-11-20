#include "storage/store/node_group.h"

#include "common/assert.h"
#include "common/constants.h"
#include "storage/store/column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

NodeGroup::NodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
    bool enableCompression, uint64_t capacity)
    : nodeGroupIdx{UINT64_MAX}, numRows{0} {
    chunks.reserve(columnTypes.size());
    for (auto& type : columnTypes) {
        chunks.push_back(
            ColumnChunkFactory::createColumnChunk(type->copy(), enableCompression, capacity));
    }
}

NodeGroup::NodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression)
    : nodeGroupIdx{UINT64_MAX}, numRows{0} {
    chunks.reserve(columns.size());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        chunks.push_back(ColumnChunkFactory::createColumnChunk(
            columns[columnID]->getDataType()->copy(), enableCompression));
    }
}

void NodeGroup::resetToEmpty() {
    numRows = 0;
    nodeGroupIdx = UINT64_MAX;
    for (auto& chunk : chunks) {
        chunk->resetToEmpty();
    }
}

void NodeGroup::setAllNull() {
    for (auto& chunk : chunks) {
        chunk->getNullChunk()->resetToAllNull();
    }
}

void NodeGroup::setNumValues(common::offset_t numValues) {
    for (auto& chunk : chunks) {
        chunk->setNumValues(numValues);
    }
}

void NodeGroup::resizeChunks(uint64_t newSize) {
    for (auto& chunk : chunks) {
        chunk->resize(newSize);
    }
}

uint64_t NodeGroup::append(const std::vector<ValueVector*>& columnVectors,
    DataChunkState* columnState, uint64_t numValuesToAppend) {
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numRows);
    auto serialSkip = 0u;
    auto originalSize = columnState->selVector->selectedSize;
    columnState->selVector->selectedSize = numValuesToAppendInChunk;
    for (auto i = 0u; i < chunks.size(); i++) {
        auto chunk = chunks[i].get();
        if (chunk->getDataType()->getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            serialSkip++;
            continue;
        }
        KU_ASSERT(*chunk->getDataType() == columnVectors[i - serialSkip]->dataType);
        chunk->append(columnVectors[i - serialSkip]);
    }
    columnState->selVector->selectedSize = originalSize;
    numRows += numValuesToAppendInChunk;
    return numValuesToAppendInChunk;
}

offset_t NodeGroup::append(NodeGroup* other, offset_t offsetInOtherNodeGroup) {
    KU_ASSERT(other->chunks.size() == chunks.size());
    auto numNodesToAppend = std::min(
        other->numRows - offsetInOtherNodeGroup, StorageConstants::NODE_GROUP_SIZE - numRows);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->append(other->chunks[i].get(), offsetInOtherNodeGroup, numNodesToAppend);
    }
    numRows += numNodesToAppend;
    return numNodesToAppend;
}

void NodeGroup::write(DataChunk* dataChunk, vector_idx_t offsetVectorIdx) {
    KU_ASSERT(dataChunk->getNumValueVectors() == chunks.size() + 1);
    auto offsetVector = dataChunk->getValueVector(offsetVectorIdx).get();
    vector_idx_t vectorIdx = 0, chunkIdx = 0;
    for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
        if (i == offsetVectorIdx) {
            vectorIdx++;
            continue;
        }
        KU_ASSERT(vectorIdx < dataChunk->getNumValueVectors());
        writeToColumnChunk(chunkIdx, vectorIdx, dataChunk, offsetVector);
        chunkIdx++;
        vectorIdx++;
    }
    numRows += offsetVector->state->selVector->selectedSize;
}

void NodeGroup::finalize(uint64_t nodeGroupIdx_) {
    nodeGroupIdx = nodeGroupIdx_;
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->finalize();
    }
}

} // namespace storage
} // namespace kuzu
