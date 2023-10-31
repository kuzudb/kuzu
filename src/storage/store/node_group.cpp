#include "storage/store/node_group.h"

#include "common/constants.h"
#include "storage/store/node_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroup::NodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
    bool enableCompression, bool needFinalize, uint64_t capacity)
    : nodeGroupIdx{UINT64_MAX}, numNodes{0} {
    chunks.reserve(columnTypes.size());
    for (auto& type : columnTypes) {
        chunks.push_back(ColumnChunkFactory::createColumnChunk(
            *type, enableCompression, needFinalize, capacity));
    }
}

NodeGroup::NodeGroup(TableData* table) : nodeGroupIdx{UINT64_MAX}, numNodes{0} {
    chunks.reserve(table->getNumColumns());
    for (auto columnID = 0u; columnID < table->getNumColumns(); columnID++) {
        chunks.push_back(ColumnChunkFactory::createColumnChunk(
            table->getColumn(columnID)->getDataType(), table->compressionEnabled()));
    }
}

void NodeGroup::resetToEmpty() {
    numNodes = 0;
    nodeGroupIdx = UINT64_MAX;
    for (auto& chunk : chunks) {
        chunk->resetToEmpty();
    }
}

uint64_t NodeGroup::append(const std::vector<ValueVector*>& columnVectors,
    DataChunkState* columnState, uint64_t numValuesToAppend) {
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numNodes);
    auto serialSkip = 0u;
    auto originalSize = columnState->selVector->selectedSize;
    columnState->selVector->selectedSize = numValuesToAppendInChunk;
    for (auto i = 0u; i < chunks.size(); i++) {
        auto chunk = chunks[i].get();
        if (chunk->getDataType().getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            serialSkip++;
            continue;
        }
        assert(chunk->getDataType() == columnVectors[i - serialSkip]->dataType);
        chunk->append(columnVectors[i - serialSkip], numNodes);
    }
    columnState->selVector->selectedSize = originalSize;
    numNodes += numValuesToAppendInChunk;
    return numValuesToAppendInChunk;
}

offset_t NodeGroup::append(NodeGroup* other, offset_t offsetInOtherNodeGroup) {
    assert(other->chunks.size() == chunks.size());
    auto numNodesToAppend = std::min(
        other->numNodes - offsetInOtherNodeGroup, StorageConstants::NODE_GROUP_SIZE - numNodes);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->append(
            other->chunks[i].get(), offsetInOtherNodeGroup, numNodes, numNodesToAppend);
    }
    numNodes += numNodesToAppend;
    return numNodesToAppend;
}

void NodeGroup::write(DataChunk* dataChunk, vector_idx_t offsetVectorIdx) {
    assert(dataChunk->getNumValueVectors() == chunks.size() + 1);
    auto offsetVector = dataChunk->getValueVector(offsetVectorIdx).get();
    vector_idx_t vectorIdx = 0, chunkIdx = 0;
    for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
        if (i == offsetVectorIdx) {
            vectorIdx++;
            continue;
        }
        assert(vectorIdx < dataChunk->getNumValueVectors());
        chunks[chunkIdx++]->write(dataChunk->getValueVector(vectorIdx++).get(), offsetVector);
    }
    numNodes += offsetVector->state->selVector->selectedSize;
}

void NodeGroup::finalize(uint64_t nodeGroupIdx_) {
    nodeGroupIdx = nodeGroupIdx_;
    for (auto i = 0u; i < chunks.size(); i++) {
        auto finalizedChunk = chunks[i]->finalize();
        if (finalizedChunk) {
            chunks[i] = std::move(finalizedChunk);
        }
    }
}

std::unique_ptr<NodeGroup> NodeGroupFactory::createNodeGroup(common::ColumnDataFormat dataFormat,
    const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes, bool enableCompression,
    bool needFinalize, uint64_t capacity) {
    return dataFormat == ColumnDataFormat::REGULAR_COL ?
               std::make_unique<NodeGroup>(columnTypes, enableCompression, needFinalize, capacity) :
               std::make_unique<CSRNodeGroup>(columnTypes, enableCompression, needFinalize);
}

} // namespace storage
} // namespace kuzu
