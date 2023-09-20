#include "storage/store/node_group.h"

#include "common/constants.h"
#include "storage/store/node_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroup::NodeGroup(TableSchema* schema, CSVReaderConfig* csvReaderConfig)
    : nodeGroupIdx{UINT64_MAX}, numNodes{0} {
    chunks.reserve(schema->properties.size());
    for (auto& property : schema->properties) {
        chunks.push_back(
            ColumnChunkFactory::createColumnChunk(*property->getDataType(), csvReaderConfig));
    }
}

NodeGroup::NodeGroup(NodeTable* table) : nodeGroupIdx{UINT64_MAX}, numNodes{0} {
    chunks.reserve(table->getNumColumns());
    for (auto columnID = 0u; columnID < table->getNumColumns(); columnID++) {
        chunks.push_back(
            ColumnChunkFactory::createColumnChunk(table->getColumn(columnID)->getDataType()));
    }
}

void NodeGroup::resetToEmpty() {
    numNodes = 0;
    nodeGroupIdx = UINT64_MAX;
    for (auto& chunk : chunks) {
        chunk->resetToEmpty();
    }
}

uint64_t NodeGroup::append(
    ResultSet* resultSet, std::vector<DataPos> dataPoses, uint64_t numValuesToAppend) {
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numNodes);
    auto serialSkip = 0u;
    auto originalVectorSize =
        resultSet->getDataChunk(dataPoses[0].dataChunkPos)->state->selVector->selectedSize;
    resultSet->getDataChunk(dataPoses[0].dataChunkPos)->state->selVector->selectedSize =
        numValuesToAppendInChunk;
    for (auto i = 0u; i < chunks.size(); i++) {
        auto chunk = chunks[i].get();
        if (chunk->getDataType().getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            serialSkip++;
            continue;
        }
        auto dataPos = dataPoses[i - serialSkip];
        chunk->append(resultSet->getValueVector(dataPos).get(), numNodes, numValuesToAppendInChunk);
    }
    resultSet->getDataChunk(dataPoses[0].dataChunkPos)->state->selVector->selectedSize =
        originalVectorSize;
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

} // namespace storage
} // namespace kuzu
