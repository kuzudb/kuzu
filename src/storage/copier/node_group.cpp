#include "storage/copier/node_group.h"

#include "common/constants.h"
#include "storage/store/node_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroup::NodeGroup(TableSchema* schema, CopyDescription* copyDescription)
    : nodeGroupIdx{UINT64_MAX}, numNodes{0} {
    for (auto& property : schema->properties) {
        chunks[property->getPropertyID()] =
            ColumnChunkFactory::createColumnChunk(*property->getDataType(), copyDescription);
    }
}

NodeGroup::NodeGroup(NodeTable* table) {
    auto propertyIDs = table->getPropertyIDs();
    for (auto propertyID : propertyIDs) {
        chunks[propertyID] = ColumnChunkFactory::createColumnChunk(
            table->getPropertyColumn(propertyID)->getDataType());
    }
}

void NodeGroup::resetToEmpty() {
    numNodes = 0;
    nodeGroupIdx = UINT64_MAX;
    for (auto& [_, chunk] : chunks) {
        chunk->resetToEmpty();
    }
}

uint64_t NodeGroup::append(
    ResultSet* resultSet, std::vector<DataPos> dataPoses, uint64_t numValuesToAppend) {
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numNodes);
    auto serialSkip = 0u;
    for (auto i = 0u; i < chunks.size(); i++) {
        auto chunk = chunks[i].get();
        if (chunk->getDataType().getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            serialSkip++;
            continue;
        }
        auto dataPos = dataPoses[i - serialSkip];
        chunk->append(resultSet->getValueVector(dataPos).get(), numNodes, numValuesToAppendInChunk);
    }
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
