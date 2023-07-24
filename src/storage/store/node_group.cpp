#include "storage/store/node_group.h"

#include "common/constants.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeGroup::NodeGroup(TableSchema* schema, CopyDescription* copyDescription)
    : nodeGroupIdx{UINT64_MAX}, numNodes{0}, schema{schema}, copyDescription{copyDescription} {
    for (auto& property : schema->properties) {
        chunks[property.propertyID] =
            ColumnChunkFactory::createColumnChunk(property.dataType, copyDescription);
    }
}

uint64_t NodeGroup::append(
    ResultSet* resultSet, std::vector<DataPos> dataPoses, uint64_t numValuesToAppend) {
    auto numValuesToAppendInChunk =
        std::min(numValuesToAppend, StorageConstants::NODE_GROUP_SIZE - numNodes);
    for (auto i = 0u; i < dataPoses.size(); i++) {
        auto dataPos = dataPoses[i];
        auto chunk = chunks[i].get();
        chunk->appendVector(
            resultSet->getValueVector(dataPos).get(), numNodes, numValuesToAppendInChunk);
    }
    numNodes += numValuesToAppendInChunk;
    return numValuesToAppendInChunk;
}

offset_t NodeGroup::appendNodeGroup(NodeGroup* other, offset_t offsetInOtherNodeGroup) {
    assert(other->chunks.size() == chunks.size());
    auto numNodesToAppend = std::min(
        other->numNodes - offsetInOtherNodeGroup, StorageConstants::NODE_GROUP_SIZE - numNodes);
    for (auto i = 0u; i < chunks.size(); i++) {
        chunks[i]->appendColumnChunk(
            other->chunks[i].get(), offsetInOtherNodeGroup, numNodes, numNodesToAppend);
    }
    numNodes += numNodesToAppend;
    return numNodesToAppend;
}

} // namespace storage
} // namespace kuzu
