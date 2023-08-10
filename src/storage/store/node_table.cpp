#include "storage/store/node_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
    WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, dataFH{dataFH},
      metadataFH{metadataFH}, pkPropertyID{nodeTableSchema->getPrimaryKey()->getPropertyID()},
      tableID{nodeTableSchema->tableID}, bufferManager{bufferManager}, wal{wal} {
    initializeData(nodeTableSchema);
}

void NodeTable::initializeData(NodeTableSchema* nodeTableSchema) {
    initializeColumns(nodeTableSchema);
    initializePKIndex(nodeTableSchema);
}

void NodeTable::initializeColumns(NodeTableSchema* nodeTableSchema) {
    for (auto& property : nodeTableSchema->getProperties()) {
        propertyColumns[property->getPropertyID()] =
            NodeColumnFactory::createNodeColumn(*property, dataFH, metadataFH, &bufferManager, wal);
    }
}

void NodeTable::initializePKIndex(NodeTableSchema* nodeTableSchema) {
    if (nodeTableSchema->getPrimaryKey()->getDataType()->getLogicalTypeID() !=
        LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
            *nodeTableSchema->getPrimaryKey()->getDataType(), bufferManager, wal);
    }
}

void NodeTable::read(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIds, const std::vector<ValueVector*>& outputVectors) {
    if (nodeIDVector->isSequential()) {
        scan(transaction, nodeIDVector, columnIds, outputVectors);
    } else {
        lookup(transaction, nodeIDVector, columnIds, outputVectors);
    }
}

void NodeTable::scan(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIds, const std::vector<ValueVector*>& outputVectors) {
    assert(columnIds.size() == outputVectors.size() && !nodeIDVector->state->isFlat());
    for (auto i = 0u; i < columnIds.size(); i++) {
        if (columnIds[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            propertyColumns.at(columnIds[i])->scan(transaction, nodeIDVector, outputVectors[i]);
        }
    }
}

void NodeTable::lookup(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIds, const std::vector<ValueVector*>& outputVectors) {
    assert(columnIds.size() == outputVectors.size());
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < columnIds.size(); i++) {
        if (columnIds[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            propertyColumns.at(columnIds[i])->lookup(transaction, nodeIDVector, outputVectors[i]);
        }
    }
}

offset_t NodeTable::insert(Transaction* transaction, ValueVector* primaryKeyVector) {
    auto offset = nodesStatisticsAndDeletedIDs->addNode(tableID);
    if (primaryKeyVector) {
        assert(pkIndex);
        insertPK(offset, primaryKeyVector);
    }
    auto currentNumNodeGroups = getNumNodeGroups(transaction);
    if (offset == StorageUtils::getStartOffsetForNodeGroup(currentNumNodeGroups)) {
        auto newNodeGroup = std::make_unique<NodeGroup>(this);
        newNodeGroup->setNodeGroupIdx(currentNumNodeGroups);
        append(newNodeGroup.get());
    }
    setPropertiesToNull(offset);
    return offset;
}

void NodeTable::update(
    property_id_t propertyID, ValueVector* nodeIDVector, ValueVector* vectorToWriteFrom) {
    assert(propertyColumns.contains(propertyID));
    propertyColumns.at(propertyID)->write(nodeIDVector, vectorToWriteFrom);
}

void NodeTable::delete_(
    Transaction* transaction, ValueVector* nodeIDVector, DeleteState* deleteState) {
    read(transaction, nodeIDVector, {pkPropertyID}, {deleteState->pkVector.get()});
    if (pkIndex) {
        pkIndex->delete_(deleteState->pkVector.get());
    }
    for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    }
}

void NodeTable::append(NodeGroup* nodeGroup) {
    for (auto& [propertyID, column] : propertyColumns) {
        auto columnChunk = nodeGroup->getColumnChunk(propertyID);
        auto numPages = columnChunk->getNumPages();
        auto startPageIdx = dataFH->addNewPages(numPages);
        column->append(columnChunk, startPageIdx, nodeGroup->getNodeGroupIdx());
    }
}

std::unordered_set<property_id_t> NodeTable::getPropertyIDs() const {
    std::unordered_set<property_id_t> propertyIDs;
    for (auto& [propertyID, _] : propertyColumns) {
        propertyIDs.insert(propertyID);
    }
    return propertyIDs;
}

void NodeTable::prepareCommit() {
    if (pkIndex) {
        pkIndex->prepareCommit();
    }
}

void NodeTable::prepareRollback() {
    if (pkIndex) {
        pkIndex->prepareRollback();
    }
}

void NodeTable::checkpointInMemory() {
    for (auto& [_, column] : propertyColumns) {
        column->checkpointInMemory();
    }
    pkIndex->checkpointInMemory();
}

void NodeTable::rollbackInMemory() {
    for (auto& [_, column] : propertyColumns) {
        column->rollbackInMemory();
    }
    pkIndex->rollbackInMemory();
}

void NodeTable::setPropertiesToNull(offset_t offset) {
    for (auto& [_, column] : propertyColumns) {
        column->setNull(offset);
    }
}

void NodeTable::insertPK(offset_t offset, ValueVector* primaryKeyVector) {
    assert(primaryKeyVector->state->selVector->selectedSize == 1);
    auto pkValPos = primaryKeyVector->state->selVector->selectedPositions[0];
    if (primaryKeyVector->isNull(pkValPos)) {
        throw RuntimeException(ExceptionMessage::nullPKException());
    }
    if (!pkIndex->insert(primaryKeyVector, pkValPos, offset)) {
        std::string pkStr = primaryKeyVector->dataType.getLogicalTypeID() == LogicalTypeID::INT64 ?
                                std::to_string(primaryKeyVector->getValue<int64_t>(pkValPos)) :
                                primaryKeyVector->getValue<ku_string_t>(pkValPos).getAsString();
        throw RuntimeException(ExceptionMessage::existedPKException(pkStr));
    }
}

} // namespace storage
} // namespace kuzu
