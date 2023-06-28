#include "storage/store/node_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
    WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs},
      nodeGroupsDataFH{nodeGroupsDataFH}, nodeGroupsMetaFH{nodeGroupsMetaFH},
      tableID{nodeTableSchema->tableID}, bufferManager{bufferManager}, wal{wal} {
    initializeData(nodeTableSchema);
}

void NodeTable::initializeData(NodeTableSchema* nodeTableSchema) {
    initializeColumns(nodeTableSchema);
    initializePKIndex(nodeTableSchema);
}

void NodeTable::initializeColumns(NodeTableSchema* nodeTableSchema) {
    for (auto& property : nodeTableSchema->getAllNodeProperties()) {
        propertyColumns[property.propertyID] = NodeColumnFactory::createNodeColumn(
            property, nodeGroupsDataFH, nodeGroupsMetaFH, &bufferManager, wal);
    }
}

void NodeTable::initializePKIndex(NodeTableSchema* nodeTableSchema) {
    if (nodeTableSchema->getPrimaryKey().dataType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
            nodeTableSchema->getPrimaryKey().dataType, bufferManager, wal);
    }
}

void NodeTable::read(transaction::Transaction* transaction, ValueVector* inputIDVector,
    const std::vector<column_id_t>& columnIds, const std::vector<ValueVector*>& outputVectors) {
    if (inputIDVector->isSequential()) {
        scan(transaction, inputIDVector, columnIds, outputVectors);
    } else {
        lookup(transaction, inputIDVector, columnIds, outputVectors);
    }
}

void NodeTable::write(common::property_id_t propertyID, common::ValueVector* nodeIDVector,
    common::ValueVector* vectorToWriteFrom) {
    assert(propertyColumns.contains(propertyID));
    propertyColumns.at(propertyID)->write(nodeIDVector, vectorToWriteFrom);
}

void NodeTable::scan(Transaction* transaction, ValueVector* inputIDVector,
    const std::vector<column_id_t>& columnIds, const std::vector<ValueVector*>& outputVectors) {
    assert(columnIds.size() == outputVectors.size() && !inputIDVector->state->isFlat());
    for (auto i = 0u; i < columnIds.size(); i++) {
        if (columnIds[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            propertyColumns.at(columnIds[i])->scan(transaction, inputIDVector, outputVectors[i]);
        }
    }
}

void NodeTable::lookup(Transaction* transaction, ValueVector* inputIDVector,
    const std::vector<column_id_t>& columnIds, const std::vector<ValueVector*>& outputVectors) {
    assert(columnIds.size() == outputVectors.size());
    auto pos = inputIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < columnIds.size(); i++) {
        if (columnIds[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            propertyColumns.at(columnIds[i])->lookup(transaction, inputIDVector, outputVectors[i]);
        }
    }
}

void NodeTable::appendNodeGroup(NodeGroup* nodeGroup) {
    for (auto& [propertyID, column] : propertyColumns) {
        auto columnChunk = nodeGroup->getColumnChunk(propertyID);
        auto numPagesToFlush = columnChunk->getNumPages();
        auto startPageIdx = nodeGroupsDataFH->addNewPages(numPagesToFlush);
        column->appendColumnChunk(columnChunk, startPageIdx, nodeGroup->getNodeGroupIdx());
    }
}

offset_t NodeTable::addNodeAndResetProperties() {
    auto nodeOffset = nodesStatisticsAndDeletedIDs->addNode(tableID);
    for (auto& [_, column] : propertyColumns) {
        column->setNull(nodeOffset);
    }
    return nodeOffset;
}

offset_t NodeTable::addNodeAndResetPropertiesWithPK(common::ValueVector* primaryKeyVector) {
    auto nodeOffset = addNodeAndResetProperties();
    assert(primaryKeyVector->state->selVector->selectedSize == 1);
    auto pkValPos = primaryKeyVector->state->selVector->selectedPositions[0];
    if (primaryKeyVector->isNull(pkValPos)) {
        throw RuntimeException("Null is not allowed as a primary key value.");
    }
    if (!pkIndex->insert(primaryKeyVector, pkValPos, nodeOffset)) {
        std::string pkStr = primaryKeyVector->dataType.getLogicalTypeID() == LogicalTypeID::INT64 ?
                                std::to_string(primaryKeyVector->getValue<int64_t>(pkValPos)) :
                                primaryKeyVector->getValue<ku_string_t>(pkValPos).getAsString();
        throw RuntimeException(Exception::getExistedPKExceptionMsg(pkStr));
    }
    return nodeOffset;
}

void NodeTable::deleteNodes(ValueVector* nodeIDVector, ValueVector* primaryKeyVector) {
    assert(nodeIDVector->state == primaryKeyVector->state && nodeIDVector->hasNoNullsGuarantee() &&
           primaryKeyVector->hasNoNullsGuarantee());
    if (nodeIDVector->state->selVector->selectedSize == 1) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        deleteNode(nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            deleteNode(nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
        }
    }
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
    pkIndex->rollback();
}

void NodeTable::deleteNode(offset_t nodeOffset, ValueVector* primaryKeyVector, uint32_t pos) const {
    nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    if (pkIndex) {
        pkIndex->deleteKey(primaryKeyVector, pos);
    }
}

} // namespace storage
} // namespace kuzu
