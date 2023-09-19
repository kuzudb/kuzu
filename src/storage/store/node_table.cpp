#include "storage/store/node_table.h"

#include "catalog/node_table_schema.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
    WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, dataFH{dataFH},
      metadataFH{metadataFH}, pkColumnID{nodeTableSchema->getColumnID(
                                  nodeTableSchema->getPrimaryKeyPropertyID())},
      tableID{nodeTableSchema->tableID}, bufferManager{bufferManager}, wal{wal} {
    initializeData(nodeTableSchema);
}

void NodeTable::initializeData(NodeTableSchema* nodeTableSchema) {
    initializeColumns(nodeTableSchema);
    initializePKIndex(nodeTableSchema);
}

void NodeTable::initializeColumns(NodeTableSchema* nodeTableSchema) {
    columns.reserve(nodeTableSchema->getNumProperties());
    for (auto& property : nodeTableSchema->getProperties()) {
        columns.push_back(NodeColumnFactory::createNodeColumn(*property, dataFH, metadataFH,
            &bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
            RWPropertyStats(getNodeStatisticsAndDeletedIDs(), tableID, property->getPropertyID())));
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
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    assert(columnIDs.size() == outputVectors.size() && !nodeIDVector->state->isFlat());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            assert(columnIDs[i] < columns.size());
            columns[columnIDs[i]]->scan(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        auto localStorage = transaction->getLocalStorage();
        localStorage->scan(tableID, nodeIDVector, columnIDs, outputVectors);
    }
}

void NodeTable::lookup(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            assert(columnIDs[i] < columns.size());
            columns[columnIDs[i]]->lookup(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->lookup(tableID, nodeIDVector, columnIDs, outputVectors);
    }
}

void NodeTable::insert(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<common::ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    offset_t lastOffset;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodesStatisticsAndDeletedIDs->addNode(tableID);
        nodeIDVector->setValue(pos, nodeID_t{offset, tableID});
        nodeIDVector->setNull(pos, false);
        lastOffset = offset;
    }
    if (pkIndex) {
        insertPK(nodeIDVector, propertyVectors[pkColumnID]);
    }
    auto currentNumNodeGroups = getNumNodeGroups(transaction);
    if (lastOffset >= StorageUtils::getStartOffsetOfNodeGroup(currentNumNodeGroups)) {
        auto newNodeGroup = std::make_unique<NodeGroup>(this);
        newNodeGroup->setNodeGroupIdx(currentNumNodeGroups);
        append(newNodeGroup.get());
    }
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        if (columns[columnID]->getDataType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            continue;
        }
        transaction->getLocalStorage()->update(
            tableID, columnID, nodeIDVector, propertyVectors[columnID]);
    }
    wal->addToUpdatedNodeTables(tableID);
}

void NodeTable::update(Transaction* transaction, column_id_t columnID, ValueVector* nodeIDVector,
    ValueVector* vectorToWriteFrom) {
    assert(columnID < columns.size());
    transaction->getLocalStorage()->update(tableID, columnID, nodeIDVector, vectorToWriteFrom);
}

void NodeTable::update(Transaction* transaction, column_id_t columnID, offset_t nodeOffset,
    ValueVector* propertyVector, sel_t posInPropertyVector) const {
    transaction->getLocalStorage()->update(
        tableID, columnID, nodeOffset, propertyVector, posInPropertyVector);
}

void NodeTable::delete_(
    Transaction* transaction, ValueVector* nodeIDVector, DeleteState* deleteState) {
    read(transaction, nodeIDVector, {pkColumnID}, {deleteState->pkVector.get()});
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
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto columnChunk = nodeGroup->getColumnChunk(columnID);
        auto numPages = columnChunk->getNumPages();
        auto startPageIdx = dataFH->addNewPages(numPages);
        assert(columnID < columns.size());
        columns[columnID]->append(columnChunk, startPageIdx, nodeGroup->getNodeGroupIdx());
    }
}

void NodeTable::addColumn(const catalog::Property& property,
    common::ValueVector* defaultValueVector, transaction::Transaction* transaction) {
    nodesStatisticsAndDeletedIDs->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics(!defaultValueVector->hasNoNullsGuarantee()));
    auto nodeColumn = NodeColumnFactory::createNodeColumn(property, dataFH, metadataFH,
        &bufferManager, wal, transaction,
        RWPropertyStats(nodesStatisticsAndDeletedIDs, tableID, property.getPropertyID()));
    nodeColumn->populateWithDefaultVal(
        property, nodeColumn.get(), defaultValueVector, getNumNodeGroups(transaction));
    columns.push_back(std::move(nodeColumn));
    wal->addToUpdatedNodeTables(tableID);
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
    for (auto& column : columns) {
        column->checkpointInMemory();
    }
    if (pkIndex) {
        pkIndex->checkpointInMemory();
    }
}

void NodeTable::rollbackInMemory() {
    for (auto& column : columns) {
        column->rollbackInMemory();
    }
    if (pkIndex) {
        pkIndex->rollbackInMemory();
    }
}

void NodeTable::insertPK(ValueVector* nodeIDVector, ValueVector* primaryKeyVector) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        if (primaryKeyVector->isNull(pos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(primaryKeyVector, pos, offset)) {
            std::string pkStr =
                primaryKeyVector->dataType.getLogicalTypeID() == LogicalTypeID::INT64 ?
                    std::to_string(primaryKeyVector->getValue<int64_t>(pos)) :
                    primaryKeyVector->getValue<ku_string_t>(pos).getAsString();
            throw RuntimeException(ExceptionMessage::existedPKException(pkStr));
        }
    }
}

} // namespace storage
} // namespace kuzu
