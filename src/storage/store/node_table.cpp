#include "storage/store/node_table.h"

#include "catalog/node_table_schema.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "storage/store/node_table_data.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    catalog::NodeTableSchema* nodeTableSchema,
    NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
    WAL* wal, common::AccessMode accessMode, bool enableCompression)
    : Table{nodeTableSchema, nodesStatisticsAndDeletedIDs, bufferManager, wal},
      pkColumnID{nodeTableSchema->getColumnID(nodeTableSchema->getPrimaryKeyPropertyID())} {
    tableData = std::make_unique<NodeTableData>(dataFH, metadataFH, tableID, &bufferManager, wal,
        nodeTableSchema->getProperties(), nodesStatisticsAndDeletedIDs, enableCompression);
    initializePKIndex(nodeTableSchema, accessMode);
}

void NodeTable::initializePKIndex(NodeTableSchema* nodeTableSchema, AccessMode accessMode) {
    if (nodeTableSchema->getPrimaryKey()->getDataType()->getLogicalTypeID() !=
        LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID), accessMode,
            *nodeTableSchema->getPrimaryKey()->getDataType(), bufferManager, wal);
    }
}

void NodeTable::read(Transaction* transaction, TableReadState& readState, ValueVector* nodeIDVector,
    const std::vector<ValueVector*>& outputVectors) {
    if (nodeIDVector->isSequential()) {
        tableData->scan(transaction, readState, nodeIDVector, outputVectors);
    } else {
        tableData->lookup(transaction, readState, nodeIDVector, outputVectors);
    }
}

void NodeTable::insert(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<common::ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset =
            ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                ->addNode(tableID);
        nodeIDVector->setValue(pos, nodeID_t{offset, tableID});
        nodeIDVector->setNull(pos, false);
    }
    if (pkIndex) {
        insertPK(nodeIDVector, propertyVectors[pkColumnID]);
    }
    tableData->insert(transaction, nodeIDVector, propertyVectors);
    wal->addToUpdatedTables(tableID);
}

void NodeTable::delete_(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* pkVector) {
    auto readState = std::make_unique<TableReadState>();
    tableData->initializeReadState(transaction, {pkColumnID}, nodeIDVector, readState.get());
    read(transaction, *readState, nodeIDVector, {pkVector});
    if (pkIndex) {
        pkIndex->delete_(pkVector);
    }
    for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
            ->deleteNode(tableID, nodeOffset);
    }
}

void NodeTable::addColumn(transaction::Transaction* transaction, const catalog::Property& property,
    common::ValueVector* defaultValueVector) {
    auto nodesStats =
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics);
    nodesStats->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics(!defaultValueVector->hasNoNullsGuarantee()));
    nodesStats->addMetadataDAHInfo(tableID, *property.getDataType());
    tableData->addColumn(transaction, tableData->getColumn(pkColumnID)->getMetadataDA(),
        *nodesStats->getMetadataDAHInfo(transaction, tableID, tableData->getNumColumns()), property,
        defaultValueVector, nodesStats);
    wal->addToUpdatedTables(tableID);
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
    tableData->checkpointInMemory();
    if (pkIndex) {
        pkIndex->checkpointInMemory();
    }
}

void NodeTable::rollbackInMemory() {
    tableData->rollbackInMemory();
    if (pkIndex) {
        pkIndex->rollbackInMemory();
    }
}

void NodeTable::insertPK(ValueVector* nodeIDVector, ValueVector* primaryKeyVector) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(nodeIDPos);
        auto pkPos = primaryKeyVector->state->selVector->selectedPositions[i];
        if (primaryKeyVector->isNull(pkPos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(primaryKeyVector, pkPos, offset)) {
            std::string pkStr =
                primaryKeyVector->dataType.getLogicalTypeID() == LogicalTypeID::INT64 ?
                    std::to_string(primaryKeyVector->getValue<int64_t>(pkPos)) :
                    primaryKeyVector->getValue<ku_string_t>(pkPos).getAsString();
            throw RuntimeException(ExceptionMessage::existedPKException(pkStr));
        }
    }
}

} // namespace storage
} // namespace kuzu
