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

NodeTable::NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, AccessMode accessMode,
    NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
    WAL* wal, NodeTableSchema* nodeTableSchema, bool enableCompression)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs},
      pkColumnID{nodeTableSchema->getColumnID(nodeTableSchema->getPrimaryKeyPropertyID())},
      tableID{nodeTableSchema->tableID}, bufferManager{bufferManager}, wal{wal} {
    tableData = std::make_unique<TableData>(dataFH, metadataFH, tableID, &bufferManager, wal,
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

void NodeTable::insert(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<common::ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodesStatisticsAndDeletedIDs->addNode(tableID);
        nodeIDVector->setValue(pos, nodeID_t{offset, tableID});
        nodeIDVector->setNull(pos, false);
    }
    if (pkIndex) {
        insertPK(nodeIDVector, propertyVectors[pkColumnID]);
    }
    tableData->insert(transaction, nodeIDVector, propertyVectors);
    wal->addToUpdatedNodeTables(tableID);
}

void NodeTable::delete_(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* pkVector) {
    read(transaction, nodeIDVector, {pkColumnID}, {pkVector});
    if (pkIndex) {
        pkIndex->delete_(pkVector);
    }
    for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    }
}

void NodeTable::addColumn(const catalog::Property& property,
    common::ValueVector* defaultValueVector, transaction::Transaction* transaction) {
    nodesStatisticsAndDeletedIDs->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics(!defaultValueVector->hasNoNullsGuarantee()));
    nodesStatisticsAndDeletedIDs->addMetadataDAHInfo(tableID, *property.getDataType());
    tableData->addColumn(transaction, property, defaultValueVector, nodesStatisticsAndDeletedIDs);
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
