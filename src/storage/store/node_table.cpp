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
    NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs, MemoryManager* memoryManager,
    WAL* wal, bool readOnly, bool enableCompression)
    : Table{nodeTableSchema, nodesStatisticsAndDeletedIDs, memoryManager, wal},
      pkColumnID{nodeTableSchema->getColumnID(nodeTableSchema->getPrimaryKeyPropertyID())} {
    tableData = std::make_unique<NodeTableData>(dataFH, metadataFH, tableID, bufferManager, wal,
        nodeTableSchema->getProperties(), nodesStatisticsAndDeletedIDs, enableCompression);
    initializePKIndex(nodeTableSchema, readOnly);
}

void NodeTable::initializePKIndex(NodeTableSchema* nodeTableSchema, bool readOnly) {
    if (nodeTableSchema->getPrimaryKey()->getDataType()->getLogicalTypeID() !=
        LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID), readOnly,
            *nodeTableSchema->getPrimaryKey()->getDataType(), *bufferManager, wal);
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

offset_t NodeTable::insert(Transaction* transaction, ValueVector* nodeIDVector,
    const std::vector<common::ValueVector*>& propertyVectors) {
    auto maxNodeOffset = 0;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset =
            ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                ->addNode(tableID);
        if (offset > maxNodeOffset) {
            maxNodeOffset = offset;
        }
        nodeIDVector->setValue(pos, nodeID_t{offset, tableID});
        nodeIDVector->setNull(pos, false);
    }
    if (pkIndex) {
        insertPK(nodeIDVector, propertyVectors[pkColumnID]);
    }
    tableData->insert(transaction, nodeIDVector, propertyVectors);
    return maxNodeOffset;
}

void NodeTable::update(transaction::Transaction* transaction, common::column_id_t columnID,
    common::ValueVector* nodeIDVector, common::ValueVector* propertyVector) {
    // NOTE: We assume all input all flatten now. This is to simplify the implementation.
    // We should optimize this to take unflat input later.
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1 &&
              propertyVector->state->selVector->selectedSize == 1);
    if (columnID == pkColumnID && pkIndex) {
        updatePK(transaction, columnID, nodeIDVector, propertyVector);
    }
    tableData->update(transaction, columnID, nodeIDVector, propertyVector);
}

void NodeTable::delete_(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* pkVector) {
    auto readState = std::make_unique<TableReadState>();
    tableData->initializeReadState(transaction, {pkColumnID}, nodeIDVector, readState.get());
    read(transaction, *readState, nodeIDVector, {pkVector});
    if (pkIndex) {
        pkIndex->delete_(pkVector);
    }
    // TODO(Guodong): We actually have flatten the input here. But the code is left unchanged for
    // now, so we can remove the flattenAll logic later.
    for (auto i = 0; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
            ->deleteNode(tableID, nodeOffset);
    }
    tableData->delete_(transaction, nodeIDVector);
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
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    if (pkIndex) {
        pkIndex->prepareCommit();
    }
    tableData->prepareLocalTableToCommit(transaction, localTable->getLocalTableData(0));
    wal->addToUpdatedTables(tableID);
}

void NodeTable::prepareRollback(LocalTableData* localTable) {
    if (pkIndex) {
        pkIndex->prepareRollback();
    }
    localTable->clear();
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

void NodeTable::updatePK(Transaction* transaction, column_id_t columnID,
    common::ValueVector* keyVector, common::ValueVector* payloadVector) {
    auto pkVector =
        std::make_unique<ValueVector>(*getColumn(pkColumnID)->getDataType(), memoryManager);
    pkVector->state = keyVector->state;
    auto readState = std::make_unique<storage::TableReadState>();
    initializeReadState(transaction, {columnID}, keyVector, readState.get());
    read(transaction, *readState, keyVector, {pkVector.get()});
    pkIndex->delete_(pkVector.get());
    insertPK(keyVector, payloadVector);
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
