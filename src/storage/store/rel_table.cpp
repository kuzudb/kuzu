#include "storage/store/rel_table.h"

#include "common/cast.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelTable::RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
    MemoryManager* memoryManager, RelTableSchema* tableSchema, WAL* wal, bool enableCompression)
    : Table{tableSchema, relsStoreStats, memoryManager, wal} {
    fwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        tableSchema, relsStoreStats, RelDataDirection::FWD, enableCompression);
    bwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        tableSchema, relsStoreStats, RelDataDirection::BWD, enableCompression);
}

void RelTable::read(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto& relReadState = ku_dynamic_cast<TableReadState&, RelDataReadState&>(readState);
    if (getTableDataFormat(relReadState.direction) == ColumnDataFormat::REGULAR &&
        !inNodeIDVector->isSequential()) {
        lookup(transaction, relReadState, inNodeIDVector, outputVectors);
    } else {
        scan(transaction, relReadState, inNodeIDVector, outputVectors);
    }
}

void RelTable::insert(Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    fwdRelTableData->insert(transaction, srcNodeIDVector, dstNodeIDVector, propertyVectors);
    bwdRelTableData->insert(transaction, dstNodeIDVector, srcNodeIDVector, propertyVectors);
    auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
    relsStats->updateNumRelsByValue(tableID, 1);
}

void RelTable::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector,
    ValueVector* propertyVector) {
    fwdRelTableData->update(transaction, columnID, srcNodeIDVector, relIDVector, propertyVector);
    bwdRelTableData->update(transaction, columnID, dstNodeIDVector, relIDVector, propertyVector);
}

void RelTable::delete_(Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    auto fwdDeleted =
        fwdRelTableData->delete_(transaction, srcNodeIDVector, dstNodeIDVector, relIDVector);
    auto bwdDeleted =
        bwdRelTableData->delete_(transaction, dstNodeIDVector, srcNodeIDVector, relIDVector);
    KU_ASSERT(fwdDeleted == bwdDeleted);
    if (fwdDeleted && bwdDeleted) {
        auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
        relsStats->updateNumRelsByValue(tableID, -1);
    }
}

void RelTable::scan(Transaction* transaction, RelDataReadState& scanState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto tableData = getDirectedTableData(scanState.direction);
    tableData->scan(transaction, scanState, inNodeIDVector, outputVectors);
}

void RelTable::lookup(Transaction* transaction, RelDataReadState& scanState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto tableData = getDirectedTableData(scanState.direction);
    tableData->lookup(transaction, scanState, inNodeIDVector, outputVectors);
}

void RelTable::addColumn(
    Transaction* transaction, const Property& property, ValueVector* defaultValueVector) {
    auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
    relsStats->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics{!defaultValueVector->hasNoNullsGuarantee()});
    relsStats->addMetadataDAHInfo(tableID, *property.getDataType());
    fwdRelTableData->addColumn(transaction, fwdRelTableData->getAdjColumn()->getMetadataDA(),
        *relsStats->getPropertyMetadataDAHInfo(
            transaction, tableID, fwdRelTableData->getNumColumns(), RelDataDirection::FWD),
        property, defaultValueVector, relsStats);
    bwdRelTableData->addColumn(transaction, bwdRelTableData->getAdjColumn()->getMetadataDA(),
        *relsStats->getPropertyMetadataDAHInfo(
            transaction, tableID, bwdRelTableData->getNumColumns(), RelDataDirection::BWD),
        property, defaultValueVector, relsStats);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

void RelTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    wal->addToUpdatedTables(tableID);
    fwdRelTableData->prepareLocalTableToCommit(transaction, localTable->getLocalTableData(0));
    bwdRelTableData->prepareLocalTableToCommit(transaction, localTable->getLocalTableData(1));
}

void RelTable::prepareRollback(LocalTableData* localTableData) {
    localTableData->clear();
}

void RelTable::checkpointInMemory() {
    fwdRelTableData->checkpointInMemory();
    bwdRelTableData->checkpointInMemory();
}

void RelTable::rollbackInMemory() {
    fwdRelTableData->rollbackInMemory();
    bwdRelTableData->rollbackInMemory();
}

} // namespace storage
} // namespace kuzu
