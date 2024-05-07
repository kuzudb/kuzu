#include "storage/store/rel_table.h"

#include "common/cast.h"
#include "common/exception/message.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/rel_table_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDetachDeleteState::RelDetachDeleteState() {
    auto tempSharedState = std::make_shared<DataChunkState>();
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    relIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    dstNodeIDVector->setState(tempSharedState);
    relIDVector->setState(tempSharedState);
}

RelTable::RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
    MemoryManager* memoryManager, RelTableCatalogEntry* relTableEntry, WAL* wal,
    bool enableCompression)
    : Table{relTableEntry, relsStoreStats, memoryManager, wal} {
    fwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        relTableEntry, relsStoreStats, RelDataDirection::FWD, enableCompression);
    bwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        relTableEntry, relsStoreStats, RelDataDirection::BWD, enableCompression);
}

void RelTable::initializeReadState(Transaction* transaction, RelDataDirection direction,
    const std::vector<column_id_t>& columnIDs, RelTableReadState& readState) {
    auto& dataState =
        common::ku_dynamic_cast<TableDataReadState&, RelDataReadState&>(*readState.dataReadState);
    return direction == common::RelDataDirection::FWD ?
               fwdRelTableData->initializeReadState(transaction, columnIDs, *readState.nodeIDVector,
                   dataState) :
               bwdRelTableData->initializeReadState(transaction, columnIDs, *readState.nodeIDVector,
                   dataState);
}

void RelTable::readInternal(Transaction* transaction, TableReadState& readState) {
    auto& relReadState = ku_dynamic_cast<TableReadState&, RelTableReadState&>(readState);
    scan(transaction, relReadState);
}

void RelTable::insert(Transaction* transaction, TableInsertState& insertState) {
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    if (localTable->insert(insertState)) {
        auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
        relsStats->updateNumTuplesByValue(tableID, 1);
    }
}

void RelTable::update(Transaction* transaction, TableUpdateState& updateState) {
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->update(updateState);
}

void RelTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    if (localTable->delete_(deleteState)) {
        auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
        relsStats->updateNumTuplesByValue(tableID, -1);
    }
}

void RelTable::detachDelete(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState) {
    KU_ASSERT(srcNodeIDVector->state->getSelVector().getSelSize() == 1);
    auto tableData =
        direction == RelDataDirection::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    auto reverseTableData =
        direction == RelDataDirection::FWD ? bwdRelTableData.get() : fwdRelTableData.get();
    std::vector<column_id_t> relIDColumns = {REL_ID_COLUMN_ID};
    auto relIDVectors = std::vector<ValueVector*>{deleteState->dstNodeIDVector.get(),
        deleteState->relIDVector.get()};
    auto relReadState = std::make_unique<RelTableReadState>(relIDColumns, direction);
    relReadState->nodeIDVector = srcNodeIDVector;
    relReadState->outputVectors = relIDVectors;
    initializeReadState(transaction, direction, relIDColumns, *relReadState);
    row_idx_t numRelsDeleted = detachDeleteForCSRRels(transaction, tableData, reverseTableData,
        srcNodeIDVector, relReadState.get(), deleteState);
    auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
    relsStats->updateNumTuplesByValue(tableID, -numRelsDeleted);
}

void RelTable::checkIfNodeHasRels(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector) {
    KU_ASSERT(srcNodeIDVector->state->isFlat());
    auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto res = direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->checkIfNodeHasRels(transaction, nodeOffset) :
                   bwdRelTableData->checkIfNodeHasRels(transaction, nodeOffset);
    if (res) {
        throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
            tableName, std::to_string(nodeOffset),
            RelDataDirectionUtils::relDirectionToString(direction)));
    }
}

// TODO(Guodong): Rework detach delete to go through local storage.
row_idx_t RelTable::detachDeleteForCSRRels(Transaction* transaction, RelTableData* tableData,
    RelTableData* reverseTableData, ValueVector* srcNodeIDVector,
    RelTableReadState* relDataReadState, RelDetachDeleteState* deleteState) {
    row_idx_t numRelsDeleted = 0;
    auto tempState = deleteState->dstNodeIDVector->state.get();
    while (relDataReadState->hasMoreToRead(transaction)) {
        scan(transaction, *relDataReadState);
        auto numRelsScanned = tempState->getSelVector().getSelSize();
        tempState->getSelVectorUnsafe().setToFiltered(1);
        for (auto i = 0u; i < numRelsScanned; i++) {
            tempState->getSelVectorUnsafe()[0] = i;
            auto deleted =
                tableData->delete_(transaction, srcNodeIDVector, deleteState->relIDVector.get());
            auto reverseDeleted = reverseTableData->delete_(transaction,
                deleteState->dstNodeIDVector.get(), deleteState->relIDVector.get());
            KU_ASSERT(deleted == reverseDeleted);
            numRelsDeleted += (deleted && reverseDeleted);
        }
        tempState->getSelVectorUnsafe().setToUnfiltered();
    }
    return numRelsDeleted;
}

void RelTable::scan(Transaction* transaction, RelTableReadState& scanState) {
    auto tableData = getDirectedTableData(scanState.direction);
    tableData->scan(transaction, *scanState.dataReadState, *scanState.nodeIDVector,
        scanState.outputVectors);
}

void RelTable::addColumn(Transaction* transaction, const Property& property,
    ValueVector* defaultValueVector) {
    auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
    relsStats->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics{!defaultValueVector->hasNoNullsGuarantee()});
    relsStats->addMetadataDAHInfo(tableID, *property.getDataType());
    fwdRelTableData->addColumn(transaction,
        RelDataDirectionUtils::relDirectionToString(RelDataDirection::FWD),
        fwdRelTableData->getNbrIDColumn()->getMetadataDA(),
        *relsStats->getColumnMetadataDAHInfo(transaction, tableID, fwdRelTableData->getNumColumns(),
            RelDataDirection::FWD),
        property, defaultValueVector, relsStats);
    bwdRelTableData->addColumn(transaction,
        RelDataDirectionUtils::relDirectionToString(RelDataDirection::BWD),
        bwdRelTableData->getNbrIDColumn()->getMetadataDA(),
        *relsStats->getColumnMetadataDAHInfo(transaction, tableID, bwdRelTableData->getNumColumns(),
            RelDataDirection::BWD),
        property, defaultValueVector, relsStats);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

void RelTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    wal->addToUpdatedTables(tableID);
    auto localRelTable = ku_dynamic_cast<LocalTable*, LocalRelTable*>(localTable);
    fwdRelTableData->prepareLocalTableToCommit(transaction,
        localRelTable->getTableData(RelDataDirection::FWD));
    bwdRelTableData->prepareLocalTableToCommit(transaction,
        localRelTable->getTableData(RelDataDirection::BWD));
    prepareCommit();
}

void RelTable::prepareRollback(LocalTable* localTable) {
    localTable->clear();
}

void RelTable::prepareCommit() {
    fwdRelTableData->prepareCommit();
    bwdRelTableData->prepareCommit();
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
