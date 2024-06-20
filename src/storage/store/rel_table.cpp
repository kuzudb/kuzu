#include "storage/store/rel_table.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/cast.h"
#include "common/exception/message.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/storage_manager.h"
#include "storage/store/rel_table_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDetachDeleteState::RelDetachDeleteState() {
    const auto tempSharedState = std::make_shared<DataChunkState>();
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    relIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    dstNodeIDVector->setState(tempSharedState);
    relIDVector->setState(tempSharedState);
}

RelTable::RelTable(RelTableCatalogEntry* relTableEntry, StorageManager* storageManager,
    MemoryManager* memoryManager, Deserializer* deSer)
    : Table{relTableEntry, storageManager, memoryManager} {
    fwdRelTableData = std::make_unique<RelTableData>(dataFH, bufferManager, wal, relTableEntry,
        RelDataDirection::FWD, enableCompression, deSer);
    bwdRelTableData = std::make_unique<RelTableData>(dataFH, bufferManager, wal, relTableEntry,
        RelDataDirection::BWD, enableCompression, deSer);
}

std::unique_ptr<RelTable> RelTable::loadTable(Deserializer& deSer, const Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem*,
    main::ClientContext*) {
    table_id_t tableID;
    std::string tableName;
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.deserializeValue<std::string>(tableName);
    auto catalogEntry = catalog.getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID)
                            ->ptrCast<RelTableCatalogEntry>();
    KU_ASSERT(catalogEntry->getName() == tableName);
    return std::make_unique<RelTable>(catalogEntry, storageManager, memoryManager, &deSer);
}

void RelTable::initializeScanState(Transaction* transaction, TableScanState& scanState) {
    // Scan always start with committed data first.
    auto& relScanState = scanState.cast<RelTableScanState>();
    relScanState.source = TableScanSource::COMMITTED;

    relScanState.boundNodeOffset =
        scanState.IDVector->readNodeOffset(scanState.IDVector->state->getSelVector()[0]);
    // Check if the node group idx is same as previous scan.
    if (relScanState.nodeGroupIdx != StorageUtils::getNodeGroupIdx(relScanState.boundNodeOffset)) {
        // We need to re-initialize the node group scan state.
        relScanState.nodeGroupScanState.resetState();
        relScanState.nodeGroup = relScanState.direction == RelDataDirection::FWD ?
                                     &fwdRelTableData->getCSRNodeGroup(relScanState.nodeGroupIdx) :
                                     &bwdRelTableData->getCSRNodeGroup(relScanState.nodeGroupIdx);
    }
    relScanState.nodeGroup->initializeScanState(transaction, scanState);
}

bool RelTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    const auto& relScanState = scanState.cast<RelTableScanState>();
    switch (relScanState.source) {
    case TableScanSource::COMMITTED: {
        // TODO:
    } break;
    case TableScanSource::UNCOMMITTED: {
        // TODO:
    } break;
    case TableScanSource::NONE: {
        return false;
    }
    }
    // const auto tableData = getDirectedTableData(relScanState.direction);
    // tableData->scan(transaction, scanState, *scanState.nodeIDVector, scanState.outputVectors);
    return true;
}

void RelTable::insert(Transaction* transaction, TableInsertState& insertState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->insert(transaction, insertState);
}

void RelTable::update(Transaction* transaction, TableUpdateState& updateState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->update(updateState);
}

void RelTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->delete_(transaction, deleteState);
}

void RelTable::detachDelete(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState) {
    KU_ASSERT(srcNodeIDVector->state->getSelVector().getSelSize() == 1);
    const auto tableData =
        direction == RelDataDirection::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    const auto reverseTableData =
        direction == RelDataDirection::FWD ? bwdRelTableData.get() : fwdRelTableData.get();
    std::vector<column_id_t> relIDColumns = {REL_ID_COLUMN_ID};
    const auto relIDVectors = std::vector<ValueVector*>{deleteState->dstNodeIDVector.get(),
        deleteState->relIDVector.get()};
    auto relReadState = std::make_unique<RelTableScanState>(tableID, relIDColumns,
        tableData->getColumns(), direction);
    relReadState->IDVector = srcNodeIDVector;
    relReadState->outputVectors = relIDVectors;
    relReadState->direction = direction;
    const auto nodeOffset =
        relReadState->IDVector->readNodeOffset(relReadState->IDVector->state->getSelVector()[0]);
    relReadState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    initializeScanState(transaction, *relReadState);
    detachDeleteForCSRRels(transaction, tableData, reverseTableData, srcNodeIDVector,
        relReadState.get(), deleteState);
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
    RelTableScanState* relDataReadState, RelDetachDeleteState* deleteState) {
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

// TODO(Guodong): Rework this.
void RelTable::addColumn(Transaction* transaction, const Property& property,
    ValueVector* defaultValueVector) {
    // fwdRelTableData->addColumn(transaction,
    // RelDataDirectionUtils::relDirectionToString(RelDataDirection::FWD),
    // RelDataDirection::FWD),
    // property, defaultValueVector);
    // bwdRelTableData->addColumn(transaction,
    // RelDataDirectionUtils::relDirectionToString(RelDataDirection::BWD),
    // RelDataDirection::BWD),
    // property, defaultValueVector);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    // wal->addToUpdatedTables(tableID);
}

void RelTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    auto& localRelTable = localTable->cast<LocalRelTable>();
    auto startRelOffset = fwdRelTableData->getNumRows();
    KU_ASSERT(startRelOffset == bwdRelTableData->getNumRows());
    const auto dataChunkState = std::make_shared<DataChunkState>();
    std::vector<column_id_t> columnIDsToScan;
    for (auto i = 0u; i < localRelTable.getNumColumns(); i++) {
        columnIDsToScan.push_back(i);
    }
    auto types = LocalRelTable::getTypesForLocalRelTable(*this);
    auto dataChunk = constructDataChunk(types);
    node_group_idx_t nodeGroupToScan = 0;
    const auto numNodeGroupsToScan = localRelTable.getNumNodeGroups();
    auto scanState = std::make_unique<RelTableScanState>(tableID, columnIDsToScan);
    for (auto& vector : dataChunk->valueVectors) {
        scanState->outputVectors.push_back(vector.get());
    }
    // RelID vector.
    // TODO(Guodong): Get rid of the hard-coded 2 here.
    scanState->IDVector = scanState->outputVectors[2];
    scanState->source = TableScanSource::UNCOMMITTED;
    while (nodeGroupToScan < numNodeGroupsToScan) {
        scanState->nodeGroupIdx = nodeGroupToScan;
        scanState->localNodeGroup = &localRelTable.getNodeGroup(scanState->nodeGroupIdx);
        scanState->localNodeGroup->initializeScanState(transaction, *scanState,
            scanState->localNodeGroupScanState);
        while (scanState->localNodeGroup->scan(transaction, *scanState,
            scanState->localNodeGroupScanState)) {
            auto IDVector = scanState->IDVector;
            const auto numRowsScanned = IDVector->state->getSelVector().getSelSize();
            for (auto i = 0u; i < numRowsScanned; i++) {
                const auto pos = IDVector->state->getSelVector().getSelectedPositions()[i];
                IDVector->setValue(pos, nodeID_t{startRelOffset + i, tableID});
            }
            // TODO(Guodong): Should handle src/dst node offsets that were within local storage.
            std::vector<ValueVector*> fwdDataVectors;
            std::vector<ValueVector*> bwdDataVectors;
            for (auto i = 0u; i < scanState->outputVectors.size(); i++) {
                if (i == 0) {
                    bwdDataVectors.push_back(scanState->outputVectors[i]);
                } else if (i == 1) {
                    fwdDataVectors.push_back(scanState->outputVectors[i]);
                } else {
                    fwdDataVectors.push_back(scanState->outputVectors[i]);
                    bwdDataVectors.push_back(scanState->outputVectors[i]);
                }
            }
            fwdRelTableData->append(transaction, scanState->outputVectors[0], fwdDataVectors);
            bwdRelTableData->append(transaction, scanState->outputVectors[1], bwdDataVectors);
        }
        nodeGroupToScan++;
    }
    wal->addToUpdatedTables(tableID);
}

void RelTable::prepareRollback(LocalTable* localTable) {
    localTable->clear();
}

void RelTable::checkpoint(Serializer&) {
    checkpointInMemory();
}

void RelTable::serialize(Serializer& serializer) const {
    Table::serialize(serializer);
    fwdRelTableData->serialize(serializer);
    bwdRelTableData->serialize(serializer);
}

} // namespace storage
} // namespace kuzu
