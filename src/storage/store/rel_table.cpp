#include "storage/store/rel_table.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/message.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/storage_manager.h"
#include "storage/store/rel_table_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

RelTable::RelTable(RelTableCatalogEntry* relTableEntry, StorageManager* storageManager,
    MemoryManager* memoryManager, Deserializer* deSer)
    : Table{relTableEntry, storageManager, memoryManager},
      fromNodeTableID{relTableEntry->getSrcTableID()},
      toNodeTableID{relTableEntry->getDstTableID()}, nextRelOffset{0} {
    if (deSer) {
        offset_t nextOffset;
        deSer->deserializeValue<offset_t>(nextOffset);
        nextRelOffset = nextOffset;
    }
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
    offset_t nextRelOffset;
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.deserializeValue<std::string>(tableName);
    deSer.deserializeValue<offset_t>(nextRelOffset);
    auto catalogEntry = catalog.getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID)
                            ->ptrCast<RelTableCatalogEntry>();
    KU_ASSERT(catalogEntry->getName() == tableName);
    auto relTable = std::make_unique<RelTable>(catalogEntry, storageManager, memoryManager, &deSer);
    relTable->nextRelOffset = nextRelOffset;
    return relTable;
}

void RelTable::initializeScanState(Transaction* transaction, TableScanState& scanState) {
    // Scan always start with committed data first.
    auto& relScanState = scanState.cast<RelTableScanState>();

    relScanState.boundNodeOffset = relScanState.boundNodeIDVector->readNodeOffset(
        relScanState.boundNodeIDVector->state->getSelVector()[0]);
    // Check if the node group idx is same as previous scan.
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(relScanState.boundNodeOffset);
    if (relScanState.nodeGroupIdx != nodeGroupIdx) {
        // We need to re-initialize the node group scan state.
        relScanState.nodeGroup = relScanState.direction == RelDataDirection::FWD ?
                                     fwdRelTableData->getNodeGroup(nodeGroupIdx) :
                                     bwdRelTableData->getNodeGroup(nodeGroupIdx);
    }
    if (relScanState.nodeGroup) {
        relScanState.source = TableScanSource::COMMITTED;
        relScanState.nodeGroup->initializeScanState(transaction, scanState);
    } else if (relScanState.localTableScanState) {
        initializeLocalRelScanState(relScanState);
    } else {
        relScanState.source = TableScanSource::NONE;
    }
}

void RelTable::initializeLocalRelScanState(RelTableScanState& relScanState) {
    relScanState.source = TableScanSource::UNCOMMITTED;
    KU_ASSERT(relScanState.localTableScanState);
    auto& localScanState = *relScanState.localTableScanState;
    KU_ASSERT(localScanState.localRelTable);
    localScanState.boundNodeOffset = relScanState.boundNodeOffset;
    localScanState.rowIdxVector->setState(relScanState.rowIdxVector->state);
    localScanState.localRelTable->initializeScan(*relScanState.localTableScanState);
    localScanState.nextRowToScan = 0;
}

bool RelTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    auto& relScanState = scanState.cast<RelTableScanState>();
    switch (relScanState.source) {
    case TableScanSource::COMMITTED: {
        const auto scanResult = relScanState.nodeGroup->scan(transaction, scanState);
        if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
            if (relScanState.localTableScanState) {
                initializeLocalRelScanState(relScanState);
            } else {
                relScanState.source = TableScanSource::NONE;
                return false;
            }
        }
        return true;
    }
    case TableScanSource::UNCOMMITTED: {
        const auto localScanState = relScanState.localTableScanState.get();
        KU_ASSERT(localScanState && localScanState->localRelTable);
        return localScanState->localRelTable->scan(transaction, scanState);
    }
    case TableScanSource::NONE: {
        return false;
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

void RelTable::insert(Transaction* transaction, TableInsertState& insertState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->insert(transaction, insertState);
}

void RelTable::update(Transaction* transaction, TableUpdateState& updateState) {
    const auto& relUpdateState = updateState.cast<RelTableUpdateState>();
    KU_ASSERT(relUpdateState.relIDVector.state->getSelVector().getSelSize() == 1);
    const auto relIDPos = relUpdateState.relIDVector.state->getSelVector()[0];
    if (const auto relOffset = relUpdateState.relIDVector.readNodeOffset(relIDPos);
        relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(loadTable);
        localTable->update(updateState);
    } else {
        fwdRelTableData->update(transaction, relUpdateState.srcNodeIDVector,
            relUpdateState.relIDVector, relUpdateState.columnID, relUpdateState.propertyVector);
        bwdRelTableData->update(transaction, relUpdateState.dstNodeIDVector,
            relUpdateState.relIDVector, relUpdateState.columnID, relUpdateState.propertyVector);
    }
}

bool RelTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& relDeleteState = deleteState.cast<RelTableDeleteState>();
    KU_ASSERT(relDeleteState.relIDVector.state->getSelVector().getSelSize() == 1);
    const auto relIDPos = relDeleteState.relIDVector.state->getSelVector()[0];
    if (const auto relOffset = relDeleteState.relIDVector.readNodeOffset(relIDPos);
        relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(loadTable);
        return localTable->delete_(transaction, deleteState);
    }
    const auto fwdDeleted = fwdRelTableData->delete_(transaction, relDeleteState.srcNodeIDVector,
        relDeleteState.relIDVector);
    if (!fwdDeleted) {
        return false;
    }
    const auto bwdDeleted = bwdRelTableData->delete_(transaction, relDeleteState.dstNodeIDVector,
        relDeleteState.relIDVector);
    return bwdDeleted;
}

void RelTable::detachDelete(Transaction* transaction, RelDataDirection direction, 
    RelTableDeleteState* deleteState) {
    KU_ASSERT(deleteState->srcNodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto tableData =
        direction == RelDataDirection::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    const auto reverseTableData =
        direction == RelDataDirection::FWD ? bwdRelTableData.get() : fwdRelTableData.get();
    std::vector<column_id_t> columnsToScan = {NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
    const auto relIDVectors = std::vector<ValueVector*>{&deleteState->dstNodeIDVector,
        &deleteState->relIDVector};
    const auto relReadState =
        std::make_unique<RelTableScanState>(tableID, columnsToScan, tableData->getColumns(),
            tableData->getCSROffsetColumn(), tableData->getCSRLengthColumn(), direction);
    relReadState->boundNodeIDVector = &deleteState->srcNodeIDVector;
    relReadState->outputVectors = relIDVectors;
    relReadState->direction = direction;
    initializeScanState(transaction, *relReadState);
    detachDeleteForCSRRels(transaction, tableData, reverseTableData, relReadState.get(), 
        deleteState);
}

void RelTable::checkIfNodeHasRels(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector) const {
    KU_ASSERT(srcNodeIDVector->state->isFlat());
    const auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    const auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    if (direction == RelDataDirection::FWD ?
            fwdRelTableData->checkIfNodeHasRels(transaction, nodeOffset) :
            bwdRelTableData->checkIfNodeHasRels(transaction, nodeOffset)) {
        throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
            tableName, std::to_string(nodeOffset),
            RelDataDirectionUtils::relDirectionToString(direction)));
    }
}

row_idx_t RelTable::detachDeleteForCSRRels(Transaction* transaction, RelTableData* tableData, 
    RelTableData* reverseTableData, RelTableScanState* relDataReadState, 
    RelTableDeleteState* deleteState) {
    row_idx_t numRelsDeleted = 0;
    auto tempState = deleteState->dstNodeIDVector.state.get();
    while (scan(transaction, *relDataReadState)) {
        auto numRelsScanned = tempState->getSelVector().getSelSize();
        tempState->getSelVectorUnsafe().setToFiltered(1);
        for (auto i = 0u; i < numRelsScanned; i++) {
            tempState->getSelVectorUnsafe()[0] = i;
            auto deleted = tableData->delete_(transaction, deleteState->srcNodeIDVector, 
                deleteState->relIDVector);
            auto reverseDeleted = reverseTableData->delete_(transaction,
                deleteState->dstNodeIDVector, deleteState->relIDVector);
            KU_ASSERT(deleted == reverseDeleted);
            numRelsDeleted += (deleted && reverseDeleted);
        }
        tempState->getSelVectorUnsafe().setToUnfiltered();
    }
    return numRelsDeleted;
}

void RelTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    if (localTable) {
        localTable->addColumn(transaction, addColumnState);
    }
    fwdRelTableData->addColumn(transaction, addColumnState);
    bwdRelTableData->addColumn(transaction, addColumnState);
}

NodeGroup* RelTable::getOrCreateNodeGroup(node_group_idx_t nodeGroupIdx,
    RelDataDirection direction) const {
    return direction == RelDataDirection::FWD ?
               fwdRelTableData->getOrCreateNodeGroup(nodeGroupIdx) :
               bwdRelTableData->getOrCreateNodeGroup(nodeGroupIdx);
}

void RelTable::prepareCommit(Transaction* transaction, LocalTable* localTable) {
    auto& localRelTable = localTable->cast<LocalRelTable>();
    const auto dataChunkState = std::make_shared<DataChunkState>();
    std::vector<column_id_t> columnIDsToScan;
    for (auto i = 0u; i < localRelTable.getNumColumns(); i++) {
        columnIDsToScan.push_back(i);
    }
    auto types = LocalRelTable::getTypesForLocalRelTable(*this);
    const auto dataChunk = constructDataChunk(types);
    // Update relID in local storage.
    updateRelOffsets(localRelTable);
    // Update src and dst node IDs in local storage.
    if (transaction->hasNewlyInsertedNodes(fromNodeTableID)) {
        updateNodeOffsets(localRelTable, RelDataDirection::FWD,
            transaction->getMaxNodeOffsetBeforeCommit(fromNodeTableID));
    }
    if (transaction->hasNewlyInsertedNodes(toNodeTableID)) {
        updateNodeOffsets(localRelTable, RelDataDirection::BWD,
            transaction->getMaxNodeOffsetBeforeCommit(toNodeTableID));
    }
    // For both forward and backward directions, re-org local storage into compact CSR node groups.
    auto& localNodeGroup = localRelTable.getLocalNodeGroup();
    auto& fwdIndex = localRelTable.getFWDIndex();
    for (auto& [boundNodeOffset, rowIndices] : fwdIndex) {
        auto [nodeGroupIdx, boundOffsetInGroup] =
            StorageUtils::getQuotientRemainder(boundNodeOffset, StorageConstants::NODE_GROUP_SIZE);
        auto& nodeGroup = fwdRelTableData->getOrCreateNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
        prepareCommitForNodeGroup(transaction, localNodeGroup, nodeGroup, boundOffsetInGroup,
            rowIndices, LOCAL_BOUND_NODE_ID_COLUMN_ID);
    }
    auto& bwdIndex = localRelTable.getBWDIndex();
    for (auto& [boundNodeOffset, rowIndices] : bwdIndex) {
        auto [nodeGroupIdx, boundOffsetInGroup] =
            StorageUtils::getQuotientRemainder(boundNodeOffset, StorageConstants::NODE_GROUP_SIZE);
        auto& nodeGroup = bwdRelTableData->getOrCreateNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
        prepareCommitForNodeGroup(transaction, localNodeGroup, nodeGroup, boundOffsetInGroup,
            rowIndices, LOCAL_NBR_NODE_ID_COLUMN_ID);
    }
    localRelTable.clear();
}

void RelTable::updateRelOffsets(LocalRelTable& localRelTable) {
    auto& localNodeGroup = localRelTable.getLocalNodeGroup();
    for (auto i = 0u; i < localNodeGroup.getNumChunkedGroups(); i++) {
        const auto chunkedGroup = localNodeGroup.getChunkedNodeGroup(i);
        KU_ASSERT(chunkedGroup);
        auto& internalIDData = chunkedGroup->getColumnChunk(LOCAL_REL_ID_COLUMN_ID)
                                   .getData()
                                   .cast<InternalIDChunkData>();
        const offset_t maxCommittedOffset = reserveRelOffsets(internalIDData.getNumValues());
        for (auto rowIdx = 0u; rowIdx < internalIDData.getNumValues(); rowIdx++) {
            const auto localRelOffset = internalIDData[rowIdx];
            const auto committedRelOffset = getCommittedOffset(localRelOffset, maxCommittedOffset);
            internalIDData[rowIdx] = committedRelOffset;
            internalIDData.setTableID(tableID);
        }
    }
}

void RelTable::updateNodeOffsets(LocalRelTable& localRelTable, RelDataDirection direction,
    offset_t maxCommittedOffset) {
    const auto columnID = direction == RelDataDirection::FWD ? LOCAL_BOUND_NODE_ID_COLUMN_ID :
                                                               LOCAL_NBR_NODE_ID_COLUMN_ID;
    auto& localNodeGroup = localRelTable.getLocalNodeGroup();
    for (auto i = 0u; i < localNodeGroup.getNumChunkedGroups(); i++) {
        const auto chunkedGroup = localNodeGroup.getChunkedNodeGroup(i);
        KU_ASSERT(chunkedGroup);
        auto& nodeIDData =
            chunkedGroup->getColumnChunk(columnID).getData().cast<InternalIDChunkData>();
        for (auto rowIdx = 0u; rowIdx < nodeIDData.getNumValues(); rowIdx++) {
            const auto localOffset = nodeIDData[rowIdx];
            if (localOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
                const auto committedOffset = getCommittedOffset(localOffset, maxCommittedOffset);
                nodeIDData[rowIdx] = committedOffset;
            }
        }
    }
    auto& index = direction == RelDataDirection::FWD ? localRelTable.getFWDIndex() :
                                                       localRelTable.getBWDIndex();
    for (auto& [offset, rowIndices] : index) {
        if (offset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
            const auto committedOffset = getCommittedOffset(offset, maxCommittedOffset);
            auto kvPair = index.extract(offset);
            kvPair.key() = committedOffset;
            index.insert(std::move(kvPair));
        }
    }
}

offset_t RelTable::getCommittedOffset(offset_t uncommittedOffset, offset_t maxCommittedOffset) {
    return uncommittedOffset - StorageConstants::MAX_NUM_ROWS_IN_TABLE + maxCommittedOffset;
}

void RelTable::prepareCommitForNodeGroup(Transaction* transaction, NodeGroup& localNodeGroup,
    CSRNodeGroup& csrNodeGroup, offset_t boundOffsetInGroup, const row_idx_vec_t& rowIndices,
    column_id_t skippedColumn) {
    for (const auto row : rowIndices) {
        auto [chunkedGroupIdx, rowInChunkedGroup] =
            StorageUtils::getQuotientRemainder(row, ChunkedNodeGroup::CHUNK_CAPACITY);
        std::vector<ColumnChunk*> chunks;
        const auto chunkedGroup = localNodeGroup.getChunkedNodeGroup(chunkedGroupIdx);
        for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
            if (i == skippedColumn) {
                continue;
            }
            chunks.push_back(&chunkedGroup->getColumnChunk(i));
        }
        csrNodeGroup.append(transaction, boundOffsetInGroup, chunks, rowInChunkedGroup,
            1 /*numRows*/);
    }
}

void RelTable::prepareRollback(LocalTable* localTable) {
    localTable->clear();
}

void RelTable::checkpoint(Serializer&) {
    checkpointInMemory();
}

void RelTable::serialize(Serializer& serializer) const {
    Table::serialize(serializer);
    serializer.write<offset_t>(nextRelOffset);
    fwdRelTableData->serialize(serializer);
    bwdRelTableData->serialize(serializer);
}

} // namespace storage
} // namespace kuzu
