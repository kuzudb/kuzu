#include "storage/store/rel_table.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "main/client_context.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
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
    fwdRelTableData = std::make_unique<RelTableData>(dataFH, memoryManager, shadowFile,
        relTableEntry, RelDataDirection::FWD, enableCompression, deSer);
    bwdRelTableData = std::make_unique<RelTableData>(dataFH, memoryManager, shadowFile,
        relTableEntry, RelDataDirection::BWD, enableCompression, deSer);
}

std::unique_ptr<RelTable> RelTable::loadTable(Deserializer& deSer, const Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem*,
    main::ClientContext*) {
    std::string key;
    table_id_t tableID;
    offset_t nextRelOffset;
    deSer.validateDebuggingInfo(key, "table_id");
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.validateDebuggingInfo(key, "next_rel_offset");
    deSer.deserializeValue<offset_t>(nextRelOffset);
    auto catalogEntry = catalog.getTableCatalogEntry(&DUMMY_TRANSACTION, tableID);
    if (!catalogEntry) {
        throw RuntimeException(
            stringFormat("Load table failed: table {} doesn't exist in catalog.", tableID));
    }
    auto relTable = std::make_unique<RelTable>(catalogEntry->ptrCast<RelTableCatalogEntry>(),
        storageManager, memoryManager, &deSer);
    relTable->nextRelOffset = nextRelOffset;
    return relTable;
}

void RelTable::initializeScanState(Transaction* transaction, TableScanState& scanState) {
    // Scan always start with committed data first.
    auto& relScanState = scanState.cast<RelTableScanState>();
    auto& nodeSelVector = *relScanState.nodeOriginalSelVector;
    relScanState.totalNodeIdx = nodeSelVector.getSelSize();
    relScanState.batchSize = 0;
    if (relScanState.resetCommitted) {
        // reset to read committed, in memory data
        relScanState.currNodeIdx = 0;
        relScanState.endNodeIdx = 0;
        relScanState.resetCommitted = false;
    }
    if (relScanState.resetUncommitted && relScanState.localTableScanState &&
        relScanState.source != TableScanSource::UNCOMMITTED) {
        // reset to read uncommitted, transaction-local data
        KU_ASSERT(!relScanState.resetCommitted);
        relScanState.source = TableScanSource::UNCOMMITTED;
        relScanState.currNodeIdx = 0;
        relScanState.endNodeIdx = 0;
        relScanState.resetUncommitted = false;
    }
    if (relScanState.currNodeIdx == relScanState.totalNodeIdx) {
        relScanState.source = TableScanSource::NONE;
        return;
    }
    KU_ASSERT(relScanState.totalNodeIdx > 0);
    KU_ASSERT(relScanState.endNodeIdx == relScanState.currNodeIdx);
    KU_ASSERT(relScanState.endNodeIdx < relScanState.totalNodeIdx);
    offset_t nodeOffset =
        relScanState.boundNodeIDVector->readNodeOffset(nodeSelVector[relScanState.currNodeIdx]);
    if (nodeOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE && relScanState.localTableScanState &&
        relScanState.source != TableScanSource::UNCOMMITTED) {
        relScanState.source = TableScanSource::UNCOMMITTED;
        relScanState.currNodeIdx = 0;
        relScanState.endNodeIdx = 0;
    }
    if (relScanState.source == TableScanSource::UNCOMMITTED) {
        // No more to read from committed
        relScanState.nodeGroup = nullptr;
        initializeLocalRelScanState(relScanState);
        return;
    }
    relScanState.source = TableScanSource::NONE;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    relScanState.nodeGroup = relScanState.direction == RelDataDirection::FWD ?
                                 fwdRelTableData->getNodeGroup(nodeGroupIdx) :
                                 bwdRelTableData->getNodeGroup(nodeGroupIdx);
    if (relScanState.nodeGroup) {
        relScanState.nodeGroup->initializeScanState(transaction, scanState);
        if (relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>().source !=
            CSRNodeGroupScanSource::NONE) {
            relScanState.source = TableScanSource::COMMITTED;
        }
    } else if (relScanState.localTableScanState) {
        initializeLocalRelScanState(relScanState);
    }
}

void RelTable::initializeLocalRelScanState(RelTableScanState& relScanState) {
    relScanState.source = TableScanSource::UNCOMMITTED;
    KU_ASSERT(relScanState.localTableScanState);
    auto& localScanState = *relScanState.localTableScanState;
    KU_ASSERT(localScanState.localRelTable);
    localScanState.nodeOriginalSelVector = relScanState.nodeOriginalSelVector;
    localScanState.endNodeIdx = relScanState.endNodeIdx;
    localScanState.rowIdxVector->setState(relScanState.rowIdxVector->state);
    localScanState.localRelTable->initializeScan(*relScanState.localTableScanState);
    relScanState.endNodeIdx = localScanState.endNodeIdx;
}

bool RelTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    auto& relScanState = scanState.cast<RelTableScanState>();
    auto& outSelVector = relScanState.outputVectors[0]->state->getSelVectorUnsafe();
    KU_ASSERT(relScanState.nodeOriginalSelVector);
    auto& nodeIDSelVector = *relScanState.nodeOriginalSelVector;
    relScanState.boundNodeIDVector->state->setSelVector(relScanState.nodeOriginalSelVector);
    relScanState.boundNodeIDVector->state->setToUnflat();
    // We reinitialize per node for in memory and local table data
    if (relScanState.currNodeIdx == relScanState.endNodeIdx) {
        initializeScanState(transaction, relScanState);
        // check if we still have more to scan after the re-initialization
        if (relScanState.source == TableScanSource::NONE) {
            return false;
        }
    }
    offset_t curNodeOffset =
        relScanState.boundNodeIDVector->readNodeOffset(nodeIDSelVector[relScanState.currNodeIdx]);
    row_idx_t currCSRSize = INVALID_OFFSET;
    switch (relScanState.source) {
    case TableScanSource::COMMITTED: {
        auto& csrNodeGroupScanState =
            relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
        if (csrNodeGroupScanState.source == CSRNodeGroupScanSource::COMMITTED_PERSISTENT) {
            currCSRSize = csrNodeGroupScanState.persistentCSRLists[relScanState.currNodeIdx].length;
        } else {
            KU_ASSERT(csrNodeGroupScanState.source == CSRNodeGroupScanSource::COMMITTED_IN_MEMORY);
            currCSRSize = csrNodeGroupScanState.inMemCSRList.getNumRows();
        }
    } break;
    case TableScanSource::UNCOMMITTED: {
        auto localTable = relScanState.localTableScanState->localRelTable;
        auto& index = relScanState.direction == RelDataDirection::FWD ? localTable->getFWDIndex() :
                                                                        localTable->getBWDIndex();
        currCSRSize = index.contains(curNodeOffset) ? index[curNodeOffset].size() : 0;
    } break;
    case TableScanSource::NONE: {
        return false;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    KU_ASSERT(currCSRSize != INVALID_OFFSET);
    // We rescan after last batch is all processed (max 2048 tuples)
    if (relScanState.currentCSRIdx >= relScanState.batchSize) {
        scanNext(transaction, relScanState);
    }
    // This assumes nodeIDVector is initially unfiltered
    relScanState.nodeOutputSelVector->getMultableBuffer()[0] =
        nodeIDSelVector[relScanState.currNodeIdx];
    relScanState.boundNodeIDVector->state->setSelVector(relScanState.nodeOutputSelVector);
    relScanState.boundNodeIDVector->state->setToFlat();
    if (relScanState.source == TableScanSource::COMMITTED) {
        // Accommodate for gaps in persistent data scan
        auto& csrNodeGroupScanState =
            relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>();
        relScanState.currentCSRIdx += csrNodeGroupScanState.getGap(relScanState.currNodeIdx);
    }
    if (relScanState.currentCSRIdx == 0) {
        KU_ASSERT(currCSRSize >= relScanState.posInLastCSR);
        currCSRSize -= relScanState.posInLastCSR;
    }
    auto spaceLeft = relScanState.batchSize - relScanState.currentCSRIdx;
    if (currCSRSize > spaceLeft) {
        currCSRSize = spaceLeft;
    } else {
        relScanState.currNodeIdx++;
    }
    row_idx_t numRels = 0;
    bool checkVersion = relScanState.versionedBatchSize != relScanState.batchSize;
    for (; numRels < currCSRSize; numRels++) {
        if (checkVersion) {
            if (relScanState.currentSelIdx >= relScanState.versionedBatchSize ||
                outSelVector[relScanState.currentSelIdx] >=
                    relScanState.currentCSRIdx + currCSRSize) {
                break;
            }
            outSelVector.getMultableBuffer()[numRels] = outSelVector[relScanState.currentSelIdx++];
        } else {
            outSelVector.getMultableBuffer()[numRels] = numRels + relScanState.currentCSRIdx;
        }
    }
    KU_ASSERT(!outSelVector.isUnfiltered() || numRels == currCSRSize);
    relScanState.currentCSRIdx += currCSRSize;
    outSelVector.setToFiltered(numRels);
    return true;
}

void RelTable::scanNext(Transaction* transaction, TableScanState& scanState) {
    auto& relScanState = scanState.cast<RelTableScanState>();
    relScanState.currentCSRIdx = 0;
    relScanState.currentSelIdx = 0;
    relScanState.IDVector->state->getSelVectorUnsafe().setToUnfiltered();
    switch (relScanState.source) {
    case TableScanSource::COMMITTED: {
        relScanState.posInLastCSR =
            relScanState.nodeGroupScanState->cast<CSRNodeGroupScanState>().nextRowToScan;
        const auto scanResult = relScanState.nodeGroup->scan(transaction, scanState);
        relScanState.batchSize = scanResult.numRows;
    } break;
    case TableScanSource::UNCOMMITTED: {
        const auto localScanState = relScanState.localTableScanState.get();
        KU_ASSERT(localScanState && localScanState->localRelTable);
        relScanState.posInLastCSR = localScanState->nextRowToScan;
        localScanState->localRelTable->scan(transaction, scanState);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    relScanState.versionedBatchSize =
        relScanState.outputVectors[0]->state->getSelVector().getSelSize();
}

void RelTable::insert(Transaction* transaction, TableInsertState& insertState) {
    KU_ASSERT(transaction->getLocalStorage());
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->insert(&DUMMY_TRANSACTION, insertState);
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        auto& relInsertState = insertState.cast<RelTableInsertState>();
        std::vector<ValueVector*> vectorsToLog;
        vectorsToLog.push_back(&relInsertState.srcNodeIDVector);
        vectorsToLog.push_back(&relInsertState.dstNodeIDVector);
        vectorsToLog.insert(vectorsToLog.end(), relInsertState.propertyVectors.begin(),
            relInsertState.propertyVectors.end());
        KU_ASSERT(relInsertState.srcNodeIDVector.state->getSelVector().getSelSize() == 1);
        wal.logTableInsertion(tableID, TableType::REL,
            relInsertState.srcNodeIDVector.state->getSelVector().getSelSize(), vectorsToLog);
    }
    hasChanges = true;
}

void RelTable::update(Transaction* transaction, TableUpdateState& updateState) {
    const auto& relUpdateState = updateState.cast<RelTableUpdateState>();
    KU_ASSERT(relUpdateState.relIDVector.state->getSelVector().getSelSize() == 1);
    const auto relIDPos = relUpdateState.relIDVector.state->getSelVector()[0];
    if (const auto relOffset = relUpdateState.relIDVector.readNodeOffset(relIDPos);
        relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        localTable->update(&DUMMY_TRANSACTION, updateState);
    } else {
        fwdRelTableData->update(transaction, relUpdateState.srcNodeIDVector,
            relUpdateState.relIDVector, relUpdateState.columnID, relUpdateState.propertyVector);
        bwdRelTableData->update(transaction, relUpdateState.dstNodeIDVector,
            relUpdateState.relIDVector, relUpdateState.columnID, relUpdateState.propertyVector);
    }
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logRelUpdate(tableID, relUpdateState.columnID, &relUpdateState.srcNodeIDVector,
            &relUpdateState.dstNodeIDVector, &relUpdateState.relIDVector,
            &relUpdateState.propertyVector);
    }
    hasChanges = true;
}

bool RelTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& relDeleteState = deleteState.cast<RelTableDeleteState>();
    KU_ASSERT(relDeleteState.relIDVector.state->getSelVector().getSelSize() == 1);
    const auto relIDPos = relDeleteState.relIDVector.state->getSelVector()[0];
    bool isDeleted;
    if (const auto relOffset = relDeleteState.relIDVector.readNodeOffset(relIDPos);
        relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        isDeleted = localTable->delete_(&DUMMY_TRANSACTION, deleteState);
    } else {
        isDeleted = fwdRelTableData->delete_(transaction, relDeleteState.srcNodeIDVector,
            relDeleteState.relIDVector);
        if (isDeleted) {
            isDeleted = bwdRelTableData->delete_(transaction, relDeleteState.dstNodeIDVector,
                relDeleteState.relIDVector);
        }
    }
    if (isDeleted) {
        hasChanges = true;
        if (transaction->shouldLogToWAL()) {
            KU_ASSERT(transaction->isWriteTransaction());
            KU_ASSERT(transaction->getClientContext());
            auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
            wal.logRelDelete(tableID, &relDeleteState.srcNodeIDVector,
                &relDeleteState.dstNodeIDVector, &relDeleteState.relIDVector);
        }
    }
    return isDeleted;
}

void RelTable::detachDelete(Transaction* transaction, RelDataDirection direction,
    RelTableDeleteState* deleteState) {
    KU_ASSERT(deleteState->srcNodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto tableData =
        direction == RelDataDirection::FWD ? fwdRelTableData.get() : bwdRelTableData.get();
    const auto reverseTableData =
        direction == RelDataDirection::FWD ? bwdRelTableData.get() : fwdRelTableData.get();
    std::vector<column_id_t> columnsToScan = {NBR_ID_COLUMN_ID, REL_ID_COLUMN_ID};
    const auto relReadState =
        std::make_unique<RelTableScanState>(columnsToScan, tableData->getColumns(),
            tableData->getCSROffsetColumn(), tableData->getCSRLengthColumn(), direction);
    relReadState->boundNodeIDVector = &deleteState->srcNodeIDVector;
    relReadState->nodeOriginalSelVector =
        relReadState->boundNodeIDVector->state->getSelVectorShared();
    relReadState->outputVectors =
        std::vector<ValueVector*>{&deleteState->dstNodeIDVector, &deleteState->relIDVector};
    relReadState->IDVector = relReadState->outputVectors[1];
    relReadState->rowIdxVector->state = relReadState->IDVector->state;
    if (const auto localRelTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL)) {
        auto localTableColumnIDs =
            LocalRelTable::rewriteLocalColumnIDs(direction, relReadState->columnIDs);
        relReadState->localTableScanState = std::make_unique<LocalRelTableScanState>(*relReadState,
            localTableColumnIDs, localRelTable->ptrCast<LocalRelTable>());
        relReadState->localTableScanState->rowIdxVector->state = relReadState->IDVector->state;
    }
    initializeScanState(transaction, *relReadState);
    detachDeleteForCSRRels(transaction, tableData, reverseTableData, relReadState.get(),
        deleteState);
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logRelDetachDelete(tableID, direction, &deleteState->srcNodeIDVector);
    }
    hasChanges = true;
}

void RelTable::checkIfNodeHasRels(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector) const {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    if (localTable) {
        localTable->cast<LocalRelTable>().checkIfNodeHasRels(srcNodeIDVector);
    }
    direction == RelDataDirection::FWD ?
        fwdRelTableData->checkIfNodeHasRels(transaction, srcNodeIDVector) :
        bwdRelTableData->checkIfNodeHasRels(transaction, srcNodeIDVector);
}

void RelTable::detachDeleteForCSRRels(Transaction* transaction, RelTableData* tableData,
    RelTableData* reverseTableData, RelTableScanState* relDataReadState,
    RelTableDeleteState* deleteState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    auto tempState = deleteState->dstNodeIDVector.state.get();
    while (scan(transaction, *relDataReadState)) {
        auto numRelsScanned = tempState->getSelVector().getSelSize();
        tempState->getSelVectorUnsafe().setToFiltered(1);
        for (auto i = 0u; i < numRelsScanned; i++) {
            tempState->getSelVectorUnsafe()[0] = i;
            const auto relIDPos = deleteState->relIDVector.state->getSelVector()[0];
            const auto relOffset = deleteState->relIDVector.readNodeOffset(relIDPos);
            if (relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
                KU_ASSERT(localTable);
                localTable->delete_(&DUMMY_TRANSACTION, *deleteState);
                continue;
            }
            const auto deleted = tableData->delete_(transaction, deleteState->srcNodeIDVector,
                deleteState->relIDVector);
            const auto reverseDeleted = reverseTableData->delete_(transaction,
                deleteState->dstNodeIDVector, deleteState->relIDVector);
            KU_ASSERT(deleted == reverseDeleted);
            KU_UNUSED(deleted);
            KU_UNUSED(reverseDeleted);
        }
        tempState->getSelVectorUnsafe().setToUnfiltered();
    }
}

void RelTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    LocalTable* localTable = nullptr;
    if (transaction->getLocalStorage()) {
        localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
    }
    if (localTable) {
        localTable->addColumn(transaction, addColumnState);
    }
    fwdRelTableData->addColumn(transaction, addColumnState);
    bwdRelTableData->addColumn(transaction, addColumnState);
    hasChanges = true;
}

NodeGroup* RelTable::getOrCreateNodeGroup(node_group_idx_t nodeGroupIdx,
    RelDataDirection direction) const {
    return direction == RelDataDirection::FWD ?
               fwdRelTableData->getOrCreateNodeGroup(nodeGroupIdx) :
               bwdRelTableData->getOrCreateNodeGroup(nodeGroupIdx);
}

void RelTable::commit(Transaction* transaction, LocalTable* localTable) {
    auto& localRelTable = localTable->cast<LocalRelTable>();
    if (localRelTable.isEmpty()) {
        localTable->clear();
        return;
    }
    // Update relID in local storage.
    updateRelOffsets(localRelTable);
    updateNodeOffsets(transaction, localRelTable);
    // For both forward and backward directions, re-org local storage into compact CSR node groups.
    auto& localNodeGroup = localRelTable.getLocalNodeGroup();
    // Scan from local node group and write to WAL.
    std::vector<column_id_t> columnIDsToScan;
    for (auto i = 0u; i < localRelTable.getNumColumns(); i++) {
        columnIDsToScan.push_back(i);
    }
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

static offset_t getCommittedOffset(offset_t uncommittedOffset, offset_t maxCommittedOffset) {
    return uncommittedOffset - StorageConstants::MAX_NUM_ROWS_IN_TABLE + maxCommittedOffset;
}

void RelTable::updateRelOffsets(const LocalRelTable& localRelTable) {
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

static void updateIndexNodeOffsets(LocalRelTable& localRelTable, column_id_t columnID,
    offset_t maxCommittedOffset) {
    KU_ASSERT(columnID == LOCAL_BOUND_NODE_ID_COLUMN_ID || columnID == LOCAL_NBR_NODE_ID_COLUMN_ID);
    auto& index = columnID == LOCAL_BOUND_NODE_ID_COLUMN_ID ? localRelTable.getFWDIndex() :
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

void RelTable::updateNodeOffsets(const Transaction* transaction, LocalRelTable& localRelTable) {
    std::unordered_map<column_id_t, offset_t> columnsToUpdate;
    for (auto& [columnID, tableID] : localRelTable.getNodeOffsetColumns()) {
        if (transaction->hasNewlyInsertedNodes(tableID)) {
            const auto maxCommittedOffset = transaction->getMaxNodeOffsetBeforeCommit(tableID);
            columnsToUpdate[columnID] = maxCommittedOffset;
            if (columnID == LOCAL_BOUND_NODE_ID_COLUMN_ID ||
                columnID == LOCAL_NBR_NODE_ID_COLUMN_ID) {
                updateIndexNodeOffsets(localRelTable, columnID, maxCommittedOffset);
            }
        }
    }
    auto& localNodeGroup = localRelTable.getLocalNodeGroup();
    for (auto i = 0u; i < localNodeGroup.getNumChunkedGroups(); i++) {
        const auto chunkedGroup = localNodeGroup.getChunkedNodeGroup(i);
        KU_ASSERT(chunkedGroup);
        for (auto& [columnID, maxCommittedOffset] : columnsToUpdate) {
            auto& nodeIDData =
                chunkedGroup->getColumnChunk(columnID).getData().cast<InternalIDChunkData>();
            for (auto rowIdx = 0u; rowIdx < nodeIDData.getNumValues(); rowIdx++) {
                const auto localOffset = nodeIDData[rowIdx];
                if (localOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
                    const auto committedOffset =
                        getCommittedOffset(localOffset, maxCommittedOffset);
                    nodeIDData[rowIdx] = committedOffset;
                }
            }
        }
    }
}

offset_t RelTable::getCommittedOffset(offset_t uncommittedOffset, offset_t maxCommittedOffset) {
    return uncommittedOffset - StorageConstants::MAX_NUM_ROWS_IN_TABLE + maxCommittedOffset;
}

void RelTable::prepareCommitForNodeGroup(const Transaction* transaction, NodeGroup& localNodeGroup,
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

void RelTable::checkpoint(Serializer& ser, TableCatalogEntry* tableEntry) {
    if (hasChanges) {
        // Deleted columns are vaccumed and not checkpointed or serialized.
        std::vector<column_id_t> columnIDs;
        columnIDs.push_back(0);
        for (auto& property : tableEntry->getProperties()) {
            columnIDs.push_back(tableEntry->getColumnID(property.getName()));
        }
        fwdRelTableData->checkpoint(columnIDs);
        bwdRelTableData->checkpoint(columnIDs);
        tableEntry->vacuumColumnIDs(1);
        hasChanges = false;
    }
    Table::serialize(ser);
    ser.writeDebuggingInfo("next_rel_offset");
    ser.write<offset_t>(nextRelOffset);
    fwdRelTableData->serialize(ser);
    bwdRelTableData->serialize(ser);
}

} // namespace storage
} // namespace kuzu
