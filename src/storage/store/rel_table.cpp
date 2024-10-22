#include "storage/store/rel_table.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "main/client_context.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_storage.h"
#include "storage/local_storage/local_table.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table_data.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

RelTableScanState::RelTableScanState(MemoryManager& mm, table_id_t tableID,
    const std::vector<column_id_t>& columnIDs, const std::vector<const Column*>& columns,
    Column* csrOffsetCol, Column* csrLengthCol, RelDataDirection direction,
    std::vector<ColumnPredicateSet> columnPredicateSets)
    : TableScanState{tableID, columnIDs, columns, std::move(columnPredicateSets)},
      direction{direction}, currBoundNodeIdx{0}, csrOffsetColumn{csrOffsetCol},
      csrLengthColumn{csrLengthCol}, localTableScanState{nullptr} {
    nodeGroupScanState = std::make_unique<CSRNodeGroupScanState>(mm, this->columnIDs.size());
    if (!this->columnPredicateSets.empty()) {
        // Since we insert a nbr column. We need to pad an empty nbr column predicate set.
        this->columnPredicateSets.insert(this->columnPredicateSets.begin(), ColumnPredicateSet());
    }
}

void RelTableScanState::initState(Transaction* transaction, NodeGroup* nodeGroup) {
    this->nodeGroup = nodeGroup;
    initCachedBoundNodeIDSelVector();
    if (this->nodeGroup) {
        initStateForCommitted(transaction);
    } else if (hasUnComittedData()) {
        initStateForUncommitted();
    } else {
        source = TableScanSource::NONE;
    }
}

void RelTableScanState::initCachedBoundNodeIDSelVector() {
    if (nodeIDVector->state->getSelVector().isUnfiltered()) {
        cachedBoundNodeSelVector.setToUnfiltered();
    } else {
        cachedBoundNodeSelVector.setToFiltered();
        memcpy(cachedBoundNodeSelVector.getMutableBuffer().data(),
            nodeIDVector->state->getSelVectorUnsafe().getMutableBuffer().data(),
            nodeIDVector->state->getSelVector().getSelSize() * sizeof(sel_t));
    }
    cachedBoundNodeSelVector.setSelSize(nodeIDVector->state->getSelVector().getSelSize());
}

bool RelTableScanState::hasUnComittedData() const {
    return localTableScanState && localTableScanState->localRelTable;
}

void RelTableScanState::initStateForCommitted(Transaction* transaction) {
    source = TableScanSource::COMMITTED;
    currBoundNodeIdx = 0;
    nodeGroup->initializeScanState(transaction, *this);
}

void RelTableScanState::initStateForUncommitted() {
    KU_ASSERT(localTableScanState);
    source = TableScanSource::UNCOMMITTED;
    currBoundNodeIdx = 0;
    localTableScanState->localRelTable->initializeScan(*this);
}

bool RelTableScanState::scanNext(Transaction* transaction) {
    while (true) {
        switch (source) {
        case TableScanSource::COMMITTED: {
            const auto scanResult = nodeGroup->scan(transaction, *this);
            if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
                if (hasUnComittedData()) {
                    initStateForUncommitted();
                } else {
                    source = TableScanSource::NONE;
                }
                continue;
            }
            return true;
        }
        case TableScanSource::UNCOMMITTED: {
            KU_ASSERT(localTableScanState && localTableScanState->localRelTable);
            return localTableScanState->localRelTable->scan(transaction, *this);
        }
        case TableScanSource::NONE: {
            return false;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

void RelTableScanState::setNodeIDVectorToFlat(sel_t selPos) const {
    nodeIDVector->state->setToFlat();
    nodeIDVector->state->getSelVectorShared()->setToFiltered(1);
    nodeIDVector->state->getSelVectorUnsafe()[0] = selPos;
}

RelTable::RelTable(RelTableCatalogEntry* relTableEntry, const StorageManager* storageManager,
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
    table_id_t tableID = INVALID_TABLE_ID;
    offset_t nextRelOffset = INVALID_OFFSET;
    deSer.validateDebuggingInfo(key, "table_id");
    deSer.deserializeValue<table_id_t>(tableID);
    deSer.validateDebuggingInfo(key, "next_rel_offset");
    deSer.deserializeValue<offset_t>(nextRelOffset);
    const auto catalogEntry = catalog.getTableCatalogEntry(&DUMMY_TRANSACTION, tableID);
    if (!catalogEntry) {
        throw RuntimeException(
            stringFormat("Load table failed: table {} doesn't exist in catalog.", tableID));
    }
    auto relTable = std::make_unique<RelTable>(catalogEntry->ptrCast<RelTableCatalogEntry>(),
        storageManager, memoryManager, &deSer);
    relTable->nextRelOffset = nextRelOffset;
    return relTable;
}

void RelTable::initScanState(Transaction* transaction, TableScanState& scanState) const {
    auto& relScanState = scanState.cast<RelTableScanState>();
    // Note there we directly read node at pos 0 here regardless the selVector is filtered or not.
    // This is because we're assuming the nodeIDVector is always a sequence here.
    const auto boundNodeOffset = relScanState.nodeIDVector->readNodeOffset(
        relScanState.nodeIDVector->state->getSelVector()[0]);
    NodeGroup* nodeGroup = nullptr;
    if (boundNodeOffset < StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        // Check if the node group idx is same as previous scan.
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);
        if (relScanState.nodeGroupIdx != nodeGroupIdx) {
            // We need to re-initialize the node group scan state.
            nodeGroup = relScanState.direction == RelDataDirection::FWD ?
                            fwdRelTableData->getNodeGroup(nodeGroupIdx) :
                            bwdRelTableData->getNodeGroup(nodeGroupIdx);
        } else {
            nodeGroup = relScanState.nodeGroup;
        }
    }
    scanState.initState(transaction, nodeGroup);
}

bool RelTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    return scanState.scanNext(transaction);
}

static void throwRelMultiplicityConstraintError(const std::string& tableName, offset_t nodeOffset,
    RelDataDirection direction) {
    throw RuntimeException(ExceptionMessage::violateRelMultiplicityConstraint(tableName,
        std::to_string(nodeOffset), RelDataDirectionUtils::relDirectionToString(direction)));
}

void RelTable::checkRelMultiplicityConstraint(Transaction* transaction,
    const TableInsertState& state) const {
    const auto& insertState = state.constCast<RelTableInsertState>();
    KU_ASSERT(insertState.srcNodeIDVector.state->getSelVector().getSelSize() == 1 &&
              insertState.dstNodeIDVector.state->getSelVector().getSelSize() == 1);

    if (fwdRelTableData->getMultiplicity() == common::RelMultiplicity::ONE) {
        throwIfNodeHasRels(transaction, common::RelDataDirection::FWD, &insertState.srcNodeIDVector,
            throwRelMultiplicityConstraintError);
    }
    if (bwdRelTableData->getMultiplicity() == common::RelMultiplicity::ONE) {
        throwIfNodeHasRels(transaction, common::RelDataDirection::BWD, &insertState.dstNodeIDVector,
            throwRelMultiplicityConstraintError);
    }
}

void RelTable::insert(Transaction* transaction, TableInsertState& insertState) {
    checkRelMultiplicityConstraint(transaction, insertState);

    KU_ASSERT(transaction->getLocalStorage());
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    localTable->insert(transaction, insertState);
    if (transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        const auto& relInsertState = insertState.cast<RelTableInsertState>();
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
    bool isDeleted = false;
    if (const auto relOffset = relDeleteState.relIDVector.readNodeOffset(relIDPos);
        relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        isDeleted = localTable->delete_(transaction, deleteState);
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
        std::make_unique<RelTableScanState>(*transaction->getClientContext()->getMemoryManager(),
            tableID, columnsToScan, tableData->getColumns(), tableData->getCSROffsetColumn(),
            tableData->getCSRLengthColumn(), direction);
    relReadState->nodeIDVector = &deleteState->srcNodeIDVector;
    relReadState->outputVectors =
        std::vector<ValueVector*>{&deleteState->dstNodeIDVector, &deleteState->relIDVector};
    relReadState->outState = relReadState->outputVectors[0]->state.get();
    relReadState->rowIdxVector->state = relReadState->outputVectors[0]->state;
    if (const auto localRelTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL)) {
        auto localTableColumnIDs =
            LocalRelTable::rewriteLocalColumnIDs(direction, relReadState->columnIDs);
        relReadState->localTableScanState = std::make_unique<LocalRelTableScanState>(*relReadState,
            localTableColumnIDs, localRelTable->ptrCast<LocalRelTable>());
        relReadState->localTableScanState->rowIdxVector->state = relReadState->rowIdxVector->state;
    }
    initScanState(transaction, *relReadState);
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

bool RelTable::checkIfNodeHasRels(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector) const {
    bool hasRels = false;
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    if (localTable) {
        hasRels = hasRels ||
                  localTable->cast<LocalRelTable>().checkIfNodeHasRels(srcNodeIDVector, direction);
    }
    hasRels = hasRels || ((direction == RelDataDirection::FWD) ?
                                 fwdRelTableData->checkIfNodeHasRels(transaction, srcNodeIDVector) :
                                 bwdRelTableData->checkIfNodeHasRels(transaction, srcNodeIDVector));
    return hasRels;
}

void RelTable::throwIfNodeHasRels(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector, const rel_multiplicity_constraint_throw_func_t& throwFunc) const {
    const auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    const auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    if (checkIfNodeHasRels(transaction, direction, srcNodeIDVector)) {
        throwFunc(tableName, nodeOffset, direction);
    }
}

void RelTable::detachDeleteForCSRRels(Transaction* transaction, const RelTableData* tableData,
    const RelTableData* reverseTableData, RelTableScanState* relDataReadState,
    RelTableDeleteState* deleteState) {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    const auto tempState = deleteState->dstNodeIDVector.state.get();
    while (scan(transaction, *relDataReadState)) {
        const auto numRelsScanned = tempState->getSelVector().getSelSize();
        tempState->getSelVectorUnsafe().setToFiltered(1);
        for (auto i = 0u; i < numRelsScanned; i++) {
            tempState->getSelVectorUnsafe()[0] = i;
            const auto relIDPos = deleteState->relIDVector.state->getSelVector()[0];
            const auto relOffset = deleteState->relIDVector.readNodeOffset(relIDPos);
            if (relOffset >= StorageConstants::MAX_NUM_ROWS_IN_TABLE) {
                KU_ASSERT(localTable);
                localTable->delete_(transaction, *deleteState);
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
