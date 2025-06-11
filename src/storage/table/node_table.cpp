#include "storage/table/node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/cast.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_storage.h"
#include "storage/local_storage/local_table.h"
#include "storage/storage_manager.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

NodeTableVersionRecordHandler::NodeTableVersionRecordHandler(NodeTable* table) : table(table) {}

void NodeTableVersionRecordHandler::applyFuncToChunkedGroups(version_record_handler_op_t func,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows,
    transaction_t commitTS) const {
    table->getNodeGroupNoLock(nodeGroupIdx)
        ->applyFuncToChunkedGroups(func, startRow, numRows, commitTS);
}

void NodeTableVersionRecordHandler::rollbackInsert(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, row_idx_t startRow, row_idx_t numRows) const {
    table->rollbackPKIndexInsert(transaction, startRow, numRows, nodeGroupIdx);

    // the only case where a node group would be empty (and potentially removed before) is if an
    // exception occurred while adding its first chunk
    KU_ASSERT(nodeGroupIdx < table->getNumNodeGroups() || startRow == 0);
    if (nodeGroupIdx < table->getNumNodeGroups()) {
        VersionRecordHandler::rollbackInsert(transaction, nodeGroupIdx, startRow, numRows);
        auto* nodeGroup = table->getNodeGroupNoLock(nodeGroupIdx);
        const auto numRowsToRollback = std::min(numRows, nodeGroup->getNumRows() - startRow);
        nodeGroup->rollbackInsert(startRow);
        table->rollbackGroupCollectionInsert(numRowsToRollback);
    }
}

bool NodeTableScanState::scanNext(Transaction* transaction, offset_t startOffset,
    offset_t numNodes) {
    KU_ASSERT(columns.size() == outputVectors.size());
    if (source == TableScanSource::NONE) {
        return false;
    }
    const NodeGroupScanResult scanResult =
        nodeGroup->scan(transaction, *this, startOffset, numNodes);
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto tableID = table->getTableID();
    if (source == TableScanSource::UNCOMMITTED) {
        nodeGroupStartOffset = transaction->getUncommittedOffset(tableID, nodeGroupStartOffset);
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        nodeIDVector->setValue(i,
            nodeID_t{nodeGroupStartOffset + scanResult.startRow + i, tableID});
    }
    return true;
}

std::unique_ptr<NodeTableScanState> IndexScanHelper::initScanState(const Transaction* transaction,
    DataChunk& dataChunk) {
    std::vector<ValueVector*> outVectors;
    for (auto& vector : dataChunk.valueVectors) {
        outVectors.push_back(vector.get());
    }
    auto scanState = std::make_unique<NodeTableScanState>(nullptr, outVectors, dataChunk.state);
    scanState->setToTable(transaction, table, index->getIndexInfo().columnIDs, {});
    return scanState;
}

namespace {

struct UncommittedIndexInserter final : IndexScanHelper {
    UncommittedIndexInserter(row_idx_t startNodeOffset, NodeTable* table, Index* index,
        visible_func isVisible)
        : IndexScanHelper(table, index), startNodeOffset(startNodeOffset),
          nodeIDVector(LogicalType::INTERNAL_ID()), isVisible(std::move(isVisible)) {}

    std::unique_ptr<NodeTableScanState> initScanState(const Transaction* transaction,
        DataChunk& dataChunk) override;

    bool processScanOutput(Transaction* transaction, MemoryManager* mm,
        NodeGroupScanResult scanResult, const std::vector<ValueVector*>& scannedVectors) override;

    row_idx_t startNodeOffset;
    ValueVector nodeIDVector;
    visible_func isVisible;
    std::unique_ptr<Index::InsertState> insertState;
};

struct RollbackPKDeleter final : IndexScanHelper {
    RollbackPKDeleter(row_idx_t startNodeOffset, row_idx_t numRows, NodeTable* table,
        PrimaryKeyIndex* pkIndex)
        : IndexScanHelper(table, pkIndex),
          semiMask(SemiMaskUtil::createMask(startNodeOffset + numRows)) {
        semiMask->maskRange(startNodeOffset, startNodeOffset + numRows);
        semiMask->enable();
    }

    std::unique_ptr<NodeTableScanState> initScanState(const Transaction* transaction,
        DataChunk& dataChunk) override;

    bool processScanOutput(Transaction* transaction, MemoryManager* mm,
        NodeGroupScanResult scanResult, const std::vector<ValueVector*>& scannedVectors) override;

    std::unique_ptr<SemiMask> semiMask;
};

std::unique_ptr<NodeTableScanState> UncommittedIndexInserter::initScanState(
    const Transaction* transaction, DataChunk& dataChunk) {
    auto scanState = IndexScanHelper::initScanState(transaction, dataChunk);
    nodeIDVector.setState(dataChunk.state);
    scanState->source = TableScanSource::UNCOMMITTED;
    return scanState;
}

bool UncommittedIndexInserter::processScanOutput(Transaction* transaction, MemoryManager* mm,
    NodeGroupScanResult scanResult, const std::vector<ValueVector*>& scannedVectors) {
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        nodeIDVector.setValue(i, nodeID_t{startNodeOffset + i, table->getTableID()});
    }
    if (!insertState) {
        insertState = index->initInsertState(transaction, mm, isVisible);
    }
    index->commitInsert(transaction, nodeIDVector, {scannedVectors}, *insertState);
    startNodeOffset += scanResult.numRows;
    return true;
}

std::unique_ptr<NodeTableScanState> RollbackPKDeleter::initScanState(const Transaction* transaction,
    DataChunk& dataChunk) {
    auto scanState = IndexScanHelper::initScanState(transaction, dataChunk);
    scanState->source = TableScanSource::COMMITTED;
    scanState->semiMask = semiMask.get();
    return scanState;
}

template<typename T>
concept notIndexHashable = !IndexHashable<T>;

bool RollbackPKDeleter::processScanOutput(Transaction* transaction, MemoryManager*,
    NodeGroupScanResult scanResult, const std::vector<ValueVector*>& scannedVectors) {
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    KU_ASSERT(scannedVectors.size() == 1);
    auto& scannedVector = *scannedVectors[0];
    auto& pkIndex = index->cast<PrimaryKeyIndex>();
    const auto rollbackFunc = [&]<IndexHashable T>(T) {
        for (idx_t i = 0; i < scannedVector.state->getSelSize(); ++i) {
            const auto pos = scannedVector.state->getSelVector()[i];
            T key = scannedVector.getValue<T>(pos);
            static constexpr auto isVisible = [](offset_t) { return true; };
            if (offset_t lookupOffset = 0;
                pkIndex.lookup(transaction, key, lookupOffset, isVisible)) {
                pkIndex.delete_(key);
            }
        }
    };
    TypeUtils::visit(scannedVector.dataType.getPhysicalType(), std::cref(rollbackFunc),
        []<notIndexHashable T>(T) { KU_UNREACHABLE; });
    return true;
}
} // namespace

void NodeTableScanState::setToTable(const Transaction* transaction, Table* table_,
    std::vector<column_id_t> columnIDs_, std::vector<ColumnPredicateSet> columnPredicateSets_,
    RelDataDirection) {
    TableScanState::setToTable(transaction, table_, columnIDs_, std::move(columnPredicateSets_));
    columns.resize(columnIDs.size());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (const auto columnID = columnIDs[i];
            columnID == INVALID_COLUMN_ID || columnID == ROW_IDX_COLUMN_ID) {
            columns[i] = nullptr;
        } else {
            columns[i] = &table->cast<NodeTable>().getColumn(columnID);
        }
    }
}

bool NodeTableScanState::scanNext(Transaction* transaction) {
    if (source == TableScanSource::NONE) {
        return false;
    }
    KU_ASSERT(columns.size() == outputVectors.size());
    const NodeGroupScanResult scanResult = nodeGroup->scan(transaction, *this);
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    const auto tableID = table->getTableID();
    if (source == TableScanSource::UNCOMMITTED) {
        nodeGroupStartOffset = transaction->getUncommittedOffset(tableID, nodeGroupStartOffset);
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        auto& nodeID = nodeIDVector->getValue<nodeID_t>(i);
        nodeID.tableID = tableID;
        nodeID.offset = nodeGroupStartOffset + scanResult.startRow + i;
    }
    return true;
}

NodeTable::NodeTable(const StorageManager* storageManager,
    const NodeTableCatalogEntry* nodeTableEntry, MemoryManager* memoryManager)
    : Table{nodeTableEntry, storageManager, memoryManager},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyName())},
      versionRecordHandler(this) {
    const auto maxColumnID = nodeTableEntry->getMaxColumnID();
    columns.resize(maxColumnID + 1);
    for (auto& property : nodeTableEntry->getProperties()) {
        const auto columnID = nodeTableEntry->getColumnID(property.getName());
        const auto columnName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
        columns[columnID] = ColumnFactory::createColumn(columnName, property.getType().copy(),
            dataFH, memoryManager, shadowFile, enableCompression);
    }
    auto& pkDefinition = nodeTableEntry->getPrimaryKeyDefinition();
    auto pkColumnID = nodeTableEntry->getColumnID(pkDefinition.getName());
    KU_ASSERT(pkColumnID != INVALID_COLUMN_ID);
    auto hashIndexType = PrimaryKeyIndex::getIndexType();
    IndexInfo indexInfo{PrimaryKeyIndex::DEFAULT_NAME, hashIndexType.typeName, tableID,
        {pkColumnID}, {pkDefinition.getType().getPhysicalType()},
        hashIndexType.constraintType == IndexConstraintType::PRIMARY,
        hashIndexType.definitionType == IndexDefinitionType::BUILTIN};
    indexes.push_back(IndexHolder{
        PrimaryKeyIndex::createNewIndex(indexInfo, inMemory, *memoryManager, dataFH, shadowFile)});
    nodeGroups = std::make_unique<NodeGroupCollection>(
        LocalNodeTable::getNodeTableColumnTypes(*nodeTableEntry), enableCompression,
        storageManager->getDataFH(), &versionRecordHandler);
}

row_idx_t NodeTable::getNumTotalRows(const Transaction* transaction) {
    auto numLocalRows = 0u;
    if (transaction->getLocalStorage()) {
        if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID)) {
            numLocalRows = localTable->getNumTotalRows();
        }
    }
    return numLocalRows + nodeGroups->getNumTotalRows();
}

void NodeTable::initScanState(Transaction* transaction, TableScanState& scanState, bool) const {
    auto& nodeScanState = scanState.cast<NodeTableScanState>();
    NodeGroup* nodeGroup = nullptr;
    switch (nodeScanState.source) {
    case TableScanSource::COMMITTED: {
        nodeGroup = nodeGroups->getNodeGroup(nodeScanState.nodeGroupIdx);
    } break;
    case TableScanSource::UNCOMMITTED: {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID);
        KU_ASSERT(localTable);
        const auto& localNodeTable = localTable->cast<LocalNodeTable>();
        nodeGroup = localNodeTable.getNodeGroup(nodeScanState.nodeGroupIdx);
        KU_ASSERT(nodeGroup);
    } break;
    case TableScanSource::NONE: {
        // DO NOTHING.
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    nodeScanState.initState(transaction, nodeGroup);
}

void NodeTable::initScanState(Transaction* transaction, TableScanState& scanState,
    table_id_t tableID, offset_t startOffset) const {
    if (transaction->isUnCommitted(tableID, startOffset)) {
        scanState.source = TableScanSource::UNCOMMITTED;
        scanState.nodeGroupIdx =
            StorageUtils::getNodeGroupIdx(transaction->getLocalRowIdx(tableID, startOffset));
    } else {
        scanState.source = TableScanSource::COMMITTED;
        scanState.nodeGroupIdx = StorageUtils::getNodeGroupIdx(startOffset);
    }
    initScanState(transaction, scanState);
}

bool NodeTable::scanInternal(Transaction* transaction, TableScanState& scanState) {
    scanState.resetOutVectors();
    return scanState.scanNext(transaction);
}

bool NodeTable::lookup(const Transaction* transaction, const TableScanState& scanState) const {
    KU_ASSERT(scanState.nodeIDVector->state->getSelVector().getSelSize() == 1);
    const auto nodeIDPos = scanState.nodeIDVector->state->getSelVector()[0];
    if (scanState.nodeIDVector->isNull(nodeIDPos)) {
        return false;
    }
    const auto nodeOffset = scanState.nodeIDVector->readNodeOffset(nodeIDPos);
    const offset_t rowIdxInGroup =
        transaction->isUnCommitted(tableID, nodeOffset) ?
            transaction->getLocalRowIdx(tableID, nodeOffset) -
                StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx) :
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx);
    scanState.rowIdxVector->setValue<row_idx_t>(nodeIDPos, rowIdxInGroup);
    return scanState.nodeGroup->lookup(transaction, scanState);
}

void NodeTable::lookupMultiple(Transaction* transaction, TableScanState& scanState) const {
    const auto numRowsToRead = scanState.nodeIDVector->state->getSelSize();
    for (auto i = 0u; i < numRowsToRead; i++) {
        const auto nodeIDPos = scanState.nodeIDVector->state->getSelVector()[i];
        if (scanState.nodeIDVector->isNull(nodeIDPos)) {
            continue;
        }
        const auto nodeOffset = scanState.nodeIDVector->readNodeOffset(nodeIDPos);
        const auto isUnCommitted = transaction->isUnCommitted(tableID, nodeOffset);
        const auto source =
            isUnCommitted ? TableScanSource::UNCOMMITTED : TableScanSource::COMMITTED;
        const auto nodeGroupIdx =
            isUnCommitted ?
                StorageUtils::getNodeGroupIdx(transaction->getLocalRowIdx(tableID, nodeOffset)) :
                StorageUtils::getNodeGroupIdx(nodeOffset);
        const offset_t rowIdxInGroup =
            isUnCommitted ? transaction->getLocalRowIdx(tableID, nodeOffset) -
                                StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx) :
                            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        if (scanState.source == source && scanState.nodeGroupIdx == nodeGroupIdx) {
            // If the scan state is already initialized for the same source and node group, we can
            // skip re-initialization.
        } else {
            scanState.source = source;
            scanState.nodeGroupIdx = nodeGroupIdx;
            initScanState(transaction, scanState);
        }
        scanState.rowIdxVector->setValue<row_idx_t>(nodeIDPos, rowIdxInGroup);
        [[maybe_unused]] auto res = scanState.nodeGroup->lookup(transaction, scanState, i);
    }
}

offset_t NodeTable::validateUniquenessConstraint(const Transaction* transaction,
    const std::vector<ValueVector*>& propertyVectors) const {
    const auto pkVector = propertyVectors[pkColumnID];
    KU_ASSERT(pkVector->state->getSelVector().getSelSize() == 1);
    const auto pkVectorPos = pkVector->state->getSelVector()[0];
    if (offset_t offset = INVALID_OFFSET;
        getPKIndex()->lookup(transaction, propertyVectors[pkColumnID], pkVectorPos, offset,
            [&](offset_t offset_) { return isVisible(transaction, offset_); })) {
        return offset;
    }
    if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID)) {
        return localTable->cast<LocalNodeTable>().validateUniquenessConstraint(transaction,
            *pkVector);
    }
    return INVALID_OFFSET;
}

void NodeTable::validatePkNotExists(const Transaction* transaction, ValueVector* pkVector) const {
    offset_t dummyOffset = INVALID_OFFSET;
    auto& selVector = pkVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1);
    if (pkVector->isNull(selVector[0])) {
        throw RuntimeException(ExceptionMessage::nullPKException());
    }
    if (getPKIndex()->lookup(transaction, pkVector, selVector[0], dummyOffset,
            [&](offset_t offset) { return isVisible(transaction, offset); })) {
        throw RuntimeException(
            ExceptionMessage::duplicatePKException(pkVector->getAsValue(selVector[0])->toString()));
    }
}

void NodeTable::initInsertState(Transaction* transaction, TableInsertState& insertState) {
    auto& nodeInsertState = insertState.cast<NodeTableInsertState>();
    nodeInsertState.indexInsertStates.resize(indexes.size());
    for (auto i = 0u; i < indexes.size(); i++) {
        auto& indexHolder = indexes[i];
        const auto index = indexHolder.getIndex();
        nodeInsertState.indexInsertStates[i] = index->initInsertState(transaction, memoryManager,
            [&](offset_t offset) { return isVisible(transaction, offset); });
    }
}

void NodeTable::insert(Transaction* transaction, TableInsertState& insertState) {
    const auto& nodeInsertState = insertState.cast<NodeTableInsertState>();
    auto& nodeIDSelVector = nodeInsertState.nodeIDVector.state->getSelVector();
    KU_ASSERT(nodeInsertState.propertyVectors[0]->state->getSelVector().getSelSize() == 1);
    KU_ASSERT(nodeIDSelVector.getSelSize() == 1);
    if (nodeInsertState.nodeIDVector.isNull(nodeIDSelVector[0])) {
        return;
    }
    const auto localTable = transaction->getLocalStorage()->getOrCreateLocalTable(*this);
    validatePkNotExists(transaction, const_cast<ValueVector*>(&nodeInsertState.pkVector));
    localTable->insert(transaction, insertState);
    for (auto i = 0u; i < indexes.size(); i++) {
        auto index = indexes[i].getIndex();
        std::vector<ValueVector*> indexedPropertyVectors;
        for (const auto columnID : index->getIndexInfo().columnIDs) {
            indexedPropertyVectors.push_back(insertState.propertyVectors[columnID]);
        }
        index->insert(transaction, nodeInsertState.nodeIDVector, indexedPropertyVectors,
            *nodeInsertState.indexInsertStates[i]);
    }
    if (insertState.logToWAL && transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logTableInsertion(tableID, TableType::NODE,
            nodeInsertState.nodeIDVector.state->getSelVector().getSelSize(),
            insertState.propertyVectors);
    }
    hasChanges = true;
}

void NodeTable::update(Transaction* transaction, TableUpdateState& updateState) {
    // NOTE: We assume all inputs are flattened now. This is to simplify the implementation.
    // We should optimize this to take unflattened input later.
    auto& nodeUpdateState = updateState.constCast<NodeTableUpdateState>();
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1 &&
              nodeUpdateState.propertyVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    if (nodeUpdateState.nodeIDVector.isNull(pos)) {
        return;
    }
    const auto pkIndex = getPKIndex();
    if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
        validatePkNotExists(transaction, &nodeUpdateState.propertyVector);
    }
    const auto nodeOffset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (transaction->isUnCommitted(tableID, nodeOffset)) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID);
        KU_ASSERT(localTable);
        auto dummyTrx = Transaction::getDummyTransactionFromExistingOne(*transaction);
        localTable->update(&dummyTrx, updateState);
    } else {
        if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
            const auto insertState =
                pkIndex->initInsertState(transaction, memoryManager, getVisibleFunc(transaction));
            pkIndex->commitInsert(transaction, nodeUpdateState.nodeIDVector,
                {&nodeUpdateState.propertyVector}, *insertState);
        }
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups->getNodeGroup(nodeGroupIdx)
            ->update(transaction, rowIdxInGroup, nodeUpdateState.columnID,
                nodeUpdateState.propertyVector);
    }
    if (updateState.logToWAL && transaction->shouldLogToWAL()) {
        KU_ASSERT(transaction->isWriteTransaction());
        KU_ASSERT(transaction->getClientContext());
        auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
        wal.logNodeUpdate(tableID, nodeUpdateState.columnID, nodeOffset,
            &nodeUpdateState.propertyVector);
    }
    hasChanges = true;
}

bool NodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& nodeDeleteState = ku_dynamic_cast<NodeTableDeleteState&>(deleteState);
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    if (nodeDeleteState.nodeIDVector.isNull(pos)) {
        return false;
    }
    bool isDeleted = false;
    const auto nodeOffset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    for (auto& index : indexes) {
        auto indexDeleteState = index.getIndex()->initDeleteState(transaction, memoryManager,
            getVisibleFunc(transaction));
        index.getIndex()->delete_(transaction, nodeDeleteState.nodeIDVector, *indexDeleteState);
    }

    if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID);
        localTable && transaction->isUnCommitted(tableID, nodeOffset)) {
        auto dummyTrx = Transaction::getDummyTransactionFromExistingOne(*transaction);
        isDeleted = localTable->delete_(&dummyTrx, deleteState);
    } else {
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        isDeleted = nodeGroups->getNodeGroup(nodeGroupIdx)->delete_(transaction, rowIdxInGroup);
        if (transaction->shouldAppendToUndoBuffer()) {
            transaction->pushDeleteInfo(nodeGroupIdx, rowIdxInGroup, 1, &versionRecordHandler);
        }
    }
    if (isDeleted) {
        hasChanges = true;
        if (transaction->shouldLogToWAL()) {
            KU_ASSERT(transaction->isWriteTransaction());
            KU_ASSERT(transaction->getClientContext());
            auto& wal = transaction->getClientContext()->getStorageManager()->getWAL();
            wal.logNodeDeletion(tableID, nodeOffset, &nodeDeleteState.pkVector);
        }
    }
    return isDeleted;
}

void NodeTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    auto& definition = addColumnState.propertyDefinition;
    columns.push_back(ColumnFactory::createColumn(definition.getName(), definition.getType().copy(),
        dataFH, memoryManager, shadowFile, enableCompression));
    LocalTable* localTable = nullptr;
    if (transaction->getLocalStorage()) {
        localTable = transaction->getLocalStorage()->getLocalTable(tableID);
    }
    if (localTable) {
        localTable->addColumn(transaction, addColumnState);
    }
    nodeGroups->addColumn(transaction, addColumnState);
    hasChanges = true;
}

std::pair<offset_t, offset_t> NodeTable::appendToLastNodeGroup(MemoryManager& mm,
    Transaction* transaction, const std::vector<column_id_t>& columnIDs,
    ChunkedNodeGroup& chunkedGroup) {
    hasChanges = true;
    return nodeGroups->appendToLastNodeGroupAndFlushWhenFull(mm, transaction, columnIDs,
        chunkedGroup);
}

DataChunk NodeTable::constructDataChunkForColumns(const std::vector<column_id_t>& columnIDs) const {
    std::vector<LogicalType> types;
    for (const auto& columnID : columnIDs) {
        KU_ASSERT(columnID < columns.size());
        types.push_back(columns[columnID]->getDataType().copy());
    }
    return constructDataChunk(memoryManager, std::move(types));
}

void NodeTable::commit(Transaction* transaction, TableCatalogEntry* tableEntry,
    LocalTable* localTable) {
    const auto startNodeOffset = nodeGroups->getNumTotalRows();
    auto& localNodeTable = localTable->cast<LocalNodeTable>();

    std::vector<column_id_t> columnIDsToCommit;
    for (auto& property : tableEntry->getProperties()) {
        auto columnID = tableEntry->getColumnID(property.getName());
        columnIDsToCommit.push_back(columnID);
    }

    // 1. Append all tuples from local storage to nodeGroups regardless of deleted or not.
    // Note: We cannot simply remove all deleted tuples in local node table, as they may have
    // connected local rels. Directly removing them will cause shift of committed node offset,
    // leading to an inconsistent result with connected rels.
    nodeGroups->append(transaction, columnIDsToCommit, localNodeTable.getNodeGroups());
    // 2. Set deleted flag for tuples that are deleted in local storage.
    row_idx_t numLocalRows = 0u;
    for (auto localNodeGroupIdx = 0u; localNodeGroupIdx < localNodeTable.getNumNodeGroups();
         localNodeGroupIdx++) {
        const auto localNodeGroup = localNodeTable.getNodeGroup(localNodeGroupIdx);
        if (localNodeGroup->hasDeletions(transaction)) {
            // TODO(Guodong): Assume local storage is small here. Should optimize the loop away by
            // grabbing a set of deleted rows.
            for (auto row = 0u; row < localNodeGroup->getNumRows(); row++) {
                if (localNodeGroup->isDeleted(transaction, row)) {
                    const auto nodeOffset = startNodeOffset + numLocalRows + row;
                    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
                    const auto rowIdxInGroup =
                        nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
                    [[maybe_unused]] const bool isDeleted =
                        nodeGroups->getNodeGroup(nodeGroupIdx)->delete_(transaction, rowIdxInGroup);
                    KU_ASSERT(isDeleted);
                    if (transaction->shouldAppendToUndoBuffer()) {
                        transaction->pushDeleteInfo(nodeGroupIdx, rowIdxInGroup, 1,
                            &versionRecordHandler);
                    }
                }
            }
        }
        numLocalRows += localNodeGroup->getNumRows();
    }

    // 3. Scan primary key index columns for newly inserted tuples.
    // Note: for now, we only support HNSW and FTS as secondary indexes, which have their own
    // internal storage, so we do not need to scan and re-insert newly inserted tuples here.
    UncommittedIndexInserter pkInserter{startNodeOffset, this, getPKIndex(),
        getVisibleFunc(transaction)};
    // We need to scan from local storage here because some tuples in local node groups might
    // have been deleted.
    scanIndexColumns(transaction, pkInserter, localNodeTable.getNodeGroups());

    // 4. Clear local table.
    localTable->clear();
}

visible_func NodeTable::getVisibleFunc(const Transaction* transaction) const {
    return
        [this, transaction](offset_t offset_) -> bool { return isVisible(transaction, offset_); };
}

bool NodeTable::checkpoint(main::ClientContext* context, TableCatalogEntry* tableEntry) {
    const bool ret = hasChanges;
    if (hasChanges) {
        // Deleted columns are vacuumed and not checkpointed.
        std::vector<std::unique_ptr<Column>> checkpointColumns;
        std::vector<column_id_t> columnIDs;
        for (auto& property : tableEntry->getProperties()) {
            auto columnID = tableEntry->getColumnID(property.getName());
            checkpointColumns.push_back(std::move(columns[columnID]));
            columnIDs.push_back(columnID);
        }
        columns = std::move(checkpointColumns);

        std::vector<Column*> checkpointColumnPtrs;
        for (const auto& column : columns) {
            checkpointColumnPtrs.push_back(column.get());
        }

        NodeGroupCheckpointState state{columnIDs, std::move(checkpointColumnPtrs), *dataFH,
            memoryManager};
        nodeGroups->checkpoint(*memoryManager, state);
        for (auto& index : indexes) {
            index.checkpoint(context);
        }
        hasChanges = false;
        tableEntry->vacuumColumnIDs(0 /*nextColumnID*/);
    }
    return ret;
}

void NodeTable::rollbackPKIndexInsert(Transaction* transaction, row_idx_t startRow,
    row_idx_t numRows_, node_group_idx_t nodeGroupIdx_) {
    const row_idx_t startNodeOffset =
        startRow + StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx_);

    RollbackPKDeleter pkDeleter{startNodeOffset, numRows_, this, getPKIndex()};
    scanIndexColumns(transaction, pkDeleter, *nodeGroups);
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
void NodeTable::rollbackGroupCollectionInsert(row_idx_t numRows_) {
    nodeGroups->rollbackInsert(numRows_);
}

void NodeTable::rollbackCheckpoint() {
    for (auto& index : indexes) {
        index.rollbackCheckpoint();
    }
}

void NodeTable::reclaimStorage(PageManager& pageManager) const {
    nodeGroups->reclaimStorage(pageManager);
    getPKIndex()->reclaimStorage(pageManager);
}

TableStats NodeTable::getStats(const Transaction* transaction) const {
    auto stats = nodeGroups->getStats();
    if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID)) {
        const auto localStats = localTable->cast<LocalNodeTable>().getStats();
        stats.merge(localStats);
    }
    return stats;
}

bool NodeTable::isVisible(const Transaction* transaction, offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    const auto* nodeGroup = getNodeGroup(nodeGroupIdx);
    return nodeGroup->isVisible(transaction, offsetInGroup);
}

bool NodeTable::isVisibleNoLock(const Transaction* transaction, offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    if (nodeGroupIdx >= nodeGroups->getNumNodeGroupsNoLock()) {
        return false;
    }
    const auto* nodeGroup = getNodeGroupNoLock(nodeGroupIdx);
    return nodeGroup->isVisibleNoLock(transaction, offsetInGroup);
}

bool NodeTable::lookupPK(const Transaction* transaction, ValueVector* keyVector, uint64_t vectorPos,
    offset_t& result) const {
    if (transaction->getLocalStorage()) {
        if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID);
            localTable && localTable->cast<LocalNodeTable>().lookupPK(transaction, keyVector,
                              vectorPos, result)) {
            return true;
        }
    }
    return getPKIndex()->lookup(transaction, keyVector, vectorPos, result,
        [&](offset_t offset) { return isVisibleNoLock(transaction, offset); });
}

void NodeTable::scanIndexColumns(Transaction* transaction, IndexScanHelper& scanHelper,
    const NodeGroupCollection& nodeGroups_) const {
    auto dataChunk = constructDataChunkForColumns(scanHelper.index->getIndexInfo().columnIDs);
    const auto scanState = scanHelper.initScanState(transaction, dataChunk);

    const auto numNodeGroups = nodeGroups_.getNumNodeGroups();
    for (node_group_idx_t nodeGroupToScan = 0u; nodeGroupToScan < numNodeGroups;
         ++nodeGroupToScan) {
        scanState->nodeGroup = nodeGroups_.getNodeGroupNoLock(nodeGroupToScan);

        // It is possible for the node group to have no chunked groups if we are rolling back due to
        // an exception that is thrown before any chunked groups could be appended to the node group
        if (scanState->nodeGroup->getNumChunkedGroups() > 0) {
            scanState->nodeGroupIdx = nodeGroupToScan;
            KU_ASSERT(scanState->nodeGroup);
            scanState->nodeGroup->initializeScanState(transaction, *scanState);
            while (true) {
                if (const auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
                    !scanHelper.processScanOutput(transaction, memoryManager, scanResult,
                        scanState->outputVectors)) {
                    break;
                }
            }
        }
    }
}

void NodeTable::addIndex(std::unique_ptr<Index> index) {
    if (getIndex(index->getName()).has_value()) {
        throw RuntimeException("Index with name " + index->getName() + " already exists.");
    }
    indexes.push_back(IndexHolder{std::move(index)});
}

void NodeTable::dropIndex(const std::string& name) {
    KU_ASSERT(getIndex(name) != nullptr);
    for (auto it = indexes.begin(); it != indexes.end(); ++it) {
        if (common::StringUtils::caseInsensitiveEquals(it->getName(), name)) {
            KU_ASSERT(it->isLoaded());
            indexes.erase(it);
            return;
        }
    }
}

void NodeTable::serialize(Serializer& serializer) const {
    nodeGroups->serialize(serializer);
    serializer.write<uint64_t>(indexes.size());
    for (auto i = 0u; i < indexes.size(); ++i) {
        indexes[i].serialize(serializer);
    }
}

void NodeTable::deserialize(main::ClientContext* context, StorageManager* storageManager,
    Deserializer& deSer) {
    nodeGroups->deserialize(deSer, *memoryManager);
    std::vector<IndexInfo> indexInfos;
    std::vector<length_t> storageInfoBufferSizes;
    std::vector<std::unique_ptr<uint8_t[]>> storageInfoBuffers;
    uint64_t numIndexes = 0u;
    deSer.deserializeValue<uint64_t>(numIndexes);
    indexInfos.reserve(numIndexes);
    storageInfoBufferSizes.reserve(numIndexes);
    storageInfoBuffers.reserve(numIndexes);
    for (uint64_t i = 0; i < numIndexes; ++i) {
        IndexInfo indexInfo = IndexInfo::deserialize(deSer);
        indexInfos.push_back(indexInfo);
        uint64_t storageInfoSize = 0u;
        deSer.deserializeValue<uint64_t>(storageInfoSize);
        storageInfoBufferSizes.push_back(storageInfoSize);
        auto storageInfoBuffer = std::make_unique<uint8_t[]>(storageInfoSize);
        deSer.read(storageInfoBuffer.get(), storageInfoSize);
        storageInfoBuffers.push_back(std::move(storageInfoBuffer));
    }
    indexes.clear();
    indexes.reserve(indexInfos.size());
    for (auto i = 0u; i < indexInfos.size(); ++i) {
        indexes.push_back(IndexHolder(indexInfos[i], std::move(storageInfoBuffers[i]),
            storageInfoBufferSizes[i]));
        if (indexInfos[i].isBuiltin) {
            indexes[i].load(context, storageManager);
        }
    }
}

} // namespace storage
} // namespace kuzu
