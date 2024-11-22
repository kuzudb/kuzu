#include "storage/store/node_table.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/cast.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "main/db_config.h"
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
    common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow, common::row_idx_t numRows,
    common::transaction_t commitTS) const {
    auto* nodeGroup = table->getNodeGroupNoLock(nodeGroupIdx);
    nodeGroup->applyFuncToChunkedGroups(func, startRow, numRows, commitTS);
}

void NodeTableVersionRecordHandler::rollbackInsert(const transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows) const {
    table->rollbackPKIndexInsert(transaction, startRow, numRows, nodeGroupIdx);

    VersionRecordHandler::rollbackInsert(transaction, nodeGroupIdx, startRow, numRows);
    auto* nodeGroup = table->getNodeGroupNoLock(nodeGroupIdx);
    const auto numRowsToRollback = std::min(numRows, nodeGroup->getNumRows() - startRow);
    nodeGroup->rollbackInsert(startRow);
    table->rollbackGroupCollectionInsert(numRowsToRollback);
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
    if (source == TableScanSource::UNCOMMITTED) {
        nodeGroupStartOffset = transaction->getUncommittedOffset(tableID, nodeGroupStartOffset);
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        nodeIDVector->setValue(i,
            nodeID_t{nodeGroupStartOffset + scanResult.startRow + i, tableID});
    }
    return true;
}

namespace {

struct UncommittedPKInserter : public PKColumnScanHelper {
public:
    UncommittedPKInserter(row_idx_t startNodeOffset, table_id_t tableID, PrimaryKeyIndex* pkIndex,
        visible_func isVisible)
        : PKColumnScanHelper(pkIndex, tableID), startNodeOffset(startNodeOffset),
          nodeIDVector(LogicalType::INTERNAL_ID()), isVisible(std::move(isVisible)) {}

    std::unique_ptr<NodeTableScanState> initPKScanState(DataChunk& dataChunk,
        column_id_t pkColumnID, const std::vector<std::unique_ptr<Column>>& columns) override;

    bool processScanOutput(const transaction::Transaction* transaction,
        NodeGroupScanResult scanResult, const common::ValueVector& scannedVector) override;

    row_idx_t startNodeOffset;
    ValueVector nodeIDVector;
    visible_func isVisible;
};

struct RollbackPKDeleter : public PKColumnScanHelper {
public:
    RollbackPKDeleter(row_idx_t startNodeOffset, row_idx_t numRows, table_id_t tableID,
        PrimaryKeyIndex* pkIndex)
        : PKColumnScanHelper(pkIndex, tableID),
          semiMask(RoaringBitmapSemiMaskUtil::createRoaringBitmapSemiMask(tableID,
              startNodeOffset + numRows)) {
        semiMask->maskRange(startNodeOffset, startNodeOffset + numRows);
        semiMask->enable();
    }

    std::unique_ptr<NodeTableScanState> initPKScanState(DataChunk& dataChunk,
        column_id_t pkColumnID, const std::vector<std::unique_ptr<Column>>& columns) override;

    bool processScanOutput(const transaction::Transaction* transaction,
        NodeGroupScanResult scanResult, const common::ValueVector& scannedVector) override;

    std::unique_ptr<RoaringBitmapSemiMask> semiMask;
};

void insertPK(const Transaction* transaction, const ValueVector& nodeIDVector,
    const ValueVector& pkVector, PrimaryKeyIndex* pkIndex, const visible_func& isVisible) {
    for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
        const auto nodeIDPos = nodeIDVector.state->getSelVector()[i];
        const auto offset = nodeIDVector.readNodeOffset(nodeIDPos);
        auto pkPos = pkVector.state->getSelVector()[i];
        if (pkVector.isNull(pkPos)) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        if (!pkIndex->insert(transaction, &pkVector, pkPos, offset, isVisible)) {
            throw RuntimeException(
                ExceptionMessage::duplicatePKException(pkVector.getAsValue(pkPos)->toString()));
        }
    }
}

std::unique_ptr<NodeTableScanState> UncommittedPKInserter::initPKScanState(DataChunk& dataChunk,
    column_id_t pkColumnID, const std::vector<std::unique_ptr<Column>>& columns) {
    auto scanState = PKColumnScanHelper::initPKScanState(dataChunk, pkColumnID, columns);
    nodeIDVector.setState(dataChunk.state);
    scanState->source = TableScanSource::UNCOMMITTED;
    return scanState;
}

bool UncommittedPKInserter::processScanOutput(const transaction::Transaction* transaction,
    NodeGroupScanResult scanResult, const common::ValueVector& scannedVector) {
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        nodeIDVector.setValue(i, nodeID_t{startNodeOffset + i, tableID});
    }
    insertPK(transaction, nodeIDVector, scannedVector, pkIndex, isVisible);
    startNodeOffset = scanResult.startRow + scanResult.numRows;
    return true;
}

std::unique_ptr<NodeTableScanState> RollbackPKDeleter::initPKScanState(DataChunk& dataChunk,
    column_id_t pkColumnID, const std::vector<std::unique_ptr<Column>>& columns) {
    auto scanState = PKColumnScanHelper::initPKScanState(dataChunk, pkColumnID, columns);
    scanState->source = TableScanSource::COMMITTED;
    scanState->semiMask = semiMask.get();
    return scanState;
}

template<typename T>
concept notIndexHashable = !IndexHashable<T>;

bool RollbackPKDeleter::processScanOutput(const transaction::Transaction* transaction,
    NodeGroupScanResult scanResult, const common::ValueVector& scannedVector) {
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    const auto rollbackFunc = [&]<IndexHashable T>(T) {
        for (idx_t i = 0; i < scannedVector.state->getSelSize(); ++i) {
            const auto pos = scannedVector.state->getSelVector()[i];
            T key = scannedVector.getValue<T>(pos);
            static constexpr auto isVisible = [](offset_t) { return true; };
            offset_t lookupOffset = 0;
            if (pkIndex->lookup(transaction, key, lookupOffset, isVisible)) {
                pkIndex->delete_(key);
            }
        }
    };
    TypeUtils::visit(scannedVector.dataType.getPhysicalType(), std::cref(rollbackFunc),
        []<notIndexHashable T>(T) { KU_UNREACHABLE; });
    return true;
}
} // namespace

bool NodeTableScanState::scanNext(Transaction* transaction) {
    KU_ASSERT(columns.size() == outputVectors.size());
    if (source == TableScanSource::NONE) {
        return false;
    }
    const NodeGroupScanResult scanResult = nodeGroup->scan(transaction, *this);
    if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
        return false;
    }
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    if (source == TableScanSource::UNCOMMITTED) {
        nodeGroupStartOffset = transaction->getUncommittedOffset(tableID, nodeGroupStartOffset);
    }
    for (auto i = 0u; i < scanResult.numRows; i++) {
        nodeIDVector->setValue(i,
            nodeID_t{nodeGroupStartOffset + scanResult.startRow + i, tableID});
    }
    return true;
}

NodeTable::NodeTable(const StorageManager* storageManager,
    const NodeTableCatalogEntry* nodeTableEntry, MemoryManager* memoryManager,
    VirtualFileSystem* vfs, main::ClientContext* context, Deserializer* deSer)
    : Table{nodeTableEntry, storageManager, memoryManager},
      pkColumnID{nodeTableEntry->getColumnID(nodeTableEntry->getPrimaryKeyName())},
      versionRecordHandler(this) {
    const auto maxColumnID = nodeTableEntry->getMaxColumnID();
    columns.resize(maxColumnID + 1);
    for (auto i = 0u; i < nodeTableEntry->getNumProperties(); i++) {
        auto& property = nodeTableEntry->getProperty(i);
        const auto columnID = nodeTableEntry->getColumnID(property.getName());
        const auto columnName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
        columns[columnID] = ColumnFactory::createColumn(columnName, property.getType().copy(),
            dataFH, memoryManager, shadowFile, enableCompression);
    }

    nodeGroups =
        std::make_unique<NodeGroupCollection>(*memoryManager, getNodeTableColumnTypes(*this),
            enableCompression, storageManager->getDataFH(), deSer, &versionRecordHandler);
    initializePKIndex(storageManager->getDatabasePath(), nodeTableEntry,
        storageManager->isReadOnly(), vfs, context);
}

std::unique_ptr<NodeTable> NodeTable::loadTable(Deserializer& deSer, const Catalog& catalog,
    StorageManager* storageManager, MemoryManager* memoryManager, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    std::string key;
    table_id_t tableID = INVALID_TABLE_ID;
    deSer.validateDebuggingInfo(key, "table_id");
    deSer.deserializeValue<table_id_t>(tableID);
    const auto catalogEntry = catalog.getTableCatalogEntry(&DUMMY_TRANSACTION, tableID);
    if (!catalogEntry) {
        throw RuntimeException(
            stringFormat("Load table failed: table {} doesn't exist in catalog.", tableID));
    }
    return std::make_unique<NodeTable>(storageManager,
        catalogEntry->ptrCast<NodeTableCatalogEntry>(), memoryManager, vfs, context, &deSer);
}

void NodeTable::initializePKIndex(const std::string& databasePath,
    const NodeTableCatalogEntry* nodeTableEntry, bool readOnly, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    pkIndex = std::make_unique<PrimaryKeyIndex>(
        StorageUtils::getNodeIndexIDAndFName(vfs, databasePath, tableID), readOnly,
        main::DBConfig::isDBPathInMemory(databasePath),
        nodeTableEntry->getPrimaryKeyDefinition().getType().getPhysicalType(),
        *memoryManager->getBufferManager(), shadowFile, vfs, context);
}

row_idx_t NodeTable::getNumTotalRows(const Transaction* transaction) {
    auto numLocalRows = 0u;
    if (auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL)) {
        numLocalRows = localTable->getNumTotalRows();
    }
    return numLocalRows + nodeGroups->getNumTotalRows();
}

void NodeTable::initScanState(Transaction* transaction, TableScanState& scanState) const {
    auto& nodeScanState = scanState.cast<NodeTableScanState>();
    NodeGroup* nodeGroup = nullptr;
    switch (nodeScanState.source) {
    case TableScanSource::COMMITTED: {
        nodeGroup = nodeGroups->getNodeGroup(nodeScanState.nodeGroupIdx);
    } break;
    case TableScanSource::UNCOMMITTED: {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        auto& localNodeTable = localTable->cast<LocalNodeTable>();
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

bool NodeTable::lookup(Transaction* transaction, const TableScanState& scanState) const {
    KU_ASSERT(scanState.nodeIDVector->state->getSelVector().getSelSize() == 1);
    const auto nodeIDPos = scanState.nodeIDVector->state->getSelVector()[0];
    if (scanState.nodeIDVector->isNull(nodeIDPos)) {
        return false;
    }
    const auto nodeOffset = scanState.nodeIDVector->readNodeOffset(nodeIDPos);
    offset_t rowIdxInGroup =
        transaction->isUnCommitted(tableID, nodeOffset) ?
            transaction->getLocalRowIdx(tableID, nodeOffset) -
                StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx) :
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(scanState.nodeGroupIdx);
    scanState.rowIdxVector->setValue<row_idx_t>(nodeIDPos, rowIdxInGroup);
    return scanState.nodeGroup->lookup(transaction, scanState);
}

offset_t NodeTable::validateUniquenessConstraint(const Transaction* transaction,
    const std::vector<ValueVector*>& propertyVectors) const {
    const auto pkVector = propertyVectors[pkColumnID];
    KU_ASSERT(pkVector->state->getSelVector().getSelSize() == 1);
    const auto pkVectorPos = pkVector->state->getSelVector()[0];
    offset_t offset = INVALID_OFFSET;
    if (pkIndex->lookup(transaction, propertyVectors[pkColumnID], pkVectorPos, offset,
            [&](offset_t offset_) { return isVisible(transaction, offset_); })) {
        return offset;
    }
    if (const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL)) {
        return localTable->cast<LocalNodeTable>().validateUniquenessConstraint(transaction,
            *pkVector);
    }
    return INVALID_OFFSET;
}

void NodeTable::validatePkNotExists(const Transaction* transaction, ValueVector* pkVector) {
    offset_t dummyOffset = INVALID_OFFSET;
    auto& selVector = pkVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1);
    if (pkVector->isNull(selVector[0])) {
        throw RuntimeException(ExceptionMessage::nullPKException());
    }
    if (pkIndex->lookup(transaction, pkVector, selVector[0], dummyOffset,
            [&](offset_t offset) { return isVisible(transaction, offset); })) {
        throw RuntimeException(
            ExceptionMessage::duplicatePKException(pkVector->getAsValue(selVector[0])->toString()));
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
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::CREATE);
    validatePkNotExists(transaction, (ValueVector*)&nodeInsertState.pkVector);
    localTable->insert(transaction, insertState);
    if (transaction->shouldLogToWAL()) {
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
    // NOTE: We assume all input all flatten now. This is to simplify the implementation.
    // We should optimize this to take unflat input later.
    auto& nodeUpdateState = updateState.constCast<NodeTableUpdateState>();
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1 &&
              nodeUpdateState.propertyVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    if (nodeUpdateState.nodeIDVector.isNull(pos)) {
        return;
    }
    if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
        validatePkNotExists(transaction, &nodeUpdateState.propertyVector);
    }
    const auto nodeOffset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (transaction->isUnCommitted(tableID, nodeOffset)) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        KU_ASSERT(localTable);
        auto dummyTrx = Transaction::getDummyTransactionFromExistingOne(*transaction);
        localTable->update(&dummyTrx, updateState);
    } else {
        if (nodeUpdateState.columnID == pkColumnID && pkIndex) {
            insertPK(transaction, nodeUpdateState.nodeIDVector, nodeUpdateState.propertyVector,
                pkIndex.get(), getVisibleFunc(transaction));
        }
        const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        const auto rowIdxInGroup =
            nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups->getNodeGroup(nodeGroupIdx)
            ->update(transaction, rowIdxInGroup, nodeUpdateState.columnID,
                nodeUpdateState.propertyVector);
    }
    if (transaction->shouldLogToWAL()) {
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
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    if (localTable && transaction->isUnCommitted(tableID, nodeOffset)) {
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
        localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
    }
    if (localTable) {
        localTable->addColumn(transaction, addColumnState);
    }
    nodeGroups->addColumn(transaction, addColumnState);
    hasChanges = true;
}

std::pair<offset_t, offset_t> NodeTable::appendToLastNodeGroup(Transaction* transaction,
    ChunkedNodeGroup& chunkedGroup) {
    hasChanges = true;
    return nodeGroups->appendToLastNodeGroupAndFlushWhenFull(transaction, chunkedGroup);
}

common::DataChunk NodeTable::constructDataChunkForPKColumn() const {
    std::vector<LogicalType> types;
    types.push_back(columns[pkColumnID]->getDataType().copy());
    return constructDataChunk(std::move(types));
}

void NodeTable::commit(Transaction* transaction, LocalTable* localTable) {
    auto startNodeOffset = nodeGroups->getNumTotalRows();
    transaction->setMaxCommittedNodeOffset(tableID, startNodeOffset);
    auto& localNodeTable = localTable->cast<LocalNodeTable>();
    // 1. Append all tuples from local storage to nodeGroups regardless deleted or not.
    // Note: We cannot simply remove all deleted tuples in local node table, as they may have
    // connected local rels. Directly removing them will cause shift of committed node offset,
    // leading to inconsistent result with connected rels.
    nodeGroups->append(transaction, localNodeTable.getNodeGroups());
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
                    const auto nodeOffset = numLocalRows + row;
                    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
                    const auto rowIdxInGroup =
                        startNodeOffset + nodeOffset -
                        StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
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

    // 3. Scan pk column for newly inserted tuples that are not deleted and insert into pk index.
    UncommittedPKInserter pkInserter{startNodeOffset, tableID, pkIndex.get(),
        getVisibleFunc(transaction)};
    // We need to scan from local storage here because some tuples in local node groups might
    // have been deleted.
    scanPKColumn(transaction, pkInserter, localNodeTable.getNodeGroups());

    // 4. Clear local table.
    localTable->clear();
}

visible_func NodeTable::getVisibleFunc(const Transaction* transaction) const {
    return
        [this, transaction](offset_t offset_) -> bool { return isVisible(transaction, offset_); };
}

void NodeTable::checkpoint(Serializer& ser, TableCatalogEntry* tableEntry) {
    if (hasChanges) {
        // Deleted columns are vaccumed and not checkpointed or serialized.
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
        pkIndex->checkpoint();
        hasChanges = false;
        tableEntry->vacuumColumnIDs(0 /*nextColumnID*/);
    }
    serialize(ser);
}

void NodeTable::rollbackPKIndexInsert(const transaction::Transaction* transaction,
    common::row_idx_t startRow, common::row_idx_t numRows_,
    common::node_group_idx_t nodeGroupIdx_) {
    row_idx_t startNodeOffset = startRow;
    for (node_group_idx_t i = 0; i < nodeGroupIdx_; ++i) {
        startNodeOffset += nodeGroups->getNodeGroupNoLock(i)->getNumRows();
    }

    RollbackPKDeleter pkDeleter{startNodeOffset, numRows_, tableID, pkIndex.get()};
    scanPKColumn(transaction, pkDeleter, *nodeGroups);
}

void NodeTable::rollbackGroupCollectionInsert(common::row_idx_t numRows_) {
    nodeGroups->rollbackInsert(numRows_);
}

void NodeTable::rollbackCheckpoint() {
    pkIndex->rollbackCheckpoint();
}

TableStats NodeTable::getStats(const Transaction* transaction) const {
    auto stats = nodeGroups->getStats();
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    if (localTable) {
        const auto localStats = localTable->cast<LocalNodeTable>().getStats();
        stats.merge(localStats);
    }
    return stats;
}

void NodeTable::serialize(Serializer& serializer) const {
    Table::serialize(serializer);
    nodeGroups->serialize(serializer);
}

bool NodeTable::isVisible(const Transaction* transaction, offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    auto* nodeGroup = getNodeGroup(nodeGroupIdx);
    return nodeGroup->isVisible(transaction, offsetInGroup);
}

bool NodeTable::isVisibleNoLock(const Transaction* transaction, offset_t offset) const {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    if (nodeGroupIdx >= nodeGroups->getNumNodeGroupsNoLock()) {
        return false;
    }
    auto* nodeGroup = getNodeGroupNoLock(nodeGroupIdx);
    return nodeGroup->isVisibleNoLock(transaction, offsetInGroup);
}

bool NodeTable::lookupPK(const Transaction* transaction, ValueVector* keyVector, uint64_t vectorPos,
    offset_t& result) const {
    if (transaction->getLocalStorage()) {
        const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
            LocalStorage::NotExistAction::RETURN_NULL);
        if (localTable &&
            localTable->cast<LocalNodeTable>().lookupPK(transaction, keyVector, result)) {
            return true;
        }
    }
    return pkIndex->lookup(transaction, keyVector, vectorPos, result,
        [&](offset_t offset) { return isVisibleNoLock(transaction, offset); });
}

void NodeTable::scanPKColumn(const Transaction* transaction, PKColumnScanHelper& scanHelper,
    NodeGroupCollection& nodeGroups_) {
    auto dataChunk = constructDataChunkForPKColumn();
    auto scanState = scanHelper.initPKScanState(dataChunk, pkColumnID, columns);

    node_group_idx_t nodeGroupToScan = 0u;
    while (nodeGroupToScan < nodeGroups_.getNumNodeGroups()) {
        scanState->nodeGroup = nodeGroups_.getNodeGroup(nodeGroupToScan);
        scanState->nodeGroupIdx = nodeGroupToScan;
        KU_ASSERT(scanState->nodeGroup);
        scanState->nodeGroup->initializeScanState(transaction, *scanState);
        while (true) {
            auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
            if (!scanHelper.processScanOutput(transaction, scanResult,
                    *scanState->outputVectors[0])) {
                break;
            }
        }
        nodeGroupToScan++;
    }
}

std::unique_ptr<NodeTableScanState> PKColumnScanHelper::initPKScanState(DataChunk& dataChunk,
    column_id_t pkColumnID, const std::vector<std::unique_ptr<Column>>& columns) {
    std::vector<column_id_t> columnIDs{pkColumnID};
    auto scanState = std::make_unique<NodeTableScanState>(tableID, columnIDs);
    for (auto& vector : dataChunk.valueVectors) {
        scanState->outputVectors.push_back(vector.get());
    }
    scanState->outState = dataChunk.state.get();
    for (const auto& column : columns) {
        scanState->columns.push_back(column.get());
    }
    return scanState;
}

} // namespace storage
} // namespace kuzu
