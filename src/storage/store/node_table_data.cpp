#include "storage/store/node_table_data.h"

#include "common/cast.h"
#include "common/types/types.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/store/node_table.h"
#include "storage/store/table.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

bool NodeDataScanState::nextVector() {
    vectorIdx++;
    const auto startOffsetInNodeGroup = vectorIdx * DEFAULT_VECTOR_CAPACITY;
    if (startOffsetInNodeGroup >= numRowsInNodeGroup) {
        numRowsToScan = 0;
        return false;
    }
    numRowsToScan = std::min(DEFAULT_VECTOR_CAPACITY, numRowsInNodeGroup - startOffsetInNodeGroup);
    return true;
}

NodeTableData::NodeTableData(BMFileHandle* dataFH, DiskArrayCollection* metadataDAC,
    TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
    const std::vector<Property>& properties, TablesStatistics* tablesStatistics,
    bool enableCompression)
    : TableData{dataFH, metadataDAC, tableEntry, bufferManager, wal, enableCompression} {
    const auto maxColumnID =
        std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
            return a.getColumnID() < b.getColumnID();
        })->getColumnID();
    columns.resize(maxColumnID + 1);
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        const auto metadataDAHInfo = dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                                         ->getMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, i);
        const auto columnName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT, "");
        columns[property.getColumnID()] = ColumnFactory::createColumn(columnName,
            property.getDataType().copy(), *metadataDAHInfo, dataFH, *metadataDAC, bufferManager,
            wal, &DUMMY_WRITE_TRANSACTION, enableCompression);
    }
}

void NodeTableData::initializeScanState(Transaction* transaction, TableScanState& scanState) const {
    auto& dataScanState = scanState.dataScanState->cast<NodeDataScanState>();
    KU_ASSERT(dataScanState.chunkStates.size() == scanState.columnIDs.size());
    if (scanState.dataScanState) {
        initializeColumnScanStates(transaction, scanState, scanState.nodeGroupIdx);
    }
    if (transaction->isWriteTransaction()) {
        initializeLocalNodeReadState(transaction, scanState, scanState.nodeGroupIdx);
    }
    dataScanState.vectorIdx = INVALID_IDX;
    dataScanState.numRowsInNodeGroup =
        columns[0]->getMetadata(scanState.nodeGroupIdx, TransactionType::READ_ONLY).numValues;
}

void NodeTableData::initializeColumnScanStates(Transaction* transaction, TableScanState& scanState,
    node_group_idx_t nodeGroupIdx) const {
    scanState.zoneMapResult = ZoneMapCheckResult::ALWAYS_SCAN;
    auto& dataScanState = scanState.dataScanState->cast<NodeDataScanState>();
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        if (scanState.columnIDs[i] == INVALID_COLUMN_ID) {
            continue;
        }
        auto column = getColumn(scanState.columnIDs[i]);
        auto& chunkState = dataScanState.chunkStates[i];
        column->initChunkState(transaction, nodeGroupIdx, chunkState);
        if (!scanState.columnPredicateSets.empty()) {
            if (scanState.columnPredicateSets[i].checkZoneMap(chunkState.metadata.compMeta) ==
                ZoneMapCheckResult::SKIP_SCAN) {
                scanState.zoneMapResult = ZoneMapCheckResult::SKIP_SCAN;
            }
        }
    }
    KU_ASSERT(sanityCheckOnColumnNumValues(dataScanState));
}

bool NodeTableData::sanityCheckOnColumnNumValues(const NodeDataScanState& scanState) {
    // Sanity check on that all valid columns should have the same numValues.
    const auto validColumn = std::find_if(scanState.columnIDs.begin(), scanState.columnIDs.end(),
        [](column_id_t columnID) { return columnID != INVALID_COLUMN_ID; });
    if (validColumn != scanState.columnIDs.end()) {
        const auto numValues =
            scanState.chunkStates[validColumn - scanState.columnIDs.begin()].metadata.numValues;
        for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
            if (scanState.columnIDs[i] == INVALID_COLUMN_ID) {
                continue;
            }
            if (scanState.chunkStates[i].metadata.numValues != numValues) {
                KU_ASSERT(false);
                return false;
            }
        }
    }
    return true;
}

void NodeTableData::initializeLocalNodeReadState(Transaction* transaction,
    TableScanState& scanState, node_group_idx_t nodeGroupIdx) const {
    const auto localTable = transaction->getLocalStorage()->getLocalTable(tableID,
        LocalStorage::NotExistAction::RETURN_NULL);
    auto& nodeScanState = ku_dynamic_cast<TableScanState&, NodeTableScanState&>(scanState);
    nodeScanState.localNodeGroup = nullptr;
    if (localTable) {
        const auto localNodeTable = ku_dynamic_cast<LocalTable*, LocalNodeTable*>(localTable);
        if (localNodeTable->getTableData()->nodeGroups.contains(nodeGroupIdx)) {
            nodeScanState.localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(
                localNodeTable->getTableData()->nodeGroups.at(nodeGroupIdx).get());
        }
    }
}

void NodeTableData::scan(Transaction* transaction, TableDataScanState& scanState,
    ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto& nodeScanState =
        ku_dynamic_cast<const TableDataScanState&, const NodeDataScanState&>(scanState);
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        if (scanState.columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            KU_ASSERT(scanState.columnIDs[i] < columns.size());
            columns[scanState.columnIDs[i]]->scan(transaction, nodeScanState.chunkStates[i],
                nodeScanState.vectorIdx, nodeScanState.numRowsToScan, &nodeIDVector,
                outputVectors[i]);
        }
    }
}

void NodeTableData::lookup(Transaction* transaction, TableDataScanState& readState,
    const ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    for (auto columnIdx = 0u; columnIdx < readState.columnIDs.size(); columnIdx++) {
        const auto columnID = readState.columnIDs[columnIdx];
        if (columnID == INVALID_COLUMN_ID) {
            KU_ASSERT(outputVectors[columnIdx]->state == nodeIDVector.state);
            for (auto i = 0u; i < outputVectors[columnIdx]->state->getSelVector().getSelSize();
                 i++) {
                const auto pos = outputVectors[columnIdx]->state->getSelVector()[i];
                outputVectors[i]->setNull(pos, true);
            }
        } else {
            KU_ASSERT(readState.columnIDs[columnIdx] < columns.size());
            auto& nodeDataReadState =
                ku_dynamic_cast<TableDataScanState&, NodeDataScanState&>(readState);
            // TODO: Remove `const_cast` on nodeIDVector.
            columns[readState.columnIDs[columnIdx]]->lookup(transaction,
                nodeDataReadState.chunkStates[columnIdx], const_cast<ValueVector*>(&nodeIDVector),
                outputVectors[columnIdx]);
        }
    }
}

void NodeTableData::append(Transaction* transaction, ChunkedNodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto& columnChunk = nodeGroup->getColumnChunkUnsafe(columnID);
        KU_ASSERT(columnID < columns.size());
        auto column = columns[columnID].get();
        Column::ChunkState state;
        column->initChunkState(transaction, nodeGroup->getNodeGroupIdx(), state);
        columns[columnID]->append(&columnChunk, state);
    }
}

void NodeTableData::prepareLocalNodeGroupToCommit(node_group_idx_t nodeGroupIdx,
    Transaction* transaction, LocalNodeNG* localNodeGroup) const {
    auto numNodeGroups = getNumCommittedNodeGroups();
    const auto isNewNodeGroup = nodeGroupIdx >= numNodeGroups;
    KU_ASSERT(std::find_if(columns.begin(), columns.end(), [&](const auto& column) {
        return column->getNumCommittedNodeGroups() != numNodeGroups;
    }) == columns.end());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        const auto column = columns[columnID].get();
        auto localInsertChunk = localNodeGroup->getInsertChunks().getLocalChunk(columnID);
        auto localUpdateChunk = localNodeGroup->getUpdateChunks(columnID).getLocalChunk(0);
        if (localInsertChunk.empty() && localUpdateChunk.empty()) {
            continue;
        }
        column->prepareCommitForChunk(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunk,
            localNodeGroup->getInsertInfoRef(), localUpdateChunk,
            localNodeGroup->getUpdateInfoRef(columnID), {} /* deleteInfo */);
    }
}

void NodeTableData::prepareLocalTableToCommit(Transaction* transaction,
    LocalTableData* localTable) {
    for (auto& [nodeGroupIdx, localNodeGroup] : localTable->nodeGroups) {
        prepareLocalNodeGroupToCommit(nodeGroupIdx, transaction,
            ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(localNodeGroup.get()));
    }
}

} // namespace storage
} // namespace kuzu
