#include "storage/store/node_table_data.h"

#include "common/cast.h"
#include "common/types/types.h"
#include "storage/local_storage/local_node_table.h"
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
            *property.getDataType()->copy(), *metadataDAHInfo, dataFH, *metadataDAC, bufferManager,
            wal, &DUMMY_WRITE_TRANSACTION, enableCompression);
    }
}

void NodeTableData::initializeScanState(Transaction* transaction, TableScanState& scanState) const {
    KU_UNREACHABLE;
}

void NodeTableData::scan(Transaction* transaction, TableDataScanState& scanState,
    ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_UNREACHABLE;
}

void NodeTableData::lookup(Transaction* transaction, TableDataScanState& readState,
    const ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_UNREACHABLE;
}

offset_t NodeTableData::append(Transaction* transaction, ChunkedNodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto& columnChunk = nodeGroup->getColumnChunkUnsafe(columnID);
        KU_ASSERT(columnID < columns.size());
        auto column = columns[columnID].get();
        ChunkState state;
        column->initChunkState(transaction, nodeGroup->getNodeGroupIdx(), state);
        columns[columnID]->append(&columnChunk, state);
    }
    return nodeGroup->getNodeGroupIdx() * StorageConstants::NODE_GROUP_SIZE;
}

std::unique_ptr<ChunkedNodeGroup> NodeTableData::getCommittedNodeGroup(
    node_group_idx_t nodeGroupIdx) const {
    std::vector<LogicalType> dataTypes;
    for (auto& column : columns) {
        dataTypes.push_back(column->getDataType());
    }
    auto chunkedNodeGroup = std::make_unique<ChunkedNodeGroup>(dataTypes, enableCompression, 0, 0);
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        getColumn(columnID)->setMetadataToChunk(nodeGroupIdx,
            chunkedNodeGroup->getColumnChunkUnsafe(columnID));
    }
    const row_idx_t numRows = chunkedNodeGroup->getColumnChunk(0).getFlushedMetadata().numValues;
    for (auto columnID = 1; columnID < columns.size(); columnID++) {
        KU_ASSERT(
            numRows == chunkedNodeGroup->getColumnChunk(columnID).getFlushedMetadata().numValues);
    }
    chunkedNodeGroup->setNumRows(numRows);
    return chunkedNodeGroup;
}

} // namespace storage
} // namespace kuzu
