#include "storage/store/node_table_data.h"

#include "storage/stats/nodes_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTableData::NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, table_id_t tableID,
    BufferManager* bufferManager, WAL* wal, const std::vector<Property*>& properties,
    TablesStatistics* tablesStatistics, bool enableCompression)
    : TableData{dataFH, metadataFH, tableID, bufferManager, wal, enableCompression,
          ColumnDataFormat::REGULAR} {
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto property = properties[i];
        auto metadataDAHInfo =
            dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                ->getMetadataDAHInfo(Transaction::getDummyWriteTrx().get(), tableID, i);
        columns.push_back(ColumnFactory::createColumn(*property->getDataType(), *metadataDAHInfo,
            dataFH, metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
            RWPropertyStats(tablesStatistics, tableID, property->getPropertyID()),
            enableCompression));
    }
}

void NodeTableData::scan(transaction::Transaction* transaction, TableReadState& readState,
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(readState.columnIDs.size() == outputVectors.size() && !nodeIDVector->state->isFlat());
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        if (readState.columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            KU_ASSERT(readState.columnIDs[i] < columns.size());
            columns[readState.columnIDs[i]]->scan(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->scan(
            tableID, nodeIDVector, readState.columnIDs, outputVectors);
    }
}

void NodeTableData::lookup(transaction::Transaction* transaction, TableReadState& readState,
    ValueVector* nodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            KU_ASSERT(readState.columnIDs[i] < columns.size());
            columns[readState.columnIDs[i]]->lookup(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->lookup(
            tableID, nodeIDVector, readState.columnIDs, outputVectors);
    }
}

void NodeTableData::append(kuzu::storage::NodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto columnChunk = nodeGroup->getColumnChunk(columnID);
        KU_ASSERT(columnID < columns.size());
        columns[columnID]->append(columnChunk, nodeGroup->getNodeGroupIdx());
    }
}

} // namespace storage
} // namespace kuzu
