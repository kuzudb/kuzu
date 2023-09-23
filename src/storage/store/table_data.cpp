#include "storage/store/table_data.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

TableData::TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, table_id_t tableID,
    BufferManager* bufferManager, WAL* wal, const std::vector<catalog::Property*>& properties,
    TablesStatistics* tablesStatistics)
    : dataFH{dataFH}, metadataFH{metadataFH}, tableID{tableID},
      bufferManager{bufferManager}, wal{wal} {
    columns.reserve(properties.size());
    for (auto property : properties) {
        columns.push_back(NodeColumnFactory::createNodeColumn(*property, dataFH, metadataFH,
            bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
            RWPropertyStats(tablesStatistics, tableID, property->getPropertyID())));
    }
}

void TableData::read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
    const std::vector<common::column_id_t>& columnIDs,
    const std::vector<common::ValueVector*>& outputVectors) {
    if (nodeIDVector->isSequential()) {
        scan(transaction, nodeIDVector, columnIDs, outputVectors);
    } else {
        lookup(transaction, nodeIDVector, columnIDs, outputVectors);
    }
}

void TableData::scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
    const std::vector<common::column_id_t>& columnIDs,
    const std::vector<common::ValueVector*>& outputVectors) {
    assert(columnIDs.size() == outputVectors.size() && !nodeIDVector->state->isFlat());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        if (columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            assert(columnIDs[i] < columns.size());
            columns[columnIDs[i]]->scan(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        auto localStorage = transaction->getLocalStorage();
        localStorage->scan(tableID, nodeIDVector, columnIDs, outputVectors);
    }
}

void TableData::lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
    const std::vector<common::column_id_t>& columnIDs,
    const std::vector<common::ValueVector*>& outputVectors) {
    auto pos = nodeIDVector->state->selVector->selectedPositions[0];
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(pos, true);
        } else {
            assert(columnIDs[i] < columns.size());
            columns[columnIDs[i]]->lookup(transaction, nodeIDVector, outputVectors[i]);
        }
    }
    if (transaction->isWriteTransaction()) {
        transaction->getLocalStorage()->lookup(tableID, nodeIDVector, columnIDs, outputVectors);
    }
}

void TableData::insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
    const std::vector<common::ValueVector*>& propertyVectors) {
    // We assume that offsets are given in the ascending order, thus lastOffset is the max one.
    offset_t lastOffset = nodeIDVector->readNodeOffset(
        nodeIDVector->state->selVector
            ->selectedPositions[nodeIDVector->state->selVector->selectedSize - 1]);
    auto currentNumNodeGroups = getNumNodeGroups(transaction);
    if (lastOffset >= StorageUtils::getStartOffsetOfNodeGroup(currentNumNodeGroups)) {
        auto newNodeGroup = std::make_unique<NodeGroup>(this);
        newNodeGroup->setNodeGroupIdx(currentNumNodeGroups);
        append(newNodeGroup.get());
    }
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        if (columns[columnID]->getDataType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
            continue;
        }
        transaction->getLocalStorage()->update(
            tableID, columnID, nodeIDVector, propertyVectors[columnID]);
    }
}

void TableData::update(transaction::Transaction* transaction, common::column_id_t columnID,
    common::ValueVector* nodeIDVector, common::ValueVector* propertyVector) {
    assert(columnID < columns.size());
    transaction->getLocalStorage()->update(tableID, columnID, nodeIDVector, propertyVector);
}

void TableData::update(transaction::Transaction* transaction, common::column_id_t columnID,
    common::offset_t nodeOffset, common::ValueVector* propertyVector,
    common::sel_t posInPropertyVector) const {
    transaction->getLocalStorage()->update(
        tableID, columnID, nodeOffset, propertyVector, posInPropertyVector);
}

void TableData::append(kuzu::storage::NodeGroup* nodeGroup) {
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        auto columnChunk = nodeGroup->getColumnChunk(columnID);
        auto numPages = columnChunk->getNumPages();
        auto startPageIdx = dataFH->addNewPages(numPages);
        assert(columnID < columns.size());
        columns[columnID]->append(columnChunk, startPageIdx, nodeGroup->getNodeGroupIdx());
    }
}

void TableData::addColumn(transaction::Transaction* transaction, const catalog::Property& property,
    common::ValueVector* defaultValueVector, TablesStatistics* tableStats) {
    auto nodeColumn =
        NodeColumnFactory::createNodeColumn(property, dataFH, metadataFH, bufferManager, wal,
            transaction, RWPropertyStats(tableStats, tableID, property.getPropertyID()));
    nodeColumn->populateWithDefaultVal(
        property, nodeColumn.get(), defaultValueVector, getNumNodeGroups(transaction));
    columns.push_back(std::move(nodeColumn));
}

void TableData::checkpointInMemory() {
    for (auto& column : columns) {
        column->checkpointInMemory();
    }
}

void TableData::rollbackInMemory() {
    for (auto& column : columns) {
        column->rollbackInMemory();
    }
}

} // namespace storage
} // namespace kuzu
