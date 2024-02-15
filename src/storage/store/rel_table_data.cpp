#include "storage/store/rel_table_data.h"

#include "common/assert.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/null_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDataReadState::RelDataReadState(ColumnDataFormat dataFormat)
    : dataFormat{dataFormat}, startNodeOffset{0}, numNodes{0}, currentNodeOffset{0},
      posInCurrentCSR{0}, readFromLocalStorage{false}, localNodeGroup{nullptr} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
}

bool RelDataReadState::hasMoreToReadFromLocalStorage() const {
    KU_ASSERT(localNodeGroup);
    return posInCurrentCSR < localNodeGroup->getRelNGInfo()->getNumInsertedTuples(
                                 currentNodeOffset - startNodeOffset);
}

bool RelDataReadState::trySwitchToLocalStorage() {
    if (localNodeGroup && localNodeGroup->getRelNGInfo()->getNumInsertedTuples(
                              currentNodeOffset - startNodeOffset) > 0) {
        readFromLocalStorage = true;
        posInCurrentCSR = 0;
        return true;
    }
    return false;
}

bool RelDataReadState::hasMoreToRead(transaction::Transaction* transaction) {
    if (dataFormat == ColumnDataFormat::REGULAR) {
        return false;
    }
    if (transaction->isWriteTransaction()) {
        if (readFromLocalStorage) {
            // Already read from local storage. Check if there are more in local storage.
            return hasMoreToReadFromLocalStorage();
        } else {
            if (hasMoreToReadInPersistentStorage()) {
                return true;
            } else {
                // Try switch to read from local storage.
                return trySwitchToLocalStorage();
            }
        }
    } else {
        return hasMoreToReadInPersistentStorage();
    }
}

void RelDataReadState::populateCSRListEntries() {
    for (auto i = 0u; i < numNodes; i++) {
        csrListEntries[i].size = csrHeaderChunks.getCSRLength(i);
        KU_ASSERT(csrListEntries[i].size <=
                  csrHeaderChunks.getEndCSROffset(i) - csrHeaderChunks.getStartCSROffset(i));
        csrListEntries[i].offset = csrHeaderChunks.getStartCSROffset(i);
    }
}

std::pair<offset_t, offset_t> RelDataReadState::getStartAndEndOffset() {
    auto currCSRListEntry = csrListEntries[currentNodeOffset - startNodeOffset];
    auto currCSRSize = currCSRListEntry.size;
    auto startOffset = currCSRListEntry.offset + posInCurrentCSR;
    auto numRowsToRead = std::min(currCSRSize - posInCurrentCSR, DEFAULT_VECTOR_CAPACITY);
    posInCurrentCSR += numRowsToRead;
    return {startOffset, startOffset + numRowsToRead};
}

RelTableData::RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, RelTableCatalogEntry* tableEntry,
    RelsStoreStats* relsStoreStats, RelDataDirection direction, bool enableCompression,
    ColumnDataFormat dataFormat)
    : TableData{dataFH, metadataFH, tableEntry->getTableID(), bufferManager, wal, enableCompression,
          dataFormat},
      direction{direction} {
    auto adjMetadataDAHInfo =
        relsStoreStats->getAdjMetadataDAHInfo(&DUMMY_WRITE_TRANSACTION, tableID, direction);
    auto adjColName = StorageUtils::getColumnName(
        "", StorageUtils::ColumnType::ADJ, RelDataDirectionUtils::relDirectionToString(direction));
    adjColumn = ColumnFactory::createColumn(adjColName, *LogicalType::INTERNAL_ID(),
        *adjMetadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
        RWPropertyStats::empty(), enableCompression);
    auto& properties = tableEntry->getPropertiesRef();
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        auto metadataDAHInfo = relsStoreStats->getPropertyMetadataDAHInfo(
            &DUMMY_WRITE_TRANSACTION, tableID, i, direction);
        auto colName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT,
                RelDataDirectionUtils::relDirectionToString(direction));
        columns.push_back(ColumnFactory::createColumn(colName, *property.getDataType()->copy(),
            *metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, &DUMMY_WRITE_TRANSACTION,
            RWPropertyStats(relsStoreStats, tableID, property.getPropertyID()), enableCompression));
    }
    // Set common tableID for adjColumn and relIDColumn.
    dynamic_cast<InternalIDColumn*>(adjColumn.get())
        ->setCommonTableID(tableEntry->getNbrTableID(direction));
    dynamic_cast<InternalIDColumn*>(columns[REL_ID_COLUMN_ID].get())->setCommonTableID(tableID);
}

void RelTableData::initAdjColumn(Transaction* transaction, table_id_t boundTableID,
    InMemDiskArray<ColumnChunkMetadata>* metadataDA) {
    auto defaultVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID());
    defaultVector->setAllNull();
    defaultVector->setValue<internalID_t>(0, internalID_t{0, boundTableID});
    adjColumn->populateWithDefaultVal(transaction, metadataDA, defaultVector.get());
}

void RelTableData::initializeReadState(Transaction* /*transaction*/,
    std::vector<common::column_id_t> columnIDs, ValueVector* /*inNodeIDVector*/,
    RelDataReadState* readState) {
    readState->direction = direction;
    readState->columnIDs = std::move(columnIDs);
    // Reset to read from persistent storage.
    readState->readFromLocalStorage = false;
}

LocalRelNG* RelTableData::getLocalNodeGroup(
    transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx) {
    auto localTableData = transaction->getLocalStorage()->getLocalTableData(
        tableID, getDataIdxFromDirection(direction));
    LocalRelNG* localNodeGroup = nullptr;
    if (localTableData) {
        auto localRelTableData =
            ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTableData);
        if (localRelTableData->nodeGroups.contains(nodeGroupIdx)) {
            localNodeGroup = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(
                localRelTableData->nodeGroups.at(nodeGroupIdx).get());
        }
    }
    return localNodeGroup;
}

void RelTableData::scan(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    adjColumn->scan(transaction, inNodeIDVector, outputVectors[0]);
    if (transaction->isReadOnly() && !ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->scan(
            transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
    if (transaction->isWriteTransaction()) {
        auto nodeOffset = inNodeIDVector->readNodeOffset(0);
        auto localNodeGroup =
            getLocalNodeGroup(transaction, StorageUtils::getNodeGroupIdx(nodeOffset));
        if (localNodeGroup) {
            localNodeGroup->applyLocalChangesForRegularColumns(
                inNodeIDVector, readState.columnIDs, outputVectors);
        }
        ValueVector::discardNull(*outputVectors[0]);
    }
}

void RelTableData::lookup(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(dataFormat == ColumnDataFormat::REGULAR);
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->lookup(transaction, inNodeIDVector, outputVectors[0]);
    if (transaction->isReadOnly() && !ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->lookup(
            transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
    if (transaction->isWriteTransaction()) {
        for (auto pos = 0u; pos < inNodeIDVector->state->selVector->selectedSize; pos++) {
            auto selPos = inNodeIDVector->state->selVector->selectedPositions[pos];
            auto nodeOffset = inNodeIDVector->readNodeOffset(selPos);
            auto [nodeGroupIdx, offsetInChunk] =
                StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
            auto localNodeGroup = getLocalNodeGroup(transaction, nodeGroupIdx);
            if (localNodeGroup) {
                localNodeGroup->applyLocalChangesForRegularColumns(
                    offsetInChunk, readState.columnIDs, outputVectors, selPos);
            }
        }
        ValueVector::discardNull(*outputVectors[0]);
    }
}

void RelTableData::insert(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    auto checkPersistentStorage =
        localTableData->insert(srcNodeIDVector, dstNodeIDVector, propertyVectors);
    auto [nodeGroupIdx, offset] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(
        srcNodeIDVector->getValue<nodeID_t>(srcNodeIDVector->state->selVector->selectedPositions[0])
            .offset);
    auto adjNullColumn = ku_dynamic_cast<Column*, NullColumn*>(adjColumn->getNullColumn());
    if (checkPersistentStorage && (nodeGroupIdx < adjNullColumn->getNumNodeGroups(transaction)) &&
        !adjNullColumn->isNull(transaction, nodeGroupIdx, offset)) {
        throw RuntimeException{"Many-one, one-one relationship violated."};
    }
}

void RelTableData::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* srcNodeIDVector, ValueVector* relIDVector, ValueVector* propertyVector) {
    KU_ASSERT(columnID < columns.size() && columnID != REL_ID_COLUMN_ID);
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    localTableData->update(srcNodeIDVector, relIDVector, columnID, propertyVector);
}

bool RelTableData::delete_(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    auto localTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
        transaction->getLocalStorage()->getOrCreateLocalTableData(
            tableID, columns, TableType::REL, dataFormat, getDataIdxFromDirection(direction)));
    return localTableData->delete_(srcNodeIDVector, dstNodeIDVector, relIDVector);
}

bool RelTableData::checkIfNodeHasRels(Transaction* transaction, ValueVector* srcNodeIDVector) {
    auto nodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    return !ku_dynamic_cast<Column*, NullColumn*>(adjColumn->getNullColumn())
                ->isNull(transaction, nodeGroupIdx, offsetInChunk);
}

void RelTableData::append(NodeGroup* nodeGroup) {
    adjColumn->append(nodeGroup->getColumnChunk(0), nodeGroup->getNodeGroupIdx());
    for (auto columnID = 0u; columnID < columns.size(); columnID++) {
        columns[columnID]->append(
            nodeGroup->getColumnChunk(columnID + 1), nodeGroup->getNodeGroupIdx());
    }
}

void RelTableData::resizeColumns(node_group_idx_t numNodeGroups) {
    auto currentNumNodeGroups = adjColumn->getNumNodeGroups(&DUMMY_WRITE_TRANSACTION);
    if (numNodeGroups < currentNumNodeGroups) {
        return;
    }
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    columnTypes.reserve(columns.size() + 1);
    columnTypes.push_back(LogicalType::INTERNAL_ID());
    for (auto& column : columns) {
        columnTypes.push_back(column->getDataType().copy());
    }
    auto nodeGroup = std::make_unique<NodeGroup>(
        columnTypes, enableCompression, StorageConstants::NODE_GROUP_SIZE);
    nodeGroup->setAllNull();
    nodeGroup->setNumValues(0);
    for (auto nodeGroupIdx = currentNumNodeGroups; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        nodeGroup->finalize(nodeGroupIdx);
        append(nodeGroup.get());
    }
}

void RelTableData::prepareLocalTableToCommit(
    Transaction* transaction, LocalTableData* localTableData) {
    auto localRelTableData = ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(localTableData);
    for (auto& [nodeGroupIdx, nodeGroup] : localRelTableData->nodeGroups) {
        auto relNG = ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(nodeGroup.get());
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(relNG->getRelNGInfo());
        adjColumn->prepareCommitForChunk(transaction, nodeGroupIdx, relNG->getAdjChunk(),
            relNodeGroupInfo->adjInsertInfo, {} /* updateInfo */, relNodeGroupInfo->deleteInfo);
        for (auto columnID = 0u; columnID < columns.size(); columnID++) {
            columns[columnID]->prepareCommitForChunk(transaction, nodeGroupIdx,
                relNG->getPropertyChunk(columnID), relNodeGroupInfo->insertInfoPerChunk[columnID],
                relNodeGroupInfo->updateInfoPerChunk[columnID], relNodeGroupInfo->deleteInfo);
        }
    }
}

void RelTableData::checkpointInMemory() {
    adjColumn->checkpointInMemory();
    TableData::checkpointInMemory();
}

void RelTableData::rollbackInMemory() {
    adjColumn->rollbackInMemory();
    TableData::rollbackInMemory();
}

} // namespace storage
} // namespace kuzu
