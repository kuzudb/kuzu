#include "storage/local_storage/local_rel_table.h"

#include <algorithm>

#include "common/enums/rel_direction.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/rel_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

std::vector<LogicalType> LocalRelTable::getTypesForLocalRelTable(const RelTable& table) {
    std::vector<LogicalType> types;
    types.reserve(table.getNumColumns() + 1);
    // Src node ID.
    types.push_back(LogicalType::INTERNAL_ID());
    for (auto i = 0u; i < table.getNumColumns(); i++) {
        types.push_back(table.getColumn(i, RelDataDirection::FWD)->getDataType().copy());
    }
    return types;
}

LocalRelTable::LocalRelTable(Table& table) : LocalTable{table} {
    localNodeGroup = std::make_unique<NodeGroup>(0, false,
        getTypesForLocalRelTable(table.cast<RelTable>()), INVALID_ROW_IDX);
    auto& relTable = table.cast<RelTable>();
    nodeOffsetColumns[LOCAL_BOUND_NODE_ID_COLUMN_ID] = relTable.getFromNodeTableID();
    nodeOffsetColumns[LOCAL_NBR_NODE_ID_COLUMN_ID] = relTable.getToNodeTableID();
    auto numColumns = relTable.getNumColumns();
    // skip NBR_ID and REL_ID, this assumes all INTERNAL_ID are node references
    for (auto i = 2u; i < numColumns; i++) {
        auto column = relTable.getColumn(i, RelDataDirection::FWD);
        if (column->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID) {
            nodeOffsetColumns[i + 1] = column->cast<InternalIDColumn>().getCommonTableID();
        }
    }
}

bool LocalRelTable::insert(Transaction* transaction, TableInsertState& state) {
    const auto& insertState = state.cast<RelTableInsertState>();
    KU_ASSERT(insertState.srcNodeIDVector.state->getSelVector().getSelSize() == 1 &&
              insertState.dstNodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto srcNodePos = insertState.srcNodeIDVector.state->getSelVector()[0];
    const auto dstNodePos = insertState.dstNodeIDVector.state->getSelVector()[0];
    if (insertState.srcNodeIDVector.isNull(srcNodePos) ||
        insertState.dstNodeIDVector.isNull(dstNodePos)) {
        return false;
    }
    const auto numRowsInLocalTable = localNodeGroup->getNumRows();
    const auto relOffset = StorageConstants::MAX_NUM_ROWS_IN_TABLE + numRowsInLocalTable;
    const auto relIDVector = insertState.propertyVectors[0];
    KU_ASSERT(relIDVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    const auto relIDPos = relIDVector->state->getSelVector()[0];
    relIDVector->setValue<internalID_t>(relIDPos, internalID_t{relOffset, table.getTableID()});
    relIDVector->setNull(relIDPos, false);
    std::vector<ValueVector*> insertVectors;
    insertVectors.push_back(&insertState.srcNodeIDVector);
    insertVectors.push_back(&insertState.dstNodeIDVector);
    for (auto i = 0u; i < insertState.propertyVectors.size(); i++) {
        insertVectors.push_back(insertState.propertyVectors[i]);
    }
    const auto numRowsToAppend = insertState.srcNodeIDVector.state->getSelVector().getSelSize();
    localNodeGroup->append(transaction, insertVectors, 0, numRowsToAppend);
    const auto srcNodeOffset = insertState.srcNodeIDVector.readNodeOffset(srcNodePos);
    const auto dstNodeOffset = insertState.dstNodeIDVector.readNodeOffset(dstNodePos);
    fwdIndex[srcNodeOffset].push_back(numRowsInLocalTable);
    bwdIndex[dstNodeOffset].push_back(numRowsInLocalTable);
    return true;
}

bool LocalRelTable::update(Transaction* transaction, TableUpdateState& state) {
    KU_ASSERT(transaction->isDummy());
    const auto& updateState = state.cast<RelTableUpdateState>();
    const auto srcNodePos = updateState.srcNodeIDVector.state->getSelVector()[0];
    const auto dstNodePos = updateState.dstNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = updateState.relIDVector.state->getSelVector()[0];
    if (updateState.srcNodeIDVector.isNull(srcNodePos) ||
        updateState.relIDVector.isNull(relIDPos) || updateState.relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto srcNodeOffset = updateState.srcNodeIDVector.readNodeOffset(srcNodePos);
    const auto dstNodeOffset = updateState.dstNodeIDVector.readNodeOffset(dstNodePos);
    const auto relOffset = updateState.relIDVector.readNodeOffset(relIDPos);
    const auto matchedRow =
        findMatchingRow(table.getMemoryManager(), srcNodeOffset, dstNodeOffset, relOffset);
    if (matchedRow == INVALID_ROW_IDX) {
        return false;
    }
    KU_ASSERT(updateState.columnID != NBR_ID_COLUMN_ID);
    localNodeGroup->update(transaction, matchedRow,
        rewriteLocalColumnID(RelDataDirection::FWD /* This is a dummy direction */,
            updateState.columnID),
        updateState.propertyVector);
    return true;
}

bool LocalRelTable::delete_(Transaction*, TableDeleteState& state) {
    const auto& deleteState = state.cast<RelTableDeleteState>();
    const auto srcNodePos = deleteState.srcNodeIDVector.state->getSelVector()[0];
    const auto dstNodePos = deleteState.dstNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = deleteState.relIDVector.state->getSelVector()[0];
    if (deleteState.srcNodeIDVector.isNull(srcNodePos) ||
        deleteState.relIDVector.isNull(relIDPos) || deleteState.relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto srcNodeOffset = deleteState.srcNodeIDVector.readNodeOffset(srcNodePos);
    const auto dstNodeOffset = deleteState.dstNodeIDVector.readNodeOffset(dstNodePos);
    const auto relOffset = deleteState.relIDVector.readNodeOffset(relIDPos);
    const auto matchedRow =
        findMatchingRow(table.getMemoryManager(), srcNodeOffset, dstNodeOffset, relOffset);
    if (matchedRow == INVALID_ROW_IDX) {
        return false;
    }
    std::erase(fwdIndex[srcNodeOffset], matchedRow);
    std::erase(bwdIndex[dstNodeOffset], matchedRow);
    return true;
}

bool LocalRelTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    localNodeGroup->addColumn(transaction, addColumnState, nullptr /* FileHandle */);
    return true;
}

uint64_t LocalRelTable::getEstimatedMemUsage() {
    // Esimation of fwdIndex and bwdIndex is rough.
    if (!localNodeGroup) {
        return 0;
    }
    return localNodeGroup->getEstimatedMemoryUsage() + fwdIndex.size() * sizeof(offset_t) +
           bwdIndex.size() * sizeof(offset_t);
}

void LocalRelTable::checkIfNodeHasRels(ValueVector* srcNodeIDVector) const {
    KU_ASSERT(srcNodeIDVector->state->isFlat());
    const auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    const auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    if (fwdIndex.contains(nodeOffset) && !fwdIndex.at(nodeOffset).empty()) {
        throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
            table.getTableName(), std::to_string(nodeOffset),
            RelDataDirectionUtils::relDirectionToString(RelDataDirection::FWD)));
    }
    if (bwdIndex.contains(nodeOffset) && !bwdIndex.at(nodeOffset).empty()) {
        throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
            table.getTableName(), std::to_string(nodeOffset),
            RelDataDirectionUtils::relDirectionToString(RelDataDirection::BWD)));
    }
}

void LocalRelTable::initializeScan(TableScanState& state) {
    auto& relScanState = state.cast<LocalRelTableScanState>();
    KU_ASSERT(relScanState.source == TableScanSource::UNCOMMITTED);
    relScanState.nodeGroup = localNodeGroup.get();
    auto& index = relScanState.direction == RelDataDirection::FWD ? fwdIndex : bwdIndex;
    if (index.contains(relScanState.boundNodeOffset)) {
        relScanState.rowIndices = index[relScanState.boundNodeOffset];
        KU_ASSERT(std::is_sorted(relScanState.rowIndices.begin(), relScanState.rowIndices.end()));
    } else {
        relScanState.rowIndices.clear();
    }
}

std::vector<column_id_t> LocalRelTable::rewriteLocalColumnIDs(RelDataDirection direction,
    const std::vector<column_id_t>& columnIDs) {
    std::vector<column_id_t> localColumnIDs;
    localColumnIDs.reserve(columnIDs.size());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        const auto columnID = columnIDs[i];
        localColumnIDs.push_back(rewriteLocalColumnID(direction, columnID));
    }
    return localColumnIDs;
}

column_id_t LocalRelTable::rewriteLocalColumnID(RelDataDirection direction, column_id_t columnID) {
    return columnID == NBR_ID_COLUMN_ID ? direction == RelDataDirection::FWD ?
                                          LOCAL_NBR_NODE_ID_COLUMN_ID :
                                          LOCAL_BOUND_NODE_ID_COLUMN_ID :
                                          columnID + 1;
}

bool LocalRelTable::scan(Transaction* transaction, TableScanState& state) const {
    const auto& relScanState = state.cast<RelTableScanState>();
    KU_ASSERT(relScanState.localTableScanState);
    auto& localScanState = *relScanState.localTableScanState;
    KU_ASSERT(localScanState.rowIndices.size() >= localScanState.nextRowToScan);
    const auto numToScan = std::min(localScanState.rowIndices.size() - localScanState.nextRowToScan,
        DEFAULT_VECTOR_CAPACITY);
    if (numToScan == 0) {
        return false;
    }
    for (auto i = 0u; i < numToScan; i++) {
        localScanState.rowIdxVector->setValue<row_idx_t>(i,
            localScanState.rowIndices[localScanState.nextRowToScan + i]);
    }
    localScanState.rowIdxVector->state->getSelVectorUnsafe().setSelSize(numToScan);
    localNodeGroup->lookup(transaction, localScanState);
    localScanState.nextRowToScan += numToScan;
    return true;
}

row_idx_t LocalRelTable::findMatchingRow(MemoryManager& memoryManager, offset_t srcNodeOffset,
    offset_t dstNodeOffset, offset_t relOffset) {
    auto& fwdRows = fwdIndex[srcNodeOffset];
    std::sort(fwdRows.begin(), fwdRows.end());
    auto& bwdRows = bwdIndex[dstNodeOffset];
    std::sort(bwdRows.begin(), bwdRows.end());
    std::vector<row_idx_t> intersectRows;
    std::set_intersection(fwdRows.begin(), fwdRows.end(), bwdRows.begin(), bwdRows.end(),
        std::back_inserter(intersectRows));
    // Loop over relID column chunks to find the relID.
    DataChunk scanChunk(1);
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector<column_id_t> columnIDs;
    columnIDs.push_back(LOCAL_REL_ID_COLUMN_ID);
    const auto scanState = std::make_unique<RelTableScanState>(memoryManager, columnIDs);
    scanState->IDVector = scanChunk.getValueVector(0).get();
    scanState->rowIdxVector->state = scanChunk.state;
    scanState->outputVectors.push_back(scanChunk.getValueVector(0).get());
    scanChunk.state->getSelVectorUnsafe().setSelSize(intersectRows.size());
    // TODO(Guodong): We assume intersectRows is smaller than 2048 here. Should handle edge case.
    for (auto i = 0u; i < intersectRows.size(); i++) {
        scanState->rowIdxVector->setValue<row_idx_t>(i, intersectRows[i]);
    }
    localNodeGroup->lookup(&DUMMY_TRANSACTION, *scanState);
    const auto scannedRelIDVector = scanState->outputVectors[0];
    KU_ASSERT(scannedRelIDVector->state->getSelVector().getSelSize() == intersectRows.size());
    row_idx_t matchedRow = INVALID_ROW_IDX;
    for (auto i = 0u; i < intersectRows.size(); i++) {
        if (scannedRelIDVector->getValue<internalID_t>(i).offset == relOffset) {
            matchedRow = intersectRows[i];
            break;
        }
    }
    return matchedRow;
}

} // namespace storage
} // namespace kuzu
