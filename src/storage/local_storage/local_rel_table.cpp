#include "storage/local_storage/local_rel_table.h"

#include <unistd.h>

#include "storage/storage_utils.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;

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
}

bool LocalRelTable::insert(transaction::Transaction*, TableInsertState& state) {
    const auto& insertState = state.cast<RelTableInsertState>();
    const auto srcNodePos = insertState.srcNodeIDVector.state->getSelVector()[0];
    const auto dstNodePos = insertState.dstNodeIDVector.state->getSelVector()[0];
    if (insertState.srcNodeIDVector.isNull(srcNodePos) ||
        insertState.dstNodeIDVector.isNull(dstNodePos)) {
        return false;
    }
    const auto numRowsInLocalTable = localNodeGroup->getNumRows();
    const auto relOffset = StorageConstants::MAX_NUM_ROWS_IN_TABLE + numRowsInLocalTable;
    KU_ASSERT(insertState.srcNodeIDVector.state->getSelVector().getSelSize() == 1 &&
              insertState.dstNodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto relIDVector = insertState.propertyVectors[0];
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
    localNodeGroup->append(&transaction::DUMMY_WRITE_TRANSACTION, insertVectors, 0,
        numRowsToAppend);
    const auto srcNodeOffset = insertState.srcNodeIDVector.readNodeOffset(srcNodePos);
    const auto dstNodeOffset = insertState.dstNodeIDVector.readNodeOffset(dstNodePos);
    fwdIndex[srcNodeOffset].push_back(relOffset);
    bwdIndex[dstNodeOffset].push_back(relOffset);
    return true;
}

bool LocalRelTable::update(TableUpdateState& state) {
    const auto& updateState = state.cast<RelTableUpdateState>();
    const auto matchedRow = findMatchingRow(updateState.srcNodeIDVector,
        updateState.dstNodeIDVector, updateState.relIDVector);
    if (matchedRow == INVALID_ROW_IDX) {
        return false;
    }
    localNodeGroup->update(&transaction::DUMMY_WRITE_TRANSACTION, matchedRow, updateState.columnID,
        updateState.propertyVector);
    return true;
}

bool LocalRelTable::delete_(transaction::Transaction*, TableDeleteState& state) {
    const auto& deleteState = state.cast<RelTableDeleteState>();
    const auto matchedRow = findMatchingRow(deleteState.srcNodeIDVector,
        deleteState.dstNodeIDVector, deleteState.relIDVector);
    if (matchedRow == INVALID_ROW_IDX) {
        return false;
    }
    return localNodeGroup->delete_(&transaction::DUMMY_WRITE_TRANSACTION, matchedRow);
}

void LocalRelTable::initializeScan(TableScanState& state) {
    auto& relScanState = state.cast<RelTableScanState>();
    state.source = TableScanSource::UNCOMMITTED;
    state.nodeGroup = localNodeGroup.get();
    auto& nodeGroupScanState = state.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    nodeGroupScanState.nextRowToScan = 0;
    nodeGroupScanState.inMemCSRList.isSequential = false;
    nodeGroupScanState.inMemCSRList.rowIndices = relScanState.direction == RelDataDirection::FWD ?
                                                     fwdIndex[relScanState.boundNodeOffset] :
                                                     bwdIndex[relScanState.boundNodeOffset];
    std::sort(nodeGroupScanState.inMemCSRList.rowIndices.begin(),
        nodeGroupScanState.inMemCSRList.rowIndices.end());
}

bool LocalRelTable::scan(transaction::Transaction* transaction, TableScanState& state) const {
    auto& nodeGroupScanState = state.nodeGroupScanState->cast<CSRNodeGroupScanState>();
    const auto numToScan = std::min(nodeGroupScanState.inMemCSRList.rowIndices.size() -
                                        nodeGroupScanState.nextRowToScan,
        DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numToScan; i++) {
        state.IDVector->setValue<internalID_t>(i,
            internalID_t{
                nodeGroupScanState.inMemCSRList.rowIndices[nodeGroupScanState.nextRowToScan + i],
                INVALID_TABLE_ID});
    }
    localNodeGroup->lookup(transaction, state);
    nodeGroupScanState.nextRowToScan += numToScan;
    return nodeGroupScanState.nextRowToScan < nodeGroupScanState.inMemCSRList.rowIndices.size();
}

row_idx_t LocalRelTable::findMatchingRow(const ValueVector& srcNodeIDVector,
    const ValueVector& dstNodeIDVector, const ValueVector& relIDVector) {
    const auto srcNodePos = srcNodeIDVector.state->getSelVector()[0];
    const auto dstNodePos = dstNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (srcNodeIDVector.isNull(srcNodePos) || relIDVector.isNull(relIDPos) ||
        relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto srcNodeOffset = srcNodeIDVector.readNodeOffset(srcNodePos);
    const auto relOffset = relIDVector.readNodeOffset(relIDPos);
    const auto dstNodeOffset = dstNodeIDVector.readNodeOffset(dstNodePos);
    auto& fwdRows = fwdIndex[srcNodeOffset];
    std::sort(fwdRows.begin(), fwdRows.end());
    auto& bwdRows = bwdIndex[dstNodeOffset];
    std::sort(bwdRows.begin(), bwdRows.end());
    std::vector<row_idx_t> intersectRows;
    std::set_intersection(fwdRows.begin(), fwdRows.end(), bwdRows.begin(), bwdRows.end(),
        std::back_inserter(intersectRows));
    // Loop over relID column chunks to find the relID.
    DataChunk scanChunk(2);
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    scanChunk.insert(1, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector<column_id_t> columnIDs;
    columnIDs.push_back(LOCAL_REL_ID_COLUMN_ID);
    const auto scanState = std::make_unique<RelTableScanState>(table.getTableID(), columnIDs);
    scanState->IDVector = scanChunk.getValueVector(0).get();
    scanState->outputVectors.push_back(scanChunk.getValueVector(1).get());
    scanChunk.state->getSelVectorUnsafe().setSelSize(intersectRows.size());
    // TODO(Guodong): We assume intersectRows is smaller than 2048 here. Should handle edge case.
    for (auto i = 0u; i < intersectRows.size(); i++) {
        scanState->IDVector->setValue<internalID_t>(i,
            internalID_t{intersectRows[i], INVALID_TABLE_ID});
    }
    localNodeGroup->lookup(&transaction::DUMMY_WRITE_TRANSACTION, *scanState);
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
