#include "storage/store/rel_table_data.h"

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/rel_direction.h"
#include "common/exception/message.h"
#include "common/exception/runtime.h"
#include "common/types/internal_id_t.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/node_group.h"
#include "storage/store/rel_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelTableData::RelTableData(BMFileHandle* dataFH, MemoryManager* mm, ShadowFile* shadowFile,
    const TableCatalogEntry* tableEntry, RelDataDirection direction, bool enableCompression,
    Deserializer* deSer)
    : dataFH{dataFH}, tableID{tableEntry->getTableID()}, tableName{tableEntry->getName()}, mm{mm},
      bufferManager{mm->getBufferManager()}, shadowFile{shadowFile},
      enableCompression{enableCompression}, direction{direction} {
    multiplicity = tableEntry->constCast<RelTableCatalogEntry>().getMultiplicity(direction);
    initCSRHeaderColumns();
    initPropertyColumns(tableEntry);
    nodeGroups =
        std::make_unique<NodeGroupCollection>(getColumnTypes(), enableCompression, dataFH, deSer);
}

void RelTableData::initCSRHeaderColumns() {
    // No NULL values is allowed for the csr length and offset column.
    auto csrOffsetColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_OFFSET,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.offset = std::make_unique<Column>(csrOffsetColumnName, LogicalType::UINT64(),
        dataFH, bufferManager, shadowFile, enableCompression, false /* requireNUllColumn */);
    auto csrLengthColumnName = StorageUtils::getColumnName("", StorageUtils::ColumnType::CSR_LENGTH,
        RelDataDirectionUtils::relDirectionToString(direction));
    csrHeaderColumns.length = std::make_unique<Column>(csrLengthColumnName, LogicalType::UINT64(),
        dataFH, bufferManager, shadowFile, enableCompression, false /* requireNUllColumn */);
}

void RelTableData::initPropertyColumns(const TableCatalogEntry* tableEntry) {
    // Columns (nbrID + properties).
    auto& properties = tableEntry->getPropertiesRef();
    const auto maxColumnID =
        std::max_element(properties.begin(), properties.end(), [](auto& a, auto& b) {
            return a.getColumnID() < b.getColumnID();
        })->getColumnID();
    // The first column is reserved for NBR_ID, which is not a property.
    columns.resize(maxColumnID + 2);
    auto nbrIDColName = StorageUtils::getColumnName("NBR_ID", StorageUtils::ColumnType::DEFAULT,
        RelDataDirectionUtils::relDirectionToString(direction));
    auto nbrIDColumn = std::make_unique<InternalIDColumn>(nbrIDColName, dataFH, bufferManager,
        shadowFile, enableCompression);
    columns[NBR_ID_COLUMN_ID] = std::move(nbrIDColumn);
    // Property columns.
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        const auto columnID = property.getColumnID() + 1; // Skip NBR_ID column.
        const auto colName =
            StorageUtils::getColumnName(property.getName(), StorageUtils::ColumnType::DEFAULT,
                RelDataDirectionUtils::relDirectionToString(direction));
        columns[columnID] = ColumnFactory::createColumn(colName, property.getDataType().copy(),
            dataFH, bufferManager, shadowFile, enableCompression);
    }
    // Set common tableID for nbrIDColumn and relIDColumn.
    const auto nbrTableID = tableEntry->constCast<RelTableCatalogEntry>().getNbrTableID(direction);
    columns[NBR_ID_COLUMN_ID]->cast<InternalIDColumn>().setCommonTableID(nbrTableID);
    columns[REL_ID_COLUMN_ID]->cast<InternalIDColumn>().setCommonTableID(tableID);
}

bool RelTableData::update(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector, column_id_t columnID, const ValueVector& dataVector) const {
    KU_ASSERT(boundNodeIDVector.state->getSelVector().getSelSize() == 1);
    KU_ASSERT(relIDVector.state->getSelVector().getSelSize() == 1);
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (boundNodeIDVector.isNull(boundNodePos) || relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto [source, rowIdx] = findMatchingRow(transaction, boundNodeIDVector, relIDVector);
    KU_ASSERT(rowIdx != INVALID_ROW_IDX);
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);
    auto& csrNodeGroup = getNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
    csrNodeGroup.update(transaction, source, rowIdx, columnID, dataVector);
    return true;
}

bool RelTableData::delete_(Transaction* transaction, ValueVector& boundNodeIDVector,
    const ValueVector& relIDVector) const {
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    if (boundNodeIDVector.isNull(boundNodePos) || relIDVector.isNull(relIDPos)) {
        return false;
    }
    const auto [source, rowIdx] = findMatchingRow(transaction, boundNodeIDVector, relIDVector);
    if (rowIdx == INVALID_ROW_IDX) {
        return false;
    }
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);
    auto& csrNodeGroup = getNodeGroup(nodeGroupIdx)->cast<CSRNodeGroup>();
    return csrNodeGroup.delete_(transaction, source, rowIdx);
}

void RelTableData::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    auto& property = addColumnState.property;
    columns.push_back(ColumnFactory::createColumn(property.getName(), property.getDataType().copy(),
        dataFH, bufferManager, shadowFile, enableCompression));
    nodeGroups->addColumn(transaction, addColumnState);
}

std::pair<CSRNodeGroupScanSource, row_idx_t> RelTableData::findMatchingRow(Transaction* transaction,
    ValueVector& boundNodeIDVector, const ValueVector& relIDVector) const {
    KU_ASSERT(boundNodeIDVector.state->getSelVector().getSelSize() == 1);
    KU_ASSERT(relIDVector.state->getSelVector().getSelSize() == 1);
    const auto boundNodePos = boundNodeIDVector.state->getSelVector()[0];
    const auto relIDPos = relIDVector.state->getSelVector()[0];
    const auto boundNodeOffset = boundNodeIDVector.getValue<nodeID_t>(boundNodePos).offset;
    const auto relOffset = relIDVector.getValue<nodeID_t>(relIDPos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(boundNodeOffset);

    DataChunk scanChunk(1);
    // RelID output vector.
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector<column_id_t> columnIDs = {REL_ID_COLUMN_ID, ROW_IDX_COLUMN_ID};
    std::vector<Column*> columns{getColumn(REL_ID_COLUMN_ID), nullptr};
    const auto scanState = std::make_unique<RelTableScanState>(columnIDs, columns,
        csrHeaderColumns.offset.get(), csrHeaderColumns.length.get(), direction);
    scanState->boundNodeIDVector = &boundNodeIDVector;
    scanState->outputVectors.push_back(scanChunk.getValueVector(0).get());
    scanState->IDVector = scanState->outputVectors[0];
    scanState->rowIdxVector->state = scanState->IDVector->state;
    scanState->source = TableScanSource::COMMITTED;
    scanState->boundNodeOffset = boundNodeOffset;
    scanState->nodeGroup = getNodeGroup(nodeGroupIdx);
    scanState->nodeGroup->initializeScanState(transaction, *scanState);
    row_idx_t matchingRowIdx = INVALID_ROW_IDX;
    CSRNodeGroupScanSource source;
    while (true) {
        const auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
        if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
            break;
        }
        for (auto i = 0u; i < scanState->IDVector->state->getSelVector().getSelSize(); i++) {
            const auto pos = scanState->IDVector->state->getSelVector()[i];
            if (scanState->IDVector->getValue<internalID_t>(pos).offset == relOffset) {
                const auto rowIdxPos = scanState->rowIdxVector->state->getSelVector()[i];
                matchingRowIdx = scanState->rowIdxVector->getValue<row_idx_t>(rowIdxPos);
                source = scanState->nodeGroupScanState->cast<CSRNodeGroupScanState>().source;
                break;
            }
        }
        if (matchingRowIdx != INVALID_ROW_IDX) {
            break;
        }
    }
    return {source, matchingRowIdx};
}

void RelTableData::checkIfNodeHasRels(Transaction* transaction,
    ValueVector* srcNodeIDVector) const {
    KU_ASSERT(srcNodeIDVector->state->isFlat());
    const auto nodeIDPos = srcNodeIDVector->state->getSelVector()[0];
    const auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (nodeGroupIdx >= getNumNodeGroups()) {
        return;
    }
    DataChunk scanChunk(1);
    // RelID output vector.
    scanChunk.insert(0, std::make_shared<ValueVector>(LogicalType::INTERNAL_ID()));
    std::vector<column_id_t> columnIDs = {REL_ID_COLUMN_ID};
    std::vector<Column*> columns{getColumn(REL_ID_COLUMN_ID)};
    const auto scanState = std::make_unique<RelTableScanState>(columnIDs, columns,
        csrHeaderColumns.offset.get(), csrHeaderColumns.length.get(), direction);
    scanState->boundNodeIDVector = srcNodeIDVector;
    scanState->outputVectors.push_back(scanChunk.getValueVector(0).get());
    scanState->IDVector = scanState->outputVectors[0];
    scanState->source = TableScanSource::COMMITTED;
    scanState->boundNodeOffset = nodeOffset;
    scanState->nodeGroup = getNodeGroup(nodeGroupIdx);
    scanState->nodeGroup->initializeScanState(transaction, *scanState);
    while (true) {
        const auto scanResult = scanState->nodeGroup->scan(transaction, *scanState);
        if (scanResult == NODE_GROUP_SCAN_EMMPTY_RESULT) {
            break;
        }
        if (scanState->outputVectors[0]->state->getSelVector().getSelSize() > 0) {
            throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
                tableName, std::to_string(nodeOffset),
                RelDataDirectionUtils::relDirectionToString(direction)));
        }
    }
}

void RelTableData::checkpoint() const {
    std::vector<Column*> checkpointColumns;
    std::vector<column_id_t> columnIDs;
    for (auto i = 0u; i < columns.size(); i++) {
        columnIDs.push_back(i);
        checkpointColumns.push_back(columns[i].get());
    }
    CSRNodeGroupCheckpointState state{columnIDs, checkpointColumns, *dataFH, mm,
        csrHeaderColumns.offset.get(), csrHeaderColumns.length.get()};
    nodeGroups->checkpoint(state);
}

void RelTableData::serialize(Serializer& serializer) const {
    nodeGroups->serialize(serializer);
}

} // namespace storage
} // namespace kuzu
