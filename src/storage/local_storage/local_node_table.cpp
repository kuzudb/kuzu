#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalNodeTable::LocalNodeTable(Table& table)
    : LocalTable{table}, chunkedGroups{NodeTable::getTableColumnTypes(
                             ku_dynamic_cast<const Table&, const NodeTable&>(table))} {
    auto& nodeTable = ku_dynamic_cast<const Table&, const NodeTable&>(table);
    DBFileIDAndName dbFileIDAndName{DBFileID{DBFileType::NODE_INDEX}, "in-mem-overflow"};
    overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    PageCursor cursor{0, 0};
    overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, cursor);
    hashIndex = std::make_unique<LocalHashIndex>(
        nodeTable.getColumn(nodeTable.getPKColumnID())->getDataType().getPhysicalType(),
        overflowFileHandle.get());
}

bool LocalNodeTable::insert(TableInsertState& insertState) {
    auto& nodeInsertState = insertState.constCast<NodeTableInsertState>();
    const auto numRowsInLocalTable = chunkedGroups.getNumRows();
    const auto nodeOffset = StorageConstants::MAX_NUM_NODES_IN_TABLE + numRowsInLocalTable;
    KU_ASSERT(nodeInsertState.pkVector.state->getSelVector().getSelSize() == 1);
    if (!hashIndex->insert(nodeInsertState.pkVector, nodeOffset)) {
        throw RuntimeException(
            stringFormat("Found duplicate primary key in local table {}", table.getTableID()));
    }
    const auto nodeIDPos =
        nodeInsertState.nodeIDVector.state->getSelVector().getSelectedPositions()[0];
    nodeInsertState.nodeIDVector.setValue(nodeIDPos, internalID_t{nodeOffset, table.getTableID()});
    // TODO(Guodong): Assume all property vectors have the same selVector here. Should be changed.
    chunkedGroups.append(insertState.propertyVectors,
        insertState.propertyVectors[0]->state->getSelVector());
    return true;
}

bool LocalNodeTable::update(TableUpdateState& state) {
    const auto& updateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(state);
}

bool LocalNodeTable::delete_(TableDeleteState& deleteState) {
    const auto& deleteState_ =
        ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
}

void LocalNodeTable::initializeScanState(TableScanState& scanState) const {
    auto& nodeScanState = scanState.cast<NodeTableScanState>();
    nodeScanState.vectorIdx = INVALID_VECTOR_IDX;
    nodeScanState.numTotalRows = getNumRows();
}

void LocalNodeTable::scan(TableScanState& scanState) const {
    KU_ASSERT(scanState.source == TableScanSource::UNCOMMITTED);
    auto& nodeScanState = scanState.constCast<NodeTableScanState>();
    const auto startNodeOffset = StorageConstants::MAX_NUM_NODES_IN_TABLE +
                                 nodeScanState.vectorIdx * DEFAULT_VECTOR_CAPACITY;
    for (auto i = 0u; i < nodeScanState.numRowsToScan; i++) {
        scanState.nodeIDVector->setValue(i, nodeID_t{startNodeOffset + i, table.getTableID()});
    }
    auto& chunkedGroup = chunkedGroups.getChunkedGroup(nodeScanState.vectorIdx);
    chunkedGroup.scan(scanState.columnIDs, scanState.outputVectors, 0, nodeScanState.numRowsToScan);
    scanState.nodeIDVector->state->getSelVectorUnsafe().setSelSize(nodeScanState.numRowsToScan);
}

} // namespace storage
} // namespace kuzu
