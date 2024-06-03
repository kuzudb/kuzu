#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalNodeTable::LocalNodeTable(Table& table)
    : LocalTable{table}, nodeGroups{NodeTable::getTableColumnTypes(
                                        ku_dynamic_cast<const Table&, const NodeTable&>(table)),
                             StorageConstants::MAX_NUM_NODES_IN_TABLE} {
    auto& nodeTable = ku_dynamic_cast<const Table&, const NodeTable&>(table);
    DBFileIDAndName dbFileIDAndName{DBFileID{DBFileType::NODE_INDEX}, "in-mem-overflow"};
    overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    PageCursor cursor{0, 0};
    overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, cursor);
    hashIndex = std::make_unique<LocalHashIndex>(
        nodeTable.getColumn(nodeTable.getPKColumnID())->getDataType().getPhysicalType(),
        overflowFileHandle.get());
}

bool LocalNodeTable::insert(Transaction* transaction, TableInsertState& insertState) {
    auto& nodeInsertState = insertState.constCast<NodeTableInsertState>();
    const auto numRowsInLocalTable = nodeGroups.getNumRows();
    const auto nodeOffset = StorageConstants::MAX_NUM_NODES_IN_TABLE + numRowsInLocalTable;
    KU_ASSERT(nodeInsertState.pkVector.state->getSelVector().getSelSize() == 1);
    if (!hashIndex->insert(nodeInsertState.pkVector, nodeOffset)) {
        throw RuntimeException(
            stringFormat("Found duplicate primary key in local table {}", table.getTableID()));
    }
    const auto nodeIDPos =
        nodeInsertState.nodeIDVector.state->getSelVector().getSelectedPositions()[0];
    nodeInsertState.nodeIDVector.setValue(nodeIDPos, internalID_t{nodeOffset, table.getTableID()});
    nodeGroups.append(transaction, insertState.propertyVectors);
    return true;
}

bool LocalNodeTable::update(TableUpdateState& state) {
    const auto& updateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(state);
}

bool LocalNodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    const auto& nodeDeleteState =
        ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    const auto offset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    auto& nodeGroup = nodeGroups.findNodeGroupFromOffset(offset);
    nodeGroup.delete_(transaction, offset);
    return true;
}

} // namespace storage
} // namespace kuzu
