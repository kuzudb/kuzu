#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalNodeTable::LocalNodeTable(Table& table)
    : LocalTable{table}, nodeGroups{NodeTable::getNodeTableColumnTypes(table.cast<NodeTable>()),
                             false /*enableCompression*/, StorageConstants::MAX_NUM_ROWS_IN_TABLE} {
    initLocalHashIndex();
}

void LocalNodeTable::initLocalHashIndex() {
    auto& nodeTable = ku_dynamic_cast<const Table&, const NodeTable&>(table);
    DBFileIDAndName dbFileIDAndName{DBFileID{DBFileType::NODE_INDEX}, "in-mem-overflow"};
    overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    PageCursor dummyCursor{0, 0};
    overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, dummyCursor);
    hashIndex = std::make_unique<LocalHashIndex>(
        nodeTable.getColumn(nodeTable.getPKColumnID()).getDataType().getPhysicalType(),
        overflowFileHandle.get());
}

bool LocalNodeTable::insert(Transaction*, TableInsertState& insertState) {
    auto& nodeInsertState = insertState.constCast<NodeTableInsertState>();
    const auto numRowsInLocalTable = nodeGroups.getNumRows();
    const auto nodeOffset = StorageConstants::MAX_NUM_ROWS_IN_TABLE + numRowsInLocalTable;
    KU_ASSERT(nodeInsertState.pkVector.state->getSelVector().getSelSize() == 1);
    if (!hashIndex->insert(nodeInsertState.pkVector, nodeOffset)) {
        throw RuntimeException(
            stringFormat("Found duplicate primary key in local table {}", table.getTableID()));
    }
    const auto nodeIDPos =
        nodeInsertState.nodeIDVector.state->getSelVector().getSelectedPositions()[0];
    nodeInsertState.nodeIDVector.setValue(nodeIDPos, internalID_t{nodeOffset, table.getTableID()});
    nodeGroups.append(&DUMMY_WRITE_TRANSACTION, insertState.propertyVectors);
    return true;
}

bool LocalNodeTable::update(TableUpdateState& updateState) {
    const auto& nodeUpdateState = updateState.cast<NodeTableUpdateState>();
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    const auto offset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (nodeUpdateState.columnID == table.cast<NodeTable>().getPKColumnID()) {
        KU_ASSERT(nodeUpdateState.pkVector);
        hashIndex->delete_(*nodeUpdateState.pkVector);
        hashIndex->insert(*nodeUpdateState.pkVector, offset);
    }
    const auto [nodeGroupIdx, rowIdxInGroup] = StorageUtils::getQuotientRemainder(
        offset - StorageConstants::MAX_NUM_ROWS_IN_TABLE, StorageConstants::NODE_GROUP_SIZE);
    const auto nodeGroup = nodeGroups.getNodeGroup(nodeGroupIdx);
    nodeGroup->update(&DUMMY_WRITE_TRANSACTION, rowIdxInGroup, nodeUpdateState.columnID,
        nodeUpdateState.propertyVector);
    return true;
}

bool LocalNodeTable::delete_(Transaction*, TableDeleteState& deleteState) {
    const auto& nodeDeleteState = deleteState.cast<NodeTableDeleteState>();
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    const auto offset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    hashIndex->delete_(nodeDeleteState.pkVector);
    const auto [nodeGroupIdx, rowIdxInGroup] = StorageUtils::getQuotientRemainder(
        offset - StorageConstants::MAX_NUM_ROWS_IN_TABLE, StorageConstants::NODE_GROUP_SIZE);
    const auto nodeGroup = nodeGroups.getNodeGroup(nodeGroupIdx);
    return nodeGroup->delete_(&DUMMY_WRITE_TRANSACTION, rowIdxInGroup);
}

} // namespace storage
} // namespace kuzu
