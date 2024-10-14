#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "common/exception/message.h"
#include "common/types/types.h"
#include "storage/index/hash_index.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalNodeTable::LocalNodeTable(Table& table)
    : LocalTable{table}, nodeGroups{this->table.getMemoryManager(),
                             NodeTable::getNodeTableColumnTypes(table.cast<NodeTable>()),
                             false /*enableCompression*/} {
    initLocalHashIndex();
}

void LocalNodeTable::initLocalHashIndex() {
    auto& nodeTable = ku_dynamic_cast<const NodeTable&>(table);
    DBFileIDAndName dbFileIDAndName{DBFileID{}, "in-mem-overflow"};
    overflowFile = std::make_unique<InMemOverflowFile>(dbFileIDAndName);
    overflowFileHandle = std::make_unique<OverflowFileHandle>(*overflowFile, overflowCursor);
    hashIndex = std::make_unique<LocalHashIndex>(
        nodeTable.getColumn(nodeTable.getPKColumnID()).getDataType().getPhysicalType(),
        overflowFileHandle.get());
}

bool LocalNodeTable::isVisible(const Transaction* transaction, offset_t offset) {
    auto [nodeGroupIdx, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(offset);
    auto* nodeGroup = nodeGroups.getNodeGroup(nodeGroupIdx);
    if (nodeGroup->isDeleted(transaction, offsetInGroup)) {
        return false;
    }
    return nodeGroup->isInserted(transaction, offsetInGroup);
}

offset_t LocalNodeTable::validateUniquenessConstraint(const Transaction* transaction,
    const ValueVector& pkVector) {
    KU_ASSERT(pkVector.state->getSelVector().getSelSize() == 1);
    return hashIndex->lookup(pkVector,
        [&](offset_t offset_) { return isVisible(transaction, offset_); });
}

bool LocalNodeTable::insert(Transaction* transaction, TableInsertState& insertState) {
    auto& nodeInsertState = insertState.constCast<NodeTableInsertState>();
    const auto numRowsInLocalTable = nodeGroups.getNumTotalRows();
    const auto nodeOffset = StorageConstants::MAX_NUM_ROWS_IN_TABLE + numRowsInLocalTable;
    KU_ASSERT(nodeInsertState.pkVector.state->getSelVector().getSelSize() == 1);
    if (!hashIndex->insert(nodeInsertState.pkVector, nodeOffset,
            [&](offset_t offset) { return isVisible(transaction, offset); })) {
        const auto val =
            nodeInsertState.pkVector.getAsValue(nodeInsertState.pkVector.state->getSelVector()[0]);
        throw RuntimeException(ExceptionMessage::duplicatePKException(val->toString()));
    }
    const auto nodeIDPos =
        nodeInsertState.nodeIDVector.state->getSelVector().getSelectedPositions()[0];
    nodeInsertState.nodeIDVector.setValue(nodeIDPos, internalID_t{nodeOffset, table.getTableID()});
    nodeGroups.append(transaction, insertState.propertyVectors);
    return true;
}

bool LocalNodeTable::update(Transaction* transaction, TableUpdateState& updateState) {
    KU_ASSERT(transaction->isDummy());
    const auto& nodeUpdateState = updateState.cast<NodeTableUpdateState>();
    KU_ASSERT(nodeUpdateState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeUpdateState.nodeIDVector.state->getSelVector()[0];
    const auto offset = nodeUpdateState.nodeIDVector.readNodeOffset(pos);
    if (nodeUpdateState.columnID == table.cast<NodeTable>().getPKColumnID()) {
        KU_ASSERT(nodeUpdateState.pkVector);
        hashIndex->delete_(*nodeUpdateState.pkVector);
        hashIndex->insert(*nodeUpdateState.pkVector, offset,
            [&](offset_t offset_) { return isVisible(transaction, offset_); });
    }
    const auto [nodeGroupIdx, rowIdxInGroup] = StorageUtils::getQuotientRemainder(
        offset - StorageConstants::MAX_NUM_ROWS_IN_TABLE, StorageConstants::NODE_GROUP_SIZE);
    const auto nodeGroup = nodeGroups.getNodeGroup(nodeGroupIdx);
    nodeGroup->update(transaction, rowIdxInGroup, nodeUpdateState.columnID,
        nodeUpdateState.propertyVector);
    return true;
}

bool LocalNodeTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    KU_ASSERT(transaction->isDummy());
    const auto& nodeDeleteState = deleteState.cast<NodeTableDeleteState>();
    KU_ASSERT(nodeDeleteState.nodeIDVector.state->getSelVector().getSelSize() == 1);
    const auto pos = nodeDeleteState.nodeIDVector.state->getSelVector()[0];
    const auto offset = nodeDeleteState.nodeIDVector.readNodeOffset(pos);
    hashIndex->delete_(nodeDeleteState.pkVector);
    const auto [nodeGroupIdx, rowIdxInGroup] = StorageUtils::getQuotientRemainder(
        offset - StorageConstants::MAX_NUM_ROWS_IN_TABLE, StorageConstants::NODE_GROUP_SIZE);
    const auto nodeGroup = nodeGroups.getNodeGroup(nodeGroupIdx);
    return nodeGroup->delete_(transaction, rowIdxInGroup);
}

bool LocalNodeTable::addColumn(Transaction* transaction, TableAddColumnState& addColumnState) {
    nodeGroups.addColumn(transaction, addColumnState);
    return true;
}

uint64_t LocalNodeTable::getEstimatedMemUsage() {
    return nodeGroups.getEstimatedMemoryUsage() + hashIndex->getEstimatedMemUsage();
}

void LocalNodeTable::clear() {
    auto& nodeTable = ku_dynamic_cast<const NodeTable&>(table);
    hashIndex = std::make_unique<LocalHashIndex>(
        nodeTable.getColumn(nodeTable.getPKColumnID()).getDataType().getPhysicalType(),
        overflowFileHandle.get());
    nodeGroups.clear();
}

bool LocalNodeTable::lookupPK(const Transaction* transaction, const ValueVector* keyVector,
    offset_t& result) {
    result = hashIndex->lookup(*keyVector,
        [&](offset_t offset) { return isVisible(transaction, offset); });
    return result != INVALID_OFFSET;
}

} // namespace storage
} // namespace kuzu
