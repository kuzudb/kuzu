#include "src/storage/store/include/node_table.h"

namespace kuzu {
namespace storage {

NodeTable::NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
    BufferManager& bufferManager, bool isInMemory, WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, tableID{nodeTableSchema->tableID},
      isInMemory{isInMemory} {
    loadColumnsAndListsFromDisk(nodeTableSchema, bufferManager, wal);
}

void NodeTable::loadColumnsAndListsFromDisk(
    NodeTableSchema* nodeTableSchema, BufferManager& bufferManager, WAL* wal) {
    propertyColumns.resize(nodeTableSchema->getAllNodeProperties().size());
    for (auto i = 0u; i < nodeTableSchema->getAllNodeProperties().size(); i++) {
        auto property = nodeTableSchema->getAllNodeProperties()[i];
        if (property.dataType.typeID != UNSTRUCTURED) {
            propertyColumns[i] = ColumnFactory::getColumn(
                StorageUtils::getStructuredNodePropertyColumnStructureIDAndFName(
                    wal->getDirectory(), property),
                property.dataType, bufferManager, isInMemory, wal);
        }
    }
    unstrPropertyLists = make_unique<UnstructuredPropertyLists>(
        StorageUtils::getUnstructuredNodePropertyListsStructureIDAndFName(
            wal->getDirectory(), tableID),
        bufferManager, isInMemory, wal);
    pkIndex = make_unique<PrimaryKeyIndex>(
        StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
        nodeTableSchema->getPrimaryKey().dataType, bufferManager, wal);
}

node_offset_t NodeTable::addNodeAndResetProperties(ValueVector* primaryKeyVector) {
    auto nodeOffset = nodesStatisticsAndDeletedIDs->addNode(tableID);
    assert(primaryKeyVector->state->isFlat());
    if (!pkIndex->insert(
            primaryKeyVector, primaryKeyVector->state->getPositionOfCurrIdx(), nodeOffset)) {
        auto pkValPos = primaryKeyVector->state->getPositionOfCurrIdx();
        string pkStr = primaryKeyVector->dataType.typeID == INT64 ?
                           to_string(((int64_t*)(primaryKeyVector->values))[pkValPos]) :
                           (((ku_string_t*)primaryKeyVector->values))[pkValPos].getAsString();
        throw RuntimeException(Exception::getExistedPKExceptionMsg(pkStr));
    }
    for (auto& column : propertyColumns) {
        column->setNodeOffsetToNull(nodeOffset);
    }
    unstrPropertyLists->initEmptyPropertyLists(nodeOffset);
    return nodeOffset;
}

void NodeTable::deleteNodes(ValueVector* nodeIDVector, ValueVector* primaryKeyVector) {
    assert(nodeIDVector->state == primaryKeyVector->state && nodeIDVector->hasNoNullsGuarantee() &&
           primaryKeyVector->hasNoNullsGuarantee());
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        deleteNode(nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            deleteNode(nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
        }
    }
}

void NodeTable::deleteNode(
    node_offset_t nodeOffset, ValueVector* primaryKeyVector, uint32_t pos) const {
    nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    pkIndex->deleteKey(primaryKeyVector, pos);
}

void NodeTable::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    unstrPropertyLists->prepareCommitOrRollbackIfNecessary(isCommit);
    pkIndex->prepareCommitOrRollbackIfNecessary(isCommit);
}

} // namespace storage
} // namespace kuzu
