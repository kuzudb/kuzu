#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace storage {

NodeTable::NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
    BufferManager& bufferManager, bool isInMemory, WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, tableID{nodeTableSchema->tableID},
      isInMemory{isInMemory}, wal{wal} {
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
    IDIndex =
        make_unique<HashIndex>(StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
            nodeTableSchema->getPrimaryKey().dataType, bufferManager, wal);
}

node_offset_t NodeTable::addNodeAndResetProperties(
    Transaction* trx, ValueVector* primaryKeyVector) {
    auto nodeOffset = nodesStatisticsAndDeletedIDs->addNode(tableID);
    assert(primaryKeyVector->state->isFlat());
    IDIndex->insert(
        trx, primaryKeyVector, primaryKeyVector->state->getPositionOfCurrIdx(), nodeOffset);
    for (auto& column : propertyColumns) {
        column->setNodeOffsetToNull(nodeOffset);
    }
    unstrPropertyLists->initEmptyPropertyLists(nodeOffset);
    return nodeOffset;
}

void NodeTable::deleteNodes(
    Transaction* trx, ValueVector* nodeIDVector, ValueVector* primaryKeyVector) {
    assert(nodeIDVector->state == primaryKeyVector->state && nodeIDVector->hasNoNullsGuarantee() &&
           primaryKeyVector->hasNoNullsGuarantee());
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        deleteNode(trx, nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            deleteNode(trx, nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
        }
    }
}

void NodeTable::deleteNode(
    Transaction* trx, node_offset_t nodeOffset, ValueVector* primaryKeyVector, uint32_t pos) const {
    nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    IDIndex->deleteKey(trx, primaryKeyVector, pos);
}

void NodeTable::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    unstrPropertyLists->prepareCommitOrRollbackIfNecessary(isCommit);
    IDIndex->prepareCommitOrRollbackIfNecessary(isCommit);
}

} // namespace storage
} // namespace graphflow
