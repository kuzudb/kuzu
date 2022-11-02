#include "src/storage/store/include/node_table.h"

namespace graphflow {
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
    IDIndex =
        make_unique<HashIndex>(StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
            nodeTableSchema->getPrimaryKey().dataType, bufferManager, wal);
}

node_offset_t NodeTable::addNode() {
    auto nodeOffset = nodesStatisticsAndDeletedIDs->addNode(tableID);
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
        deleteNode(nodeIDVector, primaryKeyVector, pos);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            deleteNode(nodeIDVector, primaryKeyVector, pos);
        }
    }
}

void NodeTable::deleteNode(
    ValueVector* nodeIDVector, ValueVector* primaryKeyVector, uint32_t pos) const {
    auto nodeOffset = nodeIDVector->readNodeOffset(pos);
    nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    // TODO(Guodong): delete primary key index
}

} // namespace storage
} // namespace graphflow
