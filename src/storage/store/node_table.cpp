#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace storage {

NodeTable::NodeTable(NodesMetadata* nodesMetadata, BufferManager& bufferManager, bool isInMemory,
    WAL* wal, NodeLabel* nodeLabel)
    : nodesMetadata{nodesMetadata}, labelID{nodeLabel->labelId} {
    for (const auto& property : nodeLabel->getAllNodeProperties()) {
        if (property.dataType.typeID != UNSTRUCTURED) {
            propertyColumns.push_back(ColumnFactory::getColumn(
                StorageUtils::getStructuredNodePropertyColumnStructureIDAndFName(
                    wal->getDirectory(), property),
                property.dataType, bufferManager, isInMemory, wal));
        }
    }
    unstrPropertyLists = make_unique<UnstructuredPropertyLists>(
        StorageUtils::getUnstructuredNodePropertyListsStructureIDAndFName(
            wal->getDirectory(), labelID),
        bufferManager, isInMemory, wal);
    IDIndex =
        make_unique<HashIndex>(StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), labelID),
            nodeLabel->getPrimaryKey().dataType, bufferManager, isInMemory);
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
    nodesMetadata->deleteNode(labelID, nodeOffset);
    // TODO(Guodong): delete primary key index
}

} // namespace storage
} // namespace graphflow
