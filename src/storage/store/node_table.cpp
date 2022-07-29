#include "src/storage/store/include/node_table.h"

#include "src/loader/in_mem_storage_structure/include/in_mem_column.h"
#include "src/loader/in_mem_storage_structure/include/in_mem_lists.h"

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

void NodeTable::createEmptyDBFilesForNewNodeTable(
    Catalog* catalog, label_t label, string directory) {
    auto nodeLabel = catalog->getReadOnlyVersion()->getNodeLabel(label);
    for (auto& property : nodeLabel->structuredProperties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            directory, nodeLabel->labelId, property.propertyID);
        loader::InMemColumnFactory::getInMemPropertyColumn(
            fName, property.dataType, 0 /* numNodes */)
            ->saveToFile();
    }
    auto unstrPropertyLists = make_unique<loader::InMemUnstructuredLists>(
        StorageUtils::getNodeUnstrPropertyListsFName(directory, nodeLabel->labelId),
        0 /* numNodes */);
    unstrPropertyLists->getListsMetadataBuilder()->initLargeListPageLists(0 /* largeListIdx */);
    unstrPropertyLists->saveToFile();
    auto IDIndex =
        make_unique<InMemHashIndex>(StorageUtils::getNodeIndexFName(directory, nodeLabel->labelId),
            nodeLabel->getPrimaryKey().dataType);
    IDIndex->bulkReserve(0 /* numNodes */);
    IDIndex->flush();
}

} // namespace storage
} // namespace graphflow
