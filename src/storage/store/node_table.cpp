#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace storage {

NodeTable::NodeTable(NodeMetadata* nodeMetadata, BufferManager& bufferManager, bool isInMemory,
    const vector<catalog::Property>& properties, const string& directory, WAL* wal)
    : nodeMetadata{nodeMetadata} {
    //    bool hasUnstructuredProperties = false;
    auto IDPropertyIdx = UINT32_MAX;
    for (const auto& property : properties) {
        if (property.name == LoaderConfig::ID_FIELD) {
            IDPropertyIdx = property.propertyID;
        }
        if (property.dataType.typeID != UNSTRUCTURED) {
            //            hasUnstructuredProperties = true;
            //        } else {
            propertyColumns.push_back(ColumnFactory::getColumn(
                StorageUtils::getStructuredNodePropertyColumnStructureIDAndFName(
                    directory, property),
                property.dataType, bufferManager, isInMemory, wal));
        }
    }
    //    if (hasUnstructuredProperties) {
    unstrPropertyLists = make_unique<UnstructuredPropertyLists>(
        StorageUtils::getUnstructuredNodePropertyListsStructureIDAndFName(
            directory, nodeMetadata->getLabelID()),
        bufferManager, isInMemory, wal);
    //    }
    IDIndex = make_unique<HashIndex>(
        StorageUtils::getNodeIndexIDAndFName(directory, nodeMetadata->getLabelID()),
        properties[IDPropertyIdx].dataType, bufferManager, isInMemory);
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

void NodeTable::deleteNode(ValueVector* nodeIDVector, ValueVector* primaryKeyVector, uint32_t pos) {
    auto nodeOffset = nodeIDVector->readNodeOffset(pos);
    nodeMetadata->deleteNode(nodeOffset);
    // TODO(Guodong): delete primary key index
}

} // namespace storage
} // namespace graphflow
