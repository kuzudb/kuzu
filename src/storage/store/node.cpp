#include "src/storage/include/store/node.h"

namespace graphflow {
namespace storage {

Node::Node(label_t labelID, BufferManager& bufferManager, bool isInMemory,
    const vector<catalog::Property>& properties, const string& directory, WAL* wal) {
    bool hasUnstructuredProperties = false;
    auto IDPropertyIdx = UINT32_MAX;
    for (const auto& property : properties) {
        if (property.name == LoaderConfig::ID_FIELD) {
            IDPropertyIdx = property.propertyID;
        }
        if (property.dataType.typeID == UNSTRUCTURED) {
            hasUnstructuredProperties = true;
        } else {
            propertyColumns.push_back(ColumnFactory::getColumn(
                StorageUtils::getNodePropertyColumnFName(directory, labelID, property.name),
                property, UINT32_MAX /* no relLabel */, bufferManager, isInMemory, wal));
        }
    }
    if (hasUnstructuredProperties) {
        unstrPropertyLists = make_unique<UnstructuredPropertyLists>(
            StorageUtils::getNodeUnstrPropertyListsFName(directory, labelID), bufferManager,
            isInMemory);
    }
    IDIndex = make_unique<HashIndex>(StorageUtils::getNodeIndexFName(directory, labelID),
        properties[IDPropertyIdx].dataType, bufferManager, isInMemory);
}

} // namespace storage
} // namespace graphflow
