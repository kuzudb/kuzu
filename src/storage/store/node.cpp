#include "src/storage/include/store/node.h"

namespace graphflow {
namespace storage {

Node::Node(label_t labelID, BufferManager& bufferManager, bool isInMemory,
    const vector<catalog::Property>& properties, const string& directory, WAL* wal)
    : labelID{labelID} {
    bool hasUnstructuredProperties = false;
    for (const auto& property : properties) {
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
}

} // namespace storage
} // namespace graphflow
