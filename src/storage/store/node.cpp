#include "src/storage/include/store/node.h"

namespace graphflow {
namespace storage {

Node::Node(label_t labelID, BufferManager& bufferManager, bool isInMemory,
    const vector<catalog::PropertyDefinition>& propertyDefinitions, const string& directory)
    : labelID{labelID} {
    bool hasUnstructuredProperties = false;
    for (const auto& propertyDefinition : propertyDefinitions) {
        if (propertyDefinition.dataType.typeID == UNSTRUCTURED) {
            hasUnstructuredProperties = true;
        } else {
            propertyColumns.push_back(
                ColumnFactory::getColumn(StorageUtils::getNodePropertyColumnFName(
                                             directory, labelID, propertyDefinition.name),
                    propertyDefinition.dataType, bufferManager, isInMemory));
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
