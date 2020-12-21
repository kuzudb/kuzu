#include "src/loader/include/utils.h"

#include <string.h>

#include "src/common/include/configs.h"

namespace graphflow {
namespace loader {

NodeIDMap::~NodeIDMap() {
    for (auto& a : nodeIDToOffsetMapping) {
        delete[] a.first;
    }
}

void NodeIDMap::setOffset(const char* nodeID, node_offset_t offset) {
    auto len = strlen(nodeID);
    auto nodeIDcopy = new char[len + 1];
    memcpy(nodeIDcopy, nodeID, len);
    nodeIDcopy[len] = 0;
    nodeIDToOffsetMapping.insert({{nodeIDcopy, offset}});
}

node_offset_t NodeIDMap::getOffset(const char* nodeID) {
    return nodeIDToOffsetMapping[nodeID];
}

void NodeIDMap::merge(NodeIDMap& localMap) {
    lock_guard lck(nodeIDMapMutex);
    nodeIDToOffsetMapping.merge(localMap.nodeIDToOffsetMapping);
}

vector<DataType> createPropertyDataTypes(const unordered_map<string, Property>& propertyMap) {
    vector<DataType> propertyDataTypes{propertyMap.size()};
    for (auto property = propertyMap.begin(); property != propertyMap.end(); property++) {
        propertyDataTypes[property->second.idx] = property->second.dataType;
    }
    return propertyDataTypes;
}

} // namespace loader
} // namespace graphflow
