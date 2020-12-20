#include "src/loader/include/utils.h"

#include <string.h>

#include "src/common/include/configs.h"

namespace graphflow {
namespace loader {

NodeIDMap::~NodeIDMap() {
    for (auto& charArray : nodeIDToOffsetMap) {
        delete[] charArray.first;
    }
}

void NodeIDMap::set(const char* nodeID, node_offset_t nodeOffset) {
    auto len = strlen(nodeID);
    auto nodeIDcopy = new char[len + 1];
    memcpy(nodeIDcopy, nodeID, len);
    nodeIDcopy[len] = 0;
    offsetToNodeIDMap[nodeOffset] = nodeIDcopy;
}

node_offset_t NodeIDMap::get(const char* nodeID) {
    return nodeIDToOffsetMap.at(nodeID);
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
