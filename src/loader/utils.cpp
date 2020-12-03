#include "src/loader/include/utils.h"

#include <string.h>

#include "src/common/include/configs.h"

namespace graphflow {
namespace loader {

void NodeIDMap::setOffset(string nodeID, gfNodeOffset_t offset) {
    nodeIDToOffsetMapping.insert(make_pair(nodeID, offset));
}

gfNodeOffset_t NodeIDMap::getOffset(string& nodeID) {
    return nodeIDToOffsetMapping[nodeID];
}

bool NodeIDMap::hasNodeID(string& nodeID) {
    return nodeIDToOffsetMapping.find(nodeID) != nodeIDToOffsetMapping.end();
}

void NodeIDMap::merge(NodeIDMap& localMap) {
    nodeIDToOffsetMapping.merge(localMap.nodeIDToOffsetMapping);
}

} // namespace loader
} // namespace graphflow
