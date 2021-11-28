#include "src/loader/include/node_id_map.h"

namespace graphflow {
namespace loader {

NodeIDMap::~NodeIDMap() {
    for (auto i = 0u; i < size; ++i) {
        delete[] offsetToNodeIDMap[i];
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

} // namespace loader
} // namespace graphflow
