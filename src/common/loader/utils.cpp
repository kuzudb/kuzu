#include "src/common/loader/include/utils.h"

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

#include "src/common/include/configs.h"

namespace graphflow {
namespace common {

void NodeIDMap::setOffset(string nodeID, gfNodeOffset_t offset) {
    nodeIDToOffsetMapping.insert(make_pair(nodeID, offset));
}

gfNodeOffset_t NodeIDMap::getOffset(string &nodeID) {
    return nodeIDToOffsetMapping[nodeID];
}

bool NodeIDMap::hasNodeID(string &nodeID) {
    return nodeIDToOffsetMapping.find(nodeID) != nodeIDToOffsetMapping.end();
}

void NodeIDMap::merge(NodeIDMap &localMap) {
    nodeIDToOffsetMapping.merge(localMap.nodeIDToOffsetMapping);
}

InMemColAdjList::InMemColAdjList(
    const string fname, uint64_t numElements, uint64_t numLabelBytes, uint64_t numOffsetBytes)
    : fname(fname), numElements(numElements), numLabelBytes(numLabelBytes),
      numOffsetBytes(numOffsetBytes) {
    numElementsPerPage = PAGE_SIZE / (numLabelBytes + numOffsetBytes);
    size = PAGE_SIZE * (1 + numElements / numElementsPerPage);
    data = make_unique<uint8_t[]>(size);
    fill(data.get(), data.get() + size, UINT8_MAX);
};

void InMemColAdjList::set(gfNodeOffset_t offset, gfLabel_t nbrLabel, gfNodeOffset_t nbrOffset) {
    auto writeOffset = data.get() + (PAGE_SIZE * (offset / numElementsPerPage)) +
                       ((numLabelBytes + numOffsetBytes) * (offset % numElementsPerPage));
    memcpy(writeOffset, &nbrLabel, numLabelBytes);
    memcpy(writeOffset + numLabelBytes, &nbrOffset, numOffsetBytes);
}

void InMemColAdjList::saveToFile() {
    uint32_t f = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1u == f) {
        invalid_argument("cannot create file: " + fname);
    }
    if (size != write(f, data.get(), size)) {
        invalid_argument("Cannot write in file.");
    }
    close(f);
}

} // namespace common
} // namespace graphflow
