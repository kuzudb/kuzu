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

InMemAdjEdges::InMemAdjEdges(
    const string fname, uint64_t numElements, uint64_t numBytesPerLabel, uint64_t numBytesPerOffset)
    : fname(fname), numBytesPerLabel(numBytesPerLabel), numBytesPerOffset(numBytesPerOffset) {
    numElementsPerPage = PAGE_SIZE / (numBytesPerLabel + numBytesPerOffset);
    size = PAGE_SIZE * (1 + numElements / numElementsPerPage);
    data = make_unique<uint8_t[]>(size);
    fill(data.get(), data.get() + size, UINT8_MAX);
};

void InMemAdjEdges::set(gfNodeOffset_t offset, gfLabel_t nbrLabel, gfNodeOffset_t nbrOffset) {
    auto writeOffset = data.get() + (PAGE_SIZE * (offset / numElementsPerPage)) +
                       ((numBytesPerLabel + numBytesPerOffset) * (offset % numElementsPerPage));
    memcpy(writeOffset, &nbrLabel, numBytesPerLabel);
    memcpy(writeOffset + numBytesPerLabel, &nbrOffset, numBytesPerOffset);
}

void InMemAdjEdges::saveToFile() {
    uint32_t f = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1u == f) {
        invalid_argument("cannot create file: " + fname);
    }
    if (size != write(f, data.get(), size)) {
        invalid_argument("Cannot write in file.");
    }
    close(f);
}

InMemAdjListsIndex::InMemAdjListsIndex(
    const string fname, uint64_t numPages, uint64_t numBytesPerLabel, uint64_t numBytesPerOffset)
    : fname(fname), numBytesPerLabel(numBytesPerLabel), numBytesPerOffset(numBytesPerOffset) {
    numElementsPerPage = PAGE_SIZE / (numBytesPerLabel + numBytesPerOffset);
    size = PAGE_SIZE * numPages;
    data = make_unique<uint8_t[]>(size);
    fill(data.get(), data.get() + size, UINT8_MAX);
};

void InMemAdjListsIndex::set(uint64_t pageIdx, uint16_t pageOffset, uint64_t pos,
    gfLabel_t nbrLabel, gfNodeOffset_t nbrOffset) {
    pageOffset += pos;
    while (pageOffset >= numElementsPerPage) {
        pageOffset -= numElementsPerPage;
        pageIdx++;
    }
    auto writeOffset =
        data.get() + (PAGE_SIZE * pageIdx) + ((numBytesPerLabel + numBytesPerOffset) * pageOffset);
    memcpy(writeOffset, &nbrLabel, numBytesPerLabel);
    memcpy(writeOffset + numBytesPerLabel, &nbrOffset, numBytesPerOffset);
}

void InMemAdjListsIndex::saveToFile() {
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
